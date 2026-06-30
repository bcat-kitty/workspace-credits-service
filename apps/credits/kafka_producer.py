"""
apps/credits/kafka_producer.py

Async Kafka credit event publisher.

RULES:
1. Kafka publishing must NEVER block the API response
2. If the Kafka broker is unavailable, the message must NOT be lost
3. Use a fallback strategy - described in detail below
4. All messages must include: event_type, payload, timestamp,
   correlation_id
5. Producer errors must be logged, never silently swallowed

This service runs inside a Kuma service mesh.
The Kafka broker address comes from environment variables only
(settings.KAFKA_CONFIG, which reads KAFKA_BOOTSTRAP_SERVERS). Never
hardcode broker addresses.

--- FALLBACK STRATEGY ---
When the Kafka broker is unavailable, messages fall back to a Redis-
backed dead-letter queue (DLQ). The flow:

  1. publish_*() builds the envelope and calls Producer.produce(). This
     call only enqueues into librdkafka's in-process buffer (default
     ~1 MB) and returns in microseconds; the actual broker round-trip
     happens on librdkafka's background thread.
  2. We pass a delivery callback to produce(). It fires asynchronously
     when librdkafka either gets an ack from the broker or gives up
     (after `message.timeout.ms`). On success: log. On failure: push
     the original envelope JSON onto a Redis LIST at key
     `kafka:dlq:<topic>` via LPUSH.
  3. produce() can itself raise BufferError if the local queue is
     already full - which happens when the broker has been down long
     enough for the queue to fill. In that case we poll(0.1) to drain,
     and if it's still full, the message also goes to the Redis DLQ.
  4. If the Producer can't even be constructed (broken config, lib
     missing), `_get_producer()` returns None and EVERY publish goes
     straight to the DLQ - the API response is never blocked by a
     malformed Kafka client.
  5. `replay_kafka_dlq(topic)` is the recovery hook: it RPOPs from the
     Redis list and re-publishes. In production this would be wired to
     a Celery beat schedule (e.g. every 30s). For now the helper
     exists but is unscheduled.

Guarantees:
  - At-least-once eventual delivery while Redis is reachable.
  - Survives broker restarts and pod restarts (Redis is external).
  - Never blocks the request thread on broker availability.

Does NOT guarantee:
  - Exactly-once. Replay can double-publish if a late delivery callback
    races a successful replay. Consumers must dedupe on
    `correlation_id`.
  - Ordering across the DLQ replay path vs the live produce path.
    Consumers that need per-key ordering must rely on the Kafka
    partition key (we set `key=user_id`) for the live path; messages
    that travel via the DLQ may appear out of order relative to live
    siblings.
  - Durability if Redis itself is lost. That's acceptable because the
    authoritative record - the LedgerEntry - is already committed in
    Postgres; Kafka events are derivative.
---
"""

import json
import logging
import uuid
from datetime import datetime, timezone

from django.conf import settings
from django.core.cache import cache

logger = logging.getLogger(__name__)


# Module-level singleton producer, constructed lazily so that
# `python manage.py check` doesn't try to open a TCP socket to Kafka.
_producer = None
_producer_construct_attempted = False


def _get_producer():
    """
    Return a process-wide Producer, or None if construction fails.

    Producer construction can fail in two ways:
      (a) confluent_kafka is not importable at all (unlikely - it's in
          requirements.txt - but possible in degraded environments).
      (b) Producer(KAFKA_CONFIG) raises because the config is malformed.

    Either way we want the publish path to NOT raise - callers should
    fall through to the DLQ instead of crashing the request.
    """
    global _producer, _producer_construct_attempted
    if _producer is not None:
        return _producer
    if _producer_construct_attempted:
        # We tried once and failed; don't keep retrying inline on the
        # request path. A periodic replay task can pick up the DLQ and
        # try again from a worker process.
        return None
    _producer_construct_attempted = True
    try:
        from confluent_kafka import Producer
        _producer = Producer(settings.KAFKA_CONFIG)
    except Exception as exc:
        logger.error(
            "Kafka Producer construction failed; all publishes will go to "
            "the Redis DLQ. error=%r config_keys=%s",
            exc,
            list(settings.KAFKA_CONFIG.keys()),
        )
        _producer = None
    return _producer


def _dlq_key(topic: str) -> str:
    return f"kafka:dlq:{topic}"


def _push_to_dlq(topic: str, envelope: dict, reason: str):
    """
    Push a failed envelope onto the Redis DLQ list.

    Uses django-redis's underlying client to LPUSH a JSON-serialized
    envelope. The list is bounded only by Redis memory; for production
    we'd cap it (LTRIM) and page-out the overflow to durable storage,
    but in-scope here is at-least-once during a broker outage.
    """
    try:
        client = cache.client.get_client(write=True)
        client.lpush(_dlq_key(topic), json.dumps(envelope))
        logger.warning(
            "Kafka publish failed; queued to Redis DLQ. topic=%s "
            "correlation_id=%s reason=%s",
            topic,
            envelope.get("correlation_id"),
            reason,
        )
    except Exception as exc:
        # The DLQ itself is unreachable. Log loudly - this is the only
        # place we accept message loss, and only because there is
        # genuinely nowhere left to put it. The LedgerEntry is still in
        # Postgres, so the state of truth is preserved.
        logger.error(
            "Kafka DLQ push failed; event will be lost. topic=%s "
            "correlation_id=%s kafka_reason=%s dlq_reason=%r",
            topic,
            envelope.get("correlation_id"),
            reason,
            exc,
        )


def _make_delivery_report(topic: str, envelope: dict):
    """
    Build a per-message delivery callback that closes over the original
    envelope. If delivery fails, we push the same envelope to the DLQ
    so it can be retried later.
    """
    correlation_id = envelope.get("correlation_id")

    def _delivery_report(err, msg):
        if err is not None:
            _push_to_dlq(topic, envelope, reason=f"delivery_failed: {err}")
            return
        logger.info(
            "Kafka delivery OK. topic=%s partition=%s offset=%s "
            "correlation_id=%s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
            correlation_id,
        )

    return _delivery_report


def build_message(event_type: str, payload: dict) -> dict:
    """
    Build a standardized Kafka message envelope.

    Every message published to Kafka must use this envelope.
    Do not publish raw payloads.
    """
    return {
        "event_type": event_type,
        "correlation_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


def _publish(topic: str, event_type: str, payload: dict, key: str | None):
    """
    Internal: non-blocking publish with DLQ fallback.

    WHY NON-BLOCKING
    ----------------
    The API response thread must return as soon as the database
    transaction commits. If we blocked on broker acknowledgement
    (`producer.flush()`), every booking response would wait
    `message.timeout.ms` (default 5s) on a Kafka outage - the response
    p99 would balloon into the seconds, request workers would saturate,
    and an upstream Kafka issue would manifest as a Django-side outage.
    With produce() + delivery callback, the request thread spends
    microseconds queuing the message and the broker round-trip happens
    on librdkafka's internal thread. The cost: at-most-once semantics
    on a hard pod kill (mitigated by the DLQ for the soft-failure
    cases).
    """
    envelope = build_message(event_type=event_type, payload=payload)
    producer = _get_producer()
    if producer is None:
        _push_to_dlq(topic, envelope, reason="no_producer")
        return

    try:
        # poll(0) drains delivery callbacks queued by prior produces.
        # It does NOT wait for in-flight messages - it's a one-shot
        # tick that returns immediately.
        producer.poll(0)

        producer.produce(
            topic,
            value=json.dumps(envelope).encode("utf-8"),
            key=(str(key).encode("utf-8") if key is not None else None),
            on_delivery=_make_delivery_report(topic, envelope),
        )
    except BufferError:
        # Local queue is full. Try one short drain; if still full, DLQ.
        try:
            producer.poll(0.1)
            producer.produce(
                topic,
                value=json.dumps(envelope).encode("utf-8"),
                key=(str(key).encode("utf-8") if key is not None else None),
                on_delivery=_make_delivery_report(topic, envelope),
            )
        except BufferError:
            _push_to_dlq(topic, envelope, reason="local_queue_full")
    except Exception as exc:
        # Anything else - bad config surfaced late, broker rejected
        # synchronously, etc. - DLQ it. We never re-raise on the
        # request path.
        _push_to_dlq(topic, envelope, reason=f"produce_exception: {exc!r}")


def publish_credit_consumed(booking_data: dict):
    """
    Publish a credit-consumed event to Kafka after a successful booking.
    Topic: settings.KAFKA_TOPICS["credit_consumed"].

    Keyed by user_id so that all events for a given user land on the
    same partition and stay ordered on the consumer side.
    """
    topic = settings.KAFKA_TOPICS["credit_consumed"]
    _publish(
        topic=topic,
        event_type="credit.consumed",
        payload=booking_data,
        key=booking_data.get("user_id"),
    )


def publish_credit_granted(grant_data: dict):
    """
    Publish a credit-granted event to Kafka after a successful top-up.
    Topic: settings.KAFKA_TOPICS["credit_granted"].

    Keyed by the target user_id (not the acting admin) - consumers
    want per-recipient ordering of balance changes.
    """
    topic = settings.KAFKA_TOPICS["credit_granted"]
    _publish(
        topic=topic,
        event_type="credit.granted",
        payload=grant_data,
        key=grant_data.get("user_id"),
    )


def replay_kafka_dlq(topic: str, max_messages: int = 100) -> int:
    """
    Recovery hook: drain up to `max_messages` envelopes from the Redis
    DLQ for `topic` and re-publish them.

    Intended to be called from a Celery beat task on a periodic
    schedule. Not auto-scheduled by this module - wiring it to Celery
    beat is the deployment's responsibility.

    Returns the number of messages re-published (best-effort; some may
    re-fail and re-enter the DLQ via the delivery callback).
    """
    try:
        client = cache.client.get_client(write=True)
    except Exception as exc:
        logger.error("DLQ replay: cannot reach Redis. error=%r", exc)
        return 0

    producer = _get_producer()
    if producer is None:
        logger.error("DLQ replay: no producer available; skipping.")
        return 0

    replayed = 0
    key = _dlq_key(topic)
    for _ in range(max_messages):
        raw = client.rpop(key)
        if raw is None:
            break
        try:
            envelope = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.error(
                "DLQ replay: dropping unparseable envelope. error=%r raw=%r",
                exc,
                raw[:200],
            )
            continue
        try:
            producer.poll(0)
            producer.produce(
                topic,
                value=json.dumps(envelope).encode("utf-8"),
                on_delivery=_make_delivery_report(topic, envelope),
            )
            replayed += 1
        except Exception as exc:
            # Put it back at the head so order is preserved, and stop.
            client.lpush(key, json.dumps(envelope))
            logger.warning(
                "DLQ replay halted; broker still unhealthy. topic=%s error=%r",
                topic,
                exc,
            )
            break
    return replayed
