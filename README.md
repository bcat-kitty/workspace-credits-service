# Workspace Credits Service

Backend API for a credit-based access system inside a multi-tenant SaaS platform. Tenants are
**workspaces**, users hold **credit balances**, credits are granted through plan subscriptions or
workspace top-ups, and credits are consumed when users **book** actions under a plan.

Built with Django, Django REST Framework, PostgreSQL, Redis and Kafka, and designed to run inside a
Kuma service mesh.

## Highlights

- **Append-only ledger.** Every grant, consumption and adjustment writes a `LedgerEntry`. Entries
  can't be updated or deleted, and balances are always derived by summing the ledger. There is no
  mutable balance column.
- **Three-layer permissions** on every booking: authentication, then credit eligibility, then
  object-level authorization. Workspace membership or a scoped role can never bypass the credit
  check.
- **Atomic money movement.** Credit deduction and its ledger entry commit in a single database
  transaction.
- **Non-blocking events.** Kafka publishing happens after the transaction commits and never blocks
  the response. If the broker is down, events fall back to a Redis dead-letter queue with a
  replay hook.
- **Query budget.** The plan list runs in at most 2 queries with zero N+1. Every index on `Plan`
  and `Booking` is annotated with the query it serves.
- **SQL-injection safe.** All filters go through the ORM as bound parameters. There is no `raw()`,
  `extra()` or string-built SQL.

## Architecture

```
            JWT                     +--------------------------+
client ---------> DRF views ------> | three-layer permissions  |
                     |              | 1 auth  2 credits  3 authz|
                     |              +--------------------------+
                     v
            transaction.atomic()
            +-----------------------------+
            | Booking / CreditGrant write |
            | LedgerEntry append          |
            +-----------------------------+
                     | after commit
                     v
            Kafka producer (async, librdkafka) ----> credit events topic
                     | delivery failure / buffer full / no broker
                     v
            Redis DLQ  kafka:dlq:<topic>  --- replay_kafka_dlq() --->  Kafka
```

| App | Responsibility |
|---|---|
| `apps/accounts` | Users, workspaces, memberships, scoped roles, credit grants and the permission classes. |
| `apps/credits` | Plans and bookings, serializers, the three API views and the Kafka producer. |
| `apps/ledger` | Immutable `LedgerEntry` model. |
| `config` | Settings (all from environment variables), URL routing and health probes. |

## API

All endpoints except the anonymous plan list require a JWT (`Authorization: Bearer <token>`).

| Method and path | Description |
|---|---|
| `POST /api/auth/token/` | Obtain a JWT pair. |
| `POST /api/auth/token/refresh/` | Refresh an access token. |
| `GET /api/plans/` | Plans visible to the caller. Anonymous: public plans only, cached in Redis for 5 minutes. Authenticated and eligible: public plans plus workspace-only plans for the caller's workspaces. |
| `POST /api/plans/{id}/book/` | Book an action under a plan and consume credits. `201` on success, `400` on validation errors, `403` on permission failure. |
| `PATCH /api/plans/{id}/grant/` | Issue a credit grant to a user. Workspace administrators only; follows the allowed grant state transitions and invalidates the plan cache. |
| `GET /health/` | Liveness probe. |
| `GET /health/ready/` | Readiness probe: `200` only when Postgres and Redis are reachable. |

Supported filters on `GET /api/plans/` (all optional): `?category=`, `?status=`, `?workspace=<slug>`
and `?search=` (matches name and description).

## Permission model

```
Layer 1  Authentication       valid JWT
Layer 2  Credit eligibility   available balance > 0, derived from the ledger
                              1. platform subscription grant (workspace = NULL)
                              2. top-up grant for this workspace
Layer 3  Authorization        plan list:   visibility (public vs workspace members)
                              booking:     plan access rule (open vs workspace_only)
                              grant:       ScopedRole administrator of this workspace
```

The layers are evaluated in order, and a failure at any layer stops the request.

## Event delivery

`Producer.produce()` only enqueues into librdkafka's in-process buffer, so the request thread
never waits on the broker. A delivery callback logs successes and pushes failures to a Redis list
(`kafka:dlq:<topic>`). A full local buffer or a producer that can't be constructed also routes to
the DLQ. `replay_kafka_dlq(topic)` re-publishes queued events and is meant to run on a Celery beat
schedule.

Delivery is at-least-once while Redis is reachable; consumers dedupe on `correlation_id`. The
ledger in Postgres is the source of truth, and Kafka events are derived from it.

## Getting started

Requirements: Python 3.11+, PostgreSQL, Redis and Kafka.

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env             # set SECRET_KEY and connection settings
python manage.py migrate
python manage.py runserver
```

Check that all backing services are reachable with the same configuration the app uses:

```bash
python healthcheck.py
# postgres  OK
# redis     OK
# kafka     OK (1 broker(s))
```

## Configuration

All configuration comes from environment variables (see `.env.example`). Nothing is hardcoded.

| Variable | Purpose |
|---|---|
| `SECRET_KEY`, `DEBUG`, `ALLOWED_HOSTS` | Django core settings. |
| `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT` | PostgreSQL connection. |
| `REDIS_URL` | Cache and Kafka DLQ. |
| `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_CLIENT_ID` | Kafka producer. |

Inside the mesh, hosts are service names (for example `postgres.credits.svc`) and the Kuma sidecar
handles mTLS, retries and routing. `localhost` is only for local development.

## License

MIT
