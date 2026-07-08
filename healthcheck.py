"""
Local health probe for Postgres, Redis, and Kafka.

Reads the same config the Django app uses (config/settings.py + .env), so a
green run here means the app itself will start clean. Prints one line per
service; any failure shows the actual driver error instead of aborting.

Usage:
    python healthcheck.py
"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from django.conf import settings
from django.core.cache import cache
from django.db import connection

results = {}

try:
    connection.ensure_connection()
    results["postgres"] = "OK"
except Exception as exc:
    results["postgres"] = f"FAIL: {exc}"

try:
    cache.set("_healthcheck", 1, 5)
    results["redis"] = "OK" if cache.get("_healthcheck") == 1 else "FAIL: roundtrip mismatch"
except Exception as exc:
    results["redis"] = f"FAIL: {exc}"

try:
    from confluent_kafka import Producer
    md = Producer(settings.KAFKA_CONFIG).list_topics(timeout=3)
    results["kafka"] = f"OK ({len(md.brokers)} broker(s))"
except Exception as exc:
    results["kafka"] = f"FAIL: {exc}"

width = max(len(k) for k in results)
for name, status in results.items():
    print(f"{name.ljust(width)}  {status}")

if any(v.startswith("FAIL") for v in results.values()):
    raise SystemExit(1)
