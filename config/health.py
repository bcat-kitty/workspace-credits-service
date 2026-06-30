"""
Health and readiness endpoints.
Required for Kuma service mesh probing.
Do not remove or rename these endpoints.
"""

from django.http import JsonResponse
from django.urls import path
from django.db import connection
from django.core.cache import cache


def health(request):
    """Liveness probe - is the service running?"""
    return JsonResponse({"status": "ok"})


def ready(request):
    """
    Readiness probe - is the service ready to serve traffic?
    Checks database and cache connectivity.
    Kuma will not route traffic until this returns 200.
    """
    checks = {}

    # Check database
    try:
        connection.ensure_connection()
        checks["database"] = "ok"
    except Exception as e:
        checks["database"] = f"error: {str(e)}"

    # Check Redis
    try:
        cache.set("readiness_check", "ok", timeout=5)
        checks["cache"] = "ok"
    except Exception as e:
        checks["cache"] = f"error: {str(e)}"

    all_ok = all(v == "ok" for v in checks.values())
    status = 200 if all_ok else 503

    return JsonResponse(
        {"status": "ready" if all_ok else "not_ready", "checks": checks},
        status=status
    )


urlpatterns = [
    path("", health, name="health"),
    path("ready/", ready, name="ready"),
]
