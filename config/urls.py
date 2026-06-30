from django.urls import path, include
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView

urlpatterns = [
    # Auth
    path("api/auth/token/", TokenObtainPairView.as_view(), name="token_obtain"),
    path("api/auth/token/refresh/", TokenRefreshView.as_view(), name="token_refresh"),

    # Credits API (routes in apps/credits/urls.py)
    path("api/", include("apps.credits.urls")),

    # Health endpoints - required for service mesh probing
    path("health/", include("config.health")),
]
