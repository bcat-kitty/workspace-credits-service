from django.urls import path
from apps.credits.views import PlanListView, PlanBookView, PlanGrantView

urlpatterns = [
    path("plans/", PlanListView.as_view(), name="plan_list"),
    path("plans/<int:plan_id>/book/", PlanBookView.as_view(), name="plan_book"),
    path("plans/<int:plan_id>/grant/", PlanGrantView.as_view(), name="plan_grant"),
]
