"""
apps/credits/models.py

Plan and Booking models.

Key concepts:
- Plan: a named offering that defines what actions are available and
  at what credit cost. Plans belong to a workspace.
- Booking: a record of a user consuming credits to perform an action
  under a specific plan.

Visibility rules:
- open: any user with a sufficient credit balance can book
- workspace_only: user must be an active member of the plan's workspace

Plan lifecycle:
  draft -> active -> deprecated -> archived
  No skipping states. No going backwards.
  Only the workspace Administrator (via ScopedRole) can advance states.

Booking records are immutable once created. A cancellation produces a
reversal LedgerEntry and a new Booking record with status=cancelled.
It does not modify or delete the original Booking.
"""

from django.db import models
from apps.accounts.models import UserAccount, Workspace


class Plan(models.Model):
    """
    A named offering that defines available actions and their credit cost.

    Visibility rules:
    - open: any user with sufficient credits can book against this plan
    - workspace_only: user must be an active WorkspaceMembership member

    Lifecycle states are one-directional:
    draft -> active -> deprecated -> archived
    No skipping states. No going backwards.
    Only the workspace Administrator (via ScopedRole) can advance states.
    """

    class Visibility(models.TextChoices):
        OPEN = "open", "Open"
        WORKSPACE_ONLY = "workspace_only", "Workspace Only"

    class Status(models.TextChoices):
        DRAFT = "draft", "Draft"
        ACTIVE = "active", "Active"
        DEPRECATED = "deprecated", "Deprecated"
        ARCHIVED = "archived", "Archived"

    # Valid state transitions - no skipping, no reversal
    VALID_TRANSITIONS = {
        Status.DRAFT: [Status.ACTIVE],
        Status.ACTIVE: [Status.DEPRECATED],
        Status.DEPRECATED: [Status.ARCHIVED],
        Status.ARCHIVED: [],
    }

    workspace = models.ForeignKey(
        Workspace,
        on_delete=models.CASCADE,
        related_name="plans"
    )
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    category = models.CharField(max_length=50)
    credit_cost = models.PositiveIntegerField(
        help_text="Number of credits deducted per booking against this plan."
    )
    visibility = models.CharField(
        max_length=20,
        choices=Visibility.choices,
        default=Visibility.OPEN
    )
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.DRAFT
    )
    capacity = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Maximum number of active bookings. Null = unlimited."
    )
    created_by = models.ForeignKey(
        UserAccount,
        on_delete=models.SET_NULL,
        null=True,
        related_name="created_plans"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "plans"
        indexes = [
            # Hot path: the anonymous public Plan list - the highest
            # QPS endpoint on the service. The query is
            #   WHERE visibility = 'open' AND status = 'active'
            # Both columns are low cardinality, but the composite collapses
            # the candidate set to "publicly bookable plans" in one btree
            # descent. Without this index every cache miss on
            # PLANS_CACHE_KEY is a seq scan over the entire plans table.
            models.Index(fields=["visibility", "status"], name="plan_visibility_status_idx"),

            # Authenticated PlanListView combines workspace scoping with a
            # status filter - "plans for the workspaces I belong to, that
            # are currently active". Also serves the admin's per-workspace
            # plan management screens. workspace_id is high cardinality and
            # leads.
            models.Index(fields=["workspace", "status"], name="plan_workspace_status_idx"),

            # Supports `?category=` when combined with the implicit
            # workspace scoping (admin tools and the authenticated list
            # filter). workspace first because category is low cardinality
            # on its own and a category-only filter is rare.
            models.Index(fields=["workspace", "category"], name="plan_workspace_category_idx"),

            # NOTE: no btree on `name` for `?search=` because that filter
            # uses `icontains`, which a btree cannot serve (it's a
            # full-table LIKE '%term%'). The correct production answer is
            # a Postgres trigram (`pg_trgm`) GIN index -
            #   CREATE INDEX plans_name_trgm ON plans USING gin (name gin_trgm_ops);
            # We omit it here because it requires the pg_trgm extension and
            # an explicit migration, so it is deferred for now, but it is
            # the right next index at scale.
        ]

    def __str__(self):
        return f"{self.name} ({self.workspace.name})"

    def can_transition_to(self, new_status):
        """Returns True if the transition from current status to
        new_status is valid."""
        return new_status in self.VALID_TRANSITIONS.get(self.status, [])


class Booking(models.Model):
    """
    A record of a user consuming credits to perform an action under a plan.

    Booking rules:
    1. User must have a sufficient available credit balance (checked in
       the API layer via the ledger)
    2. User must meet plan visibility requirements (open, or workspace
       member if workspace_only)
    3. Plan must be in ACTIVE status
    4. Plan must not be at capacity

    Booking records are immutable once created. Cancellation produces a
    new Booking with status=cancelled and a reversal LedgerEntry. It does
    not modify or delete the original record.
    """

    class BookingStatus(models.TextChoices):
        CONFIRMED = "confirmed", "Confirmed"
        CANCELLED = "cancelled", "Cancelled"
        REVERSED = "reversed", "Reversed"

    plan = models.ForeignKey(
        Plan,
        on_delete=models.CASCADE,
        related_name="bookings"
    )
    user = models.ForeignKey(
        UserAccount,
        on_delete=models.CASCADE,
        related_name="bookings"
    )
    status = models.CharField(
        max_length=20,
        choices=BookingStatus.choices,
        default=BookingStatus.CONFIRMED
    )
    credits_consumed = models.PositiveIntegerField(
        help_text="Credits deducted at the time of booking. Snapshot of "
                  "plan.credit_cost at booking time."
    )
    booked_at = models.DateTimeField(auto_now_add=True)
    cancelled_at = models.DateTimeField(null=True, blank=True)
    cancelled_by = models.ForeignKey(
        UserAccount,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="cancelled_bookings"
    )

    class Meta:
        db_table = "bookings"
        unique_together = [("plan", "user")]
        indexes = [
            # Serves the `Count('bookings', filter=Q(status=CONFIRMED))`
            # annotation on PlanListView - the per-plan confirmed-booking
            # count that powers booking_count and is_at_capacity. Without
            # it every list response does a sequential scan per plan,
            # which is the textbook way to N+1-ify an aggregation. Leading
            # with plan_id matches the GROUP BY.
            models.Index(fields=["plan", "status"], name="booking_plan_status_idx"),

            # Serves "my recent bookings" (`/bookings?user=me&order=-booked_at`)
            # and audit lookups by user. booked_at second makes the index
            # also usable for index-only ordering on the timestamp.
            models.Index(fields=["user", "booked_at"], name="booking_user_booked_at_idx"),

            # NOTE: unique_together = [('plan', 'user')] already creates a
            # composite unique btree on (plan_id, user_id) that serves the
            # idempotency check at booking time
            # ("does this user already have a booking on this plan?"). We
            # intentionally do NOT redeclare it here to avoid two indexes
            # serving the same lookup.
        ]

    def __str__(self):
        return f"{self.user.email} -> {self.plan.name} ({self.credits_consumed} credits)"
