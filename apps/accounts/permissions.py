"""
apps/accounts/permissions.py

THREE-LAYER PERMISSION SYSTEM
==============================

Every booking action on this platform requires ALL THREE layers to pass.
Failing any single layer blocks the action. There are no shortcuts.

LAYER 1: Authentication
    Is the user authenticated? Do they have a valid JWT?

LAYER 2: Credit Eligibility
    Does the user have a positive available credit balance?
    Available balance is computed from the ledger: total granted credits
    minus total consumed credits for this user.
    Without a sufficient balance, booking actions are BLOCKED regardless
    of workspace membership or scoped roles.
    Grant check order:
    - Platform subscription grant (source=subscription, workspace=None)
    - Workspace top-up grant (source=topup, workspace=this workspace)

LAYER 3: Authorization
    Does the user have the RIGHT to perform this specific action on this
    specific object?
    This depends on:
    - Visibility of the plan (open vs workspace_only)
    - Membership status (are they a member of this workspace?)
    - Scoped role (are they the Administrator of this specific workspace?)

All three layers must be checked IN ORDER.
Do not skip to Layer 3 without passing Layer 2.

INVARIANTS (violating any of these is a security bug):
- Layer 3 is never checked before Layer 2
- Workspace membership never bypasses credit eligibility
- A scoped role never bypasses credit eligibility
- Grant eligibility is always scoped to the workspace, never global
"""

from django.db.models import Q, Sum, IntegerField, Subquery, Value
from django.db.models.functions import Coalesce
from django.utils import timezone
from rest_framework.permissions import BasePermission

from apps.accounts.models import CreditGrant, UserAccount, WorkspaceMembership, ScopedRole


def get_available_credit_balance(user, workspace=None):
    """
    Returns the user's available credit balance as an integer, or 0 if none.

    Balance is derived from the ledger - never from a mutable column. A
    stored balance field would drift the moment a single grant or
    consumption write is missed, lost, or rolled back; the only safe
    invariant is "balance == sum of ledger movements", which is what this
    function computes.

    Eligible grants:
      - source=subscription, workspace IS NULL   - always counted
      - source=topup, workspace=<this workspace> - counted iff the caller
        passes a workspace (booking context). Top-ups are workspace-scoped
        money; a user with a workspace-A top-up cannot spend it inside
        workspace B.

    A grant is "eligible" only when it is active, not revoked, and not
    expired.

    SINGLE-QUERY DESIGN
    -------------------
    Both sums land in one SELECT by using the target UserAccount row as an
    anchor and attaching each sum as a correlated scalar subquery in the
    projection list. Postgres emits one plan, one round-trip - no Python
    loop over grants, no per-user follow-up, no N+1. The Coalesce(..., 0)
    wrappers turn an empty subquery result into a literal zero so we never
    return None.

    Why anchor on UserAccount and not .aggregate() on CreditGrant directly:
    Django's .aggregate() only accepts aggregate expressions as top-level
    aliases - a bare Subquery(Sum(...)) is rejected as "not an aggregate
    expression". Annotating both sums onto a single-row queryset over the
    user is the idiomatic way to keep the round-trip count at one.

    LedgerEntry.amount is SIGNED: positive for grants, negative for
    consumption / expiry. Storing consumed as a negative value means
    "balance = granted_total + consumed_sum" (a plain addition); we still
    clamp at zero so a torn write or bad migration can never report a
    negative balance to callers.
    """
    now = timezone.now()

    grant_filter = (
        Q(user=user)
        & Q(is_active=True)
        & Q(revoked_at__isnull=True)
        & (Q(expires_at__isnull=True) | Q(expires_at__gt=now))
    )

    source_filter = Q(
        source=CreditGrant.Source.SUBSCRIPTION,
        workspace__isnull=True,
    )
    if workspace is not None:
        source_filter = source_filter | Q(
            source=CreditGrant.Source.TOPUP,
            workspace=workspace,
        )

    # Imported lazily to avoid pulling in the ledger app at module import
    # time (accounts is a lower-level dependency of ledger).
    from apps.ledger.models import LedgerEntry

    granted_subquery = (
        CreditGrant.objects
        .filter(grant_filter & source_filter)
        .values("user")
        .annotate(s=Sum("amount"))
        .values("s")
    )
    consumed_subquery = (
        LedgerEntry.objects
        .filter(subject_user=user, entry_type="consumed")
        .values("subject_user")
        .annotate(s=Sum("amount"))
        .values("s")
    )

    row = (
        UserAccount.objects
        .filter(pk=user.pk)
        .annotate(
            granted=Coalesce(
                Subquery(granted_subquery, output_field=IntegerField()),
                Value(0),
                output_field=IntegerField(),
            ),
            consumed=Coalesce(
                Subquery(consumed_subquery, output_field=IntegerField()),
                Value(0),
                output_field=IntegerField(),
            ),
        )
        .values("granted", "consumed")
        .first()
    )
    if row is None:
        return 0
    balance = (row["granted"] or 0) + (row["consumed"] or 0)
    return max(0, balance)


def get_active_membership(user, workspace):
    """
    Returns the user's active WorkspaceMembership for the given workspace,
    or None if none exists.

    One query: a single filtered SELECT bounded by .first(). Status must be
    ACTIVE and expires_at must be null or in the future - expired or
    suspended members must not pass Layer 3.
    """
    return (
        WorkspaceMembership.objects
        .filter(
            user=user,
            workspace=workspace,
            status=WorkspaceMembership.Status.ACTIVE,
        )
        .filter(Q(expires_at__isnull=True) | Q(expires_at__gt=timezone.now()))
        .first()
    )


def get_scoped_role(user, workspace, role_type, scope_object_type, scope_object_id):
    """
    Returns the user's active ScopedRole for a specific (object_type,
    object_id) pair, or None.

    One query. The role must match all four identifying fields exactly -
    "administrator" of one workspace never implies administrator of
    another, which is the whole point of ScopedRole being scoped.
    """
    return (
        ScopedRole.objects
        .filter(
            user=user,
            workspace=workspace,
            role_type=role_type,
            scope_object_type=scope_object_type,
            scope_object_id=scope_object_id,
            is_active=True,
        )
        .filter(Q(expires_at__isnull=True) | Q(expires_at__gt=timezone.now()))
        .first()
    )


class HasSufficientCredits(BasePermission):
    """
    Layer 2 permission class.

    Blocks the request if the user does not have a positive available
    credit balance.

    WORKSPACE CONTEXT
    -----------------
    Endpoints that require this permission carry a `plan_id` URL kwarg.
    We resolve the plan's workspace and pass it to
    get_available_credit_balance() so workspace-scoped top-up grants are
    correctly included. Without this scoping, a user could spend a
    workspace-A top-up to book a workspace-B plan - the multi-tenant
    boundary on credit money would silently leak.

    We stash the resolved plan on `request._plan` so the view doesn't
    re-fetch it - one DB hit for the whole request, not two.
    """

    message = "Insufficient credit balance to perform this action."

    def has_permission(self, request, view):
        if not request.user or not request.user.is_authenticated:
            return False

        # Lazy import: avoids importing the credits app at accounts-module
        # load time (accounts is a lower-level dependency of credits).
        from apps.credits.models import Plan

        plan_id = view.kwargs.get("plan_id")
        workspace = None
        if plan_id is not None:
            try:
                plan = (
                    Plan.objects
                    .select_related("workspace")
                    .get(pk=plan_id)
                )
            except Plan.DoesNotExist:
                # Let the view emit its own 404. Returning False here would
                # produce a 403, which would leak the difference between
                # "this plan id is real but you can't see it" and "this
                # plan id does not exist".
                return True
            request._plan = plan
            workspace = plan.workspace

        balance = get_available_credit_balance(request.user, workspace=workspace)
        return balance > 0


class IsWorkspaceAdministrator(BasePermission):
    """
    Layer 3 permission class for administrator-only actions.

    Checks if the user has an active ScopedRole of type "administrator"
    for the specific workspace being accessed.

    OBJECT-LEVEL (not queryset-level) because the admin check is per-
    target-workspace. A queryset-level filter would either over-broaden
    ("admin of any workspace"), or require a JOIN on every list endpoint
    that doesn't need it. The right shape is: fetch the target object,
    then ask - is THIS user the administrator of THIS object's workspace?
    """

    message = "You must be a workspace administrator to perform this action."

    def has_object_permission(self, request, view, obj):
        if not request.user or not request.user.is_authenticated:
            return False
        workspace = getattr(obj, "workspace", None)
        if workspace is None:
            return False
        role = get_scoped_role(
            user=request.user,
            workspace=workspace,
            role_type=ScopedRole.RoleType.ADMINISTRATOR,
            scope_object_type="workspace",
            scope_object_id=workspace.pk,
        )
        return role is not None
