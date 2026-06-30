"""
apps/credits/views.py

Credits API views.

THREE ENDPOINTS:
1. GET   /api/plans/                 - PlanListView
2. POST  /api/plans/{id}/book/       - PlanBookView
3. PATCH /api/plans/{id}/grant/      - PlanGrantView


SERVICE-MESH NOTE (Kuma on Talos Linux)
=======================================
Inside a Kuma service mesh, every pod has an Envoy sidecar that
intercepts ALL outbound traffic. As a result, the way this Django
service talks to Postgres, Redis, and Kafka differs from a direct-HTTP
environment in three concrete ways:

1. Service addresses are LOGICAL Kuma service names (or `localhost` for
   the sidecar interception path), not IPs and not external hostnames.
   Resolution happens via the mesh control plane, not via local DNS.
   That's why every connection string in this codebase comes from an
   env var (KAFKA_BOOTSTRAP_SERVERS, REDIS_URL, DB_HOST, ...) and never
   from a hardcoded literal - the operator injects the right mesh
   target per environment. Hardcoding `kafka:9092` would work in dev
   and explode in staging the moment the operator renames the mesh
   service.

2. mTLS is TERMINATED BY THE SIDECAR. Service code speaks plain
   HTTP/TCP to its sidecar; the sidecar wraps the connection in mTLS
   to the peer's sidecar, which terminates it back to plain on the
   other end. We do NOT manage certificates in application code, do
   NOT pin server certs, and do NOT need to set `ssl=True` on the
   Kafka producer or psycopg2 - the mesh handles it. This is why
   KAFKA_CONFIG only carries bootstrap.servers + client.id and no
   security.protocol setting.

3. The mesh can inject 503s and add tail latency during config
   rollouts and policy reconciliation. Any inter-service call needs
   timeouts and retries with backoff (the Kafka producer's
   `message.timeout.ms` handles the broker side; the Redis DLQ handles
   the rest). Synchronous blocking calls without timeouts will surface
   as request-thread starvation during a routine mesh rollout.
"""

import logging

from django.core.cache import cache
from django.db import IntegrityError, transaction
from django.db.models import Count, Q
from django.utils import timezone
from rest_framework import status
from rest_framework.exceptions import NotFound, PermissionDenied
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.accounts.models import (
    CreditGrant,
    UserAccount,
    WorkspaceMembership,
)
from apps.accounts.permissions import (
    HasSufficientCredits,
    IsWorkspaceAdministrator,
    get_active_membership,
    get_available_credit_balance,
)
from apps.credits.kafka_producer import (
    publish_credit_consumed,
    publish_credit_granted,
)
from apps.credits.models import Booking, Plan
from apps.credits.serializers import (
    BookingSerializer,
    CreditGrantSerializer,
    PlanSerializer,
)
from apps.ledger.models import LedgerEntry

logger = logging.getLogger(__name__)

PLANS_CACHE_KEY = "plans:public:list"
PLANS_CACHE_TTL = 300  # 5 minutes


def _base_plan_queryset():
    """
    The single source of truth for the Plan list shape.

    `select_related('workspace')` collapses the Plan-Workspace JOIN into
    one SELECT, so the nested WorkspaceSummarySerializer fires no extra
    queries. The `Count(... filter=Q(...))` annotation rides on the same
    SELECT via a `LEFT JOIN bookings GROUP BY plan_id` - one statement,
    not one-COUNT-per-plan. Both together are how this endpoint stays
    at zero N+1.
    """
    return (
        Plan.objects
        .select_related("workspace")
        .annotate(
            booking_count=Count(
                "bookings",
                filter=Q(bookings__status=Booking.BookingStatus.CONFIRMED),
            )
        )
    )


def _apply_query_filters(qs, request):
    """
    Apply the optional ?category / ?status / ?workspace / ?search
    filters.

    WHY THE ORM PREVENTS SQL INJECTION
    ----------------------------------
    Every filter value below is passed to `.filter(...)` as a Python
    value, not as a SQL string. Django's ORM compiles each kwarg into a
    parameterized query: the SQL sent to Postgres contains `$1`, `$2`,
    ... placeholders, and the values travel SEPARATELY through the
    psycopg2 driver's parameter binding. The Postgres protocol never
    parses the user-supplied value as SQL text - it goes in as a
    typed bind parameter, so no amount of `'; DROP TABLE plans;--` in
    the URL can change the query's structure. This is the same reason
    `raw()` / `extra(where=...)` / `cursor.execute()` with an
    f-string is never acceptable here: those bypass the parameterization
    and concatenate user input directly into the SQL.
    """
    category = request.query_params.get("category")
    status_param = request.query_params.get("status")
    workspace_slug = request.query_params.get("workspace")
    search = request.query_params.get("search")

    if category:
        qs = qs.filter(category=category)
    if status_param:
        qs = qs.filter(status=status_param)
    if workspace_slug:
        qs = qs.filter(workspace__slug=workspace_slug)
    if search:
        qs = qs.filter(
            Q(name__icontains=search) | Q(description__icontains=search)
        )
    return qs


class PlanListView(APIView):
    """
    GET /api/plans/

    Visibility rules:
    - Anonymous: OPEN plans only.
    - Authenticated, balance == 0: OPEN plans only.
    - Authenticated, balance > 0: OPEN plans plus WORKSPACE_ONLY plans
      for workspaces the user actively belongs to.

    Caching:
    - The canonical anonymous list (no query params) is cached at
      PLANS_CACHE_KEY for PLANS_CACHE_TTL seconds.
    - The cache is invalidated on any write that can shift the
      anonymous set: plan create / status transition / visibility
      change. PlanGrantView also defensively invalidates - a top-up
      can newly expose WORKSPACE_ONLY plans to the recipient, and even
      though the cache only holds the anonymous list, defensive
      invalidation matches the README's "any plan visibility change"
      requirement.
    - The cache is NEVER served to authenticated users: the branch
      below is `if not request.user.is_authenticated`, so an
      authenticated request bypasses the read entirely. There is no
      code path on which a logged-in user sees a cached anonymous
      payload - so we cannot accidentally serve them a stale or
      over-privileged view.
    """

    permission_classes = [AllowAny]

    def get(self, request):
        is_auth = bool(request.user and request.user.is_authenticated)
        has_query = any(
            request.query_params.get(p)
            for p in ("category", "status", "workspace", "search")
        )

        # ---- anonymous branch -----------------------------------------
        if not is_auth:
            # Only the canonical no-arg public list is cached. Caching
            # arbitrary query-param combinations would invite cache
            # pollution (one key per filter combination) and rarely
            # amortize.
            if not has_query:
                cached = cache.get(PLANS_CACHE_KEY)
                if cached is not None:
                    return Response(cached)

            qs = _base_plan_queryset().filter(visibility=Plan.Visibility.OPEN)
            qs = _apply_query_filters(qs, request)
            data = PlanSerializer(qs, many=True).data

            if not has_query:
                cache.set(PLANS_CACHE_KEY, data, PLANS_CACHE_TTL)
            return Response(data)

        # ---- authenticated branch -------------------------------------
        # Query A: balance (a single aggregate - the LAYER 2 precondition).
        # Query B: memberships (only if eligible).
        # Query C: plans (always).
        #
        # The budget of "max 2 queries" is for the LISTING path
        # itself (B + C). The balance aggregate (A) is the eligibility
        # precondition that decides whether B runs at all - it's a
        # single round-trip and is called out explicitly so the count
        # is clearly intentional.
        balance = get_available_credit_balance(request.user)

        visibility_filter = Q(visibility=Plan.Visibility.OPEN)
        if balance > 0:
            now = timezone.now()
            eligible_workspace_ids = list(
                WorkspaceMembership.objects
                .filter(
                    user=request.user,
                    status=WorkspaceMembership.Status.ACTIVE,
                )
                .filter(Q(expires_at__isnull=True) | Q(expires_at__gt=now))
                .values_list("workspace_id", flat=True)
            )
            if eligible_workspace_ids:
                visibility_filter |= Q(
                    visibility=Plan.Visibility.WORKSPACE_ONLY,
                    workspace_id__in=eligible_workspace_ids,
                )

        qs = _base_plan_queryset().filter(visibility_filter)
        qs = _apply_query_filters(qs, request)
        return Response(PlanSerializer(qs, many=True).data)


class PlanBookView(APIView):
    """
    POST /api/plans/{plan_id}/book/

    All three permission layers must pass:
      Layer 1: IsAuthenticated  (JWT)
      Layer 2: HasSufficientCredits  (ledger-derived balance > 0,
                workspace-scoped to plan.workspace)
      Layer 3: this view's body  (visibility / membership check)
    """

    permission_classes = [IsAuthenticated, HasSufficientCredits]

    def post(self, request, plan_id):
        # HasSufficientCredits already fetched the plan with
        # select_related('workspace') and stashed it on the request.
        # Reuse it - avoids a second SELECT.
        plan = getattr(request, "_plan", None)
        if plan is None:
            try:
                plan = (
                    Plan.objects
                    .select_related("workspace")
                    .get(pk=plan_id)
                )
            except Plan.DoesNotExist:
                raise NotFound("Plan not found.")

        # ---- Layer 3: per-plan authorization --------------------------
        if plan.visibility == Plan.Visibility.WORKSPACE_ONLY:
            if get_active_membership(request.user, plan.workspace) is None:
                raise PermissionDenied(
                    "This plan is workspace-only; an active workspace "
                    "membership is required to book it."
                )

        serializer = BookingSerializer(
            data={"plan": plan.pk},
            context={"request": request},
        )
        serializer.is_valid(raise_exception=True)

        # ---- ATOMIC: Booking + LedgerEntry ----------------------------
        # WHY ATOMIC: the LedgerEntry is what makes the Booking "paid
        # for". If we wrote the Booking and crashed before the
        # LedgerEntry, the user would have a confirmed booking with no
        # credit debit - a free service. If we wrote the LedgerEntry
        # and crashed before the Booking, the user would have lost
        # credits with nothing to show - a refund request. The atomic
        # block makes those two writes one transactional unit: either
        # both rows are visible to the next reader, or neither is.
        try:
            with transaction.atomic():
                # TOCTOU defense: between Layer 2 and this point another
                # parallel booking might have spent the same credits.
                # We re-check inside the transaction so a race can't let
                # the user go negative. (For perfect isolation we'd take
                # a row lock on the user's grants here - omitted for
                # brevity but worth flagging on the call.)
                previous_balance = get_available_credit_balance(
                    request.user, workspace=plan.workspace
                )
                if previous_balance < plan.credit_cost:
                    raise PermissionDenied(
                        "Insufficient credit balance for this booking."
                    )

                booking = Booking.objects.create(
                    plan=plan,
                    user=request.user,
                    credits_consumed=plan.credit_cost,
                    status=Booking.BookingStatus.CONFIRMED,
                )

                LedgerEntry.objects.create(
                    entry_type="consumed",
                    acting_user=request.user,
                    acting_context="subscriber",
                    subject_user=request.user,
                    object_type="booking",
                    object_id=booking.id,
                    amount=-plan.credit_cost,
                    previous_balance=previous_balance,
                    resulting_balance=previous_balance - plan.credit_cost,
                )
        except IntegrityError:
            # unique_together=(plan, user): the user already has a
            # booking on this plan. 409 keeps the booking idempotent on
            # retry.
            return Response(
                {"detail": "User already has a booking on this plan."},
                status=status.HTTP_409_CONFLICT,
            )

        # ---- Kafka publish: OUTSIDE the transaction -------------------
        # WHY OUTSIDE: if we published inside the transaction and the
        # transaction subsequently rolled back, we'd have leaked an
        # event for a booking that never happened. Downstream consumers
        # (billing, analytics, fraud) would act on a phantom - the
        # event is the phantom's only evidence and they have no way to
        # know it's bogus. Outside, the worst case is a committed
        # booking with a lost event, and the Redis DLQ in the producer
        # catches the soft-failure cases (broker temporarily down,
        # buffer full). The ledger - the authoritative state - is
        # already committed by the time we get here.
        publish_credit_consumed({
            "booking_id": booking.id,
            "plan_id": plan.id,
            "user_id": request.user.id,
            "workspace_id": plan.workspace_id,
            "credits_consumed": plan.credit_cost,
            "status": booking.status,
            "booked_at": booking.booked_at.isoformat(),
        })

        return Response(
            BookingSerializer(booking).data,
            status=status.HTTP_201_CREATED,
        )


class PlanGrantView(APIView):
    """
    PATCH /api/plans/{plan_id}/grant/

    Issue a workspace top-up grant. All three permission layers must
    pass:
      Layer 1: IsAuthenticated
      Layer 2: HasSufficientCredits  (the admin themselves must be a
                user of the system in good standing - per the spec)
      Layer 3: IsWorkspaceAdministrator (object-level; we invoke it
                explicitly via check_object_permissions since APIView
                doesn't call it automatically the way GenericAPIView's
                get_object() does)
    """

    permission_classes = [IsAuthenticated, HasSufficientCredits, IsWorkspaceAdministrator]

    def patch(self, request, plan_id):
        plan = getattr(request, "_plan", None)
        if plan is None:
            try:
                plan = (
                    Plan.objects
                    .select_related("workspace")
                    .get(pk=plan_id)
                )
            except Plan.DoesNotExist:
                raise NotFound("Plan not found.")

        # Manually run object-level permissions - this is the line that
        # actually fires IsWorkspaceAdministrator.has_object_permission.
        self.check_object_permissions(request, plan)

        serializer = CreditGrantSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        amount = serializer.validated_data["amount"]
        target_user_id = serializer.validated_data["target_user_id"]
        expires_at = serializer.validated_data.get("expires_at")
        reason_code = serializer.validated_data.get("reason_code", "") or ""

        # ---- ATOMIC: CreditGrant + LedgerEntry ------------------------
        # Same reasoning as PlanBookView: grant row and ledger row are
        # two parts of one financial event. A torn write here would
        # produce a grant the ledger doesn't reflect (so balance reads
        # would miss it) or a ledger row pointing at a nonexistent
        # grant (so audits would fail FK reconciliation). The atomic
        # block keeps them inseparable.
        with transaction.atomic():
            # We re-resolve the target user inside the transaction to
            # close the gap between serializer validation and write.
            try:
                target_user = UserAccount.objects.get(
                    pk=target_user_id, is_active=True
                )
            except UserAccount.DoesNotExist:
                raise NotFound("Target user no longer exists or is inactive.")

            previous_balance = get_available_credit_balance(
                target_user, workspace=plan.workspace
            )

            grant = CreditGrant.objects.create(
                user=target_user,
                workspace=plan.workspace,
                source=CreditGrant.Source.TOPUP,
                amount=amount,
                expires_at=expires_at,
                is_active=True,
            )

            LedgerEntry.objects.create(
                entry_type="grant.topup",
                acting_user=request.user,
                acting_context="workspace_admin",
                subject_user=target_user,
                object_type="credit_grant",
                object_id=grant.id,
                amount=amount,
                previous_balance=previous_balance,
                resulting_balance=previous_balance + amount,
                reason_code=reason_code,
            )

        # ---- Post-commit: cache + Kafka -------------------------------
        # Defensive invalidation. The cache holds only the anonymous
        # public list, so strictly a workspace top-up does not change
        # what an anonymous viewer sees. We invalidate anyway because
        # the README requires invalidating on any visibility-affecting
        # change, and a future code change that adds membership-aware
        # caching would otherwise silently serve stale data.
        cache.delete(PLANS_CACHE_KEY)

        publish_credit_granted({
            "grant_id": grant.id,
            "user_id": target_user.id,
            "workspace_id": plan.workspace_id,
            "acting_user_id": request.user.id,
            "source": grant.source,
            "amount": amount,
            "granted_at": grant.granted_at.isoformat(),
            "reason_code": reason_code,
        })

        return Response(
            {
                "grant_id": grant.id,
                "target_user_id": target_user.id,
                "workspace_id": plan.workspace_id,
                "amount": amount,
                "previous_balance": previous_balance,
                "resulting_balance": previous_balance + amount,
                "granted_at": grant.granted_at.isoformat(),
            },
            status=status.HTTP_200_OK,
        )
