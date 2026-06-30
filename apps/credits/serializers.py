"""
apps/credits/serializers.py

DRF Serializers for Plan and Booking.

PERFORMANCE RULES:
- Serializers must never trigger additional database queries
- All related data must be loaded via select_related or prefetch_related
  in the queryset BEFORE reaching the serializer
- If your serializer hits the database for a field, that is an N+1 bug
"""

from django.utils import timezone
from rest_framework import serializers

from apps.accounts.models import UserAccount
from apps.credits.models import Plan, Booking


class WorkspaceSummarySerializer(serializers.Serializer):
    """
    Read-only summary of a workspace, nested inside PlanSerializer.

    A separate serializer (rather than inlined source='workspace.name'
    fields on PlanSerializer) gives us one canonical workspace shape that
    every endpoint can embed - plan list, booking response, grant
    response - without each one duplicating the schema. It also makes it
    impossible to accidentally leak a sensitive workspace field by adding
    it to the Workspace model: the summary only ever exposes the three
    fields below.
    """

    id = serializers.IntegerField(read_only=True)
    name = serializers.CharField(read_only=True)
    slug = serializers.SlugField(read_only=True)


class PlanSerializer(serializers.ModelSerializer):
    """
    Plan serializer for list and detail views.

    Every related field on this serializer is populated by the view's
    queryset (via select_related / annotate), never by the serializer
    itself - the serializer is a pure projection of an already-loaded
    Plan instance.
    """

    workspace = WorkspaceSummarySerializer(read_only=True)

    # booking_count is populated by the view's queryset annotation:
    #   .annotate(booking_count=Count('bookings', filter=Q(bookings__status='confirmed')))
    # Postgres computes the count once, in the same SELECT that fetches
    # the plan rows, using the (plan_id, status) index on bookings. By
    # the time the instance reaches this serializer the value is already
    # an attribute on the model instance, so reading `obj.booking_count`
    # is a pure Python attribute access - zero DB hits. Writing
    # `obj.bookings.filter(status='confirmed').count()` here would issue
    # one COUNT per plan: the textbook N+1.
    booking_count = serializers.IntegerField(read_only=True)

    is_at_capacity = serializers.SerializerMethodField()

    class Meta:
        model = Plan
        fields = [
            "id",
            "name",
            "description",
            "category",
            "credit_cost",
            "status",
            "visibility",
            "capacity",
            "created_at",
            "workspace",
            "booking_count",
            "is_at_capacity",
        ]
        read_only_fields = fields

    def get_is_at_capacity(self, obj):
        # Pure Python from already-loaded fields. obj.booking_count was
        # set by the queryset annotation upstream; if it's missing we
        # default to 0 so detail endpoints that skip the annotation still
        # return a safe value.
        count = getattr(obj, "booking_count", 0) or 0
        return obj.capacity is not None and count >= obj.capacity


class BookingSerializer(serializers.ModelSerializer):
    """
    Serializer for creating and reading bookings.

    SERVER-DERIVED FIELDS
    ---------------------
    `user` and `credits_consumed` are deliberately NOT in the write set:
      - `user` is injected from request.user in the view. Trusting a
        user_id from the request body would let any caller charge any
        other user's balance - a horizontal-privilege escalation.
      - `credits_consumed` is snapshotted from `plan.credit_cost` at
        write time. Trusting the client would let a caller submit
        `credits_consumed = 0` and book for free.

    WHY THESE VALIDATIONS LIVE HERE (not the view)
    ----------------------------------------------
    The "plan is ACTIVE" and "plan is not at capacity" checks are
    data-shape rules: they depend only on the request payload + the
    resolved Plan, with no awareness of HTTP. Putting them in the
    serializer means a non-HTTP caller (a management command, a Celery
    task, a future internal RPC handler) gets the same validation by
    instantiating the serializer with the same data, instead of each
    caller having to remember to re-implement the rules. The view stays
    focused on transactional / auth concerns.
    """

    user_email = serializers.EmailField(source="user.email", read_only=True)
    plan_id = serializers.IntegerField(source="plan.id", read_only=True)

    class Meta:
        model = Booking
        fields = [
            "id",
            "plan",        # write-only on the input side; PK on output
            "plan_id",     # explicit read-only mirror for clarity
            "user_email",
            "status",
            "credits_consumed",
            "booked_at",
        ]
        read_only_fields = [
            "id",
            "plan_id",
            "user_email",
            "status",
            "credits_consumed",
            "booked_at",
        ]
        extra_kwargs = {
            "plan": {"write_only": True},
        }

    def validate_plan(self, plan):
        if plan.status != Plan.Status.ACTIVE:
            raise serializers.ValidationError(
                f"Plan must be ACTIVE to book; current status is {plan.status!r}."
            )
        if plan.capacity is not None:
            # capacity check uses the live confirmed-booking count. A row
            # count here is cheap and necessary - without it, two parallel
            # bookings can both pass and exceed capacity. The atomic block
            # in the view re-asserts this under SELECT FOR UPDATE.
            confirmed = plan.bookings.filter(
                status=Booking.BookingStatus.CONFIRMED
            ).count()
            if confirmed >= plan.capacity:
                raise serializers.ValidationError(
                    "Plan is at capacity; no further bookings can be made."
                )
        return plan


class PlanStatusTransitionSerializer(serializers.Serializer):
    """
    Serializer for advancing a plan's lifecycle status (draft -> active ->
    deprecated -> archived).

    AUTHORIZATION IS NOT HERE
    -------------------------
    The "is the caller a workspace administrator?" check is owned by the
    view (via the IsWorkspaceAdministrator permission class) and not
    duplicated here. Serializers don't reliably have access to the
    request user across every calling context (a management command may
    pass `data` without a request at all), and serializer-level auth
    blurs the separation between "is this data valid?" and "is this
    caller allowed?". Keeping them apart means each layer has one job.
    """

    new_status = serializers.ChoiceField(choices=Plan.Status.choices)
    reason_code = serializers.CharField(
        required=False,
        allow_blank=True,
        max_length=100,
    )

    def validate(self, attrs):
        plan = self.context.get("plan")
        if plan is None:
            raise serializers.ValidationError(
                "Plan must be provided via serializer context."
            )
        if not plan.can_transition_to(attrs["new_status"]):
            raise serializers.ValidationError(
                f"Invalid transition: {plan.status} -> {attrs['new_status']}."
            )
        return attrs


class CreditGrantSerializer(serializers.Serializer):
    """
    Serializer for issuing a workspace top-up grant.

    The ACTING user (the administrator issuing the grant) is read from
    request.user in the view and never from the payload. Trusting an
    "acting_user_id" field from the body would let a non-admin caller
    impersonate an admin in the audit trail: every LedgerEntry would
    record whatever user_id the client supplied, defeating the
    immutable-audit guarantee that the rest of the system depends on.

    The TARGET user IS supplied (target_user_id) - the admin is
    legitimately picking who to grant credits to.
    """

    target_user_id = serializers.IntegerField()
    amount = serializers.IntegerField(min_value=1)
    expires_at = serializers.DateTimeField(required=False, allow_null=True)
    reason_code = serializers.CharField(
        required=False,
        allow_blank=True,
        max_length=100,
    )

    def validate_target_user_id(self, value):
        try:
            user = UserAccount.objects.get(pk=value, is_active=True)
        except UserAccount.DoesNotExist:
            raise serializers.ValidationError(
                "target_user_id does not refer to an active UserAccount."
            )
        # Stash the resolved user so the view can use it without a second
        # query (the view picks it up from `serializer.validated_data['_target_user']`).
        self.context["_target_user"] = user
        return value

    def validate_expires_at(self, value):
        if value is not None and value <= timezone.now():
            raise serializers.ValidationError(
                "expires_at must be in the future."
            )
        return value
