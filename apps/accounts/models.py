"""
apps/accounts/models.py

User identity, workspace membership, credit eligibility, and scoped roles.

Key concepts:
- UserAccount: base identity (one login, one email)
- Workspace: the multi-tenant unit (a company, team, or organization)
- WorkspaceMembership: relationship between a user and a workspace
- CreditGrant: records that credits have been allocated to a user
- ScopedRole: grants authority over a specific object
  (e.g. Administrator of workspace_id=12)

These are intentionally separate concerns.
Membership is NOT a role.
Roles are NOT credit grants.
Credit grants are NOT memberships.

A user may be a workspace member without any credit grant.
A user may hold a scoped role without any credit grant.
Without a valid credit grant covering the required amount, booking
actions are BLOCKED regardless of membership or roles.
"""

from django.db import models
from django.contrib.auth.models import AbstractBaseUser, BaseUserManager


class UserAccountManager(BaseUserManager):
    def create_user(self, email, password=None, **extra_fields):
        if not email:
            raise ValueError("Email is required")
        email = self.normalize_email(email)
        user = self.model(email=email, **extra_fields)
        user.set_password(password)
        user.save(using=self._db)
        return user


class UserAccount(AbstractBaseUser):
    """
    Base identity. One login. One email.
    This is who the user IS - not what they can do or what they can spend.
    """
    email = models.EmailField(unique=True)
    display_name = models.CharField(max_length=255)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    objects = UserAccountManager()

    USERNAME_FIELD = "email"
    REQUIRED_FIELDS = ["display_name"]

    class Meta:
        db_table = "user_accounts"
        indexes = [
            # Serves JWT login (USERNAME_FIELD lookup) and every internal
            # email-based UserAccount resolution. unique=True on the field
            # already implies a unique btree, but declaring the Index
            # explicitly pins the name and keeps the plan stable across
            # backends.
            models.Index(fields=["email"]),
        ]

    def __str__(self):
        return self.email


class Workspace(models.Model):
    """
    Multi-tenant unit. Think: a company, a team, a department.
    Every plan belongs to a workspace.
    Data must never leak across workspace boundaries.
    """
    name = models.CharField(max_length=255)
    slug = models.SlugField(unique=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "workspaces"
        indexes = [
            # Serves the `?workspace=<slug>` query parameter on
            # PlanListView and every URL-driven workspace lookup. slug is
            # the public, stable identifier; `id` is internal.
            models.Index(fields=["slug"]),
        ]

    def __str__(self):
        return self.name


class WorkspaceMembership(models.Model):
    """
    Relationship between a UserAccount and a Workspace.
    Membership affects plan visibility, not credit eligibility.
    A user can be a member of multiple workspaces simultaneously.
    """
    class Status(models.TextChoices):
        ACTIVE = "active", "Active"
        SUSPENDED = "suspended", "Suspended"
        EXPIRED = "expired", "Expired"

    user = models.ForeignKey(
        UserAccount,
        on_delete=models.CASCADE,
        related_name="memberships"
    )
    workspace = models.ForeignKey(
        Workspace,
        on_delete=models.CASCADE,
        related_name="memberships"
    )
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.ACTIVE
    )
    joined_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "workspace_memberships"
        unique_together = [("user", "workspace")]
        indexes = [
            # Serves PlanListView for authenticated users: "which
            # workspaces is this user an ACTIVE member of?" - the
            # query that drives WORKSPACE_ONLY plan visibility.
            # Leading with `user` makes the lookup high-selectivity.
            models.Index(fields=["user", "status"]),
            # Serves workspace-admin tooling: "list all active members of
            # workspace X" (member roster, invitations, billing reports).
            # Leading with `workspace` matches that access pattern.
            models.Index(fields=["workspace", "status"]),
        ]

    def __str__(self):
        return f"{self.user.email} @ {self.workspace.name}"


class CreditGrant(models.Model):
    """
    Records that a pool of credits has been allocated to a user.

    CRITICAL: CreditGrant establishes eligibility. It does NOT track
    consumption. Available balance is always computed by the ledger:
    sum of all granted amounts minus sum of all consumed amounts for
    this user. Never derive balance from a mutable field on this model.

    Grant sources:
    - "subscription": credits issued by the platform via a paid plan
    - "topup": credits issued manually by a workspace administrator
    """
    class Source(models.TextChoices):
        SUBSCRIPTION = "subscription", "Platform Subscription"
        TOPUP = "topup", "Workspace Top-Up"

    user = models.ForeignKey(
        UserAccount,
        on_delete=models.CASCADE,
        related_name="credit_grants"
    )
    workspace = models.ForeignKey(
        Workspace,
        on_delete=models.CASCADE,
        related_name="credit_grants",
        null=True,
        blank=True,
        help_text="If topup, which workspace issued this. Null = platform subscription."
    )
    source = models.CharField(max_length=20, choices=Source.choices)
    amount = models.PositiveIntegerField(
        help_text="Number of credits granted in this allocation."
    )
    is_active = models.BooleanField(default=True)
    granted_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField(null=True, blank=True)
    revoked_at = models.DateTimeField(null=True, blank=True)
    revoked_by = models.ForeignKey(
        UserAccount,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="revoked_grants"
    )

    class Meta:
        db_table = "credit_grants"
        indexes = [
            # Serves the eligible-grant scan inside
            # get_available_credit_balance(): WHERE user_id = ? AND
            # is_active = TRUE. This is the hot read on every booking
            # and every PlanListView for an authenticated user.
            models.Index(fields=["user", "is_active"]),
            # Serves the workspace-admin "show all active top-ups for
            # workspace X" view and the workspace-scoped balance branch
            # when the booking is for a workspace plan.
            models.Index(fields=["workspace", "is_active"]),
            # Serves the periodic grant-expiry sweep (a background job
            # that scans grants where expires_at <= now() and marks them
            # inactive, emitting expiry ledger entries).
            models.Index(fields=["expires_at"]),
        ]

    def __str__(self):
        return f"{self.user.email} grant ({self.source}, {self.amount} credits)"


class ScopedRole(models.Model):
    """
    Grants authority over a SPECIFIC object to a user.

    A ScopedRole is NOT global. It applies ONLY to the object it references.
    Example: Administrator for workspace_id=12. Not administrator everywhere.

    scope_object_type: what kind of object (e.g. "workspace", "plan")
    scope_object_id: which specific object

    Role types:
    - administrator: full authority over the object, including issuing
      credit top-ups and advancing grant states
    - viewer: read access only, no structural changes
    """
    class RoleType(models.TextChoices):
        ADMINISTRATOR = "administrator", "Administrator"
        VIEWER = "viewer", "Viewer"

    user = models.ForeignKey(
        UserAccount,
        on_delete=models.CASCADE,
        related_name="scoped_roles"
    )
    workspace = models.ForeignKey(
        Workspace,
        on_delete=models.CASCADE,
        related_name="scoped_roles"
    )
    role_type = models.CharField(max_length=20, choices=RoleType.choices)
    scope_object_type = models.CharField(max_length=50)
    scope_object_id = models.BigIntegerField()
    granted_by = models.ForeignKey(
        UserAccount,
        on_delete=models.SET_NULL,
        null=True,
        related_name="granted_roles"
    )
    granted_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField(null=True, blank=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = "scoped_roles"
        indexes = [
            # Serves get_scoped_role(): "is THIS user the administrator
            # of THIS specific object?" - the exact predicate used by
            # IsWorkspaceAdministrator. Leading composite matches the
            # selectivity ordering (user is the most selective field).
            models.Index(fields=["user", "scope_object_type", "scope_object_id"]),
            # Serves admin-tooling queries that enumerate roles for a
            # workspace ("list every administrator of workspace X"),
            # and audit reports that filter by role_type + active flag.
            models.Index(fields=["workspace", "role_type", "is_active"]),
        ]

    def __str__(self):
        return (
            f"{self.user.email} {self.role_type} on "
            f"{self.scope_object_type}:{self.scope_object_id}"
        )
