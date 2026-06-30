"""
apps/ledger/models.py

Immutable append-only credit ledger.

INVARIANTS:
1. LedgerEntry records are NEVER updated
2. LedgerEntry records are NEVER deleted
3. Every credit grant, consumption, adjustment, and reversal must
   produce a LedgerEntry
4. The acting_user and acting_context must always be recorded
5. previous_balance and resulting_balance must always be recorded

Credit balance for any user is always derived by querying this ledger.
There is no mutable balance field anywhere in the system; writing to a
balance column outside this table would be a design violation.

This is a financial integrity requirement, so save() on an existing
entry and delete() both raise at the model layer.
"""

from django.db import models
from apps.accounts.models import UserAccount


class LedgerEntry(models.Model):
    """
    Immutable record of a single credit movement.

    entry_type examples:
    - "grant.subscription"    - credits issued via platform subscription
    - "grant.topup"           - credits issued via workspace top-up
    - "consumed"              - credits deducted by a booking action
    - "adjustment"            - manual correction by platform operator
    - "expiry"                - credits removed at grant expiry

    object_type: what triggered this entry ("booking", "credit_grant",
                 "adjustment")
    object_id: which specific object triggered this entry

    previous_balance: user's computed balance before this entry (integer)
    resulting_balance: user's computed balance after this entry (integer)

    Both balance fields are recorded at write time for auditability.
    They are snapshots only - the authoritative balance is always
    recomputed from the full ledger, never read from these fields
    directly.
    """

    entry_type = models.CharField(max_length=100)
    acting_user = models.ForeignKey(
        UserAccount,
        on_delete=models.SET_NULL,
        null=True,
        related_name="ledger_actions"
    )
    acting_context = models.CharField(
        max_length=50,
        help_text="subscriber | workspace_admin | platform_operator"
    )
    subject_user = models.ForeignKey(
        UserAccount,
        on_delete=models.SET_NULL,
        null=True,
        related_name="ledger_entries",
        help_text="The user whose balance is affected by this entry."
    )
    object_type = models.CharField(max_length=50)
    object_id = models.BigIntegerField()
    amount = models.IntegerField(
        help_text="Positive for grants, negative for consumption and expiry."
    )
    previous_balance = models.IntegerField()
    resulting_balance = models.IntegerField()
    reason_code = models.CharField(max_length=100, blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    timestamp = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "ledger_entries"
        indexes = [
            models.Index(fields=["subject_user", "timestamp"]),
            models.Index(fields=["object_type", "object_id"]),
            models.Index(fields=["acting_user", "timestamp"]),
            models.Index(fields=["entry_type", "timestamp"]),
        ]

    def save(self, *args, **kwargs):
        """
        Enforce immutability - ledger entries can only be created,
        never updated.
        """
        if self.pk:
            raise ValueError(
                "LedgerEntry records are immutable. "
                "You cannot update an existing ledger entry."
            )
        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        """
        Enforce immutability - ledger entries can never be deleted.
        """
        raise ValueError(
            "LedgerEntry records are immutable. "
            "You cannot delete a ledger entry."
        )

    def __str__(self):
        return (
            f"[{self.timestamp}] {self.entry_type} "
            f"{self.amount:+d} credits for {self.subject_user} "
            f"on {self.object_type}:{self.object_id}"
        )
