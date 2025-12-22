# app/ledger/models.py

import uuid
from decimal import Decimal
from enum import Enum as PyEnum
from typing import Optional

from sqlalchemy import JSON, Enum, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.db import Base
from app.users.models import AuditMixin


class PostingCategory(str, PyEnum):
    """Enumeration of all possible ledger posting categories."""

    LEASE = "Lease"
    REPAIR = "Repair"
    LOAN = "Loan"
    EZPASS = "EZPass"
    PVB = "PVB"
    TLC = "TLC"
    TAXES = "Taxes"
    MISC = "Misc"
    EARNINGS = "Earnings"
    INTERIM_PAYMENT = "Interim Payment"
    DEPOSIT = "Deposit"
    CANCELLATION_FEE = "Cancellation Fee"


class EntryType(str, PyEnum):
    """Enumeration for ledger entry types (Debit or Credit)."""

    DEBIT = "DEBIT"
    CREDIT = "CREDIT"


class PostingStatus(str, PyEnum):
    """Enumeration for the status of a ledger posting."""

    POSTED = "POSTED"
    VOIDED = "VOIDED"


class BalanceStatus(str, PyEnum):
    """Enumeration for the status of a ledger balance."""

    OPEN = "OPEN"
    CLOSED = "CLOSED"


class LedgerPosting(Base, AuditMixin):
    """
    Represents an immutable record of a single financial transaction.
    This table is the auditable log of all events (earnings, obligations, payments, reversals).
    """

    __tablename__ = "ledger_postings"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    category: Mapped[PostingCategory] = mapped_column(
        Enum(PostingCategory), nullable=False, index=True
    )
    amount: Mapped[Decimal] = mapped_column(
        Numeric(10, 2),
        nullable=False,
        comment="Positive for debit (obligation), Negative for credit (earning/payment)",
    )
    entry_type: Mapped[EntryType] = mapped_column(Enum(EntryType), nullable=False)
    status: Mapped[PostingStatus] = mapped_column(
        Enum(PostingStatus), nullable=False, default=PostingStatus.POSTED
    )
    reference_id: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,
        comment="Traceability ID to the source record (e.g., LeaseSchedule ID, RepairInvoice ID)",
    )
    reversal_for_id: Mapped[Optional[str]] = mapped_column(
        String(36),
        ForeignKey("ledger_postings.id"),
        nullable=True,
        comment="If this is a reversal, points to the original posting ID",
    )

    # --- Denormalized Entity Linkage for Reporting ---
    driver_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("drivers.id"), index=True
    )
    vehicle_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("vehicles.id"), index=True
    )
    medallion_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("medallions.id"), index=True
    )
    lease_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("leases.id"), index=True
    )
    vin: Mapped[Optional[str]] = mapped_column(
        String(64), comment="Denormalized for filtering/reconciliation"
    )
    plate: Mapped[Optional[str]] = mapped_column(
        String(255), comment="Denormalized for filtering/reconciliation"
    )

    # --- Relationships ---
    driver: Mapped[Optional["Driver"]] = relationship(foreign_keys=[driver_id])
    vehicle: Mapped[Optional["Vehicle"]] = relationship(foreign_keys=[vehicle_id])
    medallion: Mapped[Optional["Medallion"]] = relationship(foreign_keys=[medallion_id])
    lease: Mapped[Optional["Lease"]] = relationship(foreign_keys=[lease_id])

    # def to_dict(self):
    #     return {
    #         "posting_id": self.id,
    #         "category": self.category.value,
    #         "amount": self.amount,
    #         "entry_type": self.entry_type.value,
    #         "status": self.status.value,
    #         "reference_id": self.reference_id,
    #         "driver_id": self.driver_id,
    #         "vehicle_id": self.vehicle_id,
    #         "medallion_id": self.medallion_id,
    #         "lease_id": self.lease_id,
    #         "created_on": self.created_on,
    #     }


class LedgerBalance(Base, AuditMixin):
    """
    Represents the rolling, real-time balance of a single financial obligation.
    Records are created when an obligation starts and are updated by postings.
    """

    __tablename__ = "ledger_balances"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    category: Mapped[PostingCategory] = mapped_column(
        Enum(PostingCategory), nullable=False, index=True
    )
    reference_id: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,
        comment="Traceability ID to the source obligation (e.g., RepairInvoice ID, Loan ID)",
    )
    original_amount: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, comment="The immutable initial obligation amount"
    )
    prior_balance: Mapped[Decimal] = mapped_column(
        Numeric(10, 2),
        nullable=False,
        default=Decimal("0.00"),
        comment="Carried over unpaid portion from previous DTR cycle",
    )
    balance: Mapped[Decimal] = mapped_column(
        Numeric(10, 2),
        nullable=False,
        comment="The current remaining unpaid portion of the obligation",
    )
    status: Mapped[BalanceStatus] = mapped_column(
        Enum(BalanceStatus), nullable=False, default=BalanceStatus.OPEN
    )
    applied_payment_refs: Mapped[Optional[dict]] = mapped_column(
        JSON,
        comment="Stores a list of Posting_IDs for payments/earnings applied to this balance",
    )

    # --- Denormalized Entity Linkage for Reporting ---
    driver_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("drivers.id"), index=True
    )
    vehicle_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("vehicles.id"), index=True
    )
    medallion_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("medallions.id"), index=True
    )
    lease_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("leases.id"), index=True
    )
    vin: Mapped[Optional[str]] = mapped_column(
        String(64), comment="Denormalized for filtering/reconciliation"
    )
    plate: Mapped[Optional[str]] = mapped_column(
        String(255), comment="Denormalized for filtering/reconciliation"
    )

    # --- Relationships ---
    driver: Mapped[Optional["Driver"]] = relationship(foreign_keys=[driver_id])
    vehicle: Mapped[Optional["Vehicle"]] = relationship(foreign_keys=[vehicle_id])
    medallion: Mapped[Optional["Medallion"]] = relationship(foreign_keys=[medallion_id])
    lease: Mapped[Optional["Lease"]] = relationship(foreign_keys=[lease_id])

    # def to_dict(self):
    #     return {
    #         "balance_id": self.id,
    #         "category": self.category.value,
    #         "reference_id": self.reference_id,
    #         "original_amount": self.original_amount,
    #         "balance": self.balance,
    #         "status": self.status.value,
    #         "driver_id": self.driver_id,
    #         "vehicle_id": self.vehicle_id,
    #         "medallion_id": self.medallion_id,
    #         "lease_id": self.lease_id,
    #         "created_on": self.created_on,
    #         "updated_on": self.updated_on,
    #     }