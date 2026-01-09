# app/interim_payments/models.py

from datetime import datetime
from decimal import Decimal
from enum import Enum as PyEnum
from typing import Optional

from sqlalchemy import (
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    Numeric,
    String,
    JSON,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.db import Base
from app.users.models import AuditMixin


class PaymentMethod(str, PyEnum):
    """Enumeration for the payment method used."""
    CASH = "Cash"
    CHECK = "Check"
    ACH = "ACH"
    DRIVER_CREDIT = "driver_credit"


class PaymentStatus(str, PyEnum):
    """Status of interim payment"""
    ACTIVE = "ACTIVE"
    VOIDED = "VOIDED"


class InterimPayment(Base, AuditMixin):
    """
    Represents a single ad-hoc payment made by a driver outside the
    weekly DTR cycle. This record tracks the payment itself and its
    allocation to various outstanding obligations in the ledger.
    """
    __tablename__ = "interim_payments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    payment_id: Mapped[str] = mapped_column(String(50), unique=True, index=True, comment="System-generated unique ID for the payment (e.g., INTPAY-[YYYY]-[#####]).")
    case_no: Mapped[str] = mapped_column(String(255), nullable=False, index=True, comment="Links to the BPM case used for creation.")

    # --- Entity Links ---
    driver_id: Mapped[int] = mapped_column(Integer, ForeignKey("drivers.id"), index=True)
    lease_id: Mapped[int] = mapped_column(Integer, ForeignKey("leases.id"), index=True)

    # --- Payment Details ---
    payment_date: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    total_amount: Mapped[Decimal] = mapped_column(Numeric(10, 2), comment="The total amount received from the driver.")
    payment_method: Mapped[PaymentMethod] = mapped_column(Enum(PaymentMethod))
    notes: Mapped[Optional[str]] = mapped_column(String(255), comment="Optional notes from the cashier.")
    
    # --- Allocation Record ---
    allocations: Mapped[Optional[dict]] = mapped_column(JSON, comment="A JSON object detailing how the payment was allocated to different ledger balances.")

    receipt_s3_key: Mapped[Optional[str]] = mapped_column(String(500), nullable=True, comment="S3 key/path for the generated receipt PDF")
    
    # --- Status Tracking ---
    status: Mapped[PaymentStatus] = mapped_column(
        Enum(PaymentStatus),
        nullable=False,
        default=PaymentStatus.ACTIVE,
        index=True,
        comment="Status of the payment (ACTIVE or VOIDED)"
    )
    voided_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp when payment was voided"
    )
    voided_by: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("users.id"),
        nullable=True,
        comment="User who voided the payment"
    )
    void_reason: Mapped[Optional[str]] = mapped_column(
        String(500),
        nullable=True,
        comment="Reason for voiding the payment"
    )
    
    # --- Relationships ---
    driver: Mapped["Driver"] = relationship()
    lease: Mapped["Lease"] = relationship()
    voided_by_user: Mapped[Optional["User"]] = relationship(
        foreign_keys=[voided_by], 
        lazy="select"
    )

    allocation_records: Mapped[list["InterimPaymentAllocation"]] = relationship(
        back_populates="interim_payment",
        cascade="all, delete-orphan",
        lazy="select"
    )


class InterimPaymentAllocation(Base, AuditMixin):
    """
    Structured storage for interim payment allocations.
    Replaces the JSON field for better querying and reporting.
    """
    __tablename__ = "interim_payment_allocations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    
    interim_payment_id: Mapped[int] = mapped_column(
        Integer, 
        ForeignKey("interim_payments.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Foreign key to interim_payments"
    )
    
    ledger_balance_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("ledger_balances.id"),
        nullable=False,
        index=True,
        comment="Foreign key to ledger_balances"
    )
    
    category: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        index=True,
        comment="Category of obligation (LEASE, REPAIR, LOAN, etc)"
    )
    
    reference_id: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,
        comment="Reference ID of the original obligation"
    )
    
    allocated_amount: Mapped[Decimal] = mapped_column(
        Numeric(10, 2),
        nullable=False,
        comment="Amount allocated to this obligation"
    )
    
    balance_before: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2),
        nullable=True,
        comment="Balance before this allocation"
    )
    
    balance_after: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2),
        nullable=True,
        comment="Balance after this allocation"
    )
    
    # Relationships
    interim_payment: Mapped["InterimPayment"] = relationship(
        back_populates="allocation_records",
        lazy="select"
    )
    
    ledger_balance: Mapped["LedgerBalance"] = relationship(
        foreign_keys=[ledger_balance_id],
        lazy="select"
    )
    