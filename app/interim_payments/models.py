### app/interim_payments/models.py

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
    voider: Mapped[Optional["User"]] = relationship(foreign_keys=[voided_by])

    def to_dict(self):
        """Converts the InterimPayment object to a dictionary."""
        return {
            "id": self.id,
            "payment_id": self.payment_id,
            "case_no": self.case_no,
            "driver_id": self.driver_id,
            "lease_id": self.lease_id,
            "payment_date": self.payment_date.isoformat() if self.payment_date else None,
            "total_amount": float(self.total_amount) if self.total_amount is not None else 0.0,
            "payment_method": self.payment_method.value,
            "notes": self.notes,
            "allocations": self.allocations,
            "status": self.status.value if self.status else None,
            "voided_at": self.voided_at.isoformat() if self.voided_at else None,
            "voided_by": self.voided_by,
            "void_reason": self.void_reason,
            "created_on": self.created_on.isoformat() if self.created_on else None,
        }