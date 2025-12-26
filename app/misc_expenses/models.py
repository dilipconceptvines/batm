### app/misc_expenses/models.py

from datetime import date
from decimal import Decimal
from enum import Enum as PyEnum
from typing import Optional

from sqlalchemy import (
    Date, Enum, ForeignKey, Integer,
    Numeric, String, Text, Index
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.db import Base
from app.users.models import AuditMixin


class MiscellaneousExpenseStatus(str, PyEnum):
    """
    Enumeration for the lifecycle status of a miscellaneous expense.
    """
    OPEN = "Open"
    RECOVERED = "Recovered"
    VOIDED = "Voided"


class PaymentType(str, PyEnum):
    """Type of miscellaneous payment"""
    EXPENSE = "EXPENSE"  # Charge to Driver
    CREDIT = "CREDIT"    # Credit to Driver


class MiscellaneousExpense(Base, AuditMixin):
    """
    Represents a single, one-time miscellaneous charge applied to a driver's
    active lease. These are immediately posted to the ledger for recovery in the
    next DTR cycle.
    """
    __tablename__ = "miscellaneous_expenses"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    expense_id: Mapped[str] = mapped_column(String(50), unique=True, index=True, comment="System-generated unique ID (e.g., MISC-YYYY-#####).")

    # --- Entity Links ---
    driver_id: Mapped[int] = mapped_column(Integer, ForeignKey("drivers.id"), index=True)
    lease_id: Mapped[int] = mapped_column(Integer, ForeignKey("leases.id"), index=True)
    vehicle_id: Mapped[int] = mapped_column(Integer, ForeignKey("vehicles.id"), index=True)
    medallion_id: Mapped[int] = mapped_column(Integer, ForeignKey("medallions.id"), index=True)

    # Type and Category
    payment_type: Mapped[PaymentType] = mapped_column(Enum(PaymentType), nullable=False, default=PaymentType.EXPENSE,
                         comment="EXPENSE (charge) or CREDIT (payment)")

    # --- Expense Details ---
    expense_date: Mapped[date] = mapped_column(Date)
    category: Mapped[str] = mapped_column(String(100), comment="Dropdown category (e.g., Lost Key, Cleaning Fee).")
    reference_number: Mapped[Optional[str]] = mapped_column(String(255), comment="Optional user-entered reference.")
    amount: Mapped[Decimal] = mapped_column(Numeric(10, 2), comment="The total amount of the charge.")
    notes: Mapped[Optional[str]] = mapped_column(Text, comment="Optional free-text notes for details.")

    # --- Lifecycle and Ledger Integration ---
    status: Mapped[MiscellaneousExpenseStatus] = mapped_column(Enum(MiscellaneousExpenseStatus), default=MiscellaneousExpenseStatus.OPEN, index=True)
    ledger_posting_ref: Mapped[str] = mapped_column(String(255), comment="Reference to the LedgerPosting ID created upon save.")
    
    # --- Receipt Storage ---
    receipt_s3_key: Mapped[Optional[str]] = mapped_column(String(512), nullable=True, comment="S3 key where the expense receipt PDF is stored")
    receipt_url: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True, comment="Presigned URL for accessing the expense receipt")

    # --- Relationships ---
    driver: Mapped["Driver"] = relationship()
    lease: Mapped["Lease"] = relationship()
    vehicle: Mapped["Vehicle"] = relationship()
    medallion: Mapped["Medallion"] = relationship()

    # Composite indexes for performance
    __table_args__ = (
        Index('idx_misc_pay_driver_date', 'driver_id', 'expense_date'),
        Index('idx_misc_pay_lease_status', 'lease_id', 'status'),
        Index('idx_misc_pay_type_status', 'payment_type', 'status'),
    )
    
    @property
    def presigned_receipt_url(self) -> Optional[str]:
        """Generate a presigned URL for the receipt if it exists."""
        if self.receipt_s3_key:
            from app.utils.s3_utils import s3_utils
            return s3_utils.generate_presigned_url(self.receipt_s3_key, expiration=3600)
        return None

    def to_dict(self):
        """Converts the MiscellaneousExpense object to a dictionary."""
        return {
            "id": self.id,
            "expense_id": self.expense_id,
            "payment_type": self.payment_type.value if self.payment_type else None,
            "driver_id": self.driver_id,
            "lease_id": self.lease_id,
            "vehicle_id": self.vehicle_id,
            "medallion_id": self.medallion_id,
            "expense_date": self.expense_date.isoformat() if self.expense_date else None,
            "category": self.category,
            "reference_number": self.reference_number,
            "amount": float(self.amount) if self.amount is not None else 0.0,
            "notes": self.notes,
            "status": self.status.value,
            "ledger_posting_ref": self.ledger_posting_ref,
            "receipt_s3_key": self.receipt_s3_key,
            "receipt_url": self.presigned_receipt_url,
            "created_on": self.created_on.isoformat() if self.created_on else None,
        }