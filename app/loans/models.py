### app/loans/models.py

from datetime import date, datetime
from decimal import Decimal
from enum import Enum as PyEnum
from typing import List, Optional

from sqlalchemy import (
    Date,
    Enum,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    DateTime,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.db import Base
from app.users.models import AuditMixin
from app.utils.s3_utils import s3_utils


class LoanStatus(str, PyEnum):
    """Enumeration for the overall status of a Driver Loan master record."""
    DRAFT = "Draft"
    OPEN = "Open"
    CLOSED = "Closed"
    HOLD = "Hold"
    CANCELLED = "Cancelled"


class LoanInstallmentStatus(str, PyEnum):
    """Enumeration for the status of a single loan installment."""
    SCHEDULED = "Scheduled"
    DUE = "Due"
    POSTED = "Posted"
    PAID = "Paid"


class DriverLoan(Base, AuditMixin):
    """
    Represents the master record for a personal loan extended to a driver.
    """
    __tablename__ = "driver_loans"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    loan_id: Mapped[str] = mapped_column(String(50), unique=True, index=True, comment="System-generated unique ID for the loan (e.g., DLN-YYYY-###).")
    
    # --- Entity Links ---
    driver_id: Mapped[int] = mapped_column(Integer, ForeignKey("drivers.id"), index=True)
    lease_id: Mapped[int] = mapped_column(Integer, ForeignKey("leases.id"), index=True)
    medallion_id: Mapped[int] = mapped_column(Integer, ForeignKey("medallions.id"), index=True)

    # --- Loan Details ---
    principal_amount: Mapped[Decimal] = mapped_column(Numeric(10, 2), comment="The total principal amount of the loan.")
    interest_rate: Mapped[Decimal] = mapped_column(Numeric(5, 2), default=0, comment="Annual interest rate (e.g., 10.00 for 10%).")
    notes: Mapped[Optional[str]] = mapped_column(Text)
    
    # --- Lifecycle and Payment ---
    status: Mapped[LoanStatus] = mapped_column(Enum(LoanStatus), default=LoanStatus.DRAFT, index=True)
    start_week: Mapped[date] = mapped_column(Date, comment="The Sunday that marks the beginning of the first repayment period.")
    loan_date: Mapped[date] = mapped_column(Date, comment="The date the loan was disbursed.")

    # --- Receipt Storage (NEW) ---
    receipt_s3_key: Mapped[Optional[str]] = mapped_column(String(512), 
                                                          comment="S3 key where the loan receipt PDF is stored")
    receipt_url: Mapped[Optional[str]] = mapped_column(String(1024), 
                                                       comment="Presigned URL for accessing the loan receipt")

    # --- Relationships ---
    driver: Mapped["Driver"] = relationship()
    lease: Mapped["Lease"] = relationship()
    medallion: Mapped["Medallion"] = relationship()
    installments: Mapped[List["LoanInstallment"]] = relationship(back_populates="loan", cascade="all, delete-orphan")

    @property
    def presigned_receipt_url(self) -> Optional[str]:
        """
        Generate a fresh presigned URL for the receipt if it exists in S3.
        This property ensures URLs are always fresh (not expired).
        """
        if self.receipt_s3_key:
            try:
                return s3_utils.generate_presigned_url(self.receipt_s3_key, expiration=3600)
            except Exception:
                return None
        return None


class LoanInstallment(Base, AuditMixin):
    """
    Represents a single, scheduled weekly installment for a DriverLoan.
    """
    __tablename__ = "loan_installments"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    loan_id: Mapped[int] = mapped_column(Integer, ForeignKey("driver_loans.id"), index=True)
    installment_id: Mapped[str] = mapped_column(String(60), unique=True, index=True, comment="Unique ID for the installment (e.g., DLN-YYYY-###-01).")

    # --- Schedule and Amounts ---
    week_start_date: Mapped[date] = mapped_column(Date)
    week_end_date: Mapped[date] = mapped_column(Date)
    principal_amount: Mapped[Decimal] = mapped_column(Numeric(10, 2), comment="The principal portion of this installment.")
    interest_amount: Mapped[Decimal] = mapped_column(Numeric(10, 2), comment="The calculated interest portion for this period.")
    total_due: Mapped[Decimal] = mapped_column(Numeric(10, 2), comment="Total amount due for this installment (Principal + Interest).")
    
    # --- Lifecycle and Ledger ---
    status: Mapped[LoanInstallmentStatus] = mapped_column(Enum(LoanInstallmentStatus), default=LoanInstallmentStatus.SCHEDULED, index=True)
    ledger_posting_ref: Mapped[Optional[str]] = mapped_column(String(255), comment="Reference to the LedgerPosting ID.")
    posted_on: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # --- Relationships ---
    loan: Mapped["DriverLoan"] = relationship(back_populates="installments")