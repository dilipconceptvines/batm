### app/repairs/models.py

from datetime import date, datetime
from decimal import Decimal
from enum import Enum as PyEnum
from typing import List, Optional

from sqlalchemy import (
    Date, Enum, ForeignKey, Integer, Numeric, String,
    Text, DateTime,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.db import Base
from app.users.models import AuditMixin
from app.utils.s3_utils import s3_utils


class RepairInvoiceStatus(str, PyEnum):
    """Enumeration for the overall status of a repair invoice."""
    DRAFT = "Draft"
    OPEN = "Open"
    CLOSED = "Closed"
    HOLD = "Hold"
    CANCELLED = "Cancelled"


class RepairInstallmentStatus(str, PyEnum):
    """Enumeration for the status of a single repair installment."""
    SCHEDULED = "Scheduled"
    DUE = "Due"
    POSTED = "Posted"
    PAID = "Paid"


class WorkshopType(str, PyEnum):
    """Enumeration for the workshop type."""
    DSW_AUTO_REPAIR = "DSW Auto Repair"
    EXTERNAL = "External Workshop"


class RepairInvoice(Base, AuditMixin):
    """
    Represents the master record for a vehicle repair invoice. This tracks the
    overall obligation.
    """
    __tablename__ = "repair_invoices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    repair_id: Mapped[str] = mapped_column(String(50), unique=True, index=True, comment="System-generated unique internal ID for the repair (e.g., RPR-YYYY-#####).")
    invoice_number: Mapped[str] = mapped_column(String(255), index=True, comment="Actual invoice number from the workshop.")
    invoice_date: Mapped[date] = mapped_column(Date)
    
    # --- Entity Links ---
    driver_id: Mapped[int] = mapped_column(Integer, ForeignKey("drivers.id"), index=True)
    lease_id: Mapped[int] = mapped_column(Integer, ForeignKey("leases.id"), index=True)
    vehicle_id: Mapped[int] = mapped_column(Integer, ForeignKey("vehicles.id"), index=True)
    medallion_id: Mapped[int] = mapped_column(Integer, ForeignKey("medallions.id"), index=True)
    
    # --- Repair Details ---
    workshop_type: Mapped[WorkshopType] = mapped_column(Enum(WorkshopType))
    description: Mapped[Optional[str]] = mapped_column(Text)
    total_amount: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    
    # --- Lifecycle and Payment ---
    status: Mapped[RepairInvoiceStatus] = mapped_column(Enum(RepairInvoiceStatus), default=RepairInvoiceStatus.DRAFT, index=True)
    start_week: Mapped[date] = mapped_column(Date, comment="The Sunday that marks the beginning of the first payment period.")

    # --- Receipt Storage (NEW) ---
    receipt_s3_key: Mapped[Optional[str]] = mapped_column(String(512), 
                                                          comment="S3 key where the repair receipt PDF is stored")
    receipt_url: Mapped[Optional[str]] = mapped_column(String(1024), 
                                                       comment="Presigned URL for accessing the repair receipt")
    
    # --- Relationships ---
    driver: Mapped["Driver"] = relationship()
    lease: Mapped["Lease"] = relationship()
    vehicle: Mapped["Vehicle"] = relationship()
    medallion: Mapped["Medallion"] = relationship()
    installments: Mapped[List["RepairInstallment"]] = relationship(back_populates="invoice", cascade="all, delete-orphan")

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


class RepairInstallment(Base, AuditMixin):
    """
    Represents a single, scheduled weekly installment for a RepairInvoice.
    Each installment is posted to the ledger when it becomes due.
    """
    __tablename__ = "repair_installments"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    invoice_id: Mapped[int] = mapped_column(Integer, ForeignKey("repair_invoices.id"), index=True)
    installment_id: Mapped[str] = mapped_column(String(60), unique=True, index=True, comment="Unique ID for the installment (e.g., RPR-YYYY-#####-01).")

    # --- Schedule and Amount ---
    week_start_date: Mapped[date] = mapped_column(Date)
    week_end_date: Mapped[date] = mapped_column(Date)
    principal_amount: Mapped[Decimal] = mapped_column(Numeric(10, 2), comment="Portion of the installment that reduces the principal loan balance.")
    
    # --- Lifecycle and Ledger ---
    status: Mapped[RepairInstallmentStatus] = mapped_column(Enum(RepairInstallmentStatus), default=RepairInstallmentStatus.SCHEDULED, index=True)
    ledger_posting_ref: Mapped[Optional[str]] = mapped_column(String(255), comment="Reference to the LedgerPosting ID once created.")
    posted_on: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # --- Relationships ---
    invoice: Mapped["RepairInvoice"] = relationship(back_populates="installments")