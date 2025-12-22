### app/tlc/models.py

from datetime import date, time
from decimal import Decimal
from enum import Enum as PyEnum
from typing import Optional

from sqlalchemy import (
    Date,
    Enum,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    Time,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.db import Base
from app.users.models import AuditMixin


class TLCViolationType(str, PyEnum):
    """Enumeration for the type of TLC Violation."""
    FI = "FI" # Failure to Inspect Vehicle
    FN = "FN" # Failure to Comply with Notice
    RF = "RF" # Reinspection Fee
    EA = "EA"


class TLCDisposition(str, PyEnum):
    """Enumeration for the disposition of a TLC Violation."""
    PAID = "Paid"
    REDUCED = "Reduced"
    DISMISSED = "Dismissed"


class TLCViolationStatus(str, PyEnum):
    """Internal status for tracking ledger posting."""
    PENDING = "Pending"
    POSTED = "Posted"
    REVERSED = "Reversed"


class TLCViolation(Base, AuditMixin):
    """
    Represents a single TLC (Taxi & Limousine Commission) violation ticket,
    entered manually through a BPM workflow.
    """

    __tablename__ = "tlc_violations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    case_no: Mapped[str] = mapped_column(String(255), nullable=False, index=True, comment="Links to the BPM case used for creation.")

    # --- Violation Details ---
    summons_no: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    issue_date: Mapped[date] = mapped_column(Date)
    issue_time: Mapped[Optional[time]] = mapped_column(Time)
    violation_type: Mapped[TLCViolationType] = mapped_column(Enum(TLCViolationType))
    description: Mapped[Optional[str]] = mapped_column(Text, comment="Required only if type is FN.")
    amount: Mapped[Decimal] = mapped_column(Numeric(10, 2), comment="Base ticket amount.")
    service_fee: Mapped[Decimal] = mapped_column(Numeric(10, 2), default=0)
    total_payable: Mapped[Decimal] = mapped_column(Numeric(10, 2), comment="Calculated as Amount + Service Fee.")
    driver_payable: Mapped[Decimal] = mapped_column(Numeric(10, 2), default=0)
    reduced_tlc_amount: Mapped[Decimal] = mapped_column(Numeric(10, 2), default=0)
    disposition: Mapped[TLCDisposition] = mapped_column(Enum(TLCDisposition), default=TLCDisposition.PAID)
    due_date: Mapped[date] = mapped_column(Date)
    note: Mapped[Optional[str]] = mapped_column(Text)
    
    # --- Entity Links ---
    driver_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("drivers.id"), index=True, nullable=True)
    medallion_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("medallions.id"), index=True , nullable=True)
    lease_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("leases.id"), index=True , nullable=True)
    vehicle_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("vehicles.id"), index=True , nullable=True)
    attachment_document_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("document.id"), comment="FK to the uploaded ticket scan." , nullable=True)

    # --- Ledger Integration ---
    status: Mapped[TLCViolationStatus] = mapped_column(Enum(TLCViolationStatus), default=TLCViolationStatus.PENDING)
    original_posting_id: Mapped[Optional[str]] = mapped_column(String(255), comment="Reference to the initial ledger posting.")
    reversal_posting_id: Mapped[Optional[str]] = mapped_column(String(255), comment="Reference to the reversal posting if disposition is changed.")

    plate: Mapped[str] = mapped_column(String(50), comment="License plate number of the vehicle.")
    state: Mapped[str] = mapped_column(String(10), default="NY", comment="State of the license plate.")
    
    # --- Relationships ---
    driver: Mapped[Optional["Driver"]] = relationship()
    medallion: Mapped["Medallion"] = relationship()
    lease: Mapped["Lease"] = relationship()
    vehicle: Mapped["Vehicle"] = relationship()
    attachment: Mapped["Document"] = relationship()

    def to_dict(self):
        return {
            "id": self.id,
            "case_no": self.case_no,
            "summons_no": self.summons_no,
            "issue_date": self.issue_date.isoformat() if self.issue_date else None,
            "issue_time": self.issue_time.isoformat() if self.issue_time else None,
            "violation_type": self.violation_type.value,
            "description": self.description,
            "amount": float(self.amount),
            "service_fee": float(self.service_fee),
            "total_payable": float(self.total_payable),
            "disposition": self.disposition.value,
            "due_date": self.due_date.isoformat() if self.due_date else None,
            "note": self.note,
            "driver_id": self.driver_id,
            "medallion_id": self.medallion_id,
            "lease_id": self.lease_id,
            "vehicle_id": self.vehicle_id,
            "plate": self.plate,
            "state": self.state,
            "attachment_document_id": self.attachment_document_id,
            "status": self.status.value,
        }