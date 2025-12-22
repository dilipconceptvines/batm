### app/pvb/models.py

from datetime import datetime, date, time
from decimal import Decimal
from enum import Enum as PyEnum
from typing import List, Optional

from sqlalchemy import (
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    Date,
    Time,
    Boolean
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.db import Base
from app.users.models import AuditMixin


class PVBImportStatus(str, PyEnum):
    """Enumeration for the overall status of a PVB CSV import batch."""

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class PVBViolationStatus(str, PyEnum):
    """Enumeration for the lifecycle status of a single PVB violation."""

    IMPORTED = "Imported"
    ASSOCIATION_PENDING = "Association Pending"
    ASSOCIATION_FAILED = "Association Failed"
    ASSOCIATED = "Associated"
    POSTING_PENDING = "Posting Pending"
    POSTING_FAILED = "Posting Failed"
    POSTED_TO_LEDGER = "Posted to Ledger"


class PVBSource(str, PyEnum):
    """Enumeration to distinguish the origin of the violation record."""

    CSV_IMPORT = "CSV_IMPORT"
    MANUAL_ENTRY = "MANUAL_ENTRY"


class PVBImport(Base, AuditMixin):
    """
    Represents a single batch import of a PVB CSV file.
    Serves as an audit log for all PVB import activities.
    """

    __tablename__ = "pvb_imports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    file_name: Mapped[str] = mapped_column(String(255), comment="Original name of the uploaded CSV file.")
    import_timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, comment="Timestamp when the import process began."
    )
    status: Mapped[PVBImportStatus] = mapped_column(
        Enum(PVBImportStatus), default=PVBImportStatus.PENDING, index=True
    )
    total_records: Mapped[int] = mapped_column(Integer, default=0, comment="Total number of rows in the CSV file.")
    successful_records: Mapped[int] = mapped_column(Integer, default=0, comment="Number of records successfully imported.")
    failed_records: Mapped[int] = mapped_column(Integer, default=0, comment="Number of records that failed validation.")

    # Relationships
    violations: Mapped[List["PVBViolation"]] = relationship(
        back_populates="import_batch", cascade="all, delete-orphan"
    )

    def to_dict(self):
        return {
            "id": self.id,
            "file_name": self.file_name,
            "import_timestamp": self.import_timestamp.isoformat(),
            "status": self.status.value,
            "total_records": self.total_records,
            "successful_records": self.successful_records,
            "failed_records": self.failed_records,
            "created_on": self.created_on.isoformat() if self.created_on else None,
        }


class PVBViolation(Base, AuditMixin):
    """
    Represents a single Parking Violation Bureau (PVB) ticket, either from a CSV
    import or manual entry.
    """

    __tablename__ = "pvb_violations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    
    # --- Source and Linkage ---
    import_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("pvb_imports.id"), index=True, nullable=True)
    source: Mapped[PVBSource] = mapped_column(Enum(PVBSource), nullable=False, index=True)
    case_no: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True, comment="Links to the BPM case if created manually.")

    # --- Raw Data from Source (CSV or Manual Entry) ---
    plate: Mapped[str] = mapped_column(String(50), index=True)
    state: Mapped[str] = mapped_column(String(10))
    type: Mapped[str] = mapped_column(String(50))
    summons: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    issue_date: Mapped[date] = mapped_column(Date)
    issue_time: Mapped[Optional[time]] = mapped_column(Time)
    fine: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    penalty: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2), default=0)
    interest: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2), default=0)
    reduction: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2), default=0)
    processing_fee: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2), default=0)
    amount_due: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    driver_payment_amount : Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2) , default=0)

    is_terminated: Mapped[bool] = mapped_column(Boolean, default=False)
    non_program : Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    system_entry_date: Mapped[Optional[date]] = mapped_column(Date , nullable=True)
    new_issue: Mapped[bool] = mapped_column(Boolean, default=False)
    hearing_ind: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    penalty_warning: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    judgement: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    payment : Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2), default=0)
    ng_pmt: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    front_or_opp: Mapped[Optional[str]] = mapped_column(String(150), nullable=True)
    house_number: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    intersect_street: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    geo_location: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    street_code_1: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    street_code_2: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    street_code_3: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # --- Processing and Association Fields ---
    status: Mapped[PVBViolationStatus] = mapped_column(
        Enum(PVBViolationStatus), default=PVBViolationStatus.IMPORTED, index=True
    )
    failure_reason: Mapped[Optional[str]] = mapped_column(Text)
    posting_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    violation_code : Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    violation_country : Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    street_name : Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # --- Mapped Foreign Keys ---
    driver_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("drivers.id"), index=True)
    vehicle_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("vehicles.id"), index=True)
    medallion_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("medallions.id"), index=True)
    lease_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("leases.id"), index=True)

    # --- Relationships ---
    import_batch: Mapped[Optional["PVBImport"]] = relationship(back_populates="violations")
    driver: Mapped[Optional["Driver"]] = relationship()
    vehicle: Mapped[Optional["Vehicle"]] = relationship()
    medallion: Mapped[Optional["Medallion"]] = relationship()
    lease: Mapped[Optional["Lease"]] = relationship()

    def to_dict(self):
        return {
            "id": self.id,
            "import_id": self.import_id,
            "source": self.source.value,
            "case_no": self.case_no,
            "plate": self.plate,
            "state": self.state,
            "type": self.type,
            "summons": self.summons,
            "issue_date": self.issue_date.isoformat() if self.issue_date else None,
            "issue_time": self.issue_time.isoformat() if self.issue_time else None,
            "fine": float(self.fine),
            "penalty": float(self.penalty),
            "interest": float(self.interest),
            "reduction": float(self.reduction),
            "amount_due": float(self.amount_due),
            "status": self.status.value,
            "failure_reason": self.failure_reason,
            "posting_date": self.posting_date.isoformat() if self.posting_date else None,
            "driver_id": self.driver_id,
            "vehicle_id": self.vehicle_id,
            "medallion_id": self.medallion_id,
            "lease_id": self.lease_id,
            "created_on": self.created_on.isoformat() if self.created_on else None,
        }