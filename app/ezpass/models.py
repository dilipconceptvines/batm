### app/ezpass/models.py

from datetime import datetime
from decimal import Decimal
from enum import Enum as PyEnum
from typing import Optional, List

from sqlalchemy import (
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.db import Base
from app.users.models import AuditMixin


class EZPassImportStatus(str, PyEnum):
    """Enumeration for the overall status of a CSV import batch."""

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class EZPassTransactionStatus(str, PyEnum):
    """Enumeration for the lifecycle status of a single EZPass transaction."""

    IMPORTED = "Imported"
    ASSOCIATION_PENDING = "Association Pending"
    ASSOCIATION_FAILED = "Association Failed"
    ASSOCIATED = "Associated"
    POSTING_PENDING = "Posting Pending"
    POSTING_FAILED = "Posting Failed"
    POSTED_TO_LEDGER = "Posted to Ledger"


class EZPassImport(Base, AuditMixin):
    """
    Represents a single batch import of an EZPass CSV file.
    Serves as an audit log for all import activities.
    """

    __tablename__ = "ezpass_imports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    file_name: Mapped[str] = mapped_column(String(255), comment="Original name of the uploaded CSV file.")
    import_timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, comment="Timestamp when the import process began."
    )
    status: Mapped[EZPassImportStatus] = mapped_column(
        Enum(EZPassImportStatus), default=EZPassImportStatus.PENDING, index=True
    )
    total_records: Mapped[int] = mapped_column(Integer, default=0, comment="Total number of rows in the CSV file.")
    successful_records: Mapped[int] = mapped_column(Integer, default=0, comment="Number of records successfully imported.")
    failed_records: Mapped[int] = mapped_column(Integer, default=0, comment="Number of records that failed validation.")

    # Relationships
    transactions: Mapped[List["EZPassTransaction"]] = relationship(
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


class EZPassTransaction(Base, AuditMixin):
    """
    Represents a single EZPass toll transaction imported from a CSV file.
    """

    __tablename__ = "ezpass_transactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    import_id: Mapped[int] = mapped_column(Integer, ForeignKey("ezpass_imports.id"), index=True)

    # --- Raw Data from CSV ---
    transaction_id: Mapped[str] = mapped_column(String(255), index=True, unique=True, comment="Unique Lane Txn ID from the CSV.")
    tag_or_plate: Mapped[str] = mapped_column(String(100), index=True, comment="The tag or plate number from the CSV.")
    agency: Mapped[str] = mapped_column(String(100), comment="Tolling agency (e.g., MTAB&T).")
    entry_plaza: Mapped[Optional[str]] = mapped_column(String(100))
    exit_plaza: Mapped[Optional[str]] = mapped_column(String(100))
    transaction_datetime: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, comment="The precise date and time of the toll transaction.")
    amount: Mapped[Decimal] = mapped_column(Numeric(10, 2), comment="The cost of the toll.")
    med_from_csv: Mapped[Optional[str]] = mapped_column(String(50), comment="The 'MED' column from the CSV, stored for reference.")
    ezpass_class: Mapped[Optional[str]] = mapped_column(String(50), comment="The 'CLASS' column from the CSV, stored for reference.")

    # --- Processing and Association Fields ---
    status: Mapped[EZPassTransactionStatus] = mapped_column(
        Enum(EZPassTransactionStatus), default=EZPassTransactionStatus.IMPORTED, index=True
    )
    failure_reason: Mapped[Optional[str]] = mapped_column(Text, comment="Stores the reason for an association or posting failure.")
    posting_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), comment="The date the transaction was posted to the ledger.")

    # --- Mapped Foreign Keys ---
    driver_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("drivers.id"), index=True)
    vehicle_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("vehicles.id"), index=True)
    medallion_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("medallions.id"), index=True)
    lease_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("leases.id"), index=True)

    # --- Relationships ---
    import_batch: Mapped["EZPassImport"] = relationship(back_populates="transactions")
    driver: Mapped[Optional["Driver"]] = relationship()
    vehicle: Mapped[Optional["Vehicle"]] = relationship()
    medallion: Mapped[Optional["Medallion"]] = relationship()
    lease: Mapped[Optional["Lease"]] = relationship()

    def to_dict(self):
        return {
            "id": self.id,
            "import_id": self.import_id,
            "transaction_id": self.transaction_id,
            "tag_or_plate": self.tag_or_plate,
            "agency": self.agency,
            "entry_plaza": self.entry_plaza,
            "exit_plaza": self.exit_plaza,
            "transaction_datetime": self.transaction_datetime.isoformat() if self.transaction_datetime else None,
            "amount": float(self.amount) if self.amount is not None else 0.0,
            "med_from_csv": self.med_from_csv,
            "status": self.status.value,
            "failure_reason": self.failure_reason,
            "posting_date": self.posting_date.isoformat() if self.posting_date else None,
            "driver_id": self.driver_id,
            "vehicle_id": self.vehicle_id,
            "medallion_id": self.medallion_id,
            "lease_id": self.lease_id,
            "created_on": self.created_on.isoformat() if self.created_on else None,
        }