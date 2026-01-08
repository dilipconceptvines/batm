### app/exports/models.py

"""
Database models for tracking async export jobs.

Stores export job metadata, status, and file locations.
"""

from datetime import datetime
from enum import Enum as PyEnum
from typing import TYPE_CHECKING, Optional

from sqlalchemy import DateTime, Enum, ForeignKey, Integer, String, Text, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.db import Base
from app.users.models import AuditMixin

if TYPE_CHECKING:
    from app.users.models import User


class ExportStatus(str, PyEnum):
    """Export job status enumeration"""
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class ExportType(str, PyEnum):
    """Export module type enumeration"""
    EZPASS = "EZPASS"
    PVB = "PVB"
    CURB = "CURB"
    LEDGER_POSTINGS = "LEDGER_POSTINGS"
    LEDGER_BALANCES = "LEDGER_BALANCES"
    CURRENT_BALANCES = "CURRENT_BALANCES"
    DRIVER_PAYMENTS = "DRIVER_PAYMENTS"


class ExportFormat(str, PyEnum):
    """Export file format enumeration"""
    EXCEL = "excel"
    CSV = "csv"
    PDF = "pdf"
    JSON = "json"


class ExportJob(Base, AuditMixin):
    """
    Model for tracking async export jobs.
    
    Stores all metadata about an export request including filters,
    status, and location of generated file.
    """
    __tablename__ = "export_jobs"
    
    # Primary Key
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # Export Configuration
    export_type: Mapped[ExportType] = mapped_column(
        Enum(ExportType),
        nullable=False,
        index=True,
        comment="Module being exported (EZPASS, PVB, CURB, etc.)"
    )
    
    format: Mapped[ExportFormat] = mapped_column(
        Enum(ExportFormat),
        nullable=False,
        comment="Export file format (excel, csv, pdf, json)"
    )
    
    # Job Status
    status: Mapped[ExportStatus] = mapped_column(
        Enum(ExportStatus),
        nullable=False,
        default=ExportStatus.PENDING,
        index=True,
        comment="Current status of export job"
    )
    
    # Celery Task Tracking
    celery_task_id: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        index=True,
        comment="Celery task ID for tracking background job"
    )
    
    # Filter Parameters (stored as JSON)
    filters: Mapped[Optional[dict]] = mapped_column(
        JSON,
        nullable=True,
        comment="JSON object containing all filter parameters applied"
    )
    
    # Results
    file_url: Mapped[Optional[str]] = mapped_column(
        String(500),
        nullable=True,
        comment="S3 URL or file path of generated export file"
    )
    
    file_name: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        comment="Generated filename"
    )
    
    total_records: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
        comment="Total number of records in export"
    )
    
    # Error Handling
    error_message: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Error details if export failed"
    )
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        index=True,
        comment="When export was requested"
    )
    
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        nullable=True,
        comment="When export finished (success or failure)"
    )
    
    # User Tracking
    created_by: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("users.id", name="fk_export_jobs_created_by"),
        nullable=False,
        index=True,
        comment="User who requested the export"
    )
    
    # Relationships
    user: Mapped["User"] = relationship(
        "User",
        foreign_keys=[created_by],
        back_populates="export_jobs"
    )
    
    def __repr__(self):
        return (
            f"<ExportJob(id={self.id}, type={self.export_type}, "
            f"format={self.format}, status={self.status})>"
        )
