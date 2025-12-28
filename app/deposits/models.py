# app/deposits/models.py

from decimal import Decimal
from enum import Enum as PyEnum
from typing import Optional

from sqlalchemy import (
    JSON, Boolean, Column, Date, DateTime, Enum, ForeignKey, Integer,
    Numeric, String, Text, func
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.db import Base
from app.users.models import AuditMixin


class DepositStatus(str, PyEnum):
    """Enumeration for deposit status."""

    PENDING = "Pending"
    PARTIALLY_PAID = "Partially Paid"
    PAID = "Paid"
    HELD = "Held"
    REFUNDED = "Refunded"


class CollectionMethod(str, PyEnum):
    """Enumeration for deposit collection methods."""

    CASH = "Cash"
    CHECK = "Check"
    ACH = "ACH"


class Deposit(Base, AuditMixin):
    """
    Deposit model for managing security deposits in lease agreements.
    """

    __tablename__ = "deposits"

    # Primary key and unique identifier
    deposit_id: Mapped[str] = mapped_column(
        String(50), primary_key=True, index=True,
        comment="Unique deposit identifier in format DEP-{LEASE_ID}-01"
    )

    # Foreign key to lease
    lease_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("leases.id"), unique=True, index=True,
        comment="Foreign key to leases table, unique constraint ensures one deposit per lease"
    )

    # Driver and vehicle information (denormalized for quick access)
    driver_tlc_license: Mapped[Optional[str]] = mapped_column(
        String(20), index=True,
        comment="Driver's TLC license number for quick lookups"
    )
    vehicle_vin: Mapped[Optional[str]] = mapped_column(
        String(17),
        comment="Vehicle VIN for reference"
    )
    vehicle_plate: Mapped[Optional[str]] = mapped_column(
        String(10),
        comment="Vehicle license plate for reference"
    )

    # Financial amounts
    required_amount: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, default=Decimal('0.00'),
        comment="Required deposit amount (default: 1 week lease fee)"
    )
    collected_amount: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, default=Decimal('0.00'),
        comment="Total amount collected so far"
    )
    outstanding_amount: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, default=Decimal('0.00'),
        comment="Remaining amount to be collected (required - collected)"
    )

    # Initial collection details
    initial_collection_amount: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2),
        comment="Amount of initial collection"
    )
    collection_method: Mapped[Optional[str]] = mapped_column(
        Enum(CollectionMethod), nullable=True,
        comment="Method used for collecting the deposit"
    )

    # Status and dates
    deposit_status: Mapped[str] = mapped_column(
        Enum(DepositStatus), nullable=False, index=True,
        comment="Current status of the deposit"
    )

    lease_start_date: Mapped[Optional[Date]] = mapped_column(
        Date,
        comment="Lease start date for reference"
    )
    lease_termination_date: Mapped[Optional[Date]] = mapped_column(
        Date,
        comment="Date when lease was terminated"
    )
    hold_expiry_date: Mapped[Optional[Date]] = mapped_column(
        Date,
        comment="Date when 30-day hold period expires"
    )

    # Refund information
    refund_amount: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2),
        comment="Amount refunded to driver"
    )
    refund_date: Mapped[Optional[Date]] = mapped_column(
        Date,
        comment="Date when refund was processed"
    )
    refund_method: Mapped[Optional[str]] = mapped_column(
        String(50),
        comment="Method used for refund (Cash, Check, ACH)"
    )
    refund_reference: Mapped[Optional[str]] = mapped_column(
        String(100),
        comment="Reference number for refund transaction"
    )

    # Tracking and notes
    reminder_flags: Mapped[Optional[dict]] = mapped_column(
        JSON,
        comment="JSON object tracking reminder alerts (e.g., {'week1': true, 'week2': true})"
    )
    notes: Mapped[Optional[str]] = mapped_column(
        Text,
        comment="Additional notes about the deposit"
    )

    # Relationships
    lease: Mapped["Lease"] = relationship(
        "Lease", back_populates="deposit"
    )

    def to_dict(self) -> dict:
        """Convert the Deposit model to a dictionary for serialization."""
        return {
            "deposit_id": self.deposit_id,
            "lease_id": self.lease_id,
            "driver_tlc_license": self.driver_tlc_license,
            "vehicle_vin": self.vehicle_vin,
            "vehicle_plate": self.vehicle_plate,
            "required_amount": float(self.required_amount) if self.required_amount else 0.0,
            "collected_amount": float(self.collected_amount) if self.collected_amount else 0.0,
            "outstanding_amount": float(self.outstanding_amount) if self.outstanding_amount else 0.0,
            "initial_collection_amount": float(self.initial_collection_amount) if self.initial_collection_amount else None,
            "collection_method": self.collection_method,
            "deposit_status": self.deposit_status,
            "lease_start_date": self.lease_start_date.isoformat() if self.lease_start_date else None,
            "lease_termination_date": self.lease_termination_date.isoformat() if self.lease_termination_date else None,
            "hold_expiry_date": self.hold_expiry_date.isoformat() if self.hold_expiry_date else None,
            "refund_amount": float(self.refund_amount) if self.refund_amount else None,
            "refund_date": self.refund_date.isoformat() if self.refund_date else None,
            "refund_method": self.refund_method,
            "refund_reference": self.refund_reference,
            "reminder_flags": self.reminder_flags,
            "notes": self.notes,
            "created_by": self.created_by,
            "modified_by": self.modified_by,
            "created_on": self.created_on.isoformat() if self.created_on else None,
            "updated_on": self.updated_on.isoformat() if self.updated_on else None,
        }