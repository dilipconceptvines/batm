### app/curb/models.py

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
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.db import Base
from app.users.models import AuditMixin


class CurbTripStatus(str, PyEnum):
    """Enumeration for the status of a CURB trip record."""

    UNRECONCILED = "UNRECONCILED"
    RECONCILED = "RECONCILED"
    MAPPED = "MAPPED"
    POSTED_TO_LEDGER = "POSTED_TO_LEDGER"
    ERROR = "ERROR"


class PaymentType(str, PyEnum):
    """Enumeration for the trip's payment type."""

    CASH = "CASH"
    CREDIT_CARD = "CREDIT_CARD"
    PRIVATE = "PRIVATE"
    UNKNOWN = "UNKNOWN"


class CurbTrip(Base, AuditMixin):
    """
    Represents a single trip or financial transaction record imported from the CURB API.
    This table consolidates data from multiple CURB endpoints and links it to the
    core entities within the BAT Connect system.
    """

    __tablename__ = "curb_trips"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    # --- Unique Identifiers from Source ---
    curb_trip_id: Mapped[str] = mapped_column(
        String(255),
        unique=True,
        index=True,
        comment="Unique identifier for the trip from CURB (e.g., ROWID).",
    )
    curb_period: Mapped[Optional[str]] = mapped_column(
        String(50), comment="The accounting period from CURB (e.g., '201903')."
    )

    # --- Local System Status ---
    status: Mapped[CurbTripStatus] = mapped_column(
        Enum(CurbTripStatus),
        nullable=False,
        default=CurbTripStatus.UNRECONCILED,
        index=True,
        comment="The processing status of the trip within the BAT system.",
    )

    # --- Foreign Key Associations ---
    driver_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("drivers.id"), index=True
    )
    lease_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("leases.id"), index=True
    )
    vehicle_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("vehicles.id"), index=True
    )
    medallion_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("medallions.id"), index=True
    )

    # --- Denormalized Identifiers from Source (for reporting and reconciliation) ---
    curb_driver_id: Mapped[str] = mapped_column(
        String(100), index=True, comment="The raw driver identifier from CURB."
    )
    curb_cab_number: Mapped[Optional[str]] = mapped_column(
        String(100), index=True, comment="The raw cab/medallion number from CURB."
    )
    plate: Mapped[Optional[str]] = mapped_column(
        String(50), comment="Vehicle plate number, if available."
    )

    # --- Trip Timestamps ---
    start_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), comment="The start date and time of the trip."
    )
    end_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), comment="The end date and time of the trip."
    )

    # --- Core Financials ---
    fare: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), comment="Base fare amount."
    )
    tips: Mapped[Decimal] = mapped_column(Numeric(10, 2), comment="Tip amount.")
    tolls: Mapped[Decimal] = mapped_column(Numeric(10, 2), comment="Toll charges.")
    extras: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2), comment="Extra charges."
    )
    total_amount: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), comment="Total amount charged for the trip."
    )

    # --- Tax & Surcharge Breakdown ---
    surcharge: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2), comment="State Surcharge (TAX)."
    )
    improvement_surcharge: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2), comment="Improvement Surcharge (IMPTAX / TIF)."
    )
    congestion_fee: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2), comment="Congestion Fee (CongFee)."
    )
    airport_fee: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2), comment="Airport Fee (airportFee)."
    )
    cbdt_fee: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2), comment="Congestion Relief Zone Toll (cbdt)."
    )

    # --- Payment & Reconciliation Details ---
    payment_type: Mapped[PaymentType] = mapped_column(
        Enum(PaymentType), comment="Method of payment (Cash, Credit, etc.)."
    )
    reconciliation_id: Mapped[Optional[str]] = mapped_column(
        String(100),
        comment="The reconciliation identifier sent back to CURB.",
        index=True,
    )
    reconciled_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), comment="Timestamp when the trip was reconciled."
    )

    start_long: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 7), comment="Starting longitude of the trip."
    )
    start_lat: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 7), comment="Starting latitude of the trip."
    )
    end_long: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 7), comment="Ending longitude of the trip."
    )
    end_lat: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 7), comment="Ending latitude of the trip."
    )
    num_service: Mapped[Optional[int]] = mapped_column(
        Integer, comment="Number of services during the trip.",
        nullable=True
    )

    transaction_date: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True, comment="Date of the transaction."
    )

    # --- Relationships ---
    driver: Mapped[Optional["Driver"]] = relationship()
    lease: Mapped[Optional["Lease"]] = relationship()
    vehicle: Mapped[Optional["Vehicle"]] = relationship()
    medallion: Mapped[Optional["Medallion"]] = relationship()

    def to_dict(self):
        """Converts the CurbTrip object to a dictionary."""
        return {
            "id": self.id,
            "curb_trip_id": self.curb_trip_id,
            "status": self.status.value,
            "driver_id": self.driver_id,
            "lease_id": self.lease_id,
            "vehicle_id": self.vehicle_id,
            "medallion_id": self.medallion_id,
            "curb_driver_id": self.curb_driver_id,
            "curb_cab_number": self.curb_cab_number,
            "plate": self.plate,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "fare": float(self.fare) if self.fare is not None else 0.0,
            "tips": float(self.tips) if self.tips is not None else 0.0,
            "tolls": float(self.tolls) if self.tolls is not None else 0.0,
            "extras": float(self.extras) if self.extras is not None else 0.0,
            "total_amount": float(self.total_amount)
            if self.total_amount is not None
            else 0.0,
            "surcharge": float(self.surcharge) if self.surcharge is not None else 0.0,
            "improvement_surcharge": float(self.improvement_surcharge)
            if self.improvement_surcharge is not None
            else 0.0,
            "congestion_fee": float(self.congestion_fee)
            if self.congestion_fee is not None
            else 0.0,
            "airport_fee": float(self.airport_fee)
            if self.airport_fee is not None
            else 0.0,
            "cbdt_fee": float(self.cbdt_fee) if self.cbdt_fee is not None else 0.0,
            "payment_type": self.payment_type.value,
            "reconciliation_id": self.reconciliation_id,
            "reconciled_at": self.reconciled_at.isoformat()
            if self.reconciled_at
            else None,
            "created_on": self.created_on.isoformat() if self.created_on else None,
            "updated_on": self.updated_on.isoformat() if self.updated_on else None,
        }