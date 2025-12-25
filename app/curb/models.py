# app/curb/models.py

"""
CURB Data Models - Simplified Architecture

This module defines the database models for CURB trip management:
1. CurbAccount: Multi-account configuration support
2. CurbTrip: Simplified trip storage with 2-status flow
3. Enums: Clean status and payment type definitions
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum as PyEnum
from typing import Optional

from sqlalchemy import (
    DateTime, Enum, ForeignKey, Integer, Numeric,
    String, Boolean, Index
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.db import Base
from app.users.models import AuditMixin


class CurbTripStatus(str, PyEnum):
    """
    Simplified 2-status flow for trip processing
    
    IMPORTED: Raw data from CURB API stored in database
    POSTED_TO_LEDGER: Individual trip posted as ledger CREDIT entry
    """
    IMPORTED = "IMPORTED"
    POSTED_TO_LEDGER = "POSTED_TO_LEDGER"


class PaymentType(str, PyEnum):
    """Payment method for trip (we only store CASH trips)"""
    CASH = "CASH"
    CREDIT_CARD = "CREDIT_CARD"  # For reference, but we don't import these
    UNKNOWN = "UNKNOWN"


class ReconciliationMode(str, PyEnum):
    """Per-account reconciliation configuration"""
    SERVER = "server"  # Use CURB API Reconciliation_TRIP_LOG
    LOCAL = "local"    # Mark as reconciled locally only


class CurbAccount(Base, AuditMixin):
    """
    Multi-account CURB configuration
    
    Supports multiple CURB accounts with individual reconciliation settings.
    Each account can be configured for server-side or local reconciliation.
    """
    
    __tablename__ = "curb_accounts"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    
    # Account identification
    account_name: Mapped[str] = mapped_column(
        String(100), unique=True, index=True,
        comment="Friendly name for this CURB account (e.g., 'Production', 'Backup')"
    )
    
    # API credentials
    merchant_id: Mapped[str] = mapped_column(
        String(50), nullable=False,
        comment="CURB Merchant ID"
    )
    username: Mapped[str] = mapped_column(
        String(100), nullable=False,
        comment="CURB API username"
    )
    password: Mapped[str] = mapped_column(
        String(255), nullable=False,
        comment="CURB API password (store encrypted in production)"
    )
    api_url: Mapped[str] = mapped_column(
        String(255), nullable=False,
        default="https://api.taxitronic.org/vts_service/taxi_service.asmx",
        comment="CURB API endpoint URL"
    )
    
    # Configuration
    is_active: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=False,
        comment="Whether to fetch data from this account"
    )
    reconciliation_mode: Mapped[ReconciliationMode] = mapped_column(
        Enum(ReconciliationMode, values_callable=lambda x: [e.value for e in x]), 
        default=ReconciliationMode.LOCAL, nullable=False,
        comment="How to handle reconciliation for this account"
    )
    
    # Relationships
    trips: Mapped[list["CurbTrip"]] = relationship(back_populates="account", lazy="select")
    
    def __repr__(self):
        return f"<CurbAccount(id={self.id}, name='{self.account_name}', active={self.is_active})>"


class CurbTrip(Base, AuditMixin):
    """
    Simplified CURB trip storage
    
    Stores individual CASH trips from GET_TRIPS_LOG10 endpoint.
    Each trip becomes an individual ledger CREDIT posting.
    
    Key improvements:
    - Only 2 statuses (IMPORTED → POSTED_TO_LEDGER)
    - Links to source account for multi-account support
    - Optimized indexes for common queries
    - Stores only essential financial data
    """

    __tablename__ = "curb_trips"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    # --- Unique Identifiers from Source ---
    curb_trip_id: Mapped[str] = mapped_column(
        String(255), unique=True, index=True, nullable=False,
        comment="Unique identifier for the trip from CURB (e.g., ROWID).",
    )
    account_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("curb_accounts.id"), nullable=False, index=True,
        comment="Which CURB account this trip came from"
    )

    status: Mapped[CurbTripStatus] = mapped_column(
        Enum(CurbTripStatus), default=CurbTripStatus.IMPORTED, nullable=False,
        index=True, comment="Current processing status"
    )

    # --- Foreign Key Associations ---
    driver_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("drivers.id"), index=True,
        comment="Internal driver ID (mapped from curb_driver_id)"
    )
    lease_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("leases.id"), index=True,
        comment="Active lease during this trip"
    )
    vehicle_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("vehicles.id"), index=True,
        comment="Vehicle used during the trip"
    )
    medallion_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("medallions.id"), index=True,
        comment="Medallion associated with the trip"
    )

    # === Raw CURB Identifiers (for mapping) ===
    curb_driver_id: Mapped[str] = mapped_column(
        String(100), index=True, nullable=False,
        comment="Driver ID from CURB system"
    )
    curb_cab_number: Mapped[str] = mapped_column(
        String(100), index=True, nullable=False,
        comment="Cab/Medallion number from CURB"
    )

    # === Trip Timestamps ===
    start_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True,
        comment="Trip start datetime (used for 3-hour windowing)"
    )
    end_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        comment="Trip end datetime"
    )

    # === Financial Data (CASH trips only) ===
    fare: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, default=Decimal("0.00"),
        comment="Base fare amount"
    )
    tips: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, default=Decimal("0.00"),
        comment="Tip amount"
    )
    tolls: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, default=Decimal("0.00"),
        comment="Toll charges"
    )
    extras: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, default=Decimal("0.00"),
        comment="Extra charges"
    )
    total_amount: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, default=Decimal("0.00"), index=True,
        comment="Total trip amount"
    )

    # === Tax & Fee Breakdown ===
    surcharge: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, default=Decimal("0.00"),
        comment="State Surcharge (MTA Tax)"
    )
    improvement_surcharge: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, default=Decimal("0.00"),
        comment="Improvement Surcharge (TIF)"
    )
    congestion_fee: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, default=Decimal("0.00"),
        comment="Congestion Fee"
    )
    airport_fee: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, default=Decimal("0.00"),
        comment="Airport Fee"
    )
    cbdt_fee: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, default=Decimal("0.00"),
        comment="Congestion Relief Zone Toll (CBDT)"
    )

    # === Payment Info ===
    payment_type: Mapped[PaymentType] = mapped_column(
        Enum(PaymentType), default=PaymentType.CASH, nullable=False, index=True,
        comment="Payment method (we only import CASH)"
    )

    # === Reconciliation Tracking ===
    reconciliation_id: Mapped[Optional[str]] = mapped_column(
        String(100), index=True,
        comment="Reconciliation batch ID sent to CURB API"
    )
    reconciled_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        comment="When this trip was marked as reconciled"
    )

    # === Ledger Integration ===
    ledger_posting_ref: Mapped[Optional[str]] = mapped_column(
        String(255), index=True,
        comment="Reference ID of the ledger posting created for this trip"
    )
    posted_to_ledger_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        comment="When this trip was posted to the ledger"
    )

    # === Additional Trip Data ===
    distance_miles: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2),
        comment="Trip distance in miles"
    )
    num_passengers: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="Number of passengers"
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
    account: Mapped["CurbAccount"] = relationship(back_populates="trips", lazy="joined")
    driver: Mapped[Optional["Driver"]] = relationship(lazy="select")
    lease: Mapped[Optional["Lease"]] = relationship(lazy="select")
    vehicle: Mapped[Optional["Vehicle"]] = relationship(lazy="select")
    medallion: Mapped[Optional["Medallion"]] = relationship(lazy="select")

    __table_args__ = (
        # For datetime window queries with status filtering
        Index('idx_curb_trip_time_status', 'start_time', 'status'),
        
        # For payment type filtering (cash vs credit)
        Index('idx_curb_payment_status', 'payment_type', 'status'),
        
        # For driver-based queries with time ordering
        Index('idx_curb_driver_time', 'driver_id', 'start_time'),
        
        # For lease-based queries
        Index('idx_curb_lease_time', 'lease_id', 'start_time'),
        
        # For account-based filtering
        Index('idx_curb_account_time', 'account_id', 'start_time'),
        
        # For finding unposted trips ready for ledger
        Index('idx_curb_ready_for_ledger', 'status', 'driver_id', 'start_time'),
    )

    def to_dict(self):
        """Convert trip to dictionary for API responses"""
        return {
            "id": self.id,
            "curb_trip_id": self.curb_trip_id,
            "account_id": self.account_id,
            "status": self.status.value,
            "driver_id": self.driver_id,
            "curb_driver_id": self.curb_driver_id,
            "curb_cab_number": self.curb_cab_number,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "fare": float(self.fare),
            "tips": float(self.tips),
            "tolls": float(self.tolls),
            "extras": float(self.extras),
            "total_amount": float(self.total_amount),
            "surcharge": float(self.surcharge),
            "improvement_surcharge": float(self.improvement_surcharge),
            "congestion_fee": float(self.congestion_fee),
            "airport_fee": float(self.airport_fee),
            "cbdt_fee": float(self.cbdt_fee),
            "payment_type": self.payment_type.value,
            "ledger_posting_ref": self.ledger_posting_ref,
            "posted_to_ledger_at": self.posted_to_ledger_at.isoformat() if self.posted_to_ledger_at else None,
            "transaction_date": self.transaction_date.isoformat() if self.transaction_date else None,
            "start_lat": float(self.start_lat) if self.start_lat else None,
            "start_long": float(self.start_long) if self.start_long else None,
            "end_lat": float(self.end_lat) if self.end_lat else None,
            "end_long": float(self.end_long) if self.end_long else None,
        }
    
    def __repr__(self):
        return f"<CurbTrip(id={self.id}, trip_id='{self.curb_trip_id}', status={self.status.value}, amount={self.total_amount})>"