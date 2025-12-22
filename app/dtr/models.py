# app/dtr/models.py

import enum
from decimal import Decimal
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Numeric, Date, DateTime, 
    ForeignKey, Enum, Boolean, JSON
)
from sqlalchemy.orm import relationship
from app.core.db import Base


class DTRStatus(str, enum.Enum):
    """DTR Status Values"""
    DRAFT = "DRAFT"  # Pending charges still being posted
    FINALIZED = "FINALIZED"  # All charges confirmed, ready for payment
    PAID = "PAID"  # Payment processed


class PaymentMethod(str, enum.Enum):
    """Payment Method Types"""
    ACH = "ACH"
    CHECK = "CHECK"


class DTR(Base):
    """
    Driver Transaction Receipt (DTR) - ONE PER LEASE
    
    Business Rules:
    - One DTR generated per lease for each week
    - Additional drivers are included in the primary driver's DTR
    - Weekly period: Sunday 00:00 - Saturday 23:59
    - Lease amounts sourced from lease_schedules, not ledger postings
    - Mid-week terminations require pro-rated charges and final DTR
    """
    __tablename__ = "dtrs"
    
    # Primary Key
    id = Column(Integer, primary_key=True, index=True)
    
    # DTR Identification
    dtr_number = Column(String(50), unique=True, nullable=False, index=True)
    receipt_number = Column(String(50), unique=True, nullable=False, index=True)
    
    # Period Information
    week_start_date = Column(Date, nullable=False, index=True)
    week_end_date = Column(Date, nullable=False)
    generation_date = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    # Lease Reference (PRIMARY - DTR is per lease)
    lease_id = Column(Integer, ForeignKey("leases.id"), nullable=False, index=True)
    
    # Primary Driver (Leaseholder)
    primary_driver_id = Column(Integer, ForeignKey("drivers.id"), nullable=False, index=True)
    
    # Vehicle & Medallion
    vehicle_id = Column(Integer, ForeignKey("vehicles.id"), nullable=True)
    medallion_id = Column(Integer, ForeignKey("medallions.id"), nullable=True)
    
    # Additional Drivers (JSON array of driver IDs consolidated in this DTR)
    additional_driver_ids = Column(JSON, nullable=True)  # [driver_id1, driver_id2, ...]
    
    # === EARNINGS (Consolidated from all drivers on lease) ===
    credit_card_earnings = Column(Numeric(10, 2), nullable=False, default=Decimal('0.00'))
    
    # === TAXES (Consolidated from all drivers) ===
    mta_fees_total = Column(Numeric(10, 2), nullable=False, default=Decimal('0.00'))
    mta_fee_mta = Column(Numeric(10, 2), nullable=False, default=Decimal('0.00'))
    mta_fee_tif = Column(Numeric(10, 2), nullable=False, default=Decimal('0.00'))
    mta_fee_congestion = Column(Numeric(10, 2), nullable=False, default=Decimal('0.00'))
    mta_fee_cbdt = Column(Numeric(10, 2), nullable=False, default=Decimal('0.00'))
    mta_fee_airport = Column(Numeric(10, 2), nullable=False, default=Decimal('0.00'))
    
    # === EZPASS (Consolidated - all outstanding as of period end) ===
    ezpass_tolls = Column(Numeric(10, 2), nullable=False, default=Decimal('0.00'))
    
    # === LEASE CHARGES (From lease_schedules, can be pro-rated) ===
    lease_amount = Column(Numeric(10, 2), nullable=False, default=Decimal('0.00'))
    is_lease_prorated = Column(Boolean, default=False)
    active_days = Column(Integer, nullable=True)  # For pro-rated leases
    
    # === VIOLATIONS ===
    pvb_violations = Column(Numeric(10, 2), nullable=False, default=Decimal('0.00'))
    tlc_tickets = Column(Numeric(10, 2), nullable=False, default=Decimal('0.00'))
    
    # === OTHER CHARGES ===
    repairs = Column(Numeric(10, 2), nullable=False, default=Decimal('0.00'))
    driver_loans = Column(Numeric(10, 2), nullable=False, default=Decimal('0.00'))
    misc_charges = Column(Numeric(10, 2), nullable=False, default=Decimal('0.00'))
    
    # === PRIOR BALANCE ===
    prior_balance = Column(Numeric(10, 2), nullable=False, default=Decimal('0.00'))
    
    # === CALCULATIONS ===
    subtotal_deductions = Column(Numeric(10, 2), nullable=False, default=Decimal('0.00'))
    net_earnings = Column(Numeric(10, 2), nullable=False, default=Decimal('0.00'))
    total_due_to_driver = Column(Numeric(10, 2), nullable=False, default=Decimal('0.00'))
    
    # === STATUS ===
    status = Column(Enum(DTRStatus), nullable=False, default=DTRStatus.DRAFT, index=True)
    
    # === PAYMENT INFORMATION ===
    payment_method = Column(Enum(PaymentMethod), nullable=True)
    ach_batch_id = Column(Integer, ForeignKey("ach_batches.id"), nullable=True)
    ach_batch_number = Column(String(50), nullable=True, index=True)
    check_number = Column(String(50), nullable=True, index=True)
    payment_date = Column(DateTime, nullable=True)
    
    # === TERMINATION HANDLING ===
    is_final_dtr = Column(Boolean, default=False)  # True if lease terminated mid-week
    termination_date = Column(Date, nullable=True)
    cancellation_fee = Column(Numeric(10, 2), nullable=True)
    
    # === PENDING CHARGES TRACKING ===
    has_pending_charges = Column(Boolean, default=False)
    pending_charge_categories = Column(JSON, nullable=True)  # ["EZPASS", "PVB", ...]
    
    # === AUDIT ===
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    finalized_at = Column(DateTime, nullable=True)
    finalized_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    
    # Relationships
    lease = relationship("Lease", back_populates="dtrs")
    primary_driver = relationship("Driver", foreign_keys=[primary_driver_id], back_populates="dtrs")
    vehicle = relationship("Vehicle", back_populates="dtrs")
    medallion = relationship("Medallion", back_populates="dtrs")
    ach_batch = relationship("app.driver_payments.models.ACHBatch", back_populates="dtrs")
    
    def __repr__(self):
        return f"<DTR {self.dtr_number} - Lease {self.lease_id} - {self.status}>"