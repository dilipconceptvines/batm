# app/driver_payments/models.py

import enum
from decimal import Decimal
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Numeric, Date, DateTime,
    Enum, Boolean, Text
)
from sqlalchemy.orm import relationship
from app.core.db import Base


class ACHBatchStatus(str, enum.Enum):
    """ACH Batch Status Values"""
    DRAFT = "DRAFT"
    NACHA_GENERATED = "NACHA_GENERATED"
    SUBMITTED = "SUBMITTED"
    PROCESSED = "PROCESSED"
    REVERSED = "REVERSED"


class ACHBatch(Base):
    """
    ACH Batch for grouping multiple DTR payments
    
    Generates NACHA file for bank processing
    Format: YYMM-XXX (e.g., 2510-987)
    """
    __tablename__ = "ach_batches"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Batch Identification
    batch_number = Column(String(50), unique=True, nullable=False, index=True)
    
    # Dates
    batch_date = Column(DateTime, nullable=False, default=datetime.utcnow)
    effective_date = Column(Date, nullable=False)
    
    # Status
    status = Column(Enum(ACHBatchStatus), nullable=False, default=ACHBatchStatus.DRAFT)
    
    # Summary
    total_payments = Column(Integer, nullable=False, default=0)
    total_amount = Column(Numeric(12, 2), nullable=False, default=Decimal('0.00'))
    
    # NACHA File
    nacha_file_path = Column(String(500), nullable=True)
    nacha_generated_at = Column(DateTime, nullable=True)
    
    # Reversal
    is_reversed = Column(Boolean, default=False)
    reversed_at = Column(DateTime, nullable=True)
    reversal_reason = Column(Text, nullable=True)
    
    # Audit
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    created_by = Column(Integer, nullable=True)
    
    # Relationships
    dtrs = relationship("DTR", back_populates="ach_batch")
    
    def __repr__(self):
        return f"<ACHBatch {self.batch_number} - {self.status} - ${self.total_amount}>"


class CompanyBankConfiguration(Base):
    """
    Company Bank Configuration for NACHA file generation
    Single active record for the company
    """
    __tablename__ = "company_bank_configurations"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Company Information
    company_name = Column(String(255), nullable=False)
    company_tax_id = Column(String(10), nullable=False)  # EIN (10 digits)
    
    # Bank Information
    bank_name = Column(String(255), nullable=False)
    bank_routing_number = Column(String(9), nullable=False)  # 9-digit ABA routing
    bank_account_number = Column(String(17), nullable=False)
    
    # NACHA Configuration
    immediate_origin = Column(String(10), nullable=False)
    immediate_destination = Column(String(10), nullable=False)
    company_entry_description = Column(String(10), nullable=False, default="DRVPAY")
    
    # Status
    is_active = Column(Boolean, default=True)
    
    # Audit
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<CompanyBankConfig {self.company_name} - {self.bank_name}>"