"""
app/current_balances/schemas.py

Pydantic schemas for Current Balances feature
"""

from enum import Enum
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


class DTRStatusEnum(str, Enum):
    """DTR Status"""
    GENERATED = "GENERATED"
    NOT_GENERATED = "NOT_GENERATED"


class PaymentTypeEnum(str, Enum):
    """Payment Type"""
    CASH = "CASH"
    ACH = "ACH"


class LeaseStatusEnum(str, Enum):
    """Lease Status"""
    ACTIVE = "Active"
    TERMINATED = "Terminated"
    TERMINATION_REQUESTED = "Termination Requested"


class DriverStatusEnum(str, Enum):
    """Driver Status"""
    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"
    BLACKLISTED = "BLACKLISTED"


class WeekPeriod(BaseModel):
    """Week period information"""
    week_start: date
    week_end: date
    week_label: str
    is_current_week: bool


class WeeklyBalanceRow(BaseModel):
    """
    Single row in the current balances table
    
    UPDATED: Added ssn field with masked SSN (XXX-XX-####)
    """
    # Identity fields
    lease_id: str
    driver_name: str
    tlc_license: Optional[str]
    ssn: Optional[str] = Field(None, description="Masked SSN (XXX-XX-####)")  # NEW
    medallion_number: str
    plate_number: Optional[str]
    vin_number: Optional[str]
    
    # Status fields
    lease_status: LeaseStatusEnum
    driver_status: DriverStatusEnum
    dtr_status: DTRStatusEnum
    payment_type: PaymentTypeEnum
    
    # Financial fields
    cc_earnings: Decimal
    weekly_lease_fee: Decimal
    mta_tif: Decimal
    ezpass_tolls: Decimal
    pvb_violations: Decimal
    tlc_tickets: Decimal
    repairs_wtd: Decimal
    loans_wtd: Decimal
    misc_charges: Decimal
    subtotal_deductions: Decimal
    prior_balance: Decimal
    deposit_amount: Decimal
    net_earnings: Decimal
    
    # Metadata
    last_updated: datetime

    class Config:
        """Pydantic configuration"""
        from_attributes = True


class CurrentBalancesFilter(BaseModel):
    """
    Filter parameters for current balances
    
    UPDATED: Added ssn_search field
    """
    # Search filters
    search: Optional[str] = None
    lease_id_search: Optional[str] = None
    driver_name_search: Optional[str] = None
    tlc_license_search: Optional[str] = None
    medallion_search: Optional[str] = None
    plate_search: Optional[str] = None
    vin_search: Optional[str] = None
    ssn_search: Optional[str] = Field(None, description="Search by SSN (full or last 4 digits)")  # NEW
    
    # Status filters
    lease_status: Optional[LeaseStatusEnum] = None
    driver_status: Optional[DriverStatusEnum] = None
    payment_type: Optional[PaymentTypeEnum] = None
    dtr_status: Optional[DTRStatusEnum] = None
    
    # Sorting
    sort_by: Optional[str] = None
    sort_order: Optional[str] = Field("asc", description="Sort order: asc or desc")


class CurrentBalancesResponse(BaseModel):
    """Response for current balances list"""
    week_period: WeekPeriod = Field(..., description="Current week period info")
    items: List[WeeklyBalanceRow] = Field(..., description="List of lease balances")
    total_items: int = Field(..., description="Total number of items")
    page: int = Field(..., description="Current page")
    per_page: int = Field(..., description="Items per page")
    total_pages: int = Field(..., description="Total pages")
    last_refresh: datetime = Field(..., description="Last data refresh timestamp")
    available_filters: Optional[dict] = Field(None, description="Available filter values")


class DailyBreakdown(BaseModel):
    """Daily breakdown of charges"""
    date: date
    cc_earnings: Decimal
    ezpass: Decimal
    mta_tif: Decimal
    violations: Decimal
    tlc_tickets: Decimal
    net_daily: Decimal

class DelayedCharge(BaseModel):
    """Delayed charge from previous weeks"""
    category: str
    amount: Decimal
    original_date: date
    system_entry_date: datetime
    description: Optional[str]


class WeeklyBalanceDetail(BaseModel):
    """
    Detailed view of a single lease balance with daily breakdown
    
    UPDATED: Added ssn field
    """
    # All fields from WeeklyBalanceRow
    lease_id: str
    driver_name: str
    tlc_license: Optional[str]
    ssn: Optional[str] = Field(None, description="Masked SSN (XXX-XX-####)")  # NEW
    medallion_number: str
    plate_number: Optional[str]
    vin_number: Optional[str]
    lease_status: LeaseStatusEnum
    driver_status: DriverStatusEnum
    dtr_status: DTRStatusEnum
    payment_type: PaymentTypeEnum
    
    cc_earnings: Decimal
    weekly_lease_fee: Decimal
    mta_tif: Decimal
    ezpass_tolls: Decimal
    pvb_violations: Decimal
    tlc_tickets: Decimal
    repairs_wtd: Decimal
    loans_wtd: Decimal
    misc_charges: Decimal
    subtotal_deductions: Decimal
    prior_balance: Decimal
    deposit_amount: Decimal
    net_earnings: Decimal
    
    # Required detailed breakdowns
    daily_breakdown: List[DailyBreakdown]
    delayed_charges: List[DelayedCharge]
    
    # Week context
    week_period: WeekPeriod
    last_updated: datetime


class DailyChargeDetail(BaseModel):
    """Detailed breakdown of a specific day's charges"""
    date: date
    charge_category: str
    items: List[dict]  # List of individual charge items with details
    total_amount: Decimal