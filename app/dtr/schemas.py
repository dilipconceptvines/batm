# app/dtr/schemas.py

from datetime import date, datetime
from decimal import Decimal
from typing import Optional, List

from pydantic import BaseModel, Field, field_validator, EmailStr

from app.dtr.models import DTRStatus, PaymentMethod


class DTRResponse(BaseModel):
    """Complete DTR response with all fields"""
    id: int
    dtr_number: str
    receipt_number: str
    
    # Period
    week_start_date: date
    week_end_date: date
    generation_date: datetime
    
    # Entity Information
    lease_id: int
    lease_number: Optional[str] = None
    
    primary_driver_id: int
    driver_name: Optional[str] = None
    tlc_license: Optional[str] = None
    
    vehicle_id: Optional[int] = None
    plate_number: Optional[str] = None
    vin: Optional[str] = None
    
    medallion_id: Optional[int] = None
    medallion_number: Optional[str] = None
    
    # Additional drivers
    additional_driver_ids: Optional[List[int]] = None
    additional_driver_count: Optional[int] = 0
    
    # Earnings
    credit_card_earnings: Decimal
    
    # Taxes
    mta_fees_total: Decimal
    mta_fee_mta: Decimal
    mta_fee_tif: Decimal
    mta_fee_congestion: Decimal
    mta_fee_cbdt: Decimal
    mta_fee_airport: Decimal
    
    # Charges
    ezpass_tolls: Decimal
    lease_amount: Decimal
    is_lease_prorated: bool
    active_days: Optional[int] = None
    pvb_violations: Decimal
    tlc_tickets: Decimal
    repairs: Decimal
    driver_loans: Decimal
    misc_charges: Decimal
    
    # Prior balance
    prior_balance: Decimal
    
    # Calculations
    subtotal_deductions: Decimal
    net_earnings: Decimal
    total_due_to_driver: Decimal
    
    # Status
    status: DTRStatus
    has_pending_charges: bool
    pending_charge_categories: Optional[List[str]] = None
    
    # Payment
    payment_method: Optional[PaymentMethod] = None
    ach_batch_number: Optional[str] = None
    check_number: Optional[str] = None
    payment_date: Optional[datetime] = None
    
    # Termination
    is_final_dtr: bool
    termination_date: Optional[date] = None
    cancellation_fee: Optional[Decimal] = None
    
    # Audit
    created_at: datetime
    finalized_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class DTRListItemResponse(BaseModel):
    """Simplified DTR response for list views"""
    id: int
    receipt_number: str
    dtr_number: str
    week_start_date: date
    week_end_date: date
    
    # Key identifiers for display
    medallion_number: Optional[str] = None
    tlc_license: Optional[str] = None
    driver_name: Optional[str] = None
    plate_number: Optional[str] = None
    
    # Financial summary
    total_due_to_driver: Decimal
    
    # Status
    status: DTRStatus
    payment_method: Optional[PaymentMethod] = None
    ach_batch_number: Optional[str] = None
    check_number: Optional[str] = None
    
    class Config:
        """Pydantic configuration"""
        from_attributes = True


class DTRListResponse(BaseModel):
    """Paginated list of DTRs"""
    items: List[DTRListItemResponse]
    total: int
    page: int
    per_page: int
    total_pages: int


class DTRGenerationRequest(BaseModel):
    """Request to generate DTR for a lease"""
    lease_id: int = Field(..., description="Lease ID")
    week_start: date = Field(..., description="Sunday - start of week")
    week_end: Optional[date] = Field(None, description="Saturday - end of week (auto-calculated if not provided)")
    force_final: bool = Field(False, description="Force generation as final DTR")
    
    @field_validator('week_start')
    @classmethod
    def validate_sunday(cls, v):
        if v.weekday() != 6:  # 6 = Sunday
            raise ValueError("week_start must be a Sunday")
        return v
    
    @field_validator('week_end')
    @classmethod
    def validate_saturday(cls, v):
        if v and v.weekday() != 5:  # 5 = Saturday
            raise ValueError("week_end must be a Saturday")
        return v


class BatchDTRGenerationRequest(BaseModel):
    """Request to generate DTRs for multiple leases"""
    week_start: date = Field(..., description="Sunday - start of week")
    lease_ids: Optional[List[int]] = Field(None, description="Specific lease IDs (if None, generates for all active leases)")
    
    @field_validator('week_start')
    @classmethod
    def validate_sunday(cls, v):
        if v.weekday() != 6:
            raise ValueError("week_start must be a Sunday")
        return v


class CheckNumberUpdateRequest(BaseModel):
    """Request to update check number"""
    check_number: str = Field(..., min_length=1, max_length=50)
    payment_date: Optional[datetime] = None


class FinalizeDTRRequest(BaseModel):
    """Request to manually finalize a DRAFT DTR"""
    confirm_all_charges_posted: bool = Field(..., description="Confirmation that all charges are posted")
    
    @field_validator('confirm_all_charges_posted')
    @classmethod
    def validate_confirmation(cls, v):
        if not v:
            raise ValueError("You must confirm all charges are posted before finalizing")
        return v


class DTRFilterParams(BaseModel):
    """Filter parameters for DTR list"""
    page: int = Field(1, ge=1)
    per_page: int = Field(50, ge=1, le=200)
    
    # Filters
    receipt_number: Optional[str] = None
    status: Optional[DTRStatus] = None
    payment_method: Optional[PaymentMethod] = None
    week_start: Optional[date] = None
    week_end: Optional[date] = None
    medallion_number: Optional[str] = None
    tlc_license: Optional[str] = None
    driver_name: Optional[str] = None
    plate_number: Optional[str] = None
    ach_batch_number: Optional[str] = None
    check_number: Optional[str] = None
    
    # Sorting
    sort_by: str = Field('generation_date', description="Field to sort by")
    sort_order: str = Field('desc', pattern='^(asc|desc)$')


class DTRSummaryResponse(BaseModel):
    """Summary statistics for DTRs"""
    total_count: int
    total_amount: Decimal
    paid_amount: Decimal
    unpaid_amount: Decimal
    
    # By status breakdown
    draft_count: Optional[int] = 0
    finalized_count: Optional[int] = 0
    paid_count: Optional[int] = 0


class SendDTREmailRequest(BaseModel):
    """Request schema for sending DTR emails"""
    dtr_ids: List[int] = Field(..., description="List of DTR IDs to send via email")
    recipient_emails: Optional[List[EmailStr]] = Field(
        None,
        description="Optional list of recipient emails. If not provided, uses each driver's email."
    )
    include_violations: bool = Field(
        True,
        description="Whether to include PVB and TLC violation reports as attachments"
    )


class SendDTREmailResponse(BaseModel):
    """Response schema for DTR email sending"""
    total_requested: int
    emails_queued: int
    emails_failed: int
    results: List[dict]