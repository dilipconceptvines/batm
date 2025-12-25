# app/curb/schemas.py

"""
CURB Module Pydantic Schemas

Defines request/response schemas for API endpoints and data validation.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator

from app.curb.models import CurbTripStatus, PaymentType, ReconciliationMode


# ==============================================================================
# CURB ACCOUNT SCHEMAS
# ==============================================================================


class CurbAccountCreate(BaseModel):
    """Schema for creating a new CURB account"""
    account_name: str = Field(..., max_length=100, description="Friendly name for this account")
    merchant_id: str = Field(..., max_length=50, description="CURB Merchant ID")
    username: str = Field(..., max_length=100, description="CURB API username")
    password: str = Field(..., max_length=255, description="CURB API password")
    api_url: str = Field(
        default="https://api.taxitronic.org/vts_service/taxi_service.asmx",
        description="CURB API endpoint URL"
    )
    reconciliation_mode: ReconciliationMode = Field(
        default=ReconciliationMode.LOCAL,
        description="Reconciliation strategy: 'server' or 'local'"
    )
    is_active: bool = Field(default=True, description="Whether to fetch data from this account")


class CurbAccountUpdate(BaseModel):
    """Schema for updating an existing CURB account"""
    account_name: Optional[str] = Field(None, max_length=100)
    merchant_id: Optional[str] = Field(None, max_length=50)
    username: Optional[str] = Field(None, max_length=100)
    password: Optional[str] = Field(None, max_length=255)
    api_url: Optional[str] = None
    reconciliation_mode: Optional[ReconciliationMode] = None
    is_active: Optional[bool] = None


class CurbAccountResponse(BaseModel):
    """Schema for CURB account API responses"""
    id: int
    account_name: str
    merchant_id: str
    username: str
    api_url: str
    is_active: bool
    reconciliation_mode: ReconciliationMode
    created_on: datetime
    updated_on: Optional[datetime] = None
    
    class Config:
        """Pydantic configuration"""
        from_attributes = True


# ==============================================================================
# CURB TRIP SCHEMAS
# ==============================================================================


class CurbTripResponse(BaseModel):
    """Detailed response schema for a single CURB trip"""
    id: int
    curb_trip_id: str
    account_id: int
    status: CurbTripStatus
    
    # Entity associations
    driver_id: Optional[int] = None
    lease_id: Optional[int] = None
    vehicle_id: Optional[int] = None
    medallion_id: Optional[int] = None
    
    # Raw CURB identifiers
    curb_driver_id: str
    curb_cab_number: str
    
    # Trip timestamps
    start_time: datetime
    end_time: datetime
    
    # Financial data
    fare: Decimal
    tips: Decimal
    tolls: Decimal
    extras: Decimal
    total_amount: Decimal
    surcharge: Decimal
    improvement_surcharge: Decimal
    congestion_fee: Decimal
    airport_fee: Decimal
    cbdt_fee: Decimal
    
    # Payment info
    payment_type: PaymentType
    
    # Ledger integration
    ledger_posting_ref: Optional[str] = None
    posted_to_ledger_at: Optional[datetime] = None
    
    # Additional data
    distance_miles: Optional[Decimal] = None
    num_passengers: Optional[int] = None
    
    # Reconciliation
    reconciliation_id: Optional[str] = None
    reconciled_at: Optional[datetime] = None

    # Coordinates
    start_long: Optional[Decimal] = None
    start_lat: Optional[Decimal] = None
    end_long: Optional[Decimal] = None
    end_lat: Optional[Decimal] = None

    # Transaction date
    transaction_date: Optional[datetime] = None
    
    class Config:
        """Pydantic configuration"""
        from_attributes = True


class PaginatedCurbTripResponse(BaseModel):
    """Paginated response for trip listings"""
    items: List[CurbTripResponse]
    total_items: int
    page: int
    per_page: int
    total_pages: int


# ==============================================================================
# DATA IMPORT SCHEMAS
# ==============================================================================


class CurbImportRequest(BaseModel):
    """Schema for triggering CURB data import"""
    account_ids: Optional[List[int]] = Field(
        None, 
        description="Specific account IDs to import from. If None, imports from all active accounts."
    )
    from_datetime: Optional[datetime] = Field(
        None,
        description="Start of datetime range. Defaults to 3 hours ago."
    )
    to_datetime: Optional[datetime] = Field(
        None,
        description="End of datetime range. Defaults to now."
    )


class AccountProcessedDetail(BaseModel):
    """Details about a processed account during import"""
    account_id: int
    account_name: str
    trips_fetched: int
    status: str


class CurbImportResponse(BaseModel):
    """Response schema for import operations"""
    status: str
    message: str
    accounts_processed: List[AccountProcessedDetail]
    datetime_range: dict
    total_trips_fetched: int
    trips_imported: int
    trips_updated: int
    trips_skipped: int
    reconciled_count: int
    reconciliation_details: dict
    processing_time_seconds: float
    errors: List[dict] = []


# ==============================================================================
# LEDGER POSTING SCHEMAS
# ==============================================================================


class CurbLedgerPostRequest(BaseModel):
    """Schema for posting trips to ledger"""
    start_date: date = Field(..., description="Start date for posting trips")
    end_date: date = Field(..., description="End date for posting trips")
    driver_ids: Optional[List[int]] = Field(None, description="Specific drivers to post for. If None, posts for all.")
    lease_ids: Optional[List[int]] = Field(None, description="Specific leases to post for. If None, posts for all.")
    
    @field_validator('end_date')
    @classmethod
    def validate_date_range(cls, v, info):
        if 'start_date' in info.data and v < info.data['start_date']:
            raise ValueError("end_date must be on or after start_date")
        return v


class CurbLedgerPostResponse(BaseModel):
    """Response schema for ledger posting operations"""
    status: str
    message: str
    date_range: dict
    trips_processed: int
    trips_posted_to_ledger: int
    trips_failed: int
    total_amount_posted: Decimal
    postings_created: List[dict]
    errors: List[dict] = []


# ==============================================================================
# FILTER & LIST SCHEMAS
# ==============================================================================


class CurbTripFilters(BaseModel):
    """Schema for filtering trip lists"""
    account_ids: Optional[List[int]] = None
    driver_ids: Optional[List[int]] = None
    lease_ids: Optional[List[int]] = None
    medallion_numbers: Optional[List[str]] = None
    status: Optional[CurbTripStatus] = None
    payment_type: Optional[PaymentType] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    min_amount: Optional[Decimal] = None
    max_amount: Optional[Decimal] = None
    posted_to_ledger: Optional[bool] = None
    
    # Pagination
    page: int = Field(default=1, ge=1)
    per_page: int = Field(default=50, ge=1, le=500)
    
    # Sorting
    sort_by: str = Field(default="start_time", description="Field to sort by")
    sort_order: str = Field(default="desc", pattern="^(asc|desc)$")


# ==============================================================================
# SUMMARY & STATISTICS SCHEMAS
# ==============================================================================


class CurbAccountStats(BaseModel):
    """Statistics for a CURB account"""
    account_id: int
    account_name: str
    is_active: bool
    total_trips: int
    trips_imported: int
    trips_posted: int
    total_earnings: Decimal
    last_import: Optional[datetime] = None


class CurbSystemStats(BaseModel):
    """Overall CURB system statistics"""
    total_accounts: int
    active_accounts: int
    total_trips: int
    trips_pending_post: int
    total_earnings_ytd: Decimal
    last_import_time: Optional[datetime] = None
    accounts: List[CurbAccountStats]