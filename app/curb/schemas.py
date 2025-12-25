# app/curb/schemas.py

"""
CURB Module Pydantic Schemas

Defines request/response schemas for API endpoints and data validation.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional, Union

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
    """Response schema for async import operations"""
    status: str
    message: str
    task_id: str
    accounts: Union[str, List[int]]
    datetime_range: dict


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
class CurbTripResponseEnhanced(BaseModel):
    """Enhanced response schema for a single CURB trip with vehicle plate"""
    id: int
    curb_trip_id: str
    account_id: int
    status: str
    
    # Entity associations
    driver_id: Optional[int] = None
    driver_name: Optional[str] = None
    lease_id: Optional[int] = None
    vehicle_id: Optional[int] = None
    vehicle_plate: Optional[str] = None  # NEW: Vehicle plate number
    medallion_id: Optional[int] = None
    medallion_number: Optional[str] = None
    
    # Raw CURB identifiers
    curb_driver_id: str
    curb_cab_number: str
    
    # Trip timestamps
    start_time: datetime
    end_time: datetime
    transaction_date: Optional[datetime] = None
    
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
    payment_type: str
    
    # Ledger integration
    ledger_posting_ref: Optional[str] = None
    posted_to_ledger_at: Optional[datetime] = None
    
    # Additional data
    distance_miles: Optional[Decimal] = None
    num_passengers: Optional[int] = None
    
    class Config:
        from_attributes = True


class CurbTripFilters(BaseModel):
    """
    Enhanced filter schema for CURB trip listings
    
    Supports millions of records with optimized filtering:
    - Comma-separated multiple selection filters
    - Date range filters (from/to)
    - Amount range filters
    - Payment mode filtering
    """
    
    # --- Comma-separated ID/Text filters ---
    trip_ids: Optional[str] = Field(
        None,
        description="Comma-separated trip IDs (curb_trip_id)"
    )
    driver_ids: Optional[str] = Field(
        None,
        description="Comma-separated driver IDs (internal DB IDs)"
    )
    vehicle_plates: Optional[str] = Field(
        None,
        description="Comma-separated vehicle plate numbers"
    )
    medallion_numbers: Optional[str] = Field(
        None,
        description="Comma-separated medallion numbers"
    )
    tlc_license_numbers: Optional[str] = Field(
        None,
        description="Comma-separated TLC license numbers"
    )
    
    # --- Date range filters ---
    trip_start_from: Optional[datetime] = Field(
        None,
        description="Trip start date filter - from datetime"
    )
    trip_start_to: Optional[datetime] = Field(
        None,
        description="Trip start date filter - to datetime"
    )
    trip_end_from: Optional[datetime] = Field(
        None,
        description="Trip end date filter - from datetime"
    )
    trip_end_to: Optional[datetime] = Field(
        None,
        description="Trip end date filter - to datetime"
    )
    transaction_date_from: Optional[datetime] = Field(
        None,
        description="Transaction date filter - from datetime"
    )
    transaction_date_to: Optional[datetime] = Field(
        None,
        description="Transaction date filter - to datetime"
    )
    
    # --- Amount range filters ---
    total_amount_from: Optional[Decimal] = Field(
        None,
        ge=0,
        description="Minimum total amount"
    )
    total_amount_to: Optional[Decimal] = Field(
        None,
        ge=0,
        description="Maximum total amount"
    )
    
    # --- Payment mode filter (comma-separated) ---
    payment_modes: Optional[str] = Field(
        None,
        description="Comma-separated payment modes (CASH, CREDIT_CARD, etc.)"
    )
    
    # --- Status filter ---
    status: Optional[str] = Field(
        None,
        description="Trip status filter"
    )
    
    # --- Account filter ---
    account_ids: Optional[List[int]] = Field(
        None,
        description="Filter by specific CURB account IDs"
    )
    
    # --- Pagination ---
    page: int = Field(default=1, ge=1, description="Page number")
    per_page: int = Field(default=50, ge=1, le=1000, description="Items per page")
    
    # --- Sorting ---
    sort_by: str = Field(
        default="start_time",
        description="Field to sort by (start_time, end_time, total_amount, etc.)"
    )
    sort_order: str = Field(
        default="desc",
        pattern="^(asc|desc)$",
        description="Sort order: asc or desc"
    )


class PaymentModeReference(BaseModel):
    """Payment mode enumeration for reference"""
    value: str
    label: str


class CurbTripListResponse(BaseModel):
    """Enhanced response for CURB trip listings with payment modes reference"""
    items: List["CurbTripResponseEnhanced"]
    total_items: int
    page: int
    per_page: int
    total_pages: int
    payment_modes: List[PaymentModeReference] = Field(
        description="Available payment modes for filtering"
    )


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