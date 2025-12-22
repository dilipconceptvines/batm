from typing import Optional, List
from datetime import date

from pydantic import BaseModel


class UnifiedPaymentListItemResponse(BaseModel):
    """Enhanced response item for unified driver payments"""
    id: int
    receipt_type: str
    receipt_number: str
    payment_date: Optional[date]
    week_start_date: Optional[date] = None
    week_end_date: Optional[date] = None
    medallion_number: Optional[str]
    tlc_license: Optional[str]
    driver_name: Optional[str]
    plate_number: Optional[str]
    total_amount: float
    status: str
    payment_method: Optional[str]
    ach_batch_number: Optional[str] = None
    check_number: Optional[str] = None
    receipt_url: Optional[str] = None  # NEW: Presigned URL for receipt


class FilterMetadata(BaseModel):
    """Available filter options and values"""
    statuses: List[str]
    payment_methods: List[str]
    receipt_types: List[str]


class UnifiedPaymentListResponse(BaseModel):
    """Unified payment list response with filter metadata"""
    items: List[UnifiedPaymentListItemResponse]
    total: int
    page: int
    per_page: int
    total_pages: int
    filters: FilterMetadata