### app/ezpass/schemas.py

from datetime import datetime , time
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field

from app.ezpass.models import EZPassTransactionStatus


class EZPassTransactionResponse(BaseModel):
    """
    Response schema for a single EZPass transaction, designed to match the UI grid.
    """
    id: Optional[int] = None
    transaction_id: Optional[str] = None
    entry_plaza: Optional[str] = None
    exit_plaza: Optional[str] = None
    transaction_date: datetime = Field(..., alias="transaction_datetime")
    transaction_time: time = None
    medallion_no: Optional[str] = None
    driver_id: Optional[str] = None
    plate_number: str = Field(..., alias="tag_or_plate")
    posting_date: Optional[datetime] = None
    status: str = None
    total_amount: Decimal = Field(..., alias="amount")
    vin: Optional[str] = None
    ezpass_class: Optional[str] = None
    
    # Extra fields for detailed view or potential future columns
    failure_reason: Optional[str] = None
    agency: Optional[str] = None
    created_on: Optional[datetime] = None
    
    class Config:
        from_attributes = True
        populate_by_name = True


class PaginatedEZPassTransactionResponse(BaseModel):
    """
    Paginated response schema for a list of EZPass transactions.
    """
    items: List[EZPassTransactionResponse]
    total_items: int
    page: int
    per_page: int
    total_pages: int
    status_list: List[EZPassTransactionStatus] = Field(
        default_factory=lambda: list(EZPassTransactionStatus)
    )


class ManualPostRequest(BaseModel):
    """
    Request schema for manually posting transactions to the ledger.
    """
    transaction_ids: List[int]


class ReassignRequest(BaseModel):
    """
    Request schema for reassigning transactions to different driver/lease
    """
    transaction_ids: List[int]
    new_driver_id: int
    new_lease_id: int
    new_medallion_id: Optional[int] = None
    new_vehicle_id: Optional[int] = None


class BulkOperationResponse(BaseModel):
    """
    Response schema for bulk operations.
    """
    success_count: int
    failed_count: int
    errors: List[dict] = []
    message: str


class ManualAssociateRequest(BaseModel):
    """Request schema for retrying automatic association on failed transactions"""
    transaction_ids: Optional[List[int]] = None  # Optional - if None, process all failed


class EZPassImportLogResponse(BaseModel):
    """Schema for individual import log entry"""
    id: int
    log_date: datetime = Field(description="Import timestamp")
    log_type: str = Field(description="Type of operation: Import, Associate, Post")
    file_name: str
    records_impacted: int = Field(description="Total records in import")
    success: int = Field(description="Successfully processed records")
    unidentified: int = Field(description="Failed records")
    log_status: str = Field(description="Success, Partial Success, or Failure")
    created_by: Optional[int] = None
    created_on: Optional[datetime] = None

    class Config:
        from_attributes = True


class PaginatedEZPassImportLogResponse(BaseModel):
    """
    Paginated response for import logs with filter metadata.
    
    Includes available_log_types and available_log_statuses for frontend
    to dynamically populate filter dropdowns.
    """
    items: List[EZPassImportLogResponse]
    total_items: int
    page: int
    per_page: int
    total_pages: int
    
    # Filter metadata for frontend
    available_log_types: List[str] = Field(
        description="List of available log types for filtering"
    )
    available_log_statuses: List[str] = Field(
        description="List of available log statuses for filtering"
    )

