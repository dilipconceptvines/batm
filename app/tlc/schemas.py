### app/tlc/schemas.py

from datetime import date, time
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field

from app.tlc.models import TLCDisposition, TLCViolationType , TLCViolationStatus
from app.leases.schemas import LeaseType


class TLCViolationListResponse(BaseModel):
    """
    Response schema for a single TLC violation in a list, matching the UI grid.
    """
    id: int
    plate: str
    state: str
    type: TLCViolationType
    summons: str = Field(..., alias="summons_no")
    issue_date: date
    issue_time: Optional[time] = None
    due_date: Optional[date] = None
    description : Optional[str] = None
    penalty_amount: Optional[Decimal] = 0
    service_fee : Optional[Decimal] = 0
    total_payable : Optional[Decimal] = 0
    driver_payable : Optional[Decimal] = 0
    disposition : Optional[TLCDisposition] = None
    note : Optional[str] = None
    
    # Fields populated after association
    driver_id: Optional[str] = None
    medallion_no: Optional[str] = None
    driver_name: Optional[str] = None
    vin: Optional[str] = None
    lease_id : Optional[str] = None
    lease_type : Optional[str] = None
    driver_email : Optional[str] = None
    driver_phone: Optional[str] = None
    document : Optional[dict] = {}
    class Config:
        from_attributes = True
        populate_by_name = True


class PaginatedTLCViolationResponse(BaseModel):
    """
    Paginated response schema for a list of TLC Violations.
    """
    items: List[TLCViolationListResponse]
    total_items: int
    page: int
    per_page: int
    total_pages: int
    types : List[TLCViolationType] = Field(
        default_factory=lambda: list(TLCViolationType)
    )
    dispositions : List[TLCDisposition] = Field(
        default_factory=lambda: list(TLCDisposition)
    )
    lease_types : List[LeaseType] = Field(
        default_factory=lambda: list(LeaseType)
    )
    statuses : List[TLCViolationStatus] = Field(
        default_factory=lambda: list(TLCViolationStatus)
    )

class TLCViolationCreateRequest(BaseModel):
    """
    Schema for the data submitted during the single-step BPM flow for creating
    a manual TLC violation.
    """
    # From Driver/Lease Selection
    driver_id: int
    lease_id: int
    medallion_id: int
    vehicle_id: int

    # From Violation Details Form
    plate: str
    state: str
    type: TLCViolationType
    summons: str
    issue_date: date
    issue_time: Optional[str] = Field(None, pattern=r"^\d{4}[AP]$") # e.g., "0550P"
    description: Optional[str] = None
    amount: Decimal = Field(..., gt=0)
    service_fee: Optional[Decimal] = Field(0, ge=0)
    disposition: TLCDisposition = TLCDisposition.PAID

    # From Attachment Step
    attachment_document_id: int