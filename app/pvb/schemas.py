### app/pvb/schemas.py

from datetime import date, time, datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from app.pvb.models import PVBViolationStatus , PVBSource


class PVBViolationResponse(BaseModel):
    """
    Response schema for a single PVB violation, designed to match the UI grid.
    """
    id: Optional[int] = None
    plate: str
    state: str
    type: str
    summons: str
    issue_datetime: Optional[datetime] = None
    source: Optional[str] = None
    
    # Fields that get populated after association
    medallion_no: Optional[str] = None
    driver_id: Optional[str] = None
    lease_id: Optional[str] = None
    vin: Optional[str] = None
    
    posting_date: Optional[datetime] = None
    status: str

    fine: Optional[Decimal] = None
    penalty: Optional[Decimal] = None
    interest: Optional[Decimal] = None
    reduction: Optional[Decimal] = None
    processing_fee: Optional[Decimal] = None
    amount: Optional[Decimal] = None
    failure_reason: Optional[str] = None
    violation_code: Optional[str] = None
    violation_country: Optional[str] = None
    street_name: Optional[str] = None

    class Config:
        from_attributes = True
        populate_by_name = True


class PaginatedPVBViolationResponse(BaseModel):
    """
    Paginated response schema for a list of PVB violations.
    """
    items: List[PVBViolationResponse]
    total_items: int
    page: int
    per_page: int
    total_pages: int
    status_list: List[PVBViolationStatus] = Field(
        default_factory=lambda: list(PVBViolationStatus)
    )
    source_list: List[PVBSource] = Field(
        default_factory=lambda: list(PVBSource)
    )
    states : Optional[List[str]] = None
    types : Optional[List[str]] = None


class PVBManualPostRequest(BaseModel):
    """Request schema for manually posting PVB violations to the ledger."""
    transaction_ids: List[int]
    all_transactions: bool = False

    @model_validator(mode='after')
    def validate_transaction_ids(self):
        if self.all_transactions and self.transaction_ids:
            raise ValueError("Cannot provide both transaction_ids and all_transactions")
        if not self.all_transactions and not self.transaction_ids:
            raise ValueError("Either transaction_ids or all_transactions must be provided")

        return self

    class Config:
        json_schema_extra = {
            "examples": [
                {"transaction_ids": [1, 2, 3]},
                {"all_transactions": True}
            ]
        }

class PVBReassignRequest(BaseModel):
    """Request schema for reassigning PVB violations to different driver/lease."""
    transaction_ids: List[int]
    new_driver_id: int
    new_lease_id: int
    new_medallion_id: Optional[int] = None
    new_vehicle_id: Optional[int] = None


class PVBManualAssociateRequest(BaseModel):
    """Request schema for retrying automatic association on failed violations."""
    transaction_ids: Optional[List[int]] = None

class PVBManualCreateRequest(BaseModel):
    """
    Schema for the data required to manually create a PVB violation via the BPM flow.
    """
    # From Step 1: Association
    medallion_no: str = Field(..., description="The medallion number to associate the violation with.")
    
    # From Step 2: Violation Details
    plate: str
    state: str
    type: str
    summons: str
    issue_date: date
    issue_time: Optional[str] = Field(None, pattern=r"^\d{4}[AP]$") # e.g., "0550P"
    fine: Decimal = Field(..., gt=0)
    penalty: Optional[Decimal] = Field(0, ge=0)
    interest: Optional[Decimal] = Field(0, ge=0)
    reduction: Optional[Decimal] = Field(0, ge=0)
    
    # From Step 3: Proof
    document_id: int = Field(..., description="The ID of the uploaded proof document.")
    
    @field_validator('issue_time')
    @classmethod
    def parse_issue_time(cls, v):
        if v:
            try:
                meridiem = v[-1].upper()
                time_str = v[:-1]
                if meridiem not in ['A', 'P'] or not time_str.isdigit() or len(time_str) != 4:
                    raise ValueError("Invalid time format. Expected HHMM(A/P), e.g., '0550P'.")
                
                hour = int(time_str[:2])
                minute = int(time_str[2:])

                if not (0 <= hour <= 12 and 0 <= minute <= 59):
                     raise ValueError("Invalid hour or minute.")
                
                if hour == 12 and meridiem == 'A': # Midnight case
                    hour = 0
                elif hour < 12 and meridiem == 'P': # PM case
                    hour += 12

                return time(hour=hour, minute=minute)
            except Exception as e:
                raise ValueError(f"Invalid time format: {e}")
        return None
    
class EditPVBViolationRequest(BaseModel):
    """
    Request payload for editing an existing PVB Violation.
    Only includes fields that are allowed to be updated.
    """

    # --- Raw / Editable Violation Fields ---
    state: Optional[str] = None
    type: Optional[str] = None
    summons: Optional[str] = None

    issue_date: Optional[date] = None
    issue_time: Optional[time] = None

    fine: Optional[Decimal] = Field(None)
    penalty: Optional[Decimal] = Field(None)
    interest: Optional[Decimal] = Field(None)
    reduction: Optional[Decimal] = Field(None)
    processing_fee: Optional[Decimal] = Field(None)
    amount_due: Optional[Decimal] = Field(None)

    payment: Optional[Decimal] = Field(None)
    driver_payment_amount: Optional[Decimal] = Field(None)

    # --- Flags & Auxiliary Fields ---
    non_program: Optional[bool] = None
    system_entry_date: Optional[date] = None
    new_issue: Optional[bool] = None
    hearing_ind: Optional[str] = None
    penalty_warning: Optional[str] = None
    judgement: Optional[bool] = None
    ng_pmt: Optional[bool] = None

    # --- Location Fields ---
    front_or_opp: Optional[str] = None
    house_number: Optional[str] = None
    intersect_street: Optional[str] = None
    geo_location: Optional[str] = None
    street_code_1: Optional[str] = None
    street_code_2: Optional[str] = None
    street_code_3: Optional[str] = None

    street_name: Optional[str] = None
    violation_code: Optional[str] = None
    violation_country: Optional[str] = None

    class Config:
        orm_mode = True
