### app/interim_payments/schemas.py

from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Dict

from pydantic import BaseModel, Field

from app.interim_payments.models import PaymentMethod


class InterimPaymentAllocation(BaseModel):
    """
    Schema representing the allocation of a portion of an interim payment
    to a specific ledger balance.
    """
    category: str = Field(..., description="The category of the obligation (e.g., Lease, Repair, EZPass).")
    reference_id: str = Field(..., description="The unique reference ID of the obligation (e.g., Lease ID, Repair Invoice ID).")
    amount: Decimal = Field(..., gt=0, description="The amount to be applied to this obligation.")


class InterimPaymentCreate(BaseModel):
    """
    Schema for the data submitted during the BPM flow for creating
    a manual interim payment.
    """
    # From Step 1: Driver & Lease Selection
    driver_id: int
    lease_id: int

    # From Step 2: Payment Details
    total_amount: Decimal = Field(..., gt=0, description="The total payment amount received from the driver.")
    payment_method: PaymentMethod
    payment_date: datetime
    notes: Optional[str] = None

    # From Step 3: Allocation
    allocations: List[InterimPaymentAllocation] = Field(..., description="A list detailing how the total amount is allocated across obligations.")


class InterimPaymentResponse(BaseModel):
    """
    Response schema for a single interim payment in a list view, matching the UI grid.
    """
    payment_id: str = Field(..., alias="payment_id_display") # Use an alias to avoid conflict with model attribute
    tlc_license: str
    lease_id: str
    category: str
    reference_id: str
    amount: Decimal
    payment_date: datetime
    payment_method: PaymentMethod
    receipt_url: Optional[str] = None

    class Config:
        """Pydantic configuration"""
        from_attributes = True
        populate_by_name = True


class PaginatedInterimPaymentResponse(BaseModel):
    """
    Paginated response schema for a list of Interim Payments.
    """
    items: List[InterimPaymentResponse]
    total_items: int
    page: int
    per_page: int
    total_pages: int
    available_categories: List[str] = []
    available_payment_methods: List[str] = []


class InterimPaymentAllocationDetail(BaseModel):
    """Details of a single allocation within an interim payment."""
    category: str
    reference_id: str
    amount: Decimal
    # NEW: Include ledger status
    ledger_balance_status: Optional[str] = None  # "OPEN" or "CLOSED"
    is_fully_paid: Optional[bool] = None

class InterimPaymentDetailResponse(BaseModel):
    """Detailed response for a single interim payment."""
    payment_id: str
    case_no: str
    driver_id: int
    driver_name: str
    lease_id: int
    lease_identifier: str
    payment_date: datetime
    total_amount: Decimal
    payment_method: PaymentMethod
    allocations: List[InterimPaymentAllocationDetail]
    notes: Optional[str] = None
    receipt_url: Optional[str] = None
    created_by: int
    created_on: datetime
    
    # NEW: Summary fields
    total_allocated: Decimal
    excess_to_lease: Optional[Decimal] = None  # If any excess was applied