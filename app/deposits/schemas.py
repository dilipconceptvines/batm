# app/deposits/schemas.py

from datetime import date
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field, ConfigDict

from app.deposits.models import DepositStatus, CollectionMethod


class DepositCreateRequest(BaseModel):
    """
    Request schema for creating a new deposit during lease creation flow.
    """
    model_config = ConfigDict(from_attributes=True, json_schema_extra={
        "examples": [
            {
                "required_amount": 2500.00,
                "collected_amount": 2500.00,
                "collection_method": "Cash",
                "notes": "Full deposit collected at lease signing"
            },
            {
                "required_amount": 2500.00,
                "collected_amount": 0.00,
                "collection_method": "ACH",
                "notes": "Deposit to be collected via ACH within 7 days"
            }
        ]
    })

    required_amount: Decimal = Field(
        ..., gt=0, description="Required deposit amount (must be positive)"
    )
    collected_amount: Decimal = Field(
        default=Decimal('0.00'), ge=0, description="Amount collected at creation (default: 0.00)"
    )
    collection_method: CollectionMethod = Field(
        ..., description="Method used for collecting the deposit"
    )
    notes: Optional[str] = Field(
        None, max_length=1000, description="Optional notes about the deposit"
    )


class DepositUpdateRequest(BaseModel):
    """
    Request schema for updating deposit collection with additional payments.
    """
    model_config = ConfigDict(from_attributes=True, json_schema_extra={
        "examples": [
            {
                "additional_amount": 500.00,
                "collection_method": "Check",
                "notes": "Partial payment received via check #1234"
            },
            {
                "additional_amount": 2000.00,
                "collection_method": "ACH",
                "notes": "Remaining balance collected via ACH"
            }
        ]
    })

    additional_amount: Decimal = Field(
        ..., gt=0, description="Additional amount being collected (must be positive)"
    )
    collection_method: CollectionMethod = Field(
        ..., description="Method used for this collection"
    )
    notes: Optional[str] = Field(
        None, max_length=1000, description="Optional notes about this collection"
    )


class DepositResponse(BaseModel):
    """
    Complete response schema for deposit details, including all fields and computed values.
    """
    model_config = ConfigDict(from_attributes=True)

    # Primary identifiers
    deposit_id: str = Field(..., description="Unique deposit identifier (e.g., DEP-123-01)")
    lease_id: int = Field(..., description="Associated lease ID")

    # Driver and vehicle information
    driver_tlc_license: Optional[str] = Field(None, description="Driver's TLC license number")
    vehicle_vin: Optional[str] = Field(None, description="Vehicle VIN")
    vehicle_plate: Optional[str] = Field(None, description="Vehicle license plate")

    # Financial amounts
    required_amount: Decimal = Field(..., description="Required deposit amount")
    collected_amount: Decimal = Field(..., description="Total amount collected so far")
    outstanding_amount: Decimal = Field(..., description="Remaining amount to be collected")

    # Collection details
    initial_collection_amount: Optional[Decimal] = Field(None, description="Amount of initial collection")
    collection_method: Optional[CollectionMethod] = Field(None, description="Method used for collecting the deposit")

    # Status and dates
    deposit_status: DepositStatus = Field(..., description="Current status of the deposit")
    lease_start_date: Optional[date] = Field(None, description="Lease start date")
    lease_termination_date: Optional[date] = Field(None, description="Date when lease was terminated")
    hold_expiry_date: Optional[date] = Field(None, description="Date when 30-day hold period expires")

    # Refund information
    refund_amount: Optional[Decimal] = Field(None, description="Amount refunded to driver")
    refund_date: Optional[date] = Field(None, description="Date when refund was processed")
    refund_method: Optional[str] = Field(None, description="Method used for refund")
    refund_reference: Optional[str] = Field(None, description="Reference number for refund transaction")

    # Tracking and notes
    reminder_flags: Optional[dict] = Field(None, description="JSON object tracking reminder alerts")
    notes: Optional[str] = Field(None, description="Additional notes about the deposit")

    # Audit fields
    created_by: Optional[str] = Field(None, description="User who created the deposit")
    modified_by: Optional[str] = Field(None, description="User who last modified the deposit")
    created_on: Optional[date] = Field(None, description="Date when deposit was created")
    updated_on: Optional[date] = Field(None, description="Date when deposit was last updated")


class DepositListResponse(BaseModel):
    """
    Simplified response schema for deposit list views and grids.
    """
    model_config = ConfigDict(from_attributes=True)

    deposit_id: str = Field(..., description="Unique deposit identifier")
    lease_id: int = Field(..., description="Associated lease ID")
    driver_tlc_license: Optional[str] = Field(None, description="Driver's TLC license number")
    required_amount: Decimal = Field(..., description="Required deposit amount")
    collected_amount: Decimal = Field(..., description="Total amount collected so far")
    deposit_status: DepositStatus = Field(..., description="Current status of the deposit")
    lease_start_date: Optional[date] = Field(None, description="Lease start date")
    hold_expiry_date: Optional[date] = Field(None, description="Date when 30-day hold period expires")


class PaginatedDepositResponse(BaseModel):
    """
    Paginated response schema for deposit list endpoints.
    """
    model_config = ConfigDict(from_attributes=True)

    items: List[DepositListResponse] = Field(..., description="List of deposits for current page")
    total_items: int = Field(..., description="Total number of deposits across all pages")
    page: int = Field(..., description="Current page number (1-based)")
    per_page: int = Field(..., description="Number of items per page")
    total_pages: int = Field(..., description="Total number of pages available")
    status_list: List[DepositStatus] = Field(
        default_factory=lambda: list(DepositStatus),
        description="Available deposit statuses for filtering"
    )
    collection_method_list: List[CollectionMethod] = Field(
        default_factory=lambda: list(CollectionMethod),
        description="Available collection methods for filtering"
    )


class DepositRefundRequest(BaseModel):
    """
    Request schema for processing a deposit refund.
    """
    model_config = ConfigDict(from_attributes=True, json_schema_extra={
        "examples": [
            {
                "refund_method": "Check",
                "refund_reference": "CHK-2025-00123",
                "notes": "Full refund processed via check"
            },
            {
                "refund_method": "ACH",
                "refund_reference": "ACH-REF-456789",
                "notes": "Refund processed via ACH to driver's account"
            }
        ]
    })

    refund_method: CollectionMethod = Field(
        ..., description="Method to use for the refund"
    )
    refund_reference: str = Field(
        ..., min_length=1, max_length=100, description="Reference number for the refund transaction"
    )
    notes: Optional[str] = Field(
        None, max_length=1000, description="Optional notes about the refund"
    )


class DepositApplicationSummary(BaseModel):
    """
    Response schema for deposit auto-application summary.
    """
    model_config = ConfigDict(from_attributes=True, json_schema_extra={
        "examples": [
            {
                "deposit_id": "DEP-123-01",
                "total_deposit": 2500.00,
                "applied_to_ezpass": 500.00,
                "applied_to_pvb": 1000.00,
                "applied_to_tlc": 750.00,
                "refund_amount": 250.00,
                "applications": [
                    {"category": "EZPASS", "amount": 500.00, "posting_id": 12345},
                    {"category": "PVB", "amount": 1000.00, "posting_id": 12346},
                    {"category": "TLC", "amount": 750.00, "posting_id": 12347}
                ]
            }
        ]
    })

    deposit_id: str = Field(..., description="Unique deposit identifier")
    total_deposit: Decimal = Field(..., description="Total deposit amount available")
    applied_to_ezpass: Decimal = Field(
        default=Decimal('0.00'), description="Amount applied to EZPASS obligations"
    )
    applied_to_pvb: Decimal = Field(
        default=Decimal('0.00'), description="Amount applied to PVB obligations"
    )
    applied_to_tlc: Decimal = Field(
        default=Decimal('0.00'), description="Amount applied to TLC obligations"
    )
    refund_amount: Decimal = Field(
        default=Decimal('0.00'), description="Amount to be refunded to driver"
    )
    applications: List[dict] = Field(
        default_factory=list,
        description="List of individual applications made (posting references, amounts, etc.)"
    )