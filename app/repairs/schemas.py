### app/repairs/schemas.py

from datetime import date
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field

from app.repairs.models import RepairInvoiceStatus, WorkshopType, RepairInstallmentStatus
from app.leases.schemas import LeaseType

class RepairInvoiceListResponse(BaseModel):
    """
    Response schema for a single repair invoice in a list, matching the UI grid.
    
    NEW: Added receipt_url field for PDF access.
    """
    repair_id: str
    invoice_number: str
    invoice_date: date = Field(..., alias="invoice_date")
    status: str
    driver_name: Optional[str] = None
    medallion_no: Optional[str] = None
    lease_type: Optional[str] = None
    workshop: str = Field(..., alias="workshop_type")
    amount: Decimal = Field(..., alias="total_amount")
    receipt_url: Optional[str] = Field(None, description="Presigned URL for repair receipt PDF")  # NEW

    class Config:
        from_attributes = True
        populate_by_name = True


class PaginatedRepairInvoiceResponse(BaseModel):
    """
    Paginated response schema for a list of Repair Invoices.
    """
    items: List[RepairInvoiceListResponse]
    total_items: int
    page: int
    per_page: int
    total_pages: int
    status_list: List[RepairInvoiceStatus] = Field(
        default_factory=lambda: list(RepairInvoiceStatus)
    )
    lease_type_list: List[LeaseType] = Field(
        default_factory=lambda: list(LeaseType)
    )
    workshop_type_list: List[WorkshopType] = Field(
        default_factory=lambda: list(WorkshopType)
    )
    installment_status_list: List[RepairInstallmentStatus] = Field(
        default_factory=lambda: list(RepairInstallmentStatus)
    )


class RepairInstallmentResponse(BaseModel):
    """
    Response schema for a single installment in the detailed view.
    """
    installment_id: str
    week_period: str # e.g., "09/28/2025-10/01/2025"
    installment: Decimal = Field(..., alias="principal_amount")
    prior_balance: Decimal
    balance: Decimal
    status: RepairInstallmentStatus
    posted_date: Optional[date] = Field(None, alias="posted_on")

    class Config:
        """Pydantic configuration"""
        from_attributes = True
        populate_by_name = True


class RepairInvoiceDetailResponse(BaseModel):
    """
    Comprehensive response schema for the detailed view of a single Repair Invoice.
    
    NEW: Added receipt_url field for PDF access.
    """
    # Header Info
    repair_id: str
    repair_amount: Decimal = Field(..., alias="total_amount")
    total_paid: Decimal
    remaining_balance: Decimal
    installments_progress: str  # e.g., "1/5"

    # Detail Cards
    repair_invoice_details: dict
    driver_details: dict
    vehicle_details: dict
    lease_details: dict

    # Payment Schedule
    payment_schedule: List[RepairInstallmentResponse]
    
    # Receipt URL (NEW)
    receipt_url: Optional[str] = Field(None, description="Presigned URL for repair receipt PDF")

    class Config:
        """Pydantic configuration"""
        from_attributes = True
        populate_by_name = True


class RepairInstallmentListResponse(BaseModel):
    """
    Response schema for a single repair installment in a list view.
    """
    installment_id: str
    repair_id: str
    invoice_number: str
    driver_name: Optional[str] = None
    medallion_no: Optional[str] = None
    lease_id: Optional[str] = None
    vehicle_id: Optional[int] = None
    week_start_date: date
    week_end_date: date
    installment: Decimal = Field(..., alias="principal_amount")
    balance: Decimal
    status: RepairInstallmentStatus
    posted_date: Optional[date] = Field(None, alias="posted_on")
    ledger_posting_ref: Optional[str] = None
    workshop_type: Optional[WorkshopType] = None

    class Config:
        """Pydantic configuration"""
        from_attributes = True
        populate_by_name = True


class PaginatedRepairInstallmentResponse(BaseModel):
    """
    Paginated response schema for a list of Repair Installments.
    """
    items: List[RepairInstallmentListResponse]
    total_items: int
    page: int
    per_page: int
    total_pages: int