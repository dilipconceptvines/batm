### app/loans/schemas.py

from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field, ConfigDict

from app.loans.models import LoanInstallmentStatus, LoanStatus


class DriverLoanListResponse(BaseModel):
    """Response model for a single loan in the list view."""
    model_config = ConfigDict(from_attributes=True)
    
    loan_id: str = Field(..., description="System-generated loan ID (e.g., DLN-2025-001)")
    status: LoanStatus = Field(..., description="Current status of the loan")
    driver_id: Optional[str] = Field(None, description="Driver's system ID")
    driver_name: Optional[str] = Field(None, description="Driver's full name")
    tlc_license: Optional[str] = Field(None, description="Driver's TLC license number")
    medallion_no: Optional[str] = Field(None, description="Medallion number")
    medallion_owner: Optional[str] = Field(None, description="Medallion owner name")
    lease_type: Optional[str] = Field(None, description="Type of lease")
    principal_amount: Decimal = Field(..., description="Principal loan amount")
    interest_rate: Decimal = Field(..., description="Annual interest rate (%)")
    start_week: date = Field(..., description="First payment week (Sunday)")
    receipt_url: Optional[str] = Field(None, description="Presigned URL for loan receipt PDF")  # NEW


class PaginatedDriverLoanResponse(BaseModel):
    """Paginated response for driver loans list."""
    items: List[DriverLoanListResponse]
    total_items: int
    page: int
    per_page: int
    total_pages: int
    lease_types: List[str] = Field(..., description="Available lease types for filtering")


class LoanInstallmentResponse(BaseModel):
    """Response model for a single loan installment."""
    model_config = ConfigDict(from_attributes=True)
    
    installment_id: str = Field(..., description="Unique installment ID")
    week_start_date: date = Field(..., description="Start of payment week (Sunday)")
    week_end_date: date = Field(..., description="End of payment week (Saturday)")
    principal_amount: Decimal = Field(..., description="Principal portion of this installment")
    interest_amount: Decimal = Field(..., description="Interest portion of this installment")
    total_due: Decimal = Field(..., description="Total amount due for this installment")
    status: LoanInstallmentStatus = Field(..., description="Current status of this installment")
    ledger_posting_id: Optional[int] = Field(None, description="ID of the corresponding ledger posting")


class DriverLoanDetailResponse(BaseModel):
    """Response model for detailed view of a single loan."""
    model_config = ConfigDict(from_attributes=True)
    
    loan_id: str = Field(..., description="System-generated loan ID")
    status: LoanStatus = Field(..., description="Current status of the loan")
    driver_id: Optional[str] = Field(None, description="Driver's system ID")
    driver_name: Optional[str] = Field(None, description="Driver's full name")
    tlc_license: Optional[str] = Field(None, description="Driver's TLC license number")
    medallion_no: Optional[str] = Field(None, description="Medallion number")
    medallion_owner: Optional[str] = Field(None, description="Medallion owner name")
    lease_type: Optional[str] = Field(None, description="Type of lease")
    principal_amount: Decimal = Field(..., description="Principal loan amount")
    interest_rate: Decimal = Field(..., description="Annual interest rate (%)")
    loan_date: date = Field(..., description="Date the loan was disbursed")
    start_week: date = Field(..., description="First payment week (Sunday)")
    notes: Optional[str] = Field(None, description="Additional notes about the loan")
    installments: List[LoanInstallmentResponse] = Field(..., description="Full repayment schedule")
    receipt_url: Optional[str] = Field(None, description="Presigned URL for loan receipt PDF")  # NEW


class LoanInstallmentListResponse(BaseModel):
    """
    Response schema for a single loan installment in a list view.
    """
    installment_id: str
    loan_id: str
    driver_name: Optional[str] = None
    medallion_no: Optional[str] = None
    lease_id: Optional[str] = None
    vehicle_id: Optional[int] = None
    week_start_date: date
    week_end_date: date
    principal: Decimal = Field(..., alias="principal_amount")
    interest: Decimal = Field(..., alias="interest_amount")
    total_due: Decimal
    status: LoanInstallmentStatus
    posted_date: Optional[date] = Field(None, alias="posted_on")
    ledger_posting_ref: Optional[str] = None

    class Config:
        from_attributes = True
        populate_by_name = True


class PaginatedLoanInstallmentResponse(BaseModel):
    """
    Paginated response schema for a list of Loan Installments.
    """
    items: List[LoanInstallmentListResponse]
    total_items: int
    page: int
    per_page: int
    total_pages: int


class PostInstallmentRequest(BaseModel):
    """
    Request schema for posting loan installments to ledger.
    Either provide specific installment_ids or post_all_due flag.
    """
    installment_ids: Optional[List[str]]
    post_all_due: bool = False

    class Config:
        """Pydantic configuration"""
        json_schema_extra = {
            "examples": [
                {
                    "installment_ids": ["DLN-2025-001-01", "DLN-2025-001-02"]
                },
                {
                    "post_all_due": True
                }
            ]
        }


class InstallmentPostingResult(BaseModel):
    """Result of posting a single installment to the ledger."""
    installment_id: str
    success: bool
    ledger_posting_id: Optional[int] = None
    message: Optional[str] = None



class PostInstallmentResponse(BaseModel):
    """Response schema for posting loan installments to ledger."""
    total_processed: int
    successful_posts: int
    failed_posts: int
    results: List[InstallmentPostingResult]
    message: str

    class Config:
        """Pydantic configuration"""
        json_schema_extra = {
            "example": {
                "total_processed": 5,
                "successful_posts": 4,
                "failed_posts": 1,
                "results": [
                    {
                        "installment_id": "DLN-2025-001-01",
                        "success": True,
                        "ledger_posting_id": "abc-123-def",
                        "posted_on": "2025-11-18T10:30:00z"
                    },
                    {
                        "installment_id": "DLN-2025-001-02",
                        "success": False,
                        "error_message": "Insufficient funds in driver's account."
                    }
                ],
                "message": "Successfully posted 4 out of 5 installments."
            }
        }