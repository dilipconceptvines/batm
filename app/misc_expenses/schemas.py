### app/misc_expenses/schemas.py

from datetime import date
from decimal import Decimal
from typing import List, Optional, Dict, Any

from pydantic import BaseModel, Field


class MiscellaneousExpenseCreate(BaseModel):
    """
    Schema for the data submitted during the BPM flow for creating
    a manual miscellaneous expense.
    """
    # From Step 1: Driver & Lease Selection
    driver_id: int
    lease_id: int
    vehicle_id: int
    medallion_id: int

    # From Step 2: Expense Information
    category: str = Field(..., min_length=1, description="The category of the expense (e.g., Lost Key, Cleaning Fee).")
    amount: Decimal = Field(..., gt=0, description="The total amount of the charge.")
    expense_date: date
    reference_number: Optional[str] = None
    notes: Optional[str] = None


class MiscellaneousExpenseResponse(BaseModel):
    """
    Response schema for a single miscellaneous expense in a list view, matching the UI grid.
    """
    expense_id: str
    reference_no: Optional[str] = Field(None, alias="reference_number")
    category: str
    expense_date: date = Field(..., alias="expense_date")
    amount: Decimal
    notes: Optional[str] = None
    driver_name: Optional[str] = None
    lease_id: Optional[str] = None
    vin_no: Optional[str] = None
    vehicle_name: Optional[str] = None # e.g., "Toyota RAV4 2021"
    plate_no: Optional[str] = None
    medallion_no: Optional[str] = None
    documents: List[dict] = Field(default_factory=list, description="Associated documents with presigned URLs")

    class Config:
        from_attributes = True
        populate_by_name = True


class PaginatedMiscellaneousExpenseResponse(BaseModel):
    """
    Paginated response schema for a list of Miscellaneous Expenses.
    Includes available categories for filter dropdown population.
    """
    items: List[MiscellaneousExpenseResponse]
    total_items: int
    page: int
    per_page: int
    total_pages: int
    available_categories: List[str] = Field(
        default_factory=list,
        description="List of distinct expense categories available in the system"
    )