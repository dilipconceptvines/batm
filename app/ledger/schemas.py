# app/ledger/schemas.py

from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field

from app.ledger.models import BalanceStatus, EntryType, PostingCategory, PostingStatus


# --- Base Schemas ---
class LedgerPostingBase(BaseModel):
    """Base response schema for a Ledger Posting."""

    id: str = Field(..., alias="posting_id")
    status: PostingStatus
    created_on: datetime = Field(..., alias="date")
    category: PostingCategory
    entry_type: EntryType = Field(..., alias="type")
    amount: Decimal
    reference_id: str

    class Config:
        from_attributes = True
        populate_by_name = True


class LedgerBalanceBase(BaseModel):
    """Base response schema for a Ledger Balance."""

    id: str = Field(..., alias="balance_id")
    category: PostingCategory
    status: BalanceStatus
    reference_id: str
    original_amount: Decimal
    prior_balance: Decimal
    balance: Decimal

    class Config:
        from_attributes = True
        populate_by_name = True


# --- Detailed Response Schemas (for API responses) ---
class LedgerPostingResponse(LedgerPostingBase):
    """Detailed response for a single Ledger Posting, matching UI."""

    driver_name: Optional[str] = None
    lease_id: Optional[int] = None
    vehicle_vin: Optional[str] = None
    medallion_no: Optional[str] = None


class LedgerBalanceResponse(LedgerBalanceBase):
    """Detailed response for a single Ledger Balance, matching UI."""

    driver_name: Optional[str] = None
    lease_id: Optional[int] = None
    vehicle_vin: Optional[str] = None


# --- Paginated List Response Schemas ---
class PaginatedLedgerPostingResponse(BaseModel):
    """Paginated response for a list of Ledger Postings."""

    items: List[LedgerPostingResponse]
    total_items: int
    page: int
    per_page: int
    total_pages: int


class PaginatedLedgerBalanceResponse(BaseModel):
    """Paginated response for a list of Ledger Balances."""

    items: List[LedgerBalanceResponse]
    total_items: int
    page: int
    per_page: int
    total_pages: int


# --- Request Body Schemas ---
class VoidPostingRequest(BaseModel):
    """Request body for voiding a ledger posting."""

    reason: str = Field(
        ..., min_length=10, description="A mandatory reason for voiding the transaction."
    )