### app/exports/builders/ledger_builder.py

"""
Ledger Export Query Builders

Handles both LedgerPosting and LedgerBalance exports.
"""

from datetime import datetime, date
from decimal import Decimal
from typing import Dict

from sqlalchemy import and_, or_
from sqlalchemy.orm import Session, joinedload, Query

from app.ledger.models import LedgerPosting, LedgerBalance, PostingCategory, EntryType, PostingStatus
from app.drivers.models import Driver
from app.leases.models import Lease
from app.vehicles.models import Vehicle
from app.medallions.models import Medallion
from app.utils.logger import get_logger

logger = get_logger(__name__)


def build_ledger_postings_export_query(db: Session, filters: Dict) -> Query:
    """
    Build Ledger Postings export query.
    
    Args:
        db: Database session
        filters: Dictionary of filter parameters
        
    Returns:
        SQLAlchemy Query object ready for streaming
    """
    logger.info(f"Building Ledger Postings export query with {len(filters)} filters")
    
    # Build base query with eager loading
    query = db.query(LedgerPosting).options(
        joinedload(LedgerPosting.driver),
        joinedload(LedgerPosting.lease),
    )
    
    # Apply filters
    start_date = filters.get("start_date")
    if start_date:
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date).date()
        start_datetime = datetime.combine(start_date, datetime.min.time())
        query = query.filter(LedgerPosting.posting_date >= start_datetime)
    
    end_date = filters.get("end_date")
    if end_date:
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date).date()
        end_datetime = datetime.combine(end_date, datetime.max.time())
        query = query.filter(LedgerPosting.posting_date <= end_datetime)
    
    category = filters.get("category")
    if category:
        query = query.filter(LedgerPosting.posting_category == category)
    
    entry_type = filters.get("entry_type")
    if entry_type:
        query = query.filter(LedgerPosting.entry_type == entry_type)
    
    status = filters.get("status")
    if status:
        query = query.filter(LedgerPosting.status == status)
    
    # Apply sorting
    sort_by = filters.get("sort_by", "posting_date")
    sort_order = filters.get("sort_order", "desc")
    
    sort_column = LedgerPosting.posting_date
    
    if sort_order.lower() == "asc":
        query = query.order_by(sort_column.asc())
    else:
        query = query.order_by(sort_column.desc())
    
    query = query.order_by(LedgerPosting.id.asc())
    
    logger.info("Ledger Postings export query built successfully")
    return query


def build_ledger_balances_export_query(db: Session, filters: Dict) -> Query:
    """
    Build Ledger Balances export query.
    
    Args:
        db: Database session
        filters: Dictionary of filter parameters
        
    Returns:
        SQLAlchemy Query object ready for streaming
    """
    logger.info(f"Building Ledger Balances export query with {len(filters)} filters")
    
    # Build base query with eager loading
    query = db.query(LedgerBalance).options(
        joinedload(LedgerBalance.driver),
        joinedload(LedgerBalance.lease),
    )
    
    # Apply filters
    category = filters.get("category")
    if category:
        query = query.filter(LedgerBalance.posting_category == category)
    
    # Apply sorting
    sort_by = filters.get("sort_by", "updated_at")
    sort_order = filters.get("sort_order", "desc")
    
    sort_column = LedgerBalance.updated_at
    
    if sort_order.lower() == "asc":
        query = query.order_by(sort_column.asc())
    else:
        query = query.order_by(sort_column.desc())
    
    query = query.order_by(LedgerBalance.id.asc())
    
    logger.info("Ledger Balances export query built successfully")
    return query


def transform_ledger_posting_row(posting: LedgerPosting) -> Dict:
    """Transform Ledger Posting ORM object to dictionary for export."""
    
    return {
        "Posting ID": posting.id,
        "Posting Date": posting.posting_date.strftime("%Y-%m-%d %H:%M:%S") if posting.posting_date else "",
        "Category": posting.posting_category.value if posting.posting_category else "",
        "Entry Type": posting.entry_type.value if posting.entry_type else "",
        "Amount": float(posting.amount) if posting.amount else 0.0,
        "Reference ID": posting.reference_id or "",
        "Description": posting.description or "",
        "Status": posting.status.value if posting.status else "",
        "Driver ID": posting.driver.driver_id if posting.driver else "",
        "Driver Name": posting.driver.full_name if posting.driver else "",
        "Lease ID": posting.lease.lease_id if posting.lease else "",
    }


def transform_ledger_balance_row(balance: LedgerBalance) -> Dict:
    """Transform Ledger Balance ORM object to dictionary for export."""
    
    return {
        "Balance ID": balance.id,
        "Category": balance.posting_category.value if balance.posting_category else "",
        "Reference ID": balance.reference_id or "",
        "Balance": float(balance.balance) if balance.balance else 0.0,
        "Driver ID": balance.driver.driver_id if balance.driver else "",
        "Driver Name": balance.driver.full_name if balance.driver else "",
        "Lease ID": balance.lease.lease_id if balance.lease else "",
        "Updated At": balance.updated_at.strftime("%Y-%m-%d %H:%M:%S") if balance.updated_at else "",
    }
