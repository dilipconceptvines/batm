### app/exports/builders/pvb_builder.py

"""
PVB Export Query Builder

Replicates filter logic from app/pvb/ for streaming exports.
"""

from datetime import datetime, date, time
from decimal import Decimal
from typing import Dict

from sqlalchemy import and_, or_, func
from sqlalchemy.orm import Session, joinedload, Query

from app.pvb.models import PVBViolation
from app.drivers.models import Driver
from app.vehicles.models import Vehicle
from app.medallions.models import Medallion
from app.utils.general import apply_multi_filter
from app.utils.logger import get_logger

logger = get_logger(__name__)


def build_pvb_export_query(db: Session, filters: Dict) -> Query:
    """
    Build PVB export query with filters.
    
    Args:
        db: Database session
        filters: Dictionary of filter parameters
        
    Returns:
        SQLAlchemy Query object ready for streaming
    """
    logger.info(f"Building PVB export query with {len(filters)} filters")
    
    # Build base query with eager loading
    query = db.query(PVBViolation).options(
        joinedload(PVBViolation.driver),
        joinedload(PVBViolation.vehicle),
        joinedload(PVBViolation.medallion),
        joinedload(PVBViolation.lease),
    )
    
    # Apply basic filters (expand based on actual PVB repository)
    plate = filters.get("plate")
    if plate:
        query = query.filter(PVBViolation.plate.ilike(f"%{plate}%"))
    
    from_issue_date = filters.get("from_issue_date")
    if from_issue_date:
        if isinstance(from_issue_date, str):
            from_issue_date = datetime.fromisoformat(from_issue_date).date()
        query = query.filter(PVBViolation.issue_date >= from_issue_date)
    
    to_issue_date = filters.get("to_issue_date")
    if to_issue_date:
        if isinstance(to_issue_date, str):
            to_issue_date = datetime.fromisoformat(to_issue_date).date()
        query = query.filter(PVBViolation.issue_date <= to_issue_date)
    
    status = filters.get("status")
    if status:
        query = query.filter(PVBViolation.status == status)
    
    # Apply sorting
    sort_by = filters.get("sort_by", "issue_date")
    sort_order = filters.get("sort_order", "desc")
    
    sort_column = PVBViolation.issue_date
    
    if sort_order.lower() == "asc":
        query = query.order_by(sort_column.asc())
    else:
        query = query.order_by(sort_column.desc())
    
    query = query.order_by(PVBViolation.id.asc())
    
    logger.info("PVB export query built successfully")
    return query


def transform_pvb_row(violation: PVBViolation) -> Dict:
    """Transform PVB violation ORM object to dictionary for export."""
    
    return {
        "Summons": violation.summons or "",
        "Plate": violation.plate or "",
        "State": violation.state or "",
        "Type": violation.type or "",
        "Issue Date": violation.issue_date.strftime("%Y-%m-%d") if violation.issue_date else "",
        "Issue Time": violation.issue_time.strftime("%H:%M:%S") if violation.issue_time else "",
        "Posting Date": violation.posting_date.strftime("%Y-%m-%d") if violation.posting_date else "",
        "Amount Due": float(violation.amount_due) if violation.amount_due else 0.0,
        "Fine": float(violation.fine) if violation.fine else 0.0,
        "Penalty": float(violation.penalty) if violation.penalty else 0.0,
        "Interest": float(violation.interest) if violation.interest else 0.0,
        "Reduction": float(violation.reduction) if violation.reduction else 0.0,
        "Processing Fee": float(violation.processing_fee) if violation.processing_fee else 0.0,
        "Status": violation.status or "",
        "Source": violation.source or "",
        "Driver ID": violation.driver.driver_id if violation.driver else "",
        "Driver Name": violation.driver.full_name if violation.driver else "",
        "Vehicle VIN": violation.vehicle.vin if violation.vehicle else "",
        "Medallion No": violation.medallion.medallion_number if violation.medallion else "",
        "Lease ID": violation.lease.lease_id if violation.lease else "",
        "Violation Code": violation.violation_code or "",
        "Street Name": violation.street_name or "",
        "Disposition": violation.disposition or "",
        "Failure Reason": violation.failure_reason or "",
    }
