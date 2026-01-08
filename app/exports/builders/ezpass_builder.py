### app/exports/builders/ezpass_builder.py

"""
EZPass Export Query Builder

Replicates EXACT filter logic from app/ezpass/repository.py (get_paginated_transactions)
WITHOUT pagination - for use in streaming exports.

IMPORTANT: This preserves all filter logic, joins, and index usage from the repository.
"""

from datetime import datetime, date, time
from decimal import Decimal
from typing import Dict, Optional

from sqlalchemy import and_, or_, func
from sqlalchemy.orm import Session, joinedload, Query

from app.ezpass.models import EZPassTransaction, EZPassTransactionStatus
from app.vehicles.models import Vehicle, VehicleRegistration
from app.drivers.models import Driver
from app.medallions.models import Medallion
from app.utils.general import apply_multi_filter
from app.utils.logger import get_logger

logger = get_logger(__name__)


def build_ezpass_export_query(db: Session, filters: Dict) -> Query:
    """
    Build EZPass export query with ALL filters from repository.
    
    This function replicates the exact filter logic from
    app/ezpass/repository.py::get_paginated_transactions()
    but WITHOUT pagination (no .limit() or .offset()).
    
    Args:
        db: Database session
        filters: Dictionary of filter parameters (same as router params)
        
    Returns:
        SQLAlchemy Query object ready for streaming
    """
    logger.info(f"Building EZPass export query with {len(filters)} filters")
    
    # Extract filter parameters
    sort_by = filters.get("sort_by", "transaction_datetime")
    sort_order = filters.get("sort_order", "desc")
    
    # Date range filters
    from_posting_date = filters.get("from_posting_date")
    to_posting_date = filters.get("to_posting_date")
    from_transaction_date = filters.get("from_transaction_date")
    to_transaction_date = filters.get("to_transaction_date")
    from_transaction_time = filters.get("from_transaction_time")
    to_transaction_time = filters.get("to_transaction_time")
    
    # Multi-value filters
    plate_number = filters.get("plate_number")
    transaction_id = filters.get("transaction_id")
    entry_lane = filters.get("entry_lane")
    exit_lane = filters.get("exit_lane")
    entry_plaza = filters.get("entry_plaza")
    exit_plaza = filters.get("exit_plaza")
    vin = filters.get("vin")
    medallion_no = filters.get("medallion_no")
    driver_id = filters.get("driver_id")
    driver_name = filters.get("driver_name")
    lease_id = filters.get("lease_id")
    status = filters.get("status")
    
    # Amount range filters
    from_amount = filters.get("from_amount")
    to_amount = filters.get("to_amount")
    from_ledger_balance = filters.get("from_ledger_balance")
    to_ledger_balance = filters.get("to_ledger_balance")
    
    # Other filters
    agency = filters.get("agency")
    ezpass_class = filters.get("ezpass_class")
    
    # ==================================================================
    # STEP 1: Build base query with strategic eager loading
    # ==================================================================
    query = db.query(EZPassTransaction)
    
    # Track which joins we need
    needs_vehicle_join = vin is not None or plate_number is not None
    needs_driver_join = driver_id is not None or driver_name is not None
    needs_medallion_join = medallion_no is not None
    needs_lease_join = lease_id is not None
    
    # Always eager load these relationships for display
    query = query.options(
        joinedload(EZPassTransaction.driver),
        joinedload(EZPassTransaction.vehicle),
        joinedload(EZPassTransaction.medallion),
        joinedload(EZPassTransaction.lease),
    )
    
    # Conditionally join for filtering
    if needs_vehicle_join:
        query = query.outerjoin(Vehicle, EZPassTransaction.vehicle_id == Vehicle.id)
        if plate_number:
            query = query.outerjoin(
                VehicleRegistration,
                and_(
                    Vehicle.id == VehicleRegistration.vehicle_id,
                    VehicleRegistration.is_active == True
                )
            )
    
    if needs_driver_join:
        query = query.outerjoin(Driver, EZPassTransaction.driver_id == Driver.id)
    
    if needs_medallion_join:
        query = query.outerjoin(Medallion, EZPassTransaction.medallion_id == Medallion.id)
    
    if needs_lease_join:
        from app.leases.models import Lease
        query = query.outerjoin(Lease, EZPassTransaction.lease_id == Lease.id)
    
    # ==================================================================
    # STEP 2: Apply filters using indexed columns where possible
    # ==================================================================
    
    # 1. Posting date range filter - uses idx_ezpass_posting_date
    if from_posting_date:
        if isinstance(from_posting_date, str):
            from_posting_date = datetime.fromisoformat(from_posting_date).date()
        from_posting_datetime = datetime.combine(from_posting_date, datetime.min.time())
        query = query.filter(EZPassTransaction.posting_date >= from_posting_datetime)
    
    if to_posting_date:
        if isinstance(to_posting_date, str):
            to_posting_date = datetime.fromisoformat(to_posting_date).date()
        to_posting_datetime = datetime.combine(to_posting_date, datetime.max.time())
        query = query.filter(EZPassTransaction.posting_date <= to_posting_datetime)
    
    # 2. Transaction date range filter - uses idx_ezpass_transaction_datetime
    if from_transaction_date:
        if isinstance(from_transaction_date, str):
            from_transaction_date = datetime.fromisoformat(from_transaction_date).date()
        from_transaction_datetime = datetime.combine(from_transaction_date, datetime.min.time())
        query = query.filter(EZPassTransaction.transaction_datetime >= from_transaction_datetime)
    
    if to_transaction_date:
        if isinstance(to_transaction_date, str):
            to_transaction_date = datetime.fromisoformat(to_transaction_date).date()
        to_transaction_datetime = datetime.combine(to_transaction_date, datetime.max.time())
        query = query.filter(EZPassTransaction.transaction_datetime <= to_transaction_datetime)
    
    # 3. Transaction time range filter
    if from_transaction_time:
        if isinstance(from_transaction_time, str):
            from_transaction_time = datetime.strptime(from_transaction_time, "%H:%M:%S").time()
        if from_transaction_date:
            from_datetime_with_time = datetime.combine(from_transaction_date, from_transaction_time)
            query = query.filter(EZPassTransaction.transaction_datetime >= from_datetime_with_time)
        else:
            query = query.filter(func.time(EZPassTransaction.transaction_datetime) >= from_transaction_time)
    
    if to_transaction_time:
        if isinstance(to_transaction_time, str):
            to_transaction_time = datetime.strptime(to_transaction_time, "%H:%M:%S").time()
        if to_transaction_date:
            to_datetime_with_time = datetime.combine(to_transaction_date, to_transaction_time)
            query = query.filter(EZPassTransaction.transaction_datetime <= to_datetime_with_time)
        else:
            query = query.filter(func.time(EZPassTransaction.transaction_datetime) <= to_transaction_time)
    
    # 4. Plate number filter (comma-separated)
    if plate_number:
        query = apply_multi_filter(query, EZPassTransaction.tag_or_plate, plate_number)
    
    # 5. Transaction ID filter (comma-separated)
    if transaction_id:
        query = apply_multi_filter(query, EZPassTransaction.transaction_id, transaction_id)
    
    # 6. Entry lane filter (comma-separated) - partial match support
    if entry_lane:
        entry_lanes = [lane.strip() for lane in entry_lane.split(',') if lane.strip()]
        if entry_lanes:
            query = query.filter(
                or_(*[
                    EZPassTransaction.entry_plaza.ilike(f"%{lane}%") 
                    for lane in entry_lanes
                ])
            )
    
    # 7. Exit lane filter (comma-separated) - partial match support
    if exit_lane:
        exit_lanes = [lane.strip() for lane in exit_lane.split(',') if lane.strip()]
        if exit_lanes:
            query = query.filter(
                or_(*[
                    EZPassTransaction.exit_plaza.ilike(f"%{lane}%") 
                    for lane in exit_lanes
                ])
            )
    
    # 8. Entry plaza filter (comma-separated)
    if entry_plaza:
        query = apply_multi_filter(query, EZPassTransaction.entry_plaza, entry_plaza)
    
    # 9. Exit plaza filter (comma-separated)
    if exit_plaza:
        query = apply_multi_filter(query, EZPassTransaction.exit_plaza, exit_plaza)
    
    # 10. Amount range filter
    if from_amount is not None:
        if isinstance(from_amount, str):
            from_amount = Decimal(from_amount)
        query = query.filter(EZPassTransaction.amount >= from_amount)
    
    if to_amount is not None:
        if isinstance(to_amount, str):
            to_amount = Decimal(to_amount)
        query = query.filter(EZPassTransaction.amount <= to_amount)
    
    # 11. VIN filter (comma-separated)
    if vin:
        query = apply_multi_filter(query, Vehicle.vin, vin)
    
    # 12. Medallion number filter (comma-separated)
    if medallion_no:
        query = apply_multi_filter(query, Medallion.medallion_number, medallion_no)
    
    # 13. Driver ID filter (comma-separated)
    if driver_id:
        query = apply_multi_filter(query, Driver.driver_id, driver_id)
    
    # 14. Driver name filter (comma-separated)
    if driver_name:
        query = apply_multi_filter(query, Driver.full_name, driver_name)
    
    # 15. Lease ID filter (comma-separated)
    if lease_id:
        from app.leases.models import Lease
        query = apply_multi_filter(query, Lease.lease_id, lease_id)
    
    # 16. Ledger balance range filter (placeholder - not yet implemented in repository)
    # Skipping as repository doesn't implement this yet
    
    # 17. Status filter (comma-separated)
    if status:
        statuses = [s.strip() for s in status.split(',') if s.strip()]
        if statuses:
            valid_statuses = []
            for s in statuses:
                try:
                    valid_statuses.append(EZPassTransactionStatus[s.upper()])
                except KeyError:
                    logger.warning(f"Invalid status: {s}")
            
            if valid_statuses:
                query = query.filter(EZPassTransaction.status.in_(valid_statuses))
    
    # 18. Agency filter
    if agency:
        query = query.filter(EZPassTransaction.agency.ilike(f"%{agency}%"))
    
    # 19. EZPass class filter
    if ezpass_class:
        query = query.filter(EZPassTransaction.ezpass_class.ilike(f"%{ezpass_class}%"))
    
    # ==================================================================
    # STEP 3: Apply sorting (NO PAGINATION)
    # ==================================================================
    sort_column_map = {
        "transaction_date": EZPassTransaction.transaction_datetime,
        "transaction_datetime": EZPassTransaction.transaction_datetime,
        "transaction_id": EZPassTransaction.transaction_id,
        "plate_number": EZPassTransaction.tag_or_plate,
        "posting_date": EZPassTransaction.posting_date,
        "amount": EZPassTransaction.amount,
        "status": EZPassTransaction.status,
        "entry_plaza": EZPassTransaction.entry_plaza,
        "exit_plaza": EZPassTransaction.exit_plaza,
        "agency": EZPassTransaction.agency,
        "driver_name": Driver.full_name,
        "lease_id": EZPassTransaction.lease_id,
    }
    
    sort_column = sort_column_map.get(sort_by, EZPassTransaction.transaction_datetime)
    
    if sort_order.lower() == "asc":
        query = query.order_by(sort_column.asc())
    else:
        query = query.order_by(sort_column.desc())
    
    # Add secondary sort for consistency
    query = query.order_by(EZPassTransaction.id.asc())
    
    logger.info("EZPass export query built successfully")
    return query


def transform_ezpass_row(transaction: EZPassTransaction) -> Dict:
    """
    Transform EZPass transaction ORM object to dictionary for export.
    
    This matches the export column structure from the existing export endpoint.
    """
    # Get medallion number with fallback logic
    medallion_number = ""
    if transaction.medallion:
        medallion_number = transaction.medallion.medallion_number
    elif transaction.vehicle and hasattr(transaction.vehicle, 'medallions') and transaction.vehicle.medallions:
        medallion_number = transaction.vehicle.medallions.medallion_number
    
    # Get active plate number
    plate_number_display = transaction.tag_or_plate
    
    return {
        "Transaction ID": transaction.transaction_id,
        "Transaction Date": transaction.transaction_datetime.strftime("%Y-%m-%d") if transaction.transaction_datetime else "",
        "Transaction Time": transaction.transaction_datetime.strftime("%H:%M:%S") if transaction.transaction_datetime else "",
        "Plate Number": plate_number_display,
        "Entry Plaza": transaction.entry_plaza or "",
        "Exit Plaza": transaction.exit_plaza or "",
        "Entry Lane": transaction.entry_plaza or "",  # Note: Model doesn't have separate lane field
        "Exit Lane": transaction.exit_plaza or "",    # Using plaza as proxy
        "Amount": float(transaction.amount) if transaction.amount else 0.0,
        "Posting Date": transaction.posting_date.strftime("%Y-%m-%d") if transaction.posting_date else "",
        "Status": transaction.status.value if transaction.status else "",
        "Agency": transaction.agency or "",
        "EZPass Class": transaction.ezpass_class or "",
        "Driver ID": transaction.driver.driver_id if transaction.driver else "",
        "Driver Name": transaction.driver.full_name if transaction.driver else "",
        "Lease ID": transaction.lease.lease_id if transaction.lease else "",
        "Vehicle VIN": transaction.vehicle.vin if transaction.vehicle else "",
        "Medallion No": medallion_number,
    }
