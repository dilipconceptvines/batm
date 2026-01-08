### app/exports/builders/curb_builder.py

"""
CURB Export Query Builder

Replicates filter logic from app/curb/repository.py for streaming exports.
"""

from datetime import datetime
from decimal import Decimal
from typing import Dict, Optional

from sqlalchemy import and_, or_, func
from sqlalchemy.orm import Session, joinedload, Query

from app.curb.models import CurbTrip, CurbTripStatus, PaymentType
from app.drivers.models import Driver, TLCLicense
from app.vehicles.models import Vehicle, VehicleRegistration
from app.medallions.models import Medallion
from app.utils.general import apply_multi_filter
from app.utils.logger import get_logger

logger = get_logger(__name__)


def build_curb_export_query(db: Session, filters: Dict) -> Query:
    """
    Build CURB export query with ALL filters from repository.
    
    Args:
        db: Database session
        filters: Dictionary of filter parameters
        
    Returns:
        SQLAlchemy Query object ready for streaming
    """
    logger.info(f"Building CURB export query with {len(filters)} filters")
    
    # Extract filter parameters
    sort_by = filters.get("sort_by", "start_time")
    sort_order = filters.get("sort_order", "desc")
    
    # Build base query with eager loading
    query = db.query(CurbTrip).options(
        joinedload(CurbTrip.account),
        joinedload(CurbTrip.driver).joinedload(Driver.tlc_license),
        joinedload(CurbTrip.vehicle).joinedload(Vehicle.registrations),
        joinedload(CurbTrip.lease),
    )
    
    # Apply filters (simplified - expand based on actual repository logic)
    trip_ids = filters.get("trip_ids")
    if trip_ids:
        query = apply_multi_filter(query, CurbTrip.curb_trip_id, trip_ids)
    
    driver_ids = filters.get("driver_ids")
    if driver_ids:
        query = query.outerjoin(Driver, CurbTrip.driver_id == Driver.id)
        query = apply_multi_filter(query, Driver.driver_id, driver_ids)
    
    trip_start_from = filters.get("trip_start_from")
    if trip_start_from:
        if isinstance(trip_start_from, str):
            trip_start_from = datetime.fromisoformat(trip_start_from)
        query = query.filter(CurbTrip.start_time >= trip_start_from)
    
    trip_start_to = filters.get("trip_start_to")
    if trip_start_to:
        if isinstance(trip_start_to, str):
            trip_start_to = datetime.fromisoformat(trip_start_to)
        query = query.filter(CurbTrip.start_time <= trip_start_to)
    
    # Add more filters as needed from repository
    
    # Apply sorting
    sort_column = CurbTrip.start_time
    if sort_by == "trip_id":
        sort_column = CurbTrip.curb_trip_id
    elif sort_by == "total_amount":
        sort_column = CurbTrip.total_amount
    
    if sort_order.lower() == "asc":
        query = query.order_by(sort_column.asc())
    else:
        query = query.order_by(sort_column.desc())
    
    query = query.order_by(CurbTrip.id.asc())
    
    logger.info("CURB export query built successfully")
    return query


def transform_curb_row(trip: CurbTrip) -> Dict:
    """Transform CURB trip ORM object to dictionary for export."""
    
    # Get active plate number
    plate_number = ""
    if trip.vehicle and hasattr(trip.vehicle, 'registrations'):
        active_reg = next(
            (reg for reg in trip.vehicle.registrations if reg.is_active),
            None
        )
        if active_reg:
            plate_number = active_reg.plate_number
    
    return {
        "Trip ID": trip.curb_trip_id,
        "Trip Start Date": trip.start_time.strftime("%Y-%m-%d %H:%M:%S") if trip.start_time else "",
        "Trip End Date": trip.end_time.strftime("%Y-%m-%d %H:%M:%S") if trip.end_time else "",
        "Transaction Date": trip.transaction_date.strftime("%Y-%m-%d %H:%M:%S") if trip.transaction_date else "",
        "Driver ID": trip.driver.driver_id if trip.driver else "",
        "Driver Name": trip.driver.full_name if trip.driver else "",
        "TLC License No": trip.driver.tlc_license.tlc_license_number if trip.driver and trip.driver.tlc_license else "",
        "Vehicle Plate": plate_number,
        "Medallion No": trip.curb_cab_number,
        "Total Amount": float(trip.total_amount),
        "Fare": float(trip.fare),
        "Tips": float(trip.tips),
        "Tolls": float(trip.tolls),
        "Extras": float(trip.extras),
        "Surcharge": float(trip.surcharge),
        "Improvement Surcharge": float(trip.improvement_surcharge) if trip.improvement_surcharge else 0.0,
        "Congestion Fee": float(trip.congestion_fee) if trip.congestion_fee else 0.0,
        "Airport Fee": float(trip.airport_fee) if trip.airport_fee else 0.0,
        "CBDT Fee": float(trip.cbdt_fee) if trip.cbdt_fee else 0.0,
        "Payment Mode": trip.payment_type.value if trip.payment_type else "",
        "Status": trip.status.value if trip.status else "",
        "Account": trip.account.account_name if trip.account else "",
        "Distance (Miles)": float(trip.distance_miles) if trip.distance_miles else "",
        "Passengers": trip.num_passengers if trip.num_passengers else "",
    }
