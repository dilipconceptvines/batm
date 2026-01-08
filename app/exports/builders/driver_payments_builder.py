### app/exports/builders/driver_payments_builder.py

"""
Query builder for Driver Payments exports.

Replicates filter logic from app/driver_payments/unified_service.py
to ensure consistency with the Driver Payments view.
"""

from datetime import date, datetime
from typing import Dict, Any, List
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import or_, and_, func

from app.dtr.models import DTR, DTRStatus as DTRStatusModel, PaymentMethod
from app.drivers.models import Driver
from app.medallions.models import Medallion
from app.vehicles.models import Vehicle, VehicleRegistration
from app.leases.models import Lease
from app.driver_payments.models import ACHBatch
from app.utils.logger import get_logger

logger = get_logger(__name__)


def build_driver_payments_export_query(db: Session, filters: Dict[str, Any]):
    """
    Build query for Driver Payments (DTR) export.
    
    Replicates filter logic from UnifiedDriverPaymentsService.get_unified_payments
    Currently focuses on DTR records as the primary payment type.
    
    Args:
        db: Database session
        filters: Dictionary containing filter parameters
            - receipt_number: Receipt number filter
            - status: Status filter
            - payment_method: Payment method filter
            - week_start_date_from: Week start date from
            - week_start_date_to: Week start date to
            - week_end_date_from: Week end date from
            - week_end_date_to: Week end date to
            - ach_batch_number: ACH batch number filter
            - total_due_min: Minimum total amount
            - total_due_max: Maximum total amount
            - receipt_type: Receipt type filter (DTR, Interim Payment, etc.)
            - medallion_number: Medallion number (comma-separated)
            - tlc_license: TLC license (comma-separated)
            - driver_name: Driver name (comma-separated)
            - plate_number: Plate number (comma-separated)
            - check_number: Check number filter
    
    Returns:
        SQLAlchemy query object for DTR records
    """
    logger.info(f"Building Driver Payments export query with {len(filters)} filters")
    
    # Base query for DTRs with eager loading
    query = (
        db.query(DTR)
        .options(
            joinedload(DTR.driver),
            joinedload(DTR.medallion),
            joinedload(DTR.vehicle).joinedload(Vehicle.registrations),
            joinedload(DTR.lease),
            joinedload(DTR.ach_batch)
        )
        .outerjoin(Driver)
        .outerjoin(Medallion)
        .outerjoin(Vehicle)
        .outerjoin(Lease)
        .outerjoin(ACHBatch)
    )
    
    # Apply filters
    
    # Receipt number (DTR number)
    if filters.get('receipt_number'):
        receipt_num = filters['receipt_number'].strip()
        query = query.filter(DTR.dtr_number.ilike(f"%{receipt_num}%"))
    
    # Status filter
    if filters.get('status'):
        status_value = filters['status'].strip().upper()
        try:
            status_enum = DTRStatusModel[status_value]
            query = query.filter(DTR.status == status_enum)
        except KeyError:
            logger.warning(f"Invalid status filter: {status_value}")
    
    # Payment method filter
    if filters.get('payment_method'):
        method_value = filters['payment_method'].strip().upper()
        try:
            method_enum = PaymentMethod[method_value]
            query = query.filter(DTR.payment_method == method_enum)
        except KeyError:
            logger.warning(f"Invalid payment method filter: {method_value}")
    
    # Week start date range
    if filters.get('week_start_date_from'):
        query = query.filter(DTR.week_start_date >= filters['week_start_date_from'])
    if filters.get('week_start_date_to'):
        query = query.filter(DTR.week_start_date <= filters['week_start_date_to'])
    
    # Week end date range
    if filters.get('week_end_date_from'):
        query = query.filter(DTR.week_end_date >= filters['week_end_date_from'])
    if filters.get('week_end_date_to'):
        query = query.filter(DTR.week_end_date <= filters['week_end_date_to'])
    
    # ACH batch number
    if filters.get('ach_batch_number'):
        batch_num = filters['ach_batch_number'].strip()
        query = query.filter(ACHBatch.batch_number.ilike(f"%{batch_num}%"))
    
    # Total amount range
    if filters.get('total_due_min'):
        query = query.filter(DTR.total_due >= filters['total_due_min'])
    if filters.get('total_due_max'):
        query = query.filter(DTR.total_due <= filters['total_due_max'])
    
    # Medallion number (comma-separated)
    if filters.get('medallion_number'):
        medallion_numbers = [m.strip() for m in filters['medallion_number'].split(',') if m.strip()]
        if medallion_numbers:
            or_conditions = [Medallion.medallion_number.ilike(f"%{med}%") for med in medallion_numbers]
            query = query.filter(or_(*or_conditions))
    
    # TLC license (comma-separated)
    if filters.get('tlc_license'):
        tlc_licenses = [t.strip() for t in filters['tlc_license'].split(',') if t.strip()]
        if tlc_licenses:
            or_conditions = [Driver.tlc_license_number.ilike(f"%{lic}%") for lic in tlc_licenses]
            query = query.filter(or_(*or_conditions))
    
    # Driver name (comma-separated)
    if filters.get('driver_name'):
        driver_names = [n.strip() for n in filters['driver_name'].split(',') if n.strip()]
        if driver_names:
            or_conditions = [Driver.full_name.ilike(f"%{name}%") for name in driver_names]
            query = query.filter(or_(*or_conditions))
    
    # Plate number (comma-separated)
    if filters.get('plate_number'):
        plate_numbers = [p.strip() for p in filters['plate_number'].split(',') if p.strip()]
        if plate_numbers:
            or_conditions = [
                Vehicle.registrations.any(VehicleRegistration.plate_number.ilike(f"%{plate}%"))
                for plate in plate_numbers
            ]
            query = query.filter(or_(*or_conditions))
    
    # Check number
    if filters.get('check_number'):
        check_num = filters['check_number'].strip()
        query = query.filter(DTR.check_number.ilike(f"%{check_num}%"))
    
    return query


def transform_driver_payment_row(dtr: DTR) -> Dict[str, Any]:
    """
    Transform a DTR object into export row format.
    
    Args:
        dtr: DTR object
    
    Returns:
        Dictionary with export column data
    """
    # Get driver info
    driver = dtr.driver
    driver_name = driver.full_name if driver else ""
    tlc_license = driver.tlc_license_number if driver else ""
    
    # Get medallion info
    medallion = dtr.medallion
    medallion_number = medallion.medallion_number if medallion else ""
    
    # Get vehicle/plate info
    vehicle = dtr.vehicle
    plate_number = ""
    if vehicle and vehicle.registrations:
        active_reg = next((r for r in vehicle.registrations if r.is_active), None)
        if active_reg:
            plate_number = active_reg.plate_number or ""
    
    # Get ACH batch info
    ach_batch = dtr.ach_batch
    ach_batch_number = ach_batch.batch_number if ach_batch else ""
    
    # Format payment date
    payment_date_str = ""
    if dtr.payment_date:
        payment_date_str = dtr.payment_date.strftime("%Y-%m-%d")
    elif dtr.payment_datetime:
        payment_date_str = dtr.payment_datetime.strftime("%Y-%m-%d")
    
    return {
        "Receipt Type": "DTR",
        "Receipt Number": dtr.dtr_number or "",
        "Payment Date": payment_date_str,
        "Week Start Date": dtr.week_start_date.strftime("%Y-%m-%d") if dtr.week_start_date else "",
        "Week End Date": dtr.week_end_date.strftime("%Y-%m-%d") if dtr.week_end_date else "",
        "Medallion Number": medallion_number,
        "TLC License": tlc_license,
        "Driver Name": driver_name,
        "Plate Number": plate_number,
        "Lease ID": dtr.lease.lease_id if dtr.lease else "",
        "Total Amount": float(dtr.total_due) if dtr.total_due else 0.0,
        "Status": dtr.status.value if dtr.status else "",
        "Payment Method": dtr.payment_method.value if dtr.payment_method else "",
        "ACH Batch Number": ach_batch_number,
        "Check Number": dtr.check_number or "",
        # Additional financial details
        "Gross Income": float(dtr.gross_income) if dtr.gross_income else 0.0,
        "Lease Fees": float(dtr.lease_fees) if dtr.lease_fees else 0.0,
        "EZ Pass": float(dtr.ez_pass) if dtr.ez_pass else 0.0,
        "PVB": float(dtr.pvb) if dtr.pvb else 0.0,
        "TLC Violations": float(dtr.tlc_violations) if dtr.tlc_violations else 0.0,
        "Repairs": float(dtr.repairs) if dtr.repairs else 0.0,
        "Loans": float(dtr.loans) if dtr.loans else 0.0,
        "Other Charges": float(dtr.other_charges) if dtr.other_charges else 0.0,
        "Total Deductions": float(dtr.total_deductions) if dtr.total_deductions else 0.0,
    }
