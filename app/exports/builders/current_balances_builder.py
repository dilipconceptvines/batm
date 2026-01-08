### app/exports/builders/current_balances_builder.py

"""
Query builder for Current Balances exports.

Replicates filter logic from app/current_balances/services_optimized.py
to ensure consistency with the Current Balances view.
"""

from datetime import date, timedelta
from typing import Dict, Any, Optional
from decimal import Decimal
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import or_, and_

from app.leases.models import Lease, LeaseDriver
from app.leases.schemas import LeaseStatus
from app.drivers.models import Driver, TLCLicense
from app.vehicles.models import Vehicle, VehicleRegistration
from app.medallions.models import Medallion
from app.utils.logger import get_logger

logger = get_logger(__name__)


def build_current_balances_export_query(db: Session, filters: Dict[str, Any]):
    """
    Build query for Current Balances export.
    
    Replicates filter logic from CurrentBalancesServiceOptimized._get_live_balances_optimized
    
    Args:
        db: Database session
        filters: Dictionary containing filter parameters
            - week_start: Week start date (Sunday)
            - search: General search term
            - lease_id_search: Lease ID search (comma-separated)
            - driver_name_search: Driver name search (comma-separated)
            - tlc_license_search: TLC license search (comma-separated)
            - medallion_search: Medallion number search (comma-separated)
            - plate_search: Plate number search (comma-separated)
            - vin_search: VIN search (comma-separated)
            - ssn_search: SSN search (comma-separated)
            - lease_status: Lease status filter
            - driver_status: Driver status filter
    
    Returns:
        SQLAlchemy query object for Lease records
    """
    logger.info(f"Building Current Balances export query with {len(filters)} filters")
    
    # Base query with eager loading
    query = (
        db.query(Lease)
        .options(
            joinedload(Lease.lease_driver).joinedload(LeaseDriver.driver).joinedload(Driver.tlc_license),
            joinedload(Lease.vehicle).joinedload(Vehicle.registrations),
            joinedload(Lease.medallion)
        )
        .outerjoin(LeaseDriver)
        .outerjoin(Driver)
        .outerjoin(Vehicle)
        .outerjoin(Medallion)
        .outerjoin(TLCLicense)
        .filter(Lease.lease_status.in_([LeaseStatus.ACTIVE, LeaseStatus.TERMINATED]))
    )
    
    # Apply filters
    
    # General search
    if filters.get('search'):
        search_term = f"%{filters['search']}%"
        query = query.filter(
            or_(
                Lease.lease_id.ilike(search_term),
                Driver.full_name.ilike(search_term),
                Vehicle.vin.ilike(search_term),
                Medallion.medallion_number.ilike(search_term),
                TLCLicense.tlc_license_number.ilike(search_term),
                Driver.ssn.ilike(search_term)
            )
        )
    
    # Lease ID search
    if filters.get('lease_id_search'):
        lease_ids = [lid.strip() for lid in filters['lease_id_search'].split(',') if lid.strip()]
        if lease_ids:
            or_conditions = [Lease.lease_id.ilike(f"%{lid}%") for lid in lease_ids]
            query = query.filter(or_(*or_conditions))
    
    # Driver name search
    if filters.get('driver_name_search'):
        driver_names = [name.strip() for name in filters['driver_name_search'].split(',') if name.strip()]
        if driver_names:
            or_conditions = [Driver.full_name.ilike(f"%{name}%") for name in driver_names]
            query = query.filter(or_(*or_conditions))
    
    # TLC license search
    if filters.get('tlc_license_search'):
        tlc_licenses = [lic.strip() for lic in filters['tlc_license_search'].split(',') if lic.strip()]
        if tlc_licenses:
            or_conditions = [TLCLicense.tlc_license_number.ilike(f"%{lic}%") for lic in tlc_licenses]
            query = query.filter(or_(*or_conditions))
    
    # Medallion search
    if filters.get('medallion_search'):
        medallions = [med.strip() for med in filters['medallion_search'].split(',') if med.strip()]
        if medallions:
            or_conditions = [Medallion.medallion_number.ilike(f"%{med}%") for med in medallions]
            query = query.filter(or_(*or_conditions))
    
    # Plate search
    if filters.get('plate_search'):
        plates = [plate.strip() for plate in filters['plate_search'].split(',') if plate.strip()]
        if plates:
            or_conditions = [
                Vehicle.registrations.any(VehicleRegistration.plate_number.ilike(f"%{plate}%"))
                for plate in plates
            ]
            query = query.filter(or_(*or_conditions))
    
    # VIN search
    if filters.get('vin_search'):
        vins = [vin.strip() for vin in filters['vin_search'].split(',') if vin.strip()]
        if vins:
            or_conditions = [Vehicle.vin.ilike(f"%{vin}%") for vin in vins]
            query = query.filter(or_(*or_conditions))
    
    # SSN search
    if filters.get('ssn_search'):
        ssns = [ssn.strip() for ssn in filters['ssn_search'].split(',') if ssn.strip()]
        if ssns:
            or_conditions = [Driver.ssn.ilike(f"%{ssn}%") for ssn in ssns]
            query = query.filter(or_(*or_conditions))
    
    # Lease status filter
    if filters.get('lease_status'):
        query = query.filter(Lease.lease_status == filters['lease_status'])
    
    # Driver status filter (active/inactive)
    if filters.get('driver_status'):
        if filters['driver_status'].lower() == 'active':
            query = query.filter(Driver.is_active == True)
        elif filters['driver_status'].lower() == 'inactive':
            query = query.filter(Driver.is_active == False)
    
    return query


def transform_current_balance_row(lease: Lease, week_start: date, week_end: date) -> Dict[str, Any]:
    """
    Transform a Lease object into export row format.
    
    Note: This provides basic lease information. Full balance calculation
    should be done using CurrentBalancesServiceOptimized for accuracy.
    
    Args:
        lease: Lease object
        week_start: Week start date
        week_end: Week end date
    
    Returns:
        Dictionary with export column data
    """
    # Get driver info
    driver = lease.lease_driver.driver if lease.lease_driver else None
    driver_name = driver.full_name if driver else ""
    
    # Get TLC license
    tlc_license = ""
    if driver and driver.tlc_license:
        tlc_license = driver.tlc_license.tlc_license_number or ""
    
    # Get vehicle info
    vehicle = lease.vehicle
    plate_number = ""
    vin = ""
    if vehicle:
        vin = vehicle.vin or ""
        # Get most recent plate
        if vehicle.registrations:
            active_reg = next((r for r in vehicle.registrations if r.is_active), None)
            if active_reg:
                plate_number = active_reg.plate_number or ""
    
    # Get medallion info
    medallion = lease.medallion
    medallion_number = medallion.medallion_number if medallion else ""
    
    # Mask SSN
    masked_ssn = ""
    if driver and driver.ssn:
        ssn_clean = ''.join(filter(str.isdigit, driver.ssn))
        if len(ssn_clean) >= 4:
            last_four = ssn_clean[-4:]
            masked_ssn = f"XXX-XX-{last_four}"
    
    return {
        "Week Start": week_start.strftime("%Y-%m-%d"),
        "Week End": week_end.strftime("%Y-%m-%d"),
        "Lease ID": lease.lease_id or "",
        "Lease Status": lease.lease_status.value if lease.lease_status else "",
        "Driver Name": driver_name,
        "TLC License": tlc_license,
        "SSN (Masked)": masked_ssn,
        "Medallion Number": medallion_number,
        "Plate Number": plate_number,
        "VIN": vin,
        "Driver Status": "Active" if (driver and driver.is_active) else "Inactive",
        "Lease Start Date": lease.start_date.strftime("%Y-%m-%d") if lease.start_date else "",
        "Lease End Date": lease.end_date.strftime("%Y-%m-%d") if lease.end_date else "",
        # Note: Balance calculations would require complex logic from CurrentBalancesServiceOptimized
        # For now, export basic info. Full balances can be calculated if needed.
    }
