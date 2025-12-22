## app/drivers/search_service.py

from datetime import datetime, timedelta , date
from typing import List

# Third party imports
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.sql import or_, func , and_, exists, not_ , select 
from sqlalchemy import desc

# Local imports
from app.drivers.models import Driver, TLCLicense, DMVLicense
from app.leases.models import LeaseDriver, Lease
from app.entities.models import Address , BankAccount
from app.drivers.schemas import DriverStatus
from app.uploads.models import Document
from app.audit_trail.models import AuditTrail
from app.vehicles.models import Vehicle
from app.medallions.models import Medallion
from app.utils.logger import get_logger
from app.utils.general import format_us_phone_number
from app.utils.general import apply_multi_filter

logger = get_logger(__name__)

def driver_lease_report(
    db: Session,
    filters: dict,
    sort_by: str,
    sort_order: str,
):
    """Get a list of drivers with their lease expiry dates"""

    try:
        query = db.query(Driver).options(
            joinedload(Driver.tlc_license),
            joinedload(Driver.dmv_license),
        ).join(
            TLCLicense, 
            Driver.tlc_license_number_id == TLCLicense.id,
             isouter=True 
            ).join(
            DMVLicense, 
            Driver.dmv_license_number_id == DMVLicense.id,
            isouter=True 
            ).filter(
            Driver.driver_status != DriverStatus.INACTIVE,
            Driver.driver_status != DriverStatus.IN_PROGRESS
            )
        
        matched_filters = []

        day_in_advance = filters.get("day_in_advance", 0)

        check_date = datetime.now() + timedelta(days=day_in_advance)

        if license_type := filters.get("license_type"):
            if license_type == "tlc_license":
                Driver.tlc_license.has(and_(
              TLCLicense.tlc_license_expiry_date <= check_date.date(),
              TLCLicense.tlc_license_expiry_date >= datetime.now().date()
        )
    )
            elif license_type == "dmv_license":
                query = query.filter(Driver.dmv_license.has(
                    and_(
                    DMVLicense.dmv_license_expiry_date <= check_date.date(),
                    DMVLicense.dmv_license_expiry_date >= datetime.now().date()
                    )
                ))
            matched_filters.append("license_type")


        
        if driver_id:= filters.get("driver_id"):
            driver_ids=[id.strip() for id in driver_id.split(",") if id.strip()]
            query= query.filter(or_(*[Driver.driver_id.like(f"%{id}%") for id in driver_ids]))
            matched_filters.append("driver_id")

        if tlc_license_number:= filters.get("tlc_license_number"):
            tlc_license_numbers=[id.strip() for id in tlc_license_number.split(",") if id.strip()]
            query= query.filter(or_(*[TLCLicense.tlc_license_number.like(f"%{id}%") for id in tlc_license_numbers]))
            matched_filters.append("tlc_license_number")

        if dmv_license_number:= filters.get("dmv_license_number"):
            dmv_license_numbers=[id.strip() for id in dmv_license_number.split(",") if id.strip()]
            query= query.filter(or_(*[DMVLicense.dmv_license_number.like(f"%{id}%") for id in dmv_license_numbers]))
            matched_filters.append("dmv_license_number")

        sort_mapping = {
            "first_name": Driver.first_name,
            "last_name": Driver.last_name,
            "driver_type": Driver.driver_type,
            "tlc_license_number": TLCLicense.tlc_license_number,
            "dmv_license_number": DMVLicense.dmv_license_number,
            "driver_status": Driver.driver_status,
            "created_on": Driver.created_on,
        }

        if sort_by in sort_mapping:
            sort_column = sort_mapping[sort_by]
            query = query.order_by(sort_column.desc() if sort_order == "desc" else sort_column.asc())
        else:
            query = query.order_by(desc(Driver.created_on))

        return query, matched_filters,check_date
    
    except Exception as e:
        logger.info("Error in driver_lease_expiry: %s", e)
        return None, [] , check_date
    
def get_total_items(db: Session, query):
    """Get the total number of items in the query"""
    return db.query(func.count()).select_from(query.subquery()).scalar()

def get_paginated_results(query, page: int, per_page: int):
    """Get paginated results from the query"""
    return query.offset((page - 1) * per_page).limit(per_page).all()
    
def build_driver_query(
    db: Session,
    filters: dict,
    sort_by: str,
    sort_order: str,
):
    """Build a query to search for drivers"""
    query = db.query(Driver).filter(Driver.driver_status != DriverStatus.IN_PROGRESS)
    joined_medallion = False
    joined_vehicle = False
    joined_vehicle_registration = False
    joined_lease_driver = False
    joined_lease = False
    joined_driver = False
    joined_tlc_license = False
    joined_dmv_license = False
    joined_lease_configuration = False

    matched_filters = []

    if filters.get("is_archived") is not None:
        query = query.filter(Driver.is_archived.is_(filters["is_archived"]))
    else:
        query = query.filter(Driver.is_archived.is_(False))

    if ids := filters.get("driver_lookup_id"):
        query = apply_multi_filter(query, Driver.driver_id, ids)
        matched_filters.append("driver_lookup_id")

    if nums := filters.get("tlc_license_number"):
        if not joined_tlc_license:
            query = query.join(TLCLicense , TLCLicense.id == Driver.tlc_license_number_id)
            joined_tlc_license = True
        query = apply_multi_filter(query, TLCLicense.tlc_license_number, nums)
        matched_filters.append("tlc_license_number")

    if nums := filters.get("dmv_license_number"):
        if not joined_dmv_license:
            query = query.join(DMVLicense , DMVLicense.id == Driver.dmv_license_number_id)
            joined_dmv_license = True
        query = apply_multi_filter(query, DMVLicense.dmv_license_number, nums)
        matched_filters.append("dmv_license_number")

    if ssn := filters.get("ssn"):
        query = apply_multi_filter(query, Driver.ssn, ssn)
        matched_filters.append("ssn")
    if vin := filters.get("vin"):
        if not joined_lease_driver:
            query = query.join(LeaseDriver, LeaseDriver.driver_id == Driver.driver_id)
            joined_lease_driver = True
        if not joined_lease:
            query = query.join(Lease, Lease.id == LeaseDriver.lease_id)
            joined_lease = True
        if not joined_vehicle:
            query = query.join(Vehicle, Vehicle.id == Lease.vehicle_id)
            joined_vehicle = True
        query = apply_multi_filter(query, Vehicle.vin, vin)
        matched_filters.append("vin")

    if medallion_number := filters.get("medallion_number"):
        if not joined_lease_driver:
            query = query.join(LeaseDriver, LeaseDriver.driver_id == Driver.driver_id)
            joined_lease_driver = True
        if not joined_lease:
            query = query.join(Lease, Lease.id == LeaseDriver.lease_id)
            joined_lease = True
        if not joined_medallion:
            query = query.join(Medallion, Medallion.id == Lease.medallion_id)
            joined_medallion = True
    
        query = apply_multi_filter(query, Medallion.medallion_number, medallion_number)
        matched_filters.append("medallion_number")

    if driver_name := filters.get("driver_name"):
        query = apply_multi_filter(query, Driver.full_name, driver_name)
        matched_filters.append("driver_name")

    for field, key in [
        (Driver.driver_type, "driver_type"),
        (Driver.driver_status, "driver_status"),
    ]:
        if val := filters.get(key):
            query = query.filter(field == val)
            matched_filters.append(key)

    if d := filters.get("tlc_license_expiry_from"):
        if not joined_tlc_license:
            query = query.join(TLCLicense, TLCLicense.id == Driver.tlc_license_number_id)
            joined_tlc_license = True
        query = query.filter(TLCLicense.tlc_license_expiry_date >= d)
    if d := filters.get("tlc_license_expiry_to"):
        if not joined_tlc_license:
            query = query.join(TLCLicense, TLCLicense.id == Driver.tlc_license_number_id)
            joined_tlc_license = True
        query = query.filter(TLCLicense.tlc_license_expiry_date <= d)
    if d := filters.get("dmv_license_expiry_from"):
        if not joined_dmv_license:
            query = query.join(DMVLicense, DMVLicense.id == Driver.dmv_license_number_id)
            joined_dmv_license = True
        query = query.filter(DMVLicense.dmv_license_expiry_date >= d)
    if d := filters.get("dmv_license_expiry_to"):
        if not joined_dmv_license:
            query = query.join(DMVLicense, DMVLicense.id == Driver.dmv_license_number_id)
            joined_dmv_license = True
        query = query.filter(DMVLicense.dmv_license_expiry_date <= d)

    if has_documents := filters.get("has_documents") is not None:
        doc_exists_clause = exists().where(and_(
            Document.object_lookup_id == Driver.id,
            Document.object_type == "driver"
        ))
        query = query.filter(doc_exists_clause if has_documents else not_(doc_exists_clause))
        matched_filters.append("has_documents")

    if (has_vehicle:= filters.get("has_vehicle")) is not None:
            if not joined_lease_driver:
                query = query.join(LeaseDriver, LeaseDriver.driver_id == Driver.driver_id)
                joined_lease_driver = True
            if not joined_lease:
                query = query.join(Lease, Lease.id == LeaseDriver.lease_id)
                joined_lease = True
            if has_vehicle:
                query = query.filter(
                    and_(
                        LeaseDriver.is_active == True,
                        Lease.vehicle_id.isnot(None)
                    )
                )
            else:
                query = query.filter(
                    or_(
                        LeaseDriver.is_active == False,
                        Lease.vehicle_id.is_(None)
                    )
                )
            matched_filters.append("has_vehicle")

    if (has_active_lease := filters.get("has_active_lease")) is not None:
        if not joined_lease_driver:
                query = query.join(LeaseDriver, LeaseDriver.driver_id == Driver.driver_id)
                joined_lease_driver = True
        if not joined_lease:
            query = query.join(Lease, Lease.id == LeaseDriver.lease_id)
            joined_lease = True
        query= query.filter(LeaseDriver.is_active == has_active_lease , Lease.is_active == has_active_lease)
        matched_filters.append("has_active_lease")
        
    if (val := filters.get("is_additional_driver")) is not None:
        if not joined_lease_driver:
                query = query.join(LeaseDriver, LeaseDriver.driver_id == Driver.driver_id)
                joined_lease_driver = True
        query = query.filter(LeaseDriver.is_additional_driver == val)
        matched_filters.append("is_additional_driver")

    if (val := filters.get("is_drive_locked")) is not None:
        query = query.filter(Driver.drive_locked == val)
        matched_filters.append("is_drive_locked")

    if val := filters.get("lease_type"):
        if not joined_lease_driver:
                query = query.join(LeaseDriver, LeaseDriver.driver_id == Driver.driver_id)
                joined_lease_driver = True
        if not joined_lease:
            query = query.join(Lease, Lease.id == LeaseDriver.lease_id)
            joined_lease = True
        query = query.filter(Lease.lease_type == val)
        matched_filters.append("lease_type")

    sort_mapping = {
        "first_name": Driver.first_name,
        "last_name": Driver.last_name,
        "driver_type": Driver.driver_type,
        "driver_status": Driver.driver_status,
        "created_on": Driver.created_on,
    }

    tlc_sort_maping = {
        "tlc_license_number": TLCLicense.tlc_license_number,
        "tlc_license_expriy": TLCLicense.tlc_license_expiry_date,
    }

    dmv_sort_maping = {
        "dmv_license_number": DMVLicense.dmv_license_number,
        "dmv_license_expriy": DMVLicense.dmv_license_expiry_date,
    }

    if sort_by and sort_order:

        sort_column = None
        
        if sort_by in sort_mapping:
            sort_column = sort_mapping[sort_by]

        elif sort_by in tlc_sort_maping:
            if not joined_tlc_license:
                query = query.join(TLCLicense, TLCLicense.id == Driver.tlc_license_number_id)
                joined_tlc_license = True
            sort_column = tlc_sort_maping[sort_by]

        elif sort_by in dmv_sort_maping:
            if not joined_dmv_license:
                query = query.join(DMVLicense, DMVLicense.id == Driver.dmv_license_number_id)
                joined_dmv_license = True
            sort_column = dmv_sort_maping[sort_by]

        if sort_column:
            query = query.order_by(sort_column.desc() if sort_order == "desc" else sort_column.asc())
        else:
            query = query.order_by(Driver.created_on.desc())
    else:
        query = query.order_by(Driver.updated_on.desc(),Driver.created_on.desc())

    return query, matched_filters

def get_formatted_drivers(drivers: List[Driver], db: Session , is_additional_driver: bool):
    """Get formatted drivers"""
    drivers_list = []
    current_date = func.current_date()

    for driver in drivers:
        audit_trail = db.query(AuditTrail).filter(AuditTrail.meta_data.contains({"driver_id": driver.id})).count()
        has_documents = db.query(Document).filter(Document.object_lookup_id == driver.id, Document.object_type == "driver").count()
        has_vehicle = db.query(exists().where(
            and_(
                LeaseDriver.driver_id == driver.driver_id,
                Lease.id == LeaseDriver.lease_id,
                Vehicle.id == Lease.vehicle_id,
                Lease.is_active == True
            )
        )).scalar()

        has_active_lease = db.query(exists().where(
            and_(
                LeaseDriver.driver_id == driver.driver_id,
                Lease.id == LeaseDriver.lease_id,
                Lease.lease_start_date <= current_date,
                Lease.is_active == True,
                or_(
                    Lease.lease_end_date >= current_date,
                    Lease.lease_end_date.is_(None)
                )
            )
        )).scalar()
        
        lease_drivers = driver.lease_drivers or []

        if is_additional_driver is not None:
            val = bool(is_additional_driver)
            lease_data = [
                ld.lease.to_dict()
                for ld in lease_drivers
                if ld.is_additional_driver == val
            ]
        else:
            lease_data = [ld.lease.to_dict() for ld in lease_drivers]

        drivers_list.append({
            "driver_details": {
                "driver_id": driver.id,
                "driver_lookup_id": driver.driver_id,
                "first_name": driver.first_name,
                "middle_name": driver.middle_name,
                "last_name": driver.last_name,
                "full_name": driver.full_name,
                "driver_type": driver.driver_type,
                "driver_status": driver.driver_status,
                "driver_ssn": f"XXX-XX-{driver.ssn[-4:]}" if driver.ssn and len(driver.ssn) >= 4 else None,
                "dob": driver.dob,
                "phone_number_1": format_us_phone_number(driver.phone_number_1),
                "phone_number_2": format_us_phone_number(driver.phone_number_2),
                "email_address": driver.email_address,
                "primary_emergency_contact_person": driver.primary_emergency_contact_person,
                "primary_emergency_contact_relationship": driver.primary_emergency_contact_relationship,
                "primary_emergency_contact_number": format_us_phone_number(driver.primary_emergency_contact_number),
                "additional_emergency_contact_person": driver.additional_emergency_contact_person,
                "additional_emergency_contact_relationship": driver.additional_emergency_contact_relationship,
                "additional_emergency_contact_number": format_us_phone_number(driver.additional_emergency_contact_number),
                "violation_due_at_registration": driver.violation_due_at_registration,
                "is_drive_locked": driver.drive_locked,
                "has_audit_trail": bool(audit_trail),  # Default to True as requested
            },
            "dmv_license_details": {
                "is_dmv_license_active": bool(driver.dmv_license),
                "dmv_license_number": driver.dmv_license.dmv_license_number if driver.dmv_license else None,
                "dmv_license_issued_state": driver.dmv_license.dmv_license_issued_state if driver.dmv_license else None,
                "dmv_license_expiry_date": driver.dmv_license.dmv_license_expiry_date if driver.dmv_license else None,
            },
            "tlc_license_details": {
                "is_tlc_license_active": bool(driver.tlc_license),
                "tlc_license_number": driver.tlc_license.tlc_license_number if driver.tlc_license else None,
                "tlc_license_expiry_date": driver.tlc_license.tlc_license_expiry_date if driver.tlc_license else None,
            },
            "primary_address_details": {
                "address_line_1": driver.primary_driver_address.address_line_1 if driver.primary_driver_address else None,
                "address_line_2": driver.primary_driver_address.address_line_2 if driver.primary_driver_address else None,
                "city": driver.primary_driver_address.city if driver.primary_driver_address else None,
                "state": driver.primary_driver_address.state if driver.primary_driver_address else None,
                "zip": driver.primary_driver_address.zip if driver.primary_driver_address else None,
                "latitude": driver.primary_driver_address.latitude if driver.primary_driver_address else None,
                "longitude": driver.primary_driver_address.longitude if driver.primary_driver_address else None
            },
            "secondary_address_details": {
                "latitude": driver.secondary_driver_address.latitude if driver.secondary_driver_address else None,
                "longitude": driver.secondary_driver_address.longitude if driver.secondary_driver_address else None
            },
            "payee_details": {
                "pay_to_mode": driver.pay_to_mode,
                "bank_name": driver.driver_bank_account.bank_name if driver.driver_bank_account else None,
                "bank_account_number": driver.driver_bank_account.bank_account_number if driver.driver_bank_account else None,
                "address_line_1": "",
                "address_line_2": "",
                "city": "",
                "state": "",
                "zip": "",
                "pay_to": driver.pay_to,
            },
            "lease_info": {
                "has_active_lease": has_active_lease,
                "lease_type": driver.lease_drivers[0].lease.lease_type if driver.lease_drivers else None,
                "lease_data": lease_data
            },
            "has_documents": has_documents,
            "has_vehicle": has_vehicle,
            "is_archived": driver.is_archived
        })

    return drivers_list


def get_drivers_for_export(db, filters: dict, sort_by=None, sort_order="asc"):
    stmt = (
        select(
            # Driver
            Driver.driver_id.label("driver_id"),
            Driver.first_name.label("first_name"),
            Driver.last_name.label("last_name"),
            Driver.driver_type.label("driver_type"),
            Driver.driver_status.label("driver_status"),
            Driver.drive_locked.label("is_drive_locked"),
            Driver.is_archived.label("is_archived"),

            # TLC
            TLCLicense.tlc_license_number.label("tlc_license_number"),
            TLCLicense.tlc_license_expiry_date.label("tlc_license_expiry_date"),

            # DMV
            DMVLicense.dmv_license_number.label("dmv_license_number"),
            DMVLicense.dmv_license_expiry_date.label("dmv_license_expiry_date"),

            # Address
            Address.address_line_1.label("address_line_1"),
            Address.address_line_2.label("address_line_2"),
            Address.city.label("city"),
            Address.state.label("state"),
            Address.zip.label("zip"),

            # Bank
            BankAccount.bank_name.label("bank_name"),
            BankAccount.bank_account_number.label("bank_account_number"),
        )
        .select_from(Driver)
        .outerjoin(TLCLicense, Driver.tlc_license_number_id == TLCLicense.id)
        .outerjoin(DMVLicense, Driver.dmv_license_number_id == DMVLicense.id)
        .outerjoin(Address, Driver.primary_address_id == Address.id)
        .outerjoin(BankAccount, Driver.bank_account_id == BankAccount.id)
    )

    # ✅ Filters
    if filters.get("driver_lookup_id"):
        stmt = stmt.where(Driver.driver_id == filters["driver_lookup_id"])

    if filters.get("driver_status"):
        stmt = stmt.where(Driver.driver_status == filters["driver_status"])

    # ✅ Sorting (safe)
    if sort_by:
        column = getattr(Driver, sort_by, None)
        if column is not None:
            stmt = stmt.order_by(
                column.asc() if sort_order == "asc" else column.desc()
            )

    results = db.execute(stmt).mappings().all()

    cleaned_results = [
        {key: ("" if value is None else value) for key, value in row.items()}
        for row in results
    ]

    return cleaned_results



