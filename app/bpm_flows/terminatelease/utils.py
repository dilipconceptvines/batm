## app/bpm_flows/terminatelease/utils.py

# Standard library imports
from datetime import datetime

# Third party imports
from sqlalchemy import func
from sqlalchemy.orm import Session

# Local imports
from app.utils.logger import get_logger
from app.leases.models import Lease, LeaseDriver, LeaseDriverDocument
from app.drivers.models import Driver
from app.vehicles.models import Vehicle, VehicleRegistration
from app.medallions.models import Medallion

logger = get_logger(__name__)


def fetch_lease_by_Leasedriver(db: Session, driver_lease:LeaseDriver):
    return db.query(Lease).filter(Lease.id==driver_lease.lease_id).first()

def fetch_vehicle_by_lease(db: Session, lease: Lease):
    return db.query(Vehicle).filter(Vehicle.id==lease.vehicle_id).first()

def fetch_driver_lease(db: Session, driver_lease_id: str):
    return db.query(LeaseDriver).filter(LeaseDriver.id == driver_lease_id, LeaseDriver.is_active == True).first()


def fetch_lease_information_for_driver(db: Session, driver_id: str = None):

    active_lease_drivers_query = db.query(
        LeaseDriver.id.label("driver_lease_id"),
        Lease.lease_id,
        Medallion.medallion_number,
        Driver.first_name,
        Driver.last_name,
        Driver.driver_id,
        Vehicle.vin,
        VehicleRegistration.plate_number,
        Lease.lease_date,
        Lease.id.label("lease_id_pk")
    ).join(
        Driver, Driver.driver_id == LeaseDriver.driver_id
    ).join(
        Lease, Lease.id == LeaseDriver.lease_id
    ).join(
        Medallion, Medallion.id == Lease.medallion_id
    ).join(
        Vehicle, Vehicle.id == Lease.vehicle_id
    ).join(
        VehicleRegistration, VehicleRegistration.vehicle_id == Vehicle.id
    ).filter(
        Driver.is_active == True,
        LeaseDriver.is_active == True,
        VehicleRegistration.is_active == True
    )

    if driver_id:
        active_lease_drivers_query.filter(
            LeaseDriver.driver_id == driver_id)

    active_lease_drivers = active_lease_drivers_query.all()
    lease_vehicle_info = []
    for lease_driver in active_lease_drivers:
        driver_lease_document = (
            db.query(LeaseDriverDocument)
            .join(
                LeaseDriver, LeaseDriver.id == LeaseDriverDocument.lease_driver_id
            )
            .filter(
                LeaseDriverDocument.is_active == True,
                LeaseDriver.driver_id == lease_driver.driver_id
            ).first()
        )
        if not lease_driver.lease_id:
            continue
        lease_vehicle_info.append({
            "driver_lease_id": lease_driver.driver_lease_id,
            "lease_id": lease_driver.lease_id,
            "medallion_number": lease_driver.medallion_number,
            "driver_name": f"{lease_driver.first_name} {lease_driver.last_name}",
            "vin_number": lease_driver.vin,
            "vehicle_plate_number": lease_driver.plate_number,
            "lease_date": lease_driver.lease_date,
            "lease_id_pk": lease_driver.lease_id_pk,
            "is_manager": True if driver_lease_document else False
        })
    return lease_vehicle_info
