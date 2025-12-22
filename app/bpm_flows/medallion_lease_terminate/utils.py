## app/bpm_flows/medallion_lease_terminate/utils.py

from app.medallions.models import Medallion, MOLease

from app.vehicles.models import Vehicle
from app.bat.schema import VehicleStatus
from app.bat.schema import MedallionStatus
from app.utils.logger import get_logger
from fastapi import HTTPException, status
from sqlalchemy.orm import Session


logger = get_logger(__name__)


def terminate_medallion_lease(db: Session, medallion:Medallion):


    if medallion.medallion_status == MedallionStatus.ARCHIVED:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Medallion is already terminated")
    
    if medallion.medallion_status != MedallionStatus.ASSIGNED_TO_VEHICLE or medallion.medallion_status != MedallionStatus.AVAILABLE:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Medallion is not available to terminate lease"
        )
    
    if medallion.medallion_status == MedallionStatus.ASSIGNED_TO_VEHICLE:
        vehicle = db.query(Vehicle).filter(Vehicle.medallion_id == medallion.id).first()
        vehicle.vehicle_status = VehicleStatus.AVAILABLE
        vehicle.medallion_id = None
        vehicle.is_medallion_assigned = False


    medallion_lease= db.query(MOLease).filter(MOLease.id == medallion.mo_leases_id, MOLease.is_active== True).first()

    medallion.medallion_status= MedallionStatus.ARCHIVED
    medallion.is_active = False
    medallion_lease.is_active = False

    db.flush()