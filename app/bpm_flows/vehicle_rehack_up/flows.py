## app/bpm_flows/vehicle_rehack_up/flows.py

# Third party imports
from fastapi import HTTPException

# Local imports
from app.bpm.step_info import step
from app.utils.logger import get_logger
from app.audit_trail.services import audit_trail_service
from app.vehicles.schemas import VehicleStatus, HackupStatus
from app.leases.schemas import LeaseStatus
from app.medallions.schemas import MedallionStatus
from app.bpm.services import bpm_service
from app.medallions.services import medallion_service
from app.medallions.utils import format_medallion_response
from app.vehicles.services import vehicle_service
from app.leases.services import lease_service
from app.uploads.services import upload_service

logger = get_logger(__name__)
entity_mapper = {
    "VEHICLE": "vehicles",
    "VEHICLE_IDENTIFIER": "id",
}

@step(step_id="128", name="vehicle rehack-up process", operation='process')
def process_vehicle_rehack_up(db, case_no, step_data):

    """
    Processes the vehicle re-hack-up operation.
    """
    try:
        #Enter vehicle Re-hack up
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        
        vehicle = vehicle_service.get_vehicles(db , vin=step_data.get("vin"))
        
        if not vehicle :
            raise HTTPException(status_code=404 , detail="vehicle not found")
        
        if vehicle.medallion_id :
            raise HTTPException(status_code=404 , detail="medallion is already atteched to the vehicle cannot rehack")
        
        vehicle_lease= lease_service.get_lease(
            db, vehicle_id=vehicle.id, status=LeaseStatus.ACTIVE
        )
        if vehicle_lease :
            raise HTTPException(status_code=404 , detail="vehicle lease is active , cannot rehack")

        medallion= medallion_service.get_medallion(
            db, medallion_number=step_data["medallion_number"]
        )

        if not medallion :
            raise HTTPException(status_code=404 , detail="medallion not found")
        
        if vehicle.vehicle_status != VehicleStatus.AVAILABLE :
            raise HTTPException(status_code=404 , detail=f"vehicle not in {VehicleStatus.AVAILABLE.value} status , cannot rehack")
        
        vehicle_hackup_data = vehicle_service.get_vehicle_hackup(
            db, vehicle_id=vehicle.id
        )

        if not vehicle_hackup_data :
            raise HTTPException(status_code=404 , detail="vehicle not Hakeup yet, cannot be Rehack")
        
        if vehicle_hackup_data.is_active:
            raise HTTPException(status_code=404 , detail="vehicle hackup is active stage cannot rehack")
        
        # Update the vehicle hackup data
        vehicle_hackup_data = vehicle_service.upsert_vehicle_hackup(
            db, {
                "id": vehicle_hackup_data.id,
                "status": LeaseStatus.ACTIVE,
                "is_active": True
            }
        )
        
        # Update the vehicle data
        vehicle = vehicle_service.upsert_vehicle(
            db, {
                "id": vehicle.id,
                "vehicle_status": VehicleStatus.HACKED_UP,
                "is_medallion_assigned": True,
                "medallion_id": medallion.id
            }
        )
        
        # Update the medallion data
        medallion = medallion_service.upsert_medallion(
            db, {
                "id": medallion.id,
                "medallion_status": MedallionStatus.ACTIVE
            }
        )
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            medallion_info = medallion_service.get_medallion(db, medallion_id=vehicle.medallion_id if vehicle.medallion_id else None)
            if medallion_info:
                medallion_owner = medallion_service.get_medallion_owner(db, medallion_owner_id=medallion_info.owner_id)
                if medallion_owner:
                    audit_trail_service.create_audit_trail(
                        db=db,
                        case=case,
                        description=f"Processed vehicle rehack-up for vehicle {vehicle.vin} with medallion {medallion_info.medallion_number} for owner {medallion_owner.id}",
                        meta_data={"medallion_owner_id": medallion_owner.id , "medallion_id": medallion_info.id , "vehicle_id": vehicle.id}
                    )

        return "ok"
    except Exception as e:
        logger.error("Error processing vehicle rehackup: %s", e)
        raise e
    
    
@step(step_id="128",name="vehicle rehack-up fetch", operation='fetch')
def fetch_vehicle_rehack_up(db, case_no, case_params=None):
    """
    Fetches the vehicle entity and vehicle information for the vehicle re-hack-up process.
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        vehicle = None
        
        vehicle = None
        if case_params:
            vehicle = vehicle_service.get_vehicles(db, vin=case_params['object_lookup'])
        if case_entity:
            vehicle = vehicle_service.get_vehicles(
                db, vehicle_id=int(case_entity.identifier_value)
            )

        if not vehicle:
            raise HTTPException(status_code=404, detail="Vehicle not found")
        
        medallion_info = None
        
        if vehicle.is_medallion_assigned:
            medallion_info = medallion_service.get_medallion(
                db, medallion_id=vehicle.medallion_id
            )

        if medallion_info:
            medallion_info = format_medallion_response(medallion_info)

        hackup_info = {}
        hackup_info['vehicle_info'] = {
            **vehicle.to_dict(),
            "medallion_number": medallion_info.get("medallion_number") if medallion_info else None,
            "medallion_owner": medallion_info.get("medallion_owner") if medallion_info else None    
        }
        hackup_info['hackup_info'] = vehicle_service.get_vehicle_hackup(
            db, vehicle_id=vehicle.id
        )
        # Create case entity if it doesn't exists
        if not case_entity:
            bpm_service.create_case_entity(
                db, case_no=case_no,
                entity_name=entity_mapper['VEHICLE'],
                identifier=entity_mapper['VEHICLE_IDENTIFIER'],
                identifier_value=vehicle.id
            )
        return hackup_info
    except Exception as e:
        logger.error("Error fetching vehicle rehackup: %s", e)
        raise e