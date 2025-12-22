## app/bpm_flows/allocate_medallion_vehicle/flows.py

# Third party imports
from fastapi import HTTPException

# Local imports
from app.bpm.step_info import step
from app.utils.logger import get_logger
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.medallions.services import medallion_service
from app.medallions.schemas import MedallionStatus , MedallionType , VehicleAllocation
from app.vehicles.services import vehicle_service
from app.vehicles.schemas import VehicleStatus
from app.bpm_flows.newmed.utils import format_medallion_basic_details
from app.bpm_flows.allocate_medallion_vehicle.utils import format_vehicle_details

logger = get_logger(__name__)
entity_mapper = {
    "MEDALLION": "medallion",
    "MEDALLION_IDENTIFIER": "id",
}


@step(step_id="119", name="Fetch - Medallion allocation to vehicle ", operation='fetch')
def fetch_vehicle_for_medallion(db, case_no, case_params=None):
    """
    Fetch the vehicle for the medallion allocation to vehicle step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        medallion_vehicle_info = {'medallion_info': {}, 'vehicle_info': {}}
        medallion_info = None
        if case_params:
            medallion_info = medallion_service.get_medallion(
                db, medallion_number=case_params['object_lookup']
            )
        if case_entity:
            medallion_info = medallion_service.get_medallion(
                db, medallion_id=int(case_entity.identifier_value)
            )

        if not medallion_info:
            return {}

        medallion_owner = medallion_service.get_medallion_owner(
            db, medallion_owner_id=medallion_info.owner_id
        )
        medallion_vehicle_info['medallion_info'] = format_medallion_basic_details(
            medallion_info, medallion_owner
        )

        vehicle_details = vehicle_service.get_vehicles(
            db, medallion_id=medallion_info.id
        )

        if vehicle_details:
            medallion_vehicle_info['vehicle_info'] = format_vehicle_details(vehicle_details)

        if not case_entity:
            case_entity = bpm_service.create_case_entity(
                db, case_no=case_no,
                identifier=entity_mapper['MEDALLION_IDENTIFIER'],
                identifier_value=str(medallion_info.id),
                entity_name=entity_mapper['MEDALLION']
            )

        return medallion_vehicle_info
    except Exception as e:
        logger.error("Error fetching vehicle for medallion: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e

@step(step_id="119", name="Process - Allocate medallion to vehicle", operation='process')
def process_vehicle_for_medallion(db, case_no, step_data):
    """
    Process the vehicle for the medallion allocation to vehicle step
    """
    try:
        # If a case already exists for this step then we should not process it
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        
        medallion = medallion_service.get_medallion(
            db, medallion_number=step_data['medallion_number']
        )
        
        if medallion.medallion_status != MedallionStatus.AVAILABLE:
            raise ValueError("medallion is not in Available status , cannot be assigned")
        
        storage = medallion_service.get_medallion_storage(
            db, medallion_number=medallion.medallion_number
        )
        if storage :
            if storage.retrieval_date is None:
                raise ValueError("The medallion is in storage so it cannot be assigned")
        
        vehicle = vehicle_service.get_vehicles(
            db, vin=step_data['vehicle_vin']
        )
        
        VEHICLE_ALLOCATION = VehicleAllocation()

        if (medallion.medallion_type == MedallionType.WAV and vehicle.vehicle_type not in VEHICLE_ALLOCATION.Wav) or (medallion.medallion_type == MedallionType.REGULAR and vehicle.vehicle_type not in VEHICLE_ALLOCATION.Regular):
            raise ValueError("Vehicle and Medallion are Same Type , cannot be assigned")
        
        if vehicle.vehicle_status != VehicleStatus.AVAILABLE or vehicle.is_medallion_assigned == True:
            raise ValueError(f"vehicle is not in {VehicleStatus.AVAILABLE.value} status or medallion is Already assigned , cannot be assigned")
        
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            metadata = {
                "medallion_id": medallion.id,
                "vehicle_id": vehicle.id
            }
            audit_trail_service.create_audit_trail(
                db=db ,
                case=case,
                meta_data=metadata,
                description=f"Vehicle {vehicle.vin} allocated to medallion {medallion.medallion_number}"
            )
        
        vehicle_service.upsert_vehicle(db, {
            "id": vehicle.id,
            "medallion_id": medallion.id,
            "is_medallion_assigned": True,
            "vehicle_status": VehicleStatus.AVAILABLE_FOR_HACK_UP
        })
        logger.info("Added medallion to vehicle")

        # Add correct status to medallion as well
        medallion_service.upsert_medallion(db, {
            "id": medallion.id,
            "medallion_status": MedallionStatus.ASSIGNED_TO_VEHICLE
        })
        logger.info("Modified medallion status to assigned to vehicle")
        return "Ok"
    except Exception as e:
        logger.error("Error processing vehicle for medallion: %s", str(e), exc_info=True)
        raise e
