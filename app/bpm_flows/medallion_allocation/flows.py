from app.utils.logger import get_logger
from app.bpm.step_info import step
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.vehicles.services import vehicle_service
from app.medallions.schemas import MedallionStatus , VehicleAllocation , MedallionType
from app.vehicles.schemas import VehicleStatus
from app.medallions.services import medallion_service
from app.bpm_flows.allocate_medallion_vehicle.utils import format_vehicle_details

logger = get_logger(__name__)

entity_mapper = {
    "VEHICLE": "vehicle",
    "VEHICLE_IDENTIFIER": "id"
}

@step(step_id="196" , name="fetch- Vehicle Details" , operation='fetch')
def fetch_vehicle_details(db, case_no, case_params=None):
    """
    Fetch the vehicle information for the new vehicle step
    """
    try:
        logger.info("Fetch vehicle information")
        vehicle = None
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if not vehicle and case_params and case_params.get("object_name") == "vehicle":
            vehicle = vehicle_service.get_vehicles(
                db, vehicle_id=int(case_params['object_lookup'])
            )
        if not vehicle and case_entity:
            vehicle = vehicle_service.get_vehicles(
                db, vehicle_id=case_entity.identifier_value
            )
        
        if not vehicle:
            return {}

        vehicle_details = format_vehicle_details(vehicle)
        vehicle_details["medallion_details"] = {
            "medallion_number": vehicle.medallions.medallion_number if vehicle.medallions else None,
            "medallion_type": vehicle.medallions.medallion_type if vehicle.medallions else None,
            "medallion_status": vehicle.medallions.medallion_status if vehicle.medallions else None,
        }

        if not case_entity:
            case_entity = bpm_service.create_case_entity(
                db=db ,
                case_no=case_no,
                entity_name=entity_mapper["VEHICLE"],
                identifier=entity_mapper["VEHICLE_IDENTIFIER"],
                identifier_value=str(vehicle.id)
            )

        return vehicle_details
    except Exception as e:
        logger.error("Error fetching vehicle details: %s", e, exc_info=True)
        raise e

@step(step_id="196" , name="process- medallion allocation to Vehicle" , operation='process')
def process_medallion_allocation_to_vehicle(db, case_no, step_data):
    """
    Process the medallion allocation to vehicle
    """
    try:
        logger.info("Process medallion allocation to vehicle")
        vehicle = None

        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if case_entity:
            vehicle = vehicle_service.get_vehicles(db, vehicle_id=int(case_entity.identifier_value))
        if not vehicle:
            raise ValueError("Vehicle not found")

        if vehicle.is_medallion_assigned or vehicle.medallion_id:
            raise ValueError("Medallion already assigned to vehicle")

        if vehicle.vehicle_status != VehicleStatus.AVAILABLE:
            raise ValueError(f"Vehicle is not in {VehicleStatus.AVAILABLE.value} status")

        medallion = medallion_service.get_medallion(db, medallion_number=step_data.get("medallion_number"))

        if not medallion:
            raise ValueError("Medallion not found")
        if medallion.medallion_status != MedallionStatus.AVAILABLE:
            raise ValueError(f"Medallion is not in {MedallionStatus.AVAILABLE.value} status")

        storage = medallion_service.get_medallion_storage(
            db, medallion_number=medallion.medallion_number
        )
        if storage :
            if storage.retrieval_date is None:
                raise ValueError("The medallion is in storage so it cannot be assigned")
            
        VEHICLE_ALLOCATION = VehicleAllocation()

        if (medallion.medallion_type == MedallionType.WAV and vehicle.vehicle_type not in VEHICLE_ALLOCATION.Wav) or (medallion.medallion_type == MedallionType.REGULAR and vehicle.vehicle_type not in VEHICLE_ALLOCATION.Regular):
            raise ValueError("Vehicle and Medallion are Same Type , cannot be assigned")
    

        vehicle_service.upsert_vehicle(db=db , vehicle_data={
            "id": vehicle.id,
            "medallion_id": medallion.id,
            "is_medallion_assigned": True,
            "vehicle_status": VehicleStatus.AVAILABLE_FOR_HACK_UP
        })

        medallion_service.upsert_medallion(db=db , medallion_data={
            "id": medallion.id,
            "medallion_status": MedallionStatus.ASSIGNED_TO_VEHICLE
        })

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Medallion {medallion.medallion_number} allocated to vehicle {vehicle.vin}",
                meta_data={"medallion_id": medallion.id, "vehicle_id": vehicle.id}
            )

        return "Ok"
    except Exception as e:
        logger.error("Error processing medallion allocation to vehicle: %s", e, exc_info=True)
        raise e



