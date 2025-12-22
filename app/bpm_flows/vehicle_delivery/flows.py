from app.utils.logger import get_logger
from app.bpm.step_info import step
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.vehicles.services import vehicle_service
from app.vehicles.schemas import VehicleStatus
from app.bpm_flows.allocate_medallion_vehicle.utils import format_vehicle_details

logger = get_logger(__name__)

entity_mapper = {
    "VEHICLE": "vehicles",
    "VEHICLE_IDENTIFIER": "id",
}

@step(step_id="195" , name="fetch- Vehicle Delivery Details" , operation='fetch')
def fetch_vehicle_delivery_details(db, case_no, case_params=None):
    """
    Fetch the vehicle delivery details
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        vehicle = None
        if not vehicle and case_params and case_params.get("object_name") == "vehicle":
            vehicle = vehicle_service.get_vehicles(db, vehicle_id=case_params.get("object_lookup"))
        if not vehicle and case_entity:
            vehicle = vehicle_service.get_vehicles(db, vehicle_id=int(case_entity.identifier_value))

        if not vehicle:
            return {}
        
        vehicle_details = format_vehicle_details(vehicle)
        vehicle_details["delivery_details"] = {
            "expected_delivery_date": vehicle.expected_delivery_date,
            "delivery_location": vehicle.delivery_location,
            "delivery_note": vehicle.delivery_note,
            "is_delivered": vehicle.is_delivered
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
        logger.error("Error fetching vehicle delivery details: %s", e, exc_info=True)
        raise e
    
@step(step_id="195" , name="process- Vehicle Delivery Details" , operation='process')
def process_vehicle_delivery_details(db, case_no, step_data):
    """
    Process the vehicle delivery details
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        vehicle = None
        if case_entity:
            vehicle = vehicle_service.get_vehicles(db, vehicle_id=int(case_entity.identifier_value))
        if not vehicle:
            return {}
        
        if vehicle.vehicle_status != VehicleStatus.PENDING_DELIVERY:
            raise ValueError("Vehicle is not in pending delivery status")
        if vehicle.is_delivered:
            raise ValueError("Vehicle is already delivered")
        
        vehicle_data = {
            "id": vehicle.id if vehicle else None,
            "expected_delivery_date": step_data.get("delivery_date"),
            "delivery_location": step_data.get("delivery_location"),
            "current_location": step_data.get("delivery_location"),
            "location_changed_date": step_data.get("delivery_date"),
            "delivery_note": step_data.get("delivery_note"),
            "is_delivered": step_data.get("is_delivered"),
            "vehicle_status" : VehicleStatus.AVAILABLE
        }

        vehicle_service.upsert_vehicle(
            db=db , vehicle_data=vehicle_data
        )

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Marked vehicle {vehicle.vin} as {VehicleStatus.AVAILABLE.value} with delivery details",
                meta_data={"vehicle_id": vehicle.id}
            )

        return "Ok"
    except Exception as e:
        logger.error("Error processing vehicle delivery details: %s", e, exc_info=True)
        raise e