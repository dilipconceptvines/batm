# Third party imports
from datetime import datetime

from sqlalchemy.orm import Session

from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.bpm.step_info import step
from app.bpm_flows.allocate_medallion_vehicle.utils import format_vehicle_details

# Local imports
from app.utils.logger import get_logger
from app.vehicles.schemas import VehicleLocation, VehicleStatus
from app.vehicles.services import vehicle_service

logger = get_logger(__name__)

entity_mapper = {
    "VEHICLE": "vehicles",
    "VEHICLE_IDENTIFIER": "id",
}

VEHICLE_LOCATION_RULES = {
    VehicleStatus.AVAILABLE_FOR_HACK_UP.value: {
        "editable": True,
        "allowed_locations": [
            VehicleLocation.BAT_OFFICE.value,
            VehicleLocation.BAT_GARAGE.value,
            VehicleLocation.PARKING_LOT_1.value,
            VehicleLocation.PARKING_LOT_2.value,
            VehicleLocation.HACKUP_VENDOR_A.value,
            VehicleLocation.HACKUP_VENDOR_B.value,
            VehicleLocation.HACKUP_VENDOR_C.value,
            VehicleLocation.HACKUP_VENDOR_D.value,
            VehicleLocation.OTHER.value,
        ],
        "allow_free_text": False,
        "default_location": VehicleLocation.BAT_GARAGE.value,
    },
    VehicleStatus.HACKED_UP.value: {
        "editable": True,
        "allowed_locations": [
            VehicleLocation.BAT_OFFICE.value,
            VehicleLocation.BAT_GARAGE.value,
            VehicleLocation.PARKING_LOT_1.value,
            VehicleLocation.PARKING_LOT_2.value,
            VehicleLocation.OTHER.value,
        ],
        "allow_free_text": False,
    },
    VehicleStatus.OUT_OF_SERVICE.value: {
        "editable": True,
        "allowed_locations": None,  # free text
        "allow_free_text": True,
    },
}


@step(step_id="224", name="Fetch - Vehicle Info", operation="fetch")
def fetch_vehicle_info(db: Session, case_no, case_params=None):
    """
    Fetch the vehicle info along with location rules
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        vehicle = None

        # Fetch vehicle
        if case_params and case_params.get("object_name") == "vehicle":
            vehicle = vehicle_service.get_vehicles(
                db, vehicle_id=case_params.get("object_lookup")
            )
        elif case_entity:
            vehicle = vehicle_service.get_vehicles(
                db, vehicle_id=int(case_entity.identifier_value)
            )

        if not vehicle:
            return {}

        # Create case entity if missing
        if not case_entity:
            bpm_service.create_case_entity(
                db=db,
                case_no=case_no,
                entity_name=entity_mapper["VEHICLE"],
                identifier=entity_mapper["VEHICLE_IDENTIFIER"],
                identifier_value=vehicle.id,
            )

        vehicle_details = format_vehicle_details(vehicle)
        vehicle_details["current_location"] = {
            "current_location": vehicle.current_location,
            "last_location_change_date": vehicle.location_changed_date,
            "comment": vehicle.comment_for_location_change,
        }

        allowed_locations = VEHICLE_LOCATION_RULES.get(vehicle.vehicle_status, {})

        return {
            "vehicle_details": vehicle_details,
            "allowed_locations": allowed_locations,
        }

    except Exception as e:
        logger.exception("Error in fetch_vehicle_info")
        raise


@step(step_id="224", name="Update - Vehicle Info", operation="process")
def update_vehicle_info(db: Session, case_no, step_data):
    """
    Update the vehicle info
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}

        vehicle = vehicle_service.get_vehicles(
            db=db, vehicle_id=int(case_entity.identifier_value)
        )
        if not vehicle:
            raise ValueError("Vehicle not found")

        if vehicle.vehicle_status not in [
            VehicleStatus.AVAILABLE_FOR_HACK_UP.value,
            VehicleStatus.OUT_OF_SERVICE.value,
            VehicleStatus.HACKED_UP.value,
        ]:
            raise ValueError(
                f"Update Location Not Allowed For status: {vehicle.vehicle_status}"
            )

        past_location = vehicle.current_location

        if vehicle.vin != step_data.get("vin"):
            raise ValueError(
                f"vehicle vin: {vehicle.vin} is not same as passed vin: {step_data.get('vin')}"
            )

        new_location = step_data.get("new_location", None)

        if not new_location:
            raise ValueError("New location is required")

        if new_location == past_location:
            raise ValueError("New location is same as old location")

        if new_location == "Other":
            new_location = step_data.get("specific_location", None)

        new_location_data = {
            "id": vehicle.id,
            "current_location": new_location,
            "comment_for_location_change": step_data.get("comment", None),
            "location_changed_date": datetime.utcnow().date(),
        }

        vehicle_service.upsert_vehicle(db=db, vehicle_data=new_location_data)

        audit_comment = f"Vehicle vin {vehicle.vin} location changed from ({past_location}) to ({new_location}). Comment: {step_data.get('comment', '')}"

        case = bpm_service.get_cases(db=db, case_no=case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=audit_comment,
                meta_data={"vehicle_id": vehicle.id},
            )

        return "Ok"
    except Exception as e:
        logger.error(f"Error in update_vehicle_info: {e}")
        raise e
