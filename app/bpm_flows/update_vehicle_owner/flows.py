## app/bpm_flows/update_vehicle_owner/flows.py

from sqlalchemy.orm import Session
from fastapi import HTTPException

from app.utils.logger import get_logger
from app.bpm.step_info import step
from app.core.config import settings
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.uploads.services import upload_service
from app.entities.services import entity_service
from app.vehicles.utils import format_vehicle_entity
from app.vehicles.services import vehicle_service

logger = get_logger(__name__)

entity_mapper = {
    "VEHICLE_OWNER": "vehicle_owner",
    "OWNER_IDENTIFIER": "id"
}

@step(step_id="180", name="Fetch - Vehicle Owner data", operation="fetch")
def fetch_vehicle_owner_detail(db: Session, case_no, case_params = None):
    """
    Fetches the details of the Vehicle Owner.
    """
    try:
        case_entity = bpm_service.get_case_entity(db=db, case_no=case_no)

        vehicle_owner = None

        if case_entity:
            vehicle_owner = vehicle_service.get_vehicle_entity(db=db , entity_id = case_entity.identifier_value)
        if case_params and case_params.get("object_name") == "vehicle_owner":
            vehicle_owner = vehicle_service.get_vehicle_entity(db=db , entity_id = case_params.get("object_lookup"))
        
        if not vehicle_owner:
            return {}
        
        ein = upload_service.get_documents(
            db=db , object_type="vehicle_owner", object_id=vehicle_owner.id , document_type="ein"
        )

        if not case_entity:
            case_entity = bpm_service.create_case_entity(
                db=db,
                case_no=case_no,
                entity_name=entity_mapper["VEHICLE_OWNER"],
                identifier=entity_mapper["OWNER_IDENTIFIER"],
                identifier_value=vehicle_owner.id
            )
        
        owner_details = format_vehicle_entity(vehicle_owner)
        owner_details["documents"] = [ein]
        owner_details["document_details"] = {
            "object_type": "vehicle_owner",
            "object_id": vehicle_owner.id,
            "document_type": ["ein"]
        }

        return owner_details
    except Exception as e:
        logger.error("Error fetching vehicle owner details: %s", str(e))
        raise HTTPException(status_code=500, detail="Error fetching vehicle owner details") from e
    
@step(step_id="180", name="Update - Vehicle Owner", operation="process")
def update_vehicle_owner(db: Session, case_no, step_data):
    """
    Updates the details of the Vehicle Owner.
    """
    try:
        case_entity = bpm_service.get_case_entity(db=db, case_no=case_no)
        vehicle_owner = None
        if not case_entity:
            return {}
        
        vehicle_owner = vehicle_service.get_vehicle_entity(db=db , entity_id= case_entity.identifier_value)
        
        if not vehicle_owner:
            raise HTTPException(status_code=500, detail="Vehicle Owner Not Found")
        
        entity_data = step_data.get("entity_details")

        vehicle_owner_data = {
            "id": vehicle_owner.id if vehicle_owner else None,
            "entity_name": entity_data.get("entity_name"),
            "ein": entity_data.get("ein"),
        }

        entity_address = {
            "address_line_1": entity_data.get("address_line_1"),
            "address_line_2": entity_data.get("address_line_2"),
            "city": entity_data.get("city"),
            "state": entity_data.get("state"),
            "zip": entity_data.get("zip"),
            "po_box": entity_data.get("po_box")
            }
        if entity_address:
            address = entity_service.upsert_address(db=db , address_data= entity_address)
            if address:
                vehicle_owner_data["entity_address_id"] = address.id
        
        if vehicle_owner_data:
            owner = vehicle_service.upsert_vehicle_entity(db=db , entity_data= vehicle_owner_data)
            if not owner:
                raise HTTPException(status_code=500, detail="Error creating vehicle owner")
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Updated vehicle owner {vehicle_owner.entity_name}",
                meta_data={"vehicle_owner_id": vehicle_owner.id}
            )

        return "Ok"
    except Exception as e:
        logger.error("Error updating vehicle owner: %s", str(e))
        raise HTTPException(status_code=500, detail="Error updating vehicle owner") from e