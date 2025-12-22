## app/bpm_flows/create_vehicle_owner/flows.py

from datetime import datetime
import json

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
from app.vehicles.schemas import VehicleEntityStatus
from app.utils.s3_utils import s3_utils
from app.utils.general import fill_if_missing

logger = get_logger(__name__)

entity_mapper = {
    "VEHICLE_OWNER": "vehicle_owner",
    "OWNER_IDENTIFIER": "id"
}

@step(step_id="179" , name="Update -Verify Documents Of Vehicle Owner", operation="fetch")
def fetch_owner_documents(db: Session, case_no, case_params = None):
    """
    Fetches the documents of the Vehicle Owner.
    """
    try:
        case_entity = bpm_service.get_case_entity(db=db, case_no=case_no)
        
        vehicle_owner = None

        if case_entity:
            vehicle_owner = vehicle_service.get_vehicle_entity(db=db , entity_id= case_entity.identifier_value)
        
        if not vehicle_owner:
            vehicle_owner = vehicle_service.upsert_vehicle_entity(db=db , entity_data={})
            if not vehicle_owner:
                raise HTTPException(status_code=500, detail="Error creating entity")
        
        if not case_entity:
            case_entity = bpm_service.create_case_entity(db=db,
                                                         case_no=case_no,
                                                         entity_name=entity_mapper["VEHICLE_OWNER"],
                                                         identifier=entity_mapper["OWNER_IDENTIFIER"],
                                                         identifier_value=vehicle_owner.id)
        
        ein = upload_service.get_documents(
            db=db , object_type="vehicle_owner", object_id=vehicle_owner.id , document_type="ein"
        )

        document_data = {
            "documents" : [ein],
            "document_details" : {
                "object_type": "vehicle_owner",
                "object_id": vehicle_owner.id,
                "document_type": ["ein"]
            },
            "required_documents": ["ein"]
        }

        return document_data
    except Exception as e:
        logger.error("Error fetching owner documents: %s", str(e))
        raise HTTPException(status_code=500, detail="Error fetching owner documents") from e
    
@step(step_id="179" , name= "Process - Verify Documents Of Vehicle Owner", operation="process")
def process_owner_documents(db: Session, case_no, step_data):
    """
    Processes the documents of the Vehicle Owner.
    """
    try:
        logger.info("Processing owner documents")

        case_entity = bpm_service.get_case_entity(db=db, case_no=case_no)

        if not case_entity:
            return {}
        
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Verified owner documents",
                meta_data={"vehicle_owner_id": case_entity.identifier_value}
            )
        
        return "Ok"
    except Exception as e:
        logger.error("Error processing owner documents: %s", str(e))
        raise HTTPException(status_code=500, detail="Error processing owner documents") from e
    
@step(step_id="178" , name="Fetch - Vehicle Owner Details", operation="fetch")
def fetch_vehicle_owner_details(db: Session, case_no, case_params = None):
    """
    Fetches the details of the Vehicle Owner.
    """
    try:
        case_entity = bpm_service.get_case_entity(db=db, case_no=case_no)

        if not case_entity:
            return {}

        vehicle_owner = vehicle_service.get_vehicle_entity(db=db , entity_id = case_entity.identifier_value)

        if not vehicle_owner:
            raise HTTPException(status_code=500, detail="Entity Not Found")
        ein_document = upload_service.get_documents(
            db=db , object_type="vehicle_owner", object_id=vehicle_owner.id , document_type="ein"
        )
       
        owner_details = format_vehicle_entity(vehicle_owner)
        extracted_data = {}
        metadata = {}

        if ein_document and ein_document.get("document_path"):
            metadata = s3_utils.get_file_metadata(ein_document["document_path"])
            metadata = metadata if metadata else {}
            metadata = metadata.get("extracted_data" , {}).get("extracted_data" ,{})
            extracted_data[ein_document.get("document_type")] = metadata
        
        logger.info(f"#### OCR : {extracted_data}###$$$")

        ein = extracted_data.get("ein", {})
        if ein:
            fill_if_missing(owner_details , "entity_name" , ein , "names")
            fill_if_missing(owner_details , "ein" , ein , "amounts")

        owner_details["documents"] = [ein_document]
        owner_details["document_details"] = {
            "object_type": "vehicle_owner",
            "object_id": vehicle_owner.id,
            "document_type": ["ein"]
        }

        return owner_details
    except Exception as e:
        logger.error("Error fetching vehicle owner details: %s", str(e))
        raise HTTPException(status_code=500, detail="Error fetching vehicle owner details") from e
@step(step_id="178" , name="Create - Vehicle Owner", operation="process")
def create_vehicle_owner(db: Session, case_no, step_data):
    """
    Creates the details of the Vehicle Owner.
    """
    try:
        case_entity = bpm_service.get_case_entity(db=db, case_no=case_no)
        vehicle_owner = None

        if case_entity:
            vehicle_owner = vehicle_service.get_vehicle_entity(db=db , entity_id= case_entity.identifier_value)
        
        if not vehicle_owner:
            raise HTTPException(status_code=500, detail="Entity Not Found")
        
        entity_data = step_data.get("entity_details")

        is_ein = vehicle_service.get_vehicle_entity(db=db , ein= entity_data.get("ein"))
        if is_ein and is_ein.id != vehicle_owner.id:
            raise HTTPException(status_code=500, detail="Entity Already Exists")

        vehicle_owner_data = {
            "id": vehicle_owner.id if vehicle_owner else None,
            "entity_name": entity_data.get("entity_name"),
            "ein": entity_data.get("ein"),
            "entity_status": VehicleEntityStatus.ACTIVE
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
                description=f"Created vehicle owner {vehicle_owner_data.get('entity_name')}",
                meta_data={"vehicle_owner_id": vehicle_owner.id}
            )


        return "Ok"
    except Exception as e:
        logger.error("Error creating vehicle owner: %s", str(e))
        raise HTTPException(status_code=500, detail="Error creating vehicle owner") from e
        