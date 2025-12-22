## app/bpm_flows/edit_owner/flows.py

from datetime import datetime
# Third party imports
from fastapi import HTTPException

# Local imports
from app.utils.logger import get_logger
from app.bpm.step_info import step
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.entities.services import entity_service
from app.medallions.services import medallion_service
from app.uploads.services import upload_service

logger = get_logger(__name__)

entity_mapper = {
    "OWNER": "owner",
    "OWNER_IDENTIFIER": "id"
}

@step(step_id="182" , name="Fetch Uploaded documents" , operation="fetch")
def fetch_uploaded_documents(db, case_no, case_params=None):
    """Fetch Uploaded documents"""
    try:
        case_entity = bpm_service.get_case_entity(db=db , case_no=case_no)
        owner = None

        if case_entity:
            owner = medallion_service.get_medallion_owner(db=db , medallion_owner_id=case_entity.identifier_value)
        if case_params:
            owner = medallion_service.get_medallion_owner(db=db , medallion_owner_id=case_params["object_lookup"])

        if not owner:
            return {}
        
        ssn_document = upload_service.get_documents(db=db , object_type="medallion_owner" , object_id=owner.id , document_type="ssn")
        license_document = upload_service.get_documents(db=db , object_type="medallion_owner" , object_id=owner.id , document_type="license")
        passport = upload_service.get_documents(db=db , object_type="medallion_owner" , object_id=owner.id , document_type="passport")
        ein_document = upload_service.get_documents(db=db , object_type="medallion_owner" , object_id=owner.id , document_type="ein")

        corporation_documents = {
            "documents": [ein_document],
            "required": ["ein"]
        }

        individual_documents = {
            "documents": [ssn_document, license_document, passport],
            "required": ["ssn", "license"]
        }

        if not case_entity:
            case_entity = bpm_service.create_case_entity(
                db=db , 
                case_no=case_no,
                entity_name=entity_mapper["OWNER"],
                identifier=entity_mapper["OWNER_IDENTIFIER"],
                identifier_value=owner.id
            )

        return individual_documents if owner.medallion_owner_type == "I" else corporation_documents
    except Exception as e:
        logger.error(f"Error fetching uploaded documents: {e}")
        raise HTTPException(status_code=500, detail="Error fetching uploaded documents")
    
@step(step_id="182" , name="Process Driver Data" , operation="process")
def process_driver_data(db, case_no, step_data):
    """Process Driver Data"""
    try:
        # Implement the logic to process driver data
        logger.info("Nothing to Do Here")
        return "Ok"
    except Exception as e:
        logger.error(f"Error processing driver data: {e}")
        raise HTTPException(status_code=500, detail="Error processing driver data")

@step(step_id="183" , name="Fetch- Owner Details" , operation="fetch")
def fetch_owner_details(db, case_no, case_params=None):
    """Fetch Owner Details"""
    try:
        # Implement the logic to fetch owner details
        case_entity = bpm_service.get_case_entity(db=db, case_no=case_no)
        if not case_entity:
            return {}
        owner = medallion_service.get_medallion_owner(db=db , medallion_owner_id=case_entity.identifier_value)

        if not owner:
            raise HTTPException(status_code=404, detail="Owner not found")
        
        individual_details = {}
        corporation_details = {}

        if owner.medallion_owner_type == "I":
            individual_details = {
                "type": "individual",
                "first_name": owner.individual.first_name if owner.individual.first_name else None,
                "middle_name": owner.individual.middle_name if owner.individual.middle_name else None,
                "last_name": owner.individual.last_name if owner.individual.last_name else None,
                "full_name": owner.individual.full_name if owner.individual.full_name else None,
                "ssn": owner.individual.masked_ssn if owner.individual.masked_ssn else None,
                "passport": owner.individual.passport if owner.individual.passport else None,
                "passport_expiration": owner.individual.passport_expiry_date if owner.individual.passport_expiry_date else None,
                "driving_license": owner.individual.driving_license if owner.individual.driving_license else None,
                "driving_license_expiration": owner.individual.driving_license_expiry_date if owner.individual.driving_license_expiry_date else None
            }
        else:
            corporation_details = {
                "type": "corporation",
                "corporation_name": owner.corporation.name if owner.corporation.name else None,
                "ein": owner.corporation.ein if owner.corporation.ein else None,
            }
        return individual_details if owner.medallion_owner_type == "I" else corporation_details
    except Exception as e:
        logger.error(f"Error fetching owner details: {e}")
        raise HTTPException(status_code=500, detail="Error fetching owner details")
    
@step(step_id="183" , name="Process - Owner Details" , operation="process")
def process_owner_details(db, case_no, step_data):
    """Process Owner Details"""
    try:
        case_entity = bpm_service.get_case_entity(db=db, case_no=case_no)
        if not case_entity:
           return {}
        owner = None

        owner = medallion_service.get_medallion_owner(db=db , medallion_owner_id=case_entity.identifier_value)

        if not owner:
            raise HTTPException(status_code=404, detail="Owner not found")
        
        if owner.medallion_owner_type == "I" and step_data.get("edit_type")=="individual":
            individual_owner = step_data.get("individual_owner")
            individual_data = {
                "id":owner.individual.id,
                "first_name": individual_owner.get("first_name"),
                "middle_name": individual_owner.get("middle_name"),
                "last_name": individual_owner.get("last_name"),
                "ssn": individual_owner.get("masked_ssn"),
                "passport": individual_owner.get("passport"),
                "driving_license": individual_owner.get("driving_license"),
                "dob": individual_owner.get("dob"),
                "full_name": " ".join(filter(None, [part.strip() if part else None for part in [individual_owner.get("first_name"), individual_owner.get("middle_name"), individual_owner.get("last_name")]]))
            }
            ind = entity_service.upsert_individual(db=db, individual_data=individual_data)
            if not ind:
                raise HTTPException(status_code=400, detail="Error updating individual owner details")
        elif owner.medallion_owner_type == "C" and step_data.get("edit_type")=="corporation":
            corporation_owner = step_data.get("corporation_owner")
            corporation_data = {
                "id": owner.corporation.id,
                "name": corporation_owner.get("corporation_name"),
                "ein": corporation_owner.get("ein")
            }
            corp = entity_service.upsert_corporation(db=db, corporation_data=corporation_data)
            if not corp:
                raise HTTPException(status_code=400, detail="Error updating corporation owner details")
        else:
            raise HTTPException(status_code=400, detail="Mismatch in owner type and edit type")
        
        cases = bpm_service.get_cases(db=db , case_no= case_no ,multiple=True)
        if cases:
            case_ids = [case.id for case in cases]
            audits = audit_trail_service.get_audit_trail_by_case_ids(db, case_ids)
            for audit in audits:
                audit_trail_service.update_audit_trail(db=db , audit_id=audit.id , update_data={"meta_data": {"medallion_owner_id": owner.id}})

        return "Ok"
    except Exception as e:
        logger.error(f"Error processing owner details: {e}")
        raise HTTPException(status_code=500, detail="Error processing owner details")