## app/bpm_flows/update_medallion/flows.py

from fastapi import HTTPException
from sqlalchemy.orm import Session

# Local imports
from app.bpm.step_info import step
from app.utils.logger import get_logger
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.medallions.services import medallion_service
from app.uploads.services import upload_service
from app.bpm_flows.newmed.utils import (
    format_individual_info, format_corporation_info, format_medallion_basic_details,
    format_medallion_lease
)

logger = get_logger(__name__)

entity_mapper = {
    "MEDALLION": "medallions",
    "MEDALLION_IDENTIFIER": "id"
}

@step(step_id="162", name="fetch Medallion Details", operation='fetch')
def fetch_medallion_details(db: Session, case_no: str, case_params=None):
    """
    Fetch the medallion details for the new medallion step
    """

    logger.info("Fetch-Choose medallion info")
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        medallion = None

        if case_params :
            medallion = medallion_service.get_medallion(db=db , medallion_number=case_params["object_lookup"])
        if case_entity:
            medallion = medallion_service.get_medallion(db, medallion_id=case_entity.identifier_value)
        if not medallion:
            logger.info("No medallion found in the database")
            return {}
        
        bpm_service.create_case_entity(
            db=db, case_no=case_no,
            entity_name=entity_mapper['MEDALLION'], identifier=entity_mapper['MEDALLION_IDENTIFIER'],
            identifier_value=str(medallion.id)
        )
        medallion_basic_details = {}
        medallion_owner = medallion_service.get_medallion_owner(db, medallion_owner_id=medallion.owner_id)

        medallion_basic_details.update(
            format_medallion_basic_details(medallion, medallion_owner)
        )
        renewal_document = upload_service.get_documents(
            db, object_type="medallion", object_id=medallion.id, document_type="renewal_receipt"
        )
        fs6_document = upload_service.get_documents(
            db, object_type="medallion", object_id=medallion.id, document_type="fs6"
        )
        medallion_basic_details.update({
            'object_type': "medallion",
            'last_renewal_date': medallion.last_renewal_date,
            'valid_from': medallion.validity_start_date,
            'valid_to': medallion.validity_end_date,
            'renewal_receipt_document': renewal_document,
            'medallion_storage': '',
            'renewalReceiptPath': medallion.renewal_path,
            'fs6_status': medallion.fs6_status,
            'fs6_update_date': medallion.fs6_date,
            'fs6_document': fs6_document,
            'agent_name': medallion.agent_name,
            'agent_number': medallion.agent_number,
            'first_signed': medallion.first_signed,
            'amount': medallion.default_amount
        })
        return medallion_basic_details
    except Exception as e:
        logger.error("Error fetching medallion owner information: %s", str(e))
        raise e
    

@step(step_id="162", name="Enter Medallion Details", operation='process')
def enter_medallion_details(db, case_no, step_data):
    """
    Process the medallion details for the new medallion step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        medallion_info = None

        if case_entity:
            medallion_info = medallion_service.get_medallion(db, medallion_id=case_entity.identifier_value)

        if not medallion_info :
            raise HTTPException(status_code=400, detail="Medallion not found")
        
        medallion_data = {
            "id": medallion_info.id,
            "medallion_type": step_data.get("medallionType"),
            "renewal_path": step_data.get("renewalReceiptPath"),
            "first_signed": step_data.get("firstSignedDate"),
        }
        
        medallion_service.upsert_medallion(db ,medallion_data)
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            medallion_owner = medallion_service.get_medallion_owner(db, medallion_owner_id=medallion_info.owner_id)
            if medallion_owner:
                audit_trail_service.create_audit_trail(
                    db=db,
                    case=case,
                    description=f"Updated medallion details for medallion {medallion_info.medallion_number} for owner {medallion_owner.id}",
                    meta_data={"medallion_owner_id": medallion_owner.id , "medallion_id": medallion_info.id}
                )

        return "Ok"
    except Exception as e:
        logger.error("Error entering medallion details: %s", str(e))
        raise e

