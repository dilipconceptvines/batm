## app/bpm_flows/update_medallion_payee/flows.py

# Third party imports
from fastapi import HTTPException
import json
import requests

# Local imports
from app.bpm.step_info import step
from app.utils.logger import get_logger
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.medallions.schemas import MedallionOwnerType
from app.medallions.services import medallion_service
from app.uploads.services import upload_service
from app.entities.services import entity_service
from app.core.config import settings
from app.utils.s3_utils import s3_utils
from app.utils.lambda_utils import invoke_lambda_function
from app.medallions.utils import format_medallion_response , format_medallon_owner

logger = get_logger(__name__)
entity_mapper = {
    "MEDALLION_OWNER": "medallion_owner",
    "MEDALLION_IDENTIFIER": "id"
}

@step(step_id="117", name="Fetch - Medallion Payee Documents", operation='fetch')
def fetch_medallion_owner_address_documents(db, case_no, case_params=None):
    """
    Fetch the medallion payee documents for the update payee step
    """
    try:
        logger.info("Fetch - Medallion Payee Documents")
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        medallion_owner_payee_info = {}
        owner_info = ""

        if case_params:
            owner_info = medallion_service.get_medallion_owner(
                db, medallion_owner_id=case_params['object_lookup']
            )
        if case_entity:
            owner_info = medallion_service.get_medallion_owner(
                db, medallion_owner_id=int(case_entity.identifier_value)
            )

        if not owner_info:
            return medallion_owner_payee_info
        
        medallion_owner_payee_info = format_medallon_owner(owner_info)

        payee_proof = upload_service.get_documents(
            db, object_type="medallion_owner",
            object_id=str(owner_info.id),
            document_type="owner_payee_proof"
        )

        medallion_owner_payee_info["object_type"] = "medallion_owner"
        medallion_owner_payee_info["document_type"] = "owner_payee_proof"

        medallion_owner_payee_info["owner_payee_proofs"] = [payee_proof]

        # Create case entity if it doesn't exist
        if not case_entity:
            case_entity = bpm_service.create_case_entity(
                db, case_no=case_no,
                entity_name=entity_mapper['MEDALLION_OWNER'],
                identifier=entity_mapper['MEDALLION_IDENTIFIER'],
                identifier_value=str(owner_info.id)
            )
        return medallion_owner_payee_info
    except Exception as e:
        logger.error("Error fetching medallion payee documents: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e

@step(step_id="117", name="Process - Medallion Payee Documents", operation='process')
def process_medallion_owner_address_documents(db, case_no, step_data):
    """
    Process the medallion payee documents for the update payee step
    """
    try:
        logger.info("Process - Medallion Payee Documents")
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        logger.info("Nothing to be done here, use the documents api")
        
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Processed medallion payee documents for owner {case_entity.identifier_value}",
                meta_data={"medallion_owner_id": case_entity.identifier_value}
            )
        return {}
    except Exception as e:
        logger.error("Error processing medallion payee documents: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e
    
@step(step_id="118", name="Fetch - Medallion Payee Details", operation='fetch')
def fetch_medallion_payee_details(db, case_no, case_params=None):
    """
    Fetch the medallion payee details for the update payee step
    """
    try:
        logger.info("Fetch - Medallion Payee Details")
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        owner_info = ""
        bank_account = None
        
        if case_params:
            owner_info = medallion_service.get_medallion_owner(
                db, medallion_owner_id=case_params['object_lookup']
            )
        if case_entity:
            owner_info = medallion_service.get_medallion_owner(
                db, medallion_owner_id=int(case_entity.identifier_value)
            )

        if not owner_info:
            return {}

        owner_data = format_medallon_owner(owner_info)

        extracted_data = {}

        medallion_payee_proof = upload_service.get_documents(
            db=db , object_type="medallion_owner",
            object_id=str(owner_info.id),
            document_type="owner_payee_proof",
        )

        logger.info("loging lamda function with payload: %s" , medallion_payee_proof)

        extracted_data = {}
        metadata = {}

        if medallion_payee_proof and medallion_payee_proof.get("document_path" , None):
            metadata = s3_utils.get_file_metadata(medallion_payee_proof["document_path"])
            metadata = metadata if metadata else {}
            metadata = metadata.get("extracted_data", {}).get("extracted_data", {})
            extracted_data.update(metadata)

        logger.info("$$$$$#######Extracted data from payee proof: %s", extracted_data)

        if extracted_data:
            payee_details = owner_data.get("payee_details" , {})
            if payee_details.get("pay_to_mode") != "Check":
                owner_data["payee_details"]["pay_to_mode"] = "ACH"
                
                payee_details["data"]["bank_name"] = None
                payee_details["data"]["bank_account_name"] = None
                payee_details["data"]["bank_account_number"] = None
                payee_details["data"]["bank_routing_number"] = None
                payee_details["data"]["effective_from"] = None
                
                if extracted_data.get("bank_name" , None):
                    payee_details["data"]["bank_name"] = extracted_data.get("bank_name" , None)
                if extracted_data.get("account_holder_name" , None):
                    payee_details["data"]["bank_account_name"] = extracted_data.get("account_holder_name" , None)
                if extracted_data.get("account_number" , None):
                    payee_details["data"]["bank_account_number"] = extracted_data.get("account_number" , None)

        return {
            "owner_id": owner_info.id,
            "look_up_id": owner_data["look_up_id"],
            "owner_name": owner_data["owner_name"],
            "primary_contact_number": owner_data["primary_contact_number"],
            "primary_email_address": owner_data["primary_email_address"],
            "ssn": owner_data.get("ssn") or None,
            "ein": owner_data.get("ein") or None,
            "passport": owner_data.get("passport") or None,
            "owner_address_info": owner_data["primary_address"],
            "medallion_payee_info": owner_data.get("payee_details" , {}),
            "medallion_payee_proofs": [medallion_payee_proof] if medallion_payee_proof else []
        }
    
    except Exception as e:
        logger.error("Error fetching medallion payee details: %s", str(e), exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e

@step(step_id="118", name="Process - Medallion Payee Details", operation='process')
def process_medallion_payee_details(db, case_no, step_data):
    """
    Process the medallion payee details for the update payee step
    """
    try:
        logger.info("Process - Medallion Payee Details")
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        owner = None
        if case_entity:
            owner = medallion_service.get_medallion_owner(db, medallion_owner_id=case_entity.identifier_value)
        elif step_data.get("owner_id"):
            owner = medallion_service.get_medallion_owner(db, medallion_owner_id=step_data["owner_id"])
            
        bank_account = None

        if not owner:
            raise ValueError("Medallion owner not found")
        
        owner_data = format_medallon_owner(owner)

        if owner_data.get("payee_details" , {}).get("pay_to_mode" , None) == "ACH":
            bank_account = owner_data.get("payee_details" , {}).get("data" , None)

        if step_data['payto'] == "check":
            if owner.medallion_owner_type == MedallionOwnerType.INDIVIDUAL:
                entity_service.upsert_individual(db=db , individual_data={"id": owner_data.get("look_up_id"),"payee": step_data['payee'] ,"pay_to_mode": "Check","bank_account_id": None})
            else:
                entity_service.upsert_corporation(db=db , corporation_data={"id": owner_data.get("look_up_id"),"payee": step_data['payee'] ,"pay_to_mode": "Check","bank_account_id": None})
            if bank_account:
                logger.info("Removing bank account %s", bank_account.get("id" , None))
                entity_service.delete_bank_account(db, bank_account_id=bank_account.get("id" , None))
            else:
                logger.info("No bank account to dissociate")

        if step_data['payto'] == 'ACH':
            if bank_account:
                bank_account = entity_service.upsert_bank_account(
                    db, {
                        "id": bank_account.get("id" , None),
                        "pay_to_mode": "ACH",
                        **step_data
                    }
                )
            else:
                bank_account = entity_service.upsert_bank_account(db, step_data)
                if owner.medallion_owner_type == MedallionOwnerType.INDIVIDUAL:
                    entity_service.upsert_individual(db=db , individual_data={
                        "id": owner_data.get("look_up_id"),
                        "bank_account_id": bank_account.id,
                        "pay_to_mode": "ACH",
                        "payee": None
                    })
                else:
                    entity_service.upsert_corporation(db=db , corporation_data={
                        "id": owner_data.get("look_up_id"),
                        "bank_account_id": bank_account.id,
                        "pay_to_mode": "ACH",
                        "payee": None
                    })

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Updated medallion payee details for owner {owner.id}",
                meta_data={"medallion_owner_id": owner.id}
            )

        return "Ok"
    except Exception as e:
        logger.error("Error processing medallion payee details: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e