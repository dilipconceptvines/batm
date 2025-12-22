## app/bpm_flows/update_driver_payee/flows.py

# Local imports
from datetime import datetime
import json
import requests

# Third party imports
from fastapi import HTTPException
from sqlalchemy.orm import Session

# Local imports
from app.bpm.step_info import step
from app.utils.logger import get_logger
from app.audit_trail.services import audit_trail_service
from app.core.config import settings
from app.bpm.services import bpm_service
from app.drivers.services import driver_service
from app.utils.lambda_utils import invoke_lambda_function
from app.entities.services import entity_service
from app.uploads.services import upload_service
from app.drivers.utils import format_driver_response
from app.utils.document_processor import document_processor
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)

entity_mapper = {
    "DRIVER": "drivers",
    "DRIVER_IDENTIFIER": "driver_id",
}


@step(step_id="150", name="Fetch Driver Payee Proofs", operation="fetch")
def fetch_payee_proofs(db: Session, case_no: str, case_params: dict):
    """Fetch the payee proofs for the driver"""
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        driver = None
        if case_params:
            driver = driver_service.get_drivers(db, driver_id=case_params.get("object_lookup"))
        if case_entity:
            driver = driver_service.get_drivers(db, driver_id=case_entity.identifier_value)
        if not driver:
            return {}
        
        if not case_entity:
            bpm_service.create_case_entity(
                db, case_no=case_no, entity_name=entity_mapper['DRIVER'],
                identifier=entity_mapper['DRIVER_IDENTIFIER'],
                identifier_value=driver.driver_id
            )
        
        driver_payee_proof = upload_service.get_documents(db,
                object_type="driver",
                object_id=str(driver.id),
                document_type="driver_payee_proof"
            )
        
        driver_data = format_driver_response(driver, False)
        return {
            "driver_info": {
                **driver_data["driver_details"],
                "driver_seq_id": driver.id,
                "tlc_license": driver_data['tlc_license_details']['tlc_license_number'] if driver_data['tlc_license_details']['tlc_license_number'] else "",
                "dmv_license": driver_data['dmv_license_details']['dmv_license_number'] if driver_data['dmv_license_details']['dmv_license_number'] else "",
            },
            "driver_document_info": {
                "object_type": "driver",
                "document_type": "driver_payee_proof",
                "object_id": driver.id
            },
            "driver_payee_proofs": [driver_payee_proof]
        }        
    except Exception as e:
        logger.info("Error fetching payee proofs", exc_info=e)
        raise HTTPException(status_code=500, detail=str(e)) from e
    
@step(step_id="150", name="Process payee proof", operation="process")
def process_payee_proof(db: Session, case_no: str, step_data: dict):
    """Process the payee proof"""
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        driver = None
        driver = driver_service.get_drivers(db, driver_id=step_data.get("driver_id"))
        if not driver:
            raise HTTPException(status_code=404, detail="Driver not found")
        
        driver_data = format_driver_response(driver, False)

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Processed payee proof for driver {driver.driver_id}",
                meta_data={"driver_id": driver.id}
            )
        # if not exists create the case entity

        return {
            "driver_info": {
                **driver_data["driver_details"],
                "driver_seq_id": driver.id,
                "tlc_license": driver_data['tlc_license_details']['tlc_license_number'] if driver_data['tlc_license_details']['tlc_license_number'] else "",
                "dmv_license": driver_data['dmv_license_details']['dmv_license_number'] if driver_data['dmv_license_details']['dmv_license_number'] else "",
            },
            "driver_document_info": {
                "object_type": "driver",
                "document_type": "driver_payee_proof",
                "object_id": driver.id
            },
            "driver_payee_proof": upload_service.get_documents(db,
                object_type="driver",
                object_id=str(driver.id),
                document_type="driver_payee_proof",
                multiple=True
            )
        }   
    except Exception as e:
        logger.info("Error processing payee proof", exc_info=e)
        raise HTTPException(status_code=500, detail=str(e)) from e
    
@step(step_id="151", name="Fetch - Driver payee", operation="fetch")
def fetch_driver_payee(db: Session, case_no: str, case_params: dict):
    """Fetch the driver payee"""
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        driver = None
        if case_entity:
            driver = driver_service.get_drivers(db, driver_id=case_entity.identifier_value)
        if not driver:
            return {}
        
        driver_payee_proofs = upload_service.get_documents(
            db=db,
               object_type="driver",
                object_id=str(driver.id),
                document_type="driver_payee_proof"
        )
        
        driver_data = format_driver_response(driver, False)

        extracted_data = {}
        metadata = {}

        if driver_payee_proofs and driver_payee_proofs.get("document_path"):
            metadata = s3_utils.get_file_metadata(driver_payee_proofs["document_path"])
            metadata = metadata if metadata else {}
            metadata = metadata.get("extracted_data" , {}).get("extracted_data" ,{})
            extracted_data[driver_payee_proofs.get("document_type")] = metadata

        
        logger.info(f"$$$#### payee Data OCR : {extracted_data}")
        payee_data = extracted_data.get("driver_payee_proof", {})
        
        if payee_data:
            bank_data = driver_data.setdefault("payee_details" , {}).setdefault("data" , {})
            driver_data["payee_details"]["pay_to_mode"] = "ACH"
            bank_data["bank_name"]= payee_data.get("bank_name" , None)
            bank_data["bank_account_number"] = payee_data.get("account_number" , None)
            bank_data["bank_account_name"] = payee_data.get("account_holder_name" , None)
            bank_data["bank_routing_number"] = payee_data.get("routing_number" , None)

        return {
            "driver_info": {
                **driver_data["driver_details"],
                "driver_seq_id": driver.id,
                "tlc_license": driver_data['tlc_license_details']['tlc_license_number'] if driver_data['tlc_license_details']['tlc_license_number'] else "",
                "dmv_license": driver_data['dmv_license_details']['dmv_license_number'] if driver_data['dmv_license_details']['dmv_license_number'] else "",
            },
            "driver_payee_info": driver_data["payee_details"],
            "driver_payee_proofs": [driver_payee_proofs]
        }
        
    except Exception as e:
        logger.info("Error fetching driver payee", exc_info=e)
        raise HTTPException(status_code=500, detail=str(e)) from e
    
@step(step_id="151", name="Process - Driver payee", operation="process")
def process_driver_payee(db: Session, case_no: str, step_data: dict):
    """Process the driver payee"""
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        driver = None
        driver = driver_service.get_drivers(db, driver_id=step_data.get("driver_id"))
        if not driver:
            raise HTTPException(status_code=404, detail="Driver not found")
        
        driver_data = {"id": driver.id}
        if step_data.get("pay_to_mode") == "ACH":
            driver_data["pay_to_mode"] = "ACH"
            bank_account = None
            bank_data = {}
            
            bank_account = entity_service.get_bank_account(db=db , bank_account_number=step_data.get("bank_account_number"))
            if bank_account and bank_account.id != driver.bank_account_id:
                raise ValueError("Bank account exists for another driver")
            
            bank_data["id"] = driver.bank_account_id if driver.bank_account_id else None
            
            if step_data.get("bank_name"):
                bank_data["bank_name"] = step_data.get("bank_name")

            if step_data.get("bank_account_number"):
                bank_data["bank_account_number"] = step_data.get("bank_account_number")

            if step_data.get("bank_account_name"):
                bank_data["bank_account_name"] = step_data.get("bank_account_name")

            if step_data.get("bank_routing_number"):
                bank_data["bank_routing_number"] = step_data.get("bank_routing_number")
                
            if step_data.get("effective_from"):
                bank_data["effective_from"] = datetime.fromisoformat(step_data["effective_from"])

            bank_account = entity_service.upsert_bank_account(db, bank_data)
            driver_data["bank_account_id"] = bank_account.id
            driver_data["pay_to"] = None
        else:
            driver_data["pay_to_mode"] = "Check"
            driver_data["pay_to"] = step_data.get("pay_to")
            driver_data["bank_account_id"] = None

        driver = driver_service.upsert_driver(db, driver_data)

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Updated driver payee details for driver {driver.driver_id}",
                meta_data={"driver_id": driver.id}
            )

        return "Ok"
    except Exception as e:
        logger.info("Error processing driver payee", exc_info=e)
        if isinstance(e, ValueError):
            logger.error(f"ERROR: {e}")
            raise ValueError(str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e
    