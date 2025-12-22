## app/bpm_flows/stomed/flows.py

# Standard library imports
import json
from datetime import datetime

# Third party imports
from fastapi import HTTPException, status

# Local imports
from app.core.config import settings
from app.bpm.step_info import step
from app.utils.logger import get_logger
from app.audit_trail.services import audit_trail_service
from app.utils.lambda_utils import invoke_lambda_function
from app.bpm.services import bpm_service
from app.uploads.services import upload_service
from app.medallions.services import medallion_service
from app.medallions.schemas import MedallionStatus
from app.bpm_flows.newmed.utils import format_medallion_basic_details
from app.bpm_flows.stomed.utils import prepare_storage_receipt_payload
from app.medallions.utils import format_medallion_response

logger = get_logger(__name__)
entity_mapper = {
    "MEDALLION_STORAGE": "medallion_storage",
    "MEDALLION_STORAGE_IDENTIFIER": "id"
}

@step(step_id="112" , name="Fetch - Upload Documents", operation='fetch')
def fetch_upload_documents(db, case_no, case_params=None):
    """
    Fetch the upload documents for the move to storage step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        medallion_info = None
        
        if case_params and case_params['object_lookup']:
            medallion_info = medallion_service.get_medallion(db, medallion_number=case_params['object_lookup'])

        if case_entity:
            medallion_info = medallion_service.get_medallion(db, medallion_id= case_entity.identifier_value)

        if not medallion_info:
            logger.info("No medallion info found.")
            return {}
        
        medallion_owner = medallion_service.get_medallion_owner(db, medallion_owner_id=medallion_info.owner_id)

        if not case_entity:
            case_entity = bpm_service.create_case_entity(
                db, case_no=case_no, entity_name=entity_mapper['MEDALLION_STORAGE'],
                identifier=entity_mapper['MEDALLION_STORAGE_IDENTIFIER'],
                identifier_value=str(medallion_info.id)
            )

        rate_card_document =  upload_service.get_documents(
                db, object_type="medallion", object_id=medallion_info.id, document_type="rate_card"
            )
        
        return {
            "documents": [rate_card_document],
            "medallion_number": medallion_info.medallion_number if medallion_info else None,
            "medallion_details": format_medallion_response(medallion_info , medallion_owner),
            "upload_info" : {
                "object_type": "medallion",
                "object_id": medallion_info.id if medallion_info else None,
                "document_type": ["rate_card"]
            }
        }
        
    except Exception as e:
        logger.error("Error fetching upload documents: %s", str(e))
        raise e
    
@step(step_id="112" , name="Process - Upload Documents", operation='process')
def process_upload_documents(db, case_no, step_data):
    """
    Process the upload documents for the move to storage step
    """
    try:
        logger.info("Nothing to Do Here")
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}
        
        medallion_info = medallion_service.get_medallion(db, medallion_id=case_entity.identifier_value)
        if not medallion_info:
            return {}
        medallion_owner = medallion_service.get_medallion_owner(db, medallion_owner_id=medallion_info.owner_id)
        if not medallion_owner:
            return {}
        
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                    db=db,
                    case=case,
                    description=f"Processed upload documents for storage for owner {medallion_owner.id} and medallion {medallion_info.medallion_number}",
                    meta_data={"medallion_owner_id": medallion_owner.id , "medallion_id": medallion_info.id}
                )
        return "Ok"
    except Exception as e:
        logger.error("Error processing upload documents: %s", str(e))
        raise e

@step(step_id="113", name="Fetch - Move to Storage", operation='fetch')
def fetch_storage_details(db, case_no, case_params=None):
    """
    Fetch the storage details for the move to storage step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        storage_info = {}
        medallion_storage = None
        medallion_info = ""
        if not case_entity:
            return {}

        medallion_info = medallion_service.get_medallion(db, medallion_id=case_entity.identifier_value)
        medallion_storage = medallion_service.get_medallion_storage(db, medallion_number=medallion_info.medallion_number)

        medallion_owner = medallion_service.get_medallion_owner(db, medallion_owner_id=medallion_info.owner_id)
        storage_info.update(format_medallion_basic_details(medallion_info, medallion_owner))

        if not medallion_storage:
            return storage_info


        storage_receipt_document = upload_service.get_documents(
            db,
            object_type="medallion",
            object_id=medallion_storage.id,
            document_type="storage_receipt"
        )

        rate_card_document =  upload_service.get_documents(
                db, object_type="medallion", object_id=medallion_info.id, document_type="rate_card"
            )

        storage_info['storage_info'] = {
            "storage_id": medallion_storage.id if medallion_storage else None,
            "storage_initiated_date": medallion_storage.storage_initiated_date if medallion_storage else None,
            "storage_date": medallion_storage.storage_date if medallion_storage else None,
            "storage_mode": medallion_storage.storage_mode if medallion_storage else None,
            "storage_letter_signed_by": medallion_storage.storage_letter_signed_by if medallion_storage else None,
            "storage_rate_card": medallion_storage.storage_rate_card if medallion_storage else None,
            "print_name": medallion_storage.print_name if medallion_storage else None,
            "storage_reason": medallion_storage.storage_reason if medallion_storage else None,
            "retrieval_date": medallion_storage.retrieval_date if medallion_storage else None,
            "retrieved_by": medallion_storage.retrieved_by if medallion_storage else None,
            "rate_card_document": rate_card_document,
            "storage_receipt_document": storage_receipt_document
        }
        logger.info("Fetch - Move to Storage: %s", storage_info)
        return storage_info
    except Exception as e:
        logger.error("Error fetching storage details: %s", str(e))
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)) from e


@step(step_id="113", name="Process - Move to Storage", operation='process')
def process_storage_details(db, case_no, step_data):
    """
    Process the move to storage step
    """
    try:
        logger.info("Process - Move to Storage")
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        # Fetch medallion details first
        if not case_entity:
            return {}

        medallion = medallion_service.get_medallion(db, medallion_id=case_entity.identifier_value)

        if not medallion:
            raise ValueError("Medallion not found for the given case.")
        medallion_owner = medallion_service.get_medallion_owner(db, medallion_owner_id=medallion.owner_id)
        
        # Check if the medallion's status is one of the allowed ones. Only archived or avaliable medallions can be moved to storage
        if medallion.medallion_status not in [MedallionStatus.AVAILABLE, MedallionStatus.ARCHIVED]:
            raise HTTPException(
                status_code=400,
                detail="Medallion cannot be stored because its status is not 'Available' or 'Archived'"
            )
        
        medallion_storage = medallion_service.get_medallion_storage(db, medallion_number=medallion.medallion_number)

        transformed_data = {
            "medallion_number": step_data.get("medallion_number"),
            "storage_initiated_date": step_data.get("date_place_in_storage"),
            "storage_mode": "P" if step_data.get("storage_mode") == False else "V",
            "storage_letter_signed_by": None,
            "storage_rate_card": step_data.get("rate_card_date" , None),
            "storage_reason": step_data.get("reason_for_storage" , None),
            "storage_date": step_data.get("storage_date" , None),
            "print_name": step_data.get("print_name" , None),
            "retrieval_date": None  # Optional field
        }

        storage = medallion_service.upsert_medallion_storage(db, transformed_data)
        storage_data= medallion_service.get_medallion_storage(db, medallion_number= medallion.medallion_number)
        storage_receipt_document = upload_service.get_documents(
            db,
            object_type="medallion",
            object_id=storage_data.id,
            document_type="storage_receipt"
        )

        if not storage_data:
            raise ValueError("Failed to create or update medallion storage.")
        
        medallion_details = format_medallion_basic_details(medallion, medallion_owner)
        
        storage_payload = prepare_storage_receipt_payload(storage_data, medallion_details)
        # Prepare payload for Lambda function
        payload = {
            "data": storage_payload,
            "identifier": f"form_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            "template_id": settings.storage_receipt_template_id,
            "bucket": settings.s3_bucket_name
        }

        logger.info("Calling Lambda function with payload: %s", payload)
        response = invoke_lambda_function(
            function_name="pdf_filler",
            payload=payload
        )

        logger.info("##$$#$#$ Storage Document: %s", storage_receipt_document)

        # Extract s3_key from response
        logger.info("Response from Lambda: %s", response)
        response_body = json.loads(response["body"])
        s3_key = response_body.get("s3_key")  # Use the output key we specified

        if storage_receipt_document and storage_receipt_document.get("document_path" , None):
            storage_receipt_document = upload_service.update_document(
                db=db, document_dict=storage_receipt_document,
                new_filename="Storage Receipt.pdf",
                original_extension="PDF", file_size_kb=0,
                document_path=s3_key, notes="",
                document_type="storage_receipt", object_type="medallion",
                object_id=storage.id, document_date=datetime.now().strftime('%Y-%m-%d')
            )
        else:
            upload_service.create_document(
                db, new_filename="Storage Receipt.pdf",
                original_extension="PDF", file_size_kb=0,
                document_path=s3_key, notes="",
                document_type="storage_receipt", object_type="medallion",
                object_id=storage.id, document_date=datetime.now().strftime('%Y-%m-%d')
            )
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Moved medallion {medallion.medallion_number} to storage",
                meta_data={"medallion_id": medallion.id}
            )
        
        return "Ok"
    except Exception as e:
        logger.error("Error processing storage details: %s", str(e))
        raise e
    
@step(step_id="189" , name="fetch - verify storage documents", operation='fetch')
def process_pay_to_mode(db, case_no, step_data):
    """
    Process the pay to mode step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}
        
        medallion_info = medallion_service.get_medallion(db, medallion_id=case_entity.identifier_value)
        if not medallion_info:
            raise ValueError("Medallion not found for the given case.")
        medallion_owner = medallion_service.get_medallion_owner(db, medallion_owner_id=medallion_info.owner_id)
        
        medallion_storage = medallion_service.get_medallion_storage(db, medallion_number=medallion_info.medallion_number)

        rate_card_document = upload_service.get_documents(
            db, object_type="medallion", object_id=medallion_info.id, document_type="rate_card"
        )

        if not medallion_storage:
            return [rate_card_document]
        
        signed_storage_receipt = upload_service.get_documents(
            db=db ,
            object_type="medallion",
            object_id=medallion_storage.id,
            document_type="signed_storage_receipt"
        )
        
        return {
            "medallion_details": format_medallion_response(medallion_info, medallion_owner),
            "documents" : [signed_storage_receipt , rate_card_document],
            "medallion_number": medallion_info.medallion_number if medallion_info else None,
            "storage_info": medallion_storage.to_dict()
        }
    except Exception as e:
        logger.error("Error processing pay to mode: %s", str(e))
        raise e
@step(step_id="189" , name="process - verify storage documents", operation='process')
def fetch_pay_to_mode(db, case_no, step_data):
    """
    Fetch the pay to mode step
    """
    try:
        logger.info("Nothing to Do Here")
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}
        medallion_info = medallion_service.get_medallion(db, medallion_id=case_entity.identifier_value)
        if not medallion_info:
            return {}
        medallion_owner = medallion_service.get_medallion_owner(db, medallion_owner_id=medallion_info.owner_id)
        if not medallion_owner:
            return {}
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Verified storage documents for owner {medallion_owner.id} and medallion {medallion_info.medallion_number}",
                meta_data={"medallion_owner_id": medallion_owner.id , "medallion_id": medallion_info.id}
            )
        return "Ok"
    except Exception as e:
        logger.error("Error fetching pay to mode: %s", str(e))
        raise e
