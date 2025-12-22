## app/bpm_flows/retrieve_medallion/flows.py

# Third party imports
from fastapi import HTTPException

# Local imports
from app.bpm.step_info import step
from app.utils.logger import get_logger
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.uploads.services import upload_service
from app.medallions.services import medallion_service
from app.bpm_flows.newmed.utils import format_medallion_basic_details

logger = get_logger(__name__)
entity_mapper = {
    "MEDALLION_STORAGE": "medallion_storage",
    "MEDALLION_STORAGE_IDENTIFIER": "id"
}

@step(step_id="192" , name="Fetch - Medallion Retrive Documents" , operation="fetch")
def fetch_medallion_retrive_documents(db, case_no, case_params=None):
    """
    Fetch the documents for the retrieve medallion step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        medallion_storage_info = {}
        medallion_info = {}
        medallion_storage = None
        
        if case_params:
            medallion_storage = medallion_service.get_medallion_storage(
                db, medallion_number=case_params['object_lookup']
            )
        if case_entity:
            medallion_storage = medallion_service.get_medallion_storage(
                db, medallion_storage_id=int(case_entity.identifier_value)
            )

        # Condition when neither case entity is present nor params
        if not medallion_storage:
            return medallion_storage_info

        medallion_info = medallion_service.get_medallion(
            db, medallion_number=medallion_storage.medallion_number
        )
        medallion_owner = medallion_service.get_medallion_owner(
            db, medallion_owner_id=medallion_info.owner_id
        )
        medallion_storage_info["medallion_info"] = format_medallion_basic_details(
            medallion_info, medallion_owner
        )

        retrive_documet = upload_service.get_documents(
            db, object_type="medallion", object_id=medallion_storage.id, document_type="retrived_storage_receipt"
        )

        medallion_storage_info["documents"] = [retrive_documet]

        if not case_entity:
            case_entity = bpm_service.create_case_entity(
                db,
                case_no=case_no,
                entity_name=entity_mapper['MEDALLION_STORAGE'],
                identifier=entity_mapper['MEDALLION_STORAGE_IDENTIFIER'],
                identifier_value=str(medallion_storage.id)
            )

        return medallion_storage_info
    except Exception as e:
        logger.error("Error fetching storage retrieval information: %s", str(e))
        raise e

@step(step_id="192" , name="Process - Medallion Retrive Documents" , operation="process")
def process_medallion_retrive_documents(db, case_no, step_data):
    """
    Process the documents for the retrieve medallion step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        logger.info("Nothing To Do Here")
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            case_entity = bpm_service.get_case_entity(db, case_no=case_no)
            if case_entity:
                medallion_storage = medallion_service.get_medallion_storage(db, medallion_storage_id=int(case_entity.identifier_value))
                if medallion_storage:
                    medallion = medallion_service.get_medallion(db, medallion_number=medallion_storage.medallion_number)
                    audit_trail_service.create_audit_trail(
                        db=db,
                        case=case,
                        description=f"Validated medallion retrieval documents for medallion {medallion.medallion_number}",
                        meta_data={"medallion_id": medallion.id}
                    )
        return "Ok"
    except Exception as e:
        logger.error("Error processing storage retrieval information: %s", str(e))
        raise e

@step(step_id="116", name="Fetch - Update Storage with Retrieval info", operation='fetch')
def fetch_storage_retrieval(db, case_no, case_params=None):
    """
    Fetch the storage retrieval information for the retrieve medallion step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        medallion_storage_info = {}
        medallion_info = {}
        medallion_storage = None
        
        if case_entity:
            medallion_storage = medallion_service.get_medallion_storage(
                db, medallion_storage_id=int(case_entity.identifier_value)
            )

        # Condition when neither case entity is present nor params
        if not medallion_storage:
            return medallion_storage_info

        medallion_info = medallion_service.get_medallion(
            db, medallion_number=medallion_storage.medallion_number
        )
        medallion_owner = medallion_service.get_medallion_owner(
            db, medallion_owner_id=medallion_info.owner_id
        )
        medallion_storage_info["medallion_info"] = format_medallion_basic_details(
            medallion_info, medallion_owner
        )

        medallion_storage_info["storage_info"] = {
            'retrieval_date': medallion_storage.retrieval_date,
            'retrieved_by': medallion_storage.retrieved_by
        }

        medallion_storage_info['storage_receipt_document'] = upload_service.get_documents(
            db, object_type="medallion", object_id=medallion_storage.id, document_type="retrived_storage_receipt"
        )

        return medallion_storage_info
    except Exception as e:
        logger.error("Error fetching storage retrieval information: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e

@step(step_id="116", name="Process - Update Storage with Retrieval info", operation='process')
def process_storage_retrieval(db, case_no, step_data):
    """
    Process the storage retrieval information for the retrieve medallion step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        # TODO: add conditions of retrieval based on status
        storage_data = medallion_service.get_medallion_storage(
            db, medallion_storage_id=int(case_entity.identifier_value)
        )
        
        if not storage_data:
            raise ValueError("Storage data not found")

        _ = medallion_service.upsert_medallion_storage(
            db, {
                "id": storage_data.id,
                "medallion_number": step_data['medallion_number'],
                "retrieval_date": step_data['retrieval_date'],
                "retrieved_by": step_data['retrieved_by']
            }
        )
        medallion = medallion_service.get_medallion(
            db, medallion_number=step_data['medallion_number']
        )
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Updated storage retrieval information for medallion {medallion.medallion_number}",
                meta_data={"medallion_id": medallion.id}
            )
        # TODO: Add document generation here
        return "Ok"
    except Exception as e:
        logger.error("Error processing storage retrieval information: %s", str(e))
        raise e
