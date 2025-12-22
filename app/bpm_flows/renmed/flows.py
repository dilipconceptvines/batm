## app/bpm_flows/renmed/flows.py

# Third party imports
from fastapi import HTTPException, status

# Local imports
from app.bpm.step_info import step
from app.utils.logger import get_logger
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.medallions.services import medallion_service
from app.uploads.services import upload_service
from app.bpm_flows.newmed.utils import format_medallion_basic_details

logger = get_logger(__name__)
entity_mapper = {
    "MEDALLION_RENEWALS": "medallion",
    "MEDALLION_RENEWAL_IDENTIFIER": "id"
}
@step(step_id="191" , name="Fetch - Document in Medallion Documents" , operation='fetch')
def fetch_document_in_medallion_documents(db, case_no, case_params=None):
    """
    Fetch the document in medallion documents
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        medallion_renewal_info = {}
        medallion_info = {}
        medallion_renewal = None

        if case_params:
            medallion_info = medallion_service.get_medallion(db, medallion_number=case_params['object_lookup'])
            medallion_renewal = medallion_service.get_medallion_renewal(
                db, medallion_number=medallion_info.medallion_number
            )

        if case_entity:
            medallion_info = medallion_service.get_medallion(
                db, medallion_id=case_entity.identifier_value
            )
            medallion_renewal = medallion_service.get_medallion_renewal(
                db, medallion_number=medallion_info.medallion_number
            )
        if not medallion_info:
            return medallion_renewal_info
        
        medallion_owner = medallion_service.get_medallion_owner(db, medallion_owner_id=medallion_info.owner_id)

        medallion_renewal_info.update(
            format_medallion_basic_details(medallion_info, medallion_owner)
        )

        if not case_entity:
            case_entity = bpm_service.create_case_entity(
                db, case_no=case_no, entity_name=entity_mapper['MEDALLION_RENEWALS'],
                identifier=entity_mapper['MEDALLION_RENEWAL_IDENTIFIER'],
                identifier_value=str(medallion_info.id)
            )
        renwal_document = upload_service.get_documents(
                db, object_type="medallion",
                object_id=medallion_info.id,
                document_type="renewal_receipt"
            )
        medallion_renewal_info["documents"] = [renwal_document]

        return medallion_renewal_info
    except Exception as e:
        logger.error("Error fetching document in medallion documents: %s", str(e))
        raise e
    
@step(step_id="191" , name="Process - Document in Medallion Documents" , operation='process')
def process_document_in_medallion_documents(db, case_no, step_data, case_params=None):
    """
    Process the document in medallion documents
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        logger.info("Nothing To DO Here")
        
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            medallion_info = medallion_service.get_medallion(db, medallion_id=case_entity.identifier_value)
            if medallion_info:
                medallion_owner = medallion_service.get_medallion_owner(db, medallion_owner_id=medallion_info.owner_id)
                if medallion_owner:
                    audit_trail_service.create_audit_trail(
                        db=db,
                        case=case,
                        description=f"Processed medallion renewal documents for owner {medallion_owner.id} and medallion {medallion_info.medallion_number}",
                        meta_data={"medallion_owner_id": medallion_owner.id , "medallion_id": medallion_info.id}
                    )
        
        return "Ok"
    except Exception as e:
        logger.error("Error processing document in medallion documents: %s", str(e))
        raise e
               
@step(step_id="111", name="Fetch - Initiate Medallion Renewal", operation='fetch')
def fetch_initiate_medallion_renewal(db, case_no, case_params=None):
    """
    Fetch the medallion renewal information for the initiate medallion renewal step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        medallion_renewal_info = {}
        medallion_info = {}
        medallion_renewal = None
    
        if case_entity:
            medallion_info = medallion_service.get_medallion(
                db, medallion_id=case_entity.identifier_value
            )
            medallion_renewal = medallion_service.get_medallion_renewal(
                db, medallion_number=medallion_info.medallion_number
            )

        # Condition when neither case entity is present nor params
        if not medallion_info:
            return medallion_renewal_info

        medallion_owner = medallion_service.get_medallion_owner(db, medallion_owner_id=medallion_info.owner_id)

        medallion_renewal_info.update(
            format_medallion_basic_details(medallion_info, medallion_owner)
        )

        
        medallion_renewal_info.update({
            "renewal_date": medallion_renewal.renewal_date if medallion_renewal else None,
            "renewal_from": medallion_renewal.renewal_from if medallion_renewal else None,
            "renewal_to": medallion_renewal.renewal_to if medallion_renewal else None,
            "renewal_fee": medallion_renewal.renewal_fee if medallion_renewal else None,
            "medallion_renewal_document": upload_service.get_documents(
                db, object_type="medallion",
                object_id=medallion_info.id,
                document_type="renewal_receipt"
            )
        })

        return medallion_renewal_info
    except Exception as e:
        logger.error("Error fetching initiate medallion renewal: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        ) from e

@step(step_id="111", name="Process - Initiate Medallion Renewal", operation='process')
def process_initiate_medallion_renewal(db, case_no, step_data, case_params=None):
    """
    Process the initiate medallion renewal step
    """
    try:
        if step_data["renewal_fee"] < 0:
            raise ValueError('Renewal fee cannot be negative')
        
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        # if case_entity:
        #     raise ValueError("Case entity already exists")

        medallion_renewal = medallion_service.create_medallion_renewal(
            db,
            {
                "medallion_number": step_data["medallion_number"],
                "renewal_date": step_data['renewal_date'],
                "renewal_from": step_data['renewal_from'],
                "renewal_to": step_data['renewal_to'],
                "renewal_fee": step_data['renewal_fee']
            }
        )

        medallion = None
        
        if medallion_renewal.medallion_number:
            medallion = medallion_service.get_medallion(db=db, medallion_number=medallion_renewal.medallion_number)
            if medallion:
                medallion = medallion_service.upsert_medallion(db=db , medallion_data={"id": medallion.id , "medallion_renewal_date": step_data['renewal_date']})

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Initiated medallion renewal for medallion {medallion_renewal.medallion_number}",
                meta_data={"medallion_id": medallion.id if medallion else None}
            )

        return "Ok"
    except Exception as e:
        logger.error("Error processing initiate medallion renewal: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        ) from e