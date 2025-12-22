## app/bpm_flows/newmed/flows.py

# Standard library imports
from datetime import datetime
import requests
import json

# Third party imports
from fastapi import HTTPException
from sqlalchemy.orm import Session

# Local imports
from app.bpm.step_info import step
from app.core.config import settings
from app.bpm.services import bpm_service
from app.audit_trail.services import audit_trail_service
from app.medallions.services import medallion_service
from app.medallions.schemas import MedallionStatus , MedallionOwnerType
from app.uploads.services import upload_service
from app.utils.s3_utils import s3_utils
from app.utils.lambda_utils import invoke_lambda_function
from app.bpm_flows.newmed.utils import (
    format_medallion_basic_details,
    format_medallion_lease , prepare_medallion_designation_document,
    prepare_medallion_royalty_corp_document, prepare_medallion_royalty_individual_document,
    prepare_medallion_cover_letter_document, prepare_medallion_royalty_corp_llc_document
)
from app.medallions.utils import format_medallion_response
from app.utils.logger import get_logger
from app.utils.general import get_date_from_string

logger = get_logger(__name__)
entity_mapper = {
    "MEDALLION": "medallion",
    "MEDALLION_IDENTIFIER": "id"
}

@step(step_id="108", name="Enter Medallion Details", operation='process')
def enter_medallion_details(db, case_no, step_data):
    """
    Process the medallion details for the new medallion step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        
        medallion_data = {
            "id": case_entity.identifier_value if case_entity and case_entity.identifier_value else None,
            "medallion_number": step_data.get("medallionNumber"),
            "medallion_type": step_data.get("medallionType"),
            "validity_end_date": step_data.get("expirationDate")
        }

        medallion_existing = medallion_service.get_medallion(db=db , medallion_number= medallion_data["medallion_number"])

        if medallion_existing and medallion_existing.id != medallion_data["id"]:
            raise ValueError (f"Medallion number {medallion_data['medallion_number']} already exists. Please provide a unique medallion number.")

        medallion = medallion_service.upsert_medallion(
            db=db, medallion_data=medallion_data
        )

        case = bpm_service.get_cases(db=db , case_no= case_no)
        meta_data = {
            "medallion_id": medallion.id
        }

        audit_trail_service.create_audit_trail(
            db=db ,
            description= f"Medallion details entered/updated for medallion number {medallion.medallion_number}.",
            case=case,
            meta_data=meta_data
        )

        return "Ok"
    except Exception as e:
        logger.error("Error processing medallion details: %s", str(e))
        raise e

@step(step_id="108", name="Enter Medallion Details", operation='fetch')
def fetch_medallion_details(db, case_no, case_params=None):
    """
    Fetch the medallion details for the new medallion step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if not case_entity and not case_params:
            return {}

        medallion_owner = None
        medallion = None
        if case_params and case_params.get("object_name") and case_params.get("object_lookup"):
            medallion_owner = medallion_service.get_medallion_owner(
                db, medallion_owner_id=case_params["object_lookup"]
            )
            if not medallion_owner:
                raise ValueError("Medallion owner not found. Please provide a valid owner ID.")
        if case_entity:
            medallion = medallion_service.get_medallion(db, medallion_id=case_entity.identifier_value)
            medallion_owner = medallion_service.get_medallion_owner(db=db ,medallion_owner_id=medallion.owner_id)

        if not medallion:
            medallion_data = {
            "owner_id": medallion_owner.id if medallion_owner else None,
            "medallion_status": MedallionStatus.IN_PROGRESS,
            "owner_type": MedallionOwnerType.CORPORATION if medallion_owner and medallion_owner.medallion_owner_type == MedallionOwnerType.CORPORATION else MedallionOwnerType.INDIVIDUAL
            }
            medallion = medallion_service.upsert_medallion(db=db, medallion_data=medallion_data)

        if not case_entity:
            case_entity = bpm_service.create_case_entity(
                db=db, case_no=case_no, entity_name=entity_mapper["MEDALLION"],
                identifier=entity_mapper["MEDALLION_IDENTIFIER"],
                identifier_value=medallion.id
            )


        medallion_basic_details = {}

        medallion_basic_details.update(
            format_medallion_basic_details(medallion, medallion_owner)
        )
        renewal_document = upload_service.get_documents(
            db, object_type="medallion", object_id=case_entity.identifier_value, document_type="renewal_receipt"
        )
        fs6_document = upload_service.get_documents(
            db, object_type="medallion", object_id=case_entity.identifier_value, document_type="fs6"
        )
        storage_receipt_document = upload_service.get_documents(
            db,
            object_type="medallion",
            object_id=case_entity.identifier_value,
            document_type="medallion_storage_receipt"
        )
        medallion_basic_details.update({
            'object_type': "medallion",
            'valid_to': medallion.validity_end_date,
            'renewal_receipt_document': renewal_document,
            'fs6_document': fs6_document,
            'storage_receipt_document': storage_receipt_document,
        })
        return medallion_basic_details
    except Exception as e:
        logger.error("Error fetching medallion details: %s", str(e))
        raise e

@step(step_id="109", name="Fetch Medallion Lease", operation='fetch')
def fetch_medallion_lease(db: Session, case_no: str, case_params=None):
    """
    Fetch the medallion lease for the new medallion step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}
        
        logger.info("Case entity fetched %s", case_entity.id)

        medallion = medallion_service.get_medallion(db, medallion_id=case_entity.identifier_value)
        if not medallion:
            return {}
        
        medallion_lease_details = format_medallion_lease(medallion)

        lease_document = upload_service.get_documents(
            db=db , document_type="medallion_agent_designation", object_id= medallion.id,
            object_type="medallion"
        )

        power_of_attorney_document = upload_service.get_documents(
            db=db , document_type="power_of_attorney", object_id= medallion.id,
            object_type="medallion"
        )


        royalty_document = upload_service.get_documents(
                db=db , document_type="royalty_agreement", object_id= medallion.id,
                object_type="medallion"
            )

        medallion_basic_info = {
            "object_type": "medallion",
            "lease_document": lease_document,
            "royalty_document": royalty_document,
            "power_of_attorney_document": power_of_attorney_document,
        }

        medallion_owner = medallion_service.get_medallion_owner(
            db, medallion_owner_id=medallion.owner_id
        )

        medallion_basic_info["medallion_lease_details"] = medallion_lease_details

        medallion_basic_info.update(
            format_medallion_basic_details(medallion, medallion_owner)
        )
        return medallion_basic_info
    except Exception as e:
        logger.error("Error fetching medallion lease: %s", str(e))
        raise e

@step(step_id="109", name="Create Medallion Lease", operation='process')
def create_medallion_lease(db: Session, case_no: str, step_data):
    """
    Create the medallion lease for the new medallion step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        medallion = medallion_service.get_medallion(db, medallion_id=int(case_entity.identifier_value))

        if not medallion:
            raise HTTPException(status_code=400, detail="Medallion not found")
        
        lease_data = {
            "id": medallion.mo_lease.id if medallion.mo_lease else None,
            "payee": step_data.get("payee"),
            "contract_start_date" : step_data.get("contract_effective_from"),
            "royalty_amount": step_data.get("royalty_amount"),
            "contract_signed_mode": step_data.get("contract_signed_mode"),
            "contract_term": step_data.get("contract_term"),
            "mail_sent_date": datetime.today().date() if step_data.get("contract_signed_mode") == "M" else None,
            "mail_received_date": datetime.today().date() if step_data.get("contract_signed_mode") == "M" else None,
            "contract_end_date": get_date_from_string(step_data.get("contract_effective_from") , step_data.get("contract_term")),
            "lease_signed_flag": True ,
            "lease_signed_date": datetime.today().date()
        }
        lease = medallion_service.upsert_mo_lease(db, lease_data)

        case = bpm_service.get_cases(db=db , case_no= case_no)
        meta_data = {
            "medallion_id": medallion.id
        }
        
        audit_trail_service.create_audit_trail(
            db=db ,
            description= f"Medallion lease created from {lease.contract_start_date} to {lease.contract_end_date} for medallion number {medallion.medallion_number}",
            case=case,
            meta_data=meta_data
        )

        lease_document = upload_service.get_documents(
            db=db , document_type="medallion_agent_designation", object_id= medallion.id,
            object_type="medallion"
        )

        royalty_document = upload_service.get_documents(
                db=db , document_type="royalty_agreement", object_id= medallion.id,
                object_type="medallion"
        )

        power_of_attorney = upload_service.get_documents(
            db=db , document_type="power_of_attorney" , object_id= medallion.id,
            object_type="medallion"
        )
        
        medallion_owner = format_medallion_response(medallion)
        ## Generate and store the medallion designation document
        lease_payload = prepare_medallion_designation_document(medallion, medallion_owner, lease)

        payload = {
            "data": lease_payload,
            "identifier": f"form_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            "template_id": settings.medallion_designation_template_id,
            "bucket": settings.s3_bucket_name
        }

        logger.info("Calling Lambda function with payload: %s", payload)
        response = invoke_lambda_function(
            function_name="pdf_filler",
            payload=payload
        )

        # Extract s3_key from response
        logger.info("Response from Lambda: %s", response)
        response_body = json.loads(response["body"])
        s3_key = response_body.get("s3_key")  # Use the output key we specified
        if s3_key:
            file = ("med_agent_designation.pdf", s3_utils.download_file(s3_key))

        if lease_document and lease_document.get("document_path" , None):
            lease_document = upload_service.update_document(
                db=db , document_dict=lease_document ,
                new_filename="med_agent_designation.pdf",
                original_extension="PDF", file_size_kb=0,
                document_path=s3_key, notes="",
                document_type="medallion_agent_designation", object_type="medallion",
                object_id=medallion.id, document_date=datetime.now().strftime('%Y-%m-%d')
            )
        else:
            lease_document = upload_service.create_document(
                db, new_filename="med_agent_designation.pdf",
                original_extension="PDF", file_size_kb=0,
                document_path=s3_key, notes="",
                document_type="medallion_agent_designation", object_type="medallion",
                object_id=medallion.id, document_date=datetime.now().strftime('%Y-%m-%d')
            )
        ## End of generating and storing the lease contract document

        ## Generate and store the medallion cover letter document
        # medallion_cover_letter_payload = prepare_medallion_cover_letter_document(medallion, medallion_owner)

        # payload = {
        #     "data": medallion_cover_letter_payload,
        #     "identifier": f"form_{datetime.now().strftime('%Y%m%d%H%M%S')}",
        #     "template_id": settings.medallion_cover_letter_template_id,
        #     "bucket": settings.s3_bucket_name
        # }

        # logger.info("Calling Lambda function with payload: %s", payload)
        # response = invoke_lambda_function(
        #     function_name="pdf_filler",
        #     payload=payload
        # )

        # # Extract s3_key from response
        # logger.info("Response from Lambda: %s", response)
        # response_body = json.loads(response["body"])
        # s3_key = response_body.get("s3_key")  # Use the output key we specified
        # if s3_key:
        #     file = ("medallion_cover_letter.pdf", s3_utils.download_file(s3_key))
        #     medallion_owner = format_medallion_response(medallion)

        #     if lease.contract_signed_mode == "M":
        #         email = getattr(medallion_owner, "primary_email_address", "") if medallion_owner else ""
        #         # Send email to the medallion owner
        
        # if cover_letter_document and cover_letter_document.get("document_path" , None):
        #     cover_letter_document = upload_service.update_document(
        #         db=db , document_dict=cover_letter_document ,
        #         new_filename="Medallion_Cover_Letter.pdf",
        #         original_extension="PDF", file_size_kb=0,
        #         document_path=s3_key, notes="",
        #         document_type="medallion_cover_letter", object_type="medallion",
        #         object_id=medallion.id, document_date=datetime.now().strftime('%Y-%m-%d')
        #     )
        # else:
        #     cover_letter_document = upload_service.create_document(
        #         db, new_filename="Medallion_Cover_Letter.pdf",
        #         original_extension="PDF", file_size_kb=0,
        #         document_path=s3_key, notes="",
        #         document_type="medallion_cover_letter", object_type="medallion",
        #         object_id=medallion.id, document_date=datetime.now().strftime('%Y-%m-%d')
        #     )
        
        if medallion.owner and medallion.owner.corporation:
            if medallion.owner.corporation.is_llc:
                logger.info("LLC -*-*-*- Generating BATM royalty corp document for llc medallion %s", medallion.medallion_number)
                ## Generate BATM royalty corp document and store it in s3
                royalty_corp_payload = prepare_medallion_royalty_corp_llc_document(medallion, medallion_owner, lease)

                payload = {
                    "data": royalty_corp_payload,
                    "identifier": f"form_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                    "template_id": settings.royalty_agreement_llc_template_id,
                    "bucket": settings.s3_bucket_name
                }

                logger.info("Calling Lambda function with payload: %s", payload)
                response = invoke_lambda_function(
                    function_name="pdf_filler",
                    payload=payload
                )

                # Extract s3_key from response
                logger.info("Response from Lambda: %s", response)
                response_body = json.loads(response["body"])
                s3_key = response_body.get("s3_key")  # Use the output key we specified

                file = ("medallion_royalty_corp_form.pdf",s3_utils.download_file(s3_key))
                if royalty_document and royalty_document.get("document_path" , None):
                    royalty_document = upload_service.update_document(
                        db=db , document_dict=royalty_document ,
                        new_filename=f"Medallion_Royalty_Corp_Form_{medallion.medallion_number}.pdf",
                        original_extension="PDF", file_size_kb=0,
                        document_path=s3_key, notes="",
                        document_type="royalty_agreement", object_type="medallion",
                        object_id=medallion.id, document_date=datetime.now().strftime('%Y-%m-%d')
                    )
                else:
                    royalty_document = upload_service.create_document(
                        db, new_filename=f"Medallion_Royalty_Corp_Form_{medallion.medallion_number}.pdf",
                        original_extension="PDF", file_size_kb=0,
                        document_path=s3_key, notes="",
                        document_type="royalty_agreement", object_type="medallion",
                        object_id=medallion.id, document_date=datetime.now().strftime('%Y-%m-%d')
                    )
            else:
                logger.info("-*-*-*- Generating BATM royalty corp document for medallion %s", medallion.medallion_number)
                ## Generate BATM royalty corp document and store it in s3
                royalty_corp_payload = prepare_medallion_royalty_corp_document(medallion, medallion_owner, lease)

                payload = {
                    "data": royalty_corp_payload,
                    "identifier": f"form_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                    "template_id": settings.royalty_agreement_corp_template_id,
                    "bucket": settings.s3_bucket_name
                }

                logger.info("Calling Lambda function with payload: %s", payload)
                response = invoke_lambda_function(
                    function_name="pdf_filler",
                    payload=payload
                )

                # Extract s3_key from response
                logger.info("Response from Lambda: %s", response)
                response_body = json.loads(response["body"])
                s3_key = response_body.get("s3_key")  # Use the output key we specified

                file = ("medallion_royalty_corp_form.pdf",s3_utils.download_file(s3_key))
                if royalty_document and royalty_document.get("document_path" , None):
                    royalty_document = upload_service.update_document(
                        db=db , document_dict=royalty_document ,
                        new_filename=f"Medallion_Royalty_Corp_Form_{medallion.medallion_number}.pdf",
                        original_extension="PDF", file_size_kb=0,
                        document_path=s3_key, notes="",
                        document_type="royalty_agreement", object_type="medallion",
                        object_id=medallion.id, document_date=datetime.now().strftime('%Y-%m-%d')
                    )
                else:
                    royalty_document = upload_service.create_document(
                        db, new_filename=f"Medallion_Royalty_Corp_Form_{medallion.medallion_number}.pdf",
                        original_extension="PDF", file_size_kb=0,
                        document_path=s3_key, notes="",
                        document_type="royalty_agreement", object_type="medallion",
                        object_id=medallion.id, document_date=datetime.now().strftime('%Y-%m-%d')
                    )
            ## End of generating BATM royalty corp document and storing it in s3

        ## End of generating and storing the lease contract document
        if medallion.owner and medallion.owner.individual:
            ## Generate BATM royalty corp document and store it in s3
            royalty_ind_payload = prepare_medallion_royalty_individual_document(medallion, medallion_owner, lease)

            payload = {
                "data": royalty_ind_payload,
                "identifier": f"form_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                "template_id": settings.royalty_agreement_individual_template_id,
                "bucket": settings.s3_bucket_name
            }

            logger.info("Calling Lambda function with payload: %s", payload)
            response = invoke_lambda_function(
                function_name="pdf_filler",
                payload=payload
            )

            # Extract s3_key from response
            logger.info("Response from Lambda: %s", response)
            response_body = json.loads(response["body"])
            s3_key = response_body.get("s3_key")  # Use the output key we specified

            file = ("medallion_royalty_individual_form.pdf", s3_utils.download_file(s3_key))
            medallion_owner = format_medallion_response(medallion)
            if royalty_document and royalty_document.get("document_path" , None):
                royalty_document = upload_service.update_document(
                    db=db , document_dict=royalty_document ,
                    new_filename=f"Medallion_Royalty_Individual_Form_{medallion.medallion_number}.pdf",
                    original_extension="PDF", file_size_kb=0,
                    document_path=s3_key, notes="",
                    document_type="royalty_agreement", object_type="medallion",
                    object_id=medallion.id, document_date=datetime.now().strftime('%Y-%m-%d')
                )
            else:
                royalty_document = upload_service.create_document(
                    db, new_filename=f"Medallion_Royalty_Individual_Form_{medallion.medallion_number}.pdf",
                    original_extension="PDF", file_size_kb=0,
                    document_path=s3_key, notes="",
                    document_type="royalty_agreement", object_type="medallion",
                    object_id=medallion.id, document_date=datetime.now().strftime('%Y-%m-%d')
                )
        ## End of generating BATM royalty corp document and storing it in s3

        ## Generate Power of Attorney document
        payload = {
            "data": {"medallion_owner_name": medallion_owner["medallion_owner"] if medallion_owner else ""},
            "identifier": f"form_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            "template_id": settings.power_of_attorney_template_id,
            "bucket": settings.s3_bucket_name
        }

        logger.info("Calling Lambda function with payload: %s", payload)
        response = invoke_lambda_function(
            function_name="pdf_filler",
            payload=payload
        )

        # Extract s3_key from response
        logger.info("Response from Lambda: %s", response)
        response_body = json.loads(response["body"])
        s3_key = response_body.get("s3_key")  # Use the output key we specified

        file = ("power_of_attorney.pdf",s3_utils.download_file(s3_key))
        if power_of_attorney and power_of_attorney.get("document_path" , None):
            royalty_document = upload_service.update_document(
                db=db , document_dict=power_of_attorney ,
                new_filename=f"Power_of_Attorney_{medallion.medallion_number}.pdf",
                original_extension="PDF", file_size_kb=0,
                document_path=s3_key, notes="",
                document_type="power_of_attorney", object_type="medallion",
                object_id=medallion.id, document_date=datetime.now().strftime('%Y-%m-%d')
            )
        else:
            power_of_attorney = upload_service.create_document(
                db, new_filename=f"Power_of_Attorney_{medallion.medallion_number}.pdf",
                original_extension="PDF", file_size_kb=0,
                document_path=s3_key, notes="",
                document_type="power_of_attorney", object_type="medallion",
                object_id=medallion.id, document_date=datetime.now().strftime('%Y-%m-%d')
            )

        medallion_service.upsert_medallion(
            db, {
                "id": case_entity.identifier_value,
                "mo_leases_id": lease.id,
                "default_amount": lease.royalty_amount,
                "medallion_status": MedallionStatus.AVAILABLE
            }
        )
        logger.info("Creating Medallion Lease")
        return "Ok"
    except Exception as e:
        logger.error("Error creating medallion lease: %s", str(e))
        raise e
    
@step(step_id="110", name="Process Medallion Documents", operation='process')
def process_medallion_documents(db: Session, case_no: str, step_data, case_params=None):
    """
    Process the medallion documents for the new medallion step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}
        medallion = None

        medallion = medallion_service.get_medallion(db=db , medallion_id=case_entity.identifier_value)

        if not medallion:
            return {}

        case = bpm_service.get_cases(db=db , case_no= case_no)
        meta_data = {
            "medallion_id": medallion.id
        }
        audit_trail_service.create_audit_trail(
            db=db,
            case=case,
            description=f"Processed medallion documents for medallion number {medallion.medallion_number}",
            meta_data=meta_data
        )
        
        return "Ok"
    except Exception as e:
        logger.error("Error processing medallion documents: %s", str(e))
        raise e

@step(step_id="110", name="Fetch Medallion Documents", operation='fetch')
def fetch_medallion_documents(db: Session, case_no: str, case_params=None):
    """
    Fetch the medallion documents for the new medallion step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}
        logger.info("Case entity fetched %s", case_entity.id)

        medallion_details = medallion_service.get_medallion(db, medallion_id=case_entity.identifier_value)

        mo_lease = medallion_service.get_mo_lease(db , mo_lease_id=medallion_details.mo_leases_id)

        if not medallion_details:
            return {}

        medallion_owner = medallion_service.get_medallion_owner(
            db, medallion_owner_id=medallion_details.owner_id
        )

        medallion_documents = format_medallion_basic_details(medallion_details, medallion_owner)
        renewal_document = upload_service.get_documents(
            db, object_type="medallion", object_id=case_entity.identifier_value, document_type="renewal_receipt"
        )
        fs6_document = upload_service.get_documents(
            db, object_type="medallion", object_id=case_entity.identifier_value, document_type="fs6"
        )

        medallion_lease =upload_service.get_documents(
                db, object_type="medallion", object_id=int(case_entity.identifier_value),
                document_type="signed_lease"
            )

        lease_document = upload_service.get_documents(
            db=db , document_type="medallion_agent_designation", object_id= medallion_details.id,
            object_type="medallion"
        )

        power_of_attorney_document = upload_service.get_documents(
            db=db , document_type="power_of_attorney", object_id= medallion_details.id,
            object_type="medallion"
        )
        
        storage_receipt_document = upload_service.get_documents(
            db,
            object_type="medallion",
            object_id=case_entity.identifier_value,
            document_type="medallion_storage_receipt"
        )

        signed_power_of_attorney_document = upload_service.get_documents(
            db , object_id= case_entity.identifier_value , 
            object_type="medallion" , document_type="signed_power_of_attorney"
        )

        signed_royalty_document = upload_service.get_documents(
            db , object_id= case_entity.identifier_value , 
            object_type="medallion" , document_type="signed_royalty_agreement"
        )

        royalty_document = {}
        if medallion_details.owner and medallion_details.owner.corporation:
            royalty_document = upload_service.get_documents(
                db=db , document_type="royalty_agreement", object_id= medallion_details.id,
                object_type="medallion"
            )
        elif medallion_details.owner and medallion_details.owner.individual:
            royalty_document = upload_service.get_documents(
                db=db , document_type="royalty_agreement", object_id= medallion_details.id,
                object_type="medallion"
            )

        medallion_documents.update({
            'object_type': "medallion",
            'medallion_documents':[renewal_document, fs6_document, storage_receipt_document, *([royalty_document , lease_document, power_of_attorney_document] if mo_lease and mo_lease.contract_signed_mode != "P" else [signed_royalty_document, medallion_lease, signed_power_of_attorney_document]) ],
            'document_type':["renewal_receipt", "fs6", "storage_receipt", *(["royalty_agreement", "medallion_lease", "power_of_attorney"] if mo_lease and mo_lease.contract_signed_mode != "P" else ["signed_royalty_agreement" , "signed_lease", "signed_power_of_attorney"])],
            "object_id": int(case_entity.identifier_value)
        })
        
        return medallion_documents
    except Exception as e:
        logger.error("Error fetching medallion documents: %s", str(e))
        raise e
    