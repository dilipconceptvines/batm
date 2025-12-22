## app/bpm_flows/update_medallion_address/flows.py

# Standard library imports
import re
import json
import usaddress
from datetime import datetime, timezone

# Third party imports
from sqlalchemy.orm import Session
from fastapi import HTTPException , Request

# Local imports
from app.core.config import settings
from app.bpm.step_info import step
from app.audit_trail.services import audit_trail_service
from app.utils.logger import get_logger
from app.utils.lambda_utils import invoke_lambda_function
from app.bpm.services import bpm_service
from app.medallions.services import medallion_service
from app.entities.services import entity_service
from app.uploads.services import upload_service
from app.utils.s3_utils import s3_utils
from app.bpm_flows.update_medallion_address.utils import prepare_address_update_payload
from app.medallions.utils import format_medallion_response , format_medallon_owner


logger = get_logger(__name__)
entity_mapper = {
    "MEDALLION_UPDATE_ADDRESS": "address",
    "MEDALLION_UPDATE_ADDRESS_IDENTIFIER": "id"
}

@step(step_id="114", name="Fetch - Medallion Owner Address Documents", operation="fetch")
def fetch_medallion_address_documents(db: Session, case_no, case_params) -> dict:
    """Fetch the medallion owner address documents"""
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        owner = None

        if case_entity:
            owner = medallion_service.get_medallion_owner(db, medallion_owner_id=int(case_entity.identifier_value))
        elif case_params:
            owner = medallion_service.get_medallion_owner(db, medallion_owner_id=case_params['object_lookup'])

        if not owner:
            return {}

        owner_data = format_medallon_owner(owner)
        address_info = owner_data.get("primary_address", None)
        if not address_info:
            address_info = entity_service.upsert_address(db, {
                "address_line_1": "",
            })
            if owner.medallion_owner_type == "I":
                entity_service.upsert_individual(
                    db=db , individual_data={
                        "id": owner.id,
                        "primary_address_id": address_info.id
                    }
                )
            else:
                entity_service.upsert_corporation(
                    db=db , corporation_data={
                        "id": owner.id,
                        "primary_address_id": address_info.id
                    }
                )
        address_proofs = upload_service.get_documents(
            db, object_type="medallion_owner",
            object_id=owner.id,
            document_type="owner_address_proof"
        )


        logger.info("Medallion owner address info fetched %s", address_info)

        if not case_entity:
            bpm_service.create_case_entity(
                db=db, case_no=case_no,
                entity_name=entity_mapper['MEDALLION_UPDATE_ADDRESS'],
                identifier=entity_mapper['MEDALLION_UPDATE_ADDRESS_IDENTIFIER'],
                identifier_value=str(owner.id)
            )
        
        return {
            "owner_id": owner.id,
            "owner_name": owner_data["owner_name"],
            "primary_contact_number": owner_data["primary_contact_number"],
            "primary_email_address": owner_data["primary_email_address"],
            "ssn": owner_data.get("ssn") or None,
            "ein": owner_data.get("ein") or None,
            "passport": owner_data.get("passport") or None,
            "medallion_owner_address_info": address_info,
            "medallion_owner_secondary_address_info": owner_data.get("secondary_address", {}),
            "medallion_owner_address_proofs": [address_proofs],
            "is_mailing_address_same": owner_data["is_mailing_address_same"],
            "upload_data":{
                "object_type": "medallion_owner",
                "object_id": owner.id,
                "document_type": "owner_address_proof"
            }
        }
    except Exception as e:
        logger.error("Error in fetch_medallion_address_documents: %s", str(e), exc_info=True)
        raise e
    
@step(step_id="114", name="Process - Medallion Owner Address Documents", operation="process")
def process_medallion_documents(db: Session, case_no, step_data) -> dict:
    """Process the medallion owner documents"""
    try:
       logger.info("Process - Medallion Owner Address Documents")
       case_entity = bpm_service.get_case_entity(db, case_no=case_no)
       medallion = medallion_service.get_medallion(db, medallion_number=step_data.get("medallion_number"))

       address_proofs = upload_service.get_documents(
            db, object_type="medallion",
            object_id=medallion.id,
            document_type="medallion_address_proof",
            multiple=True
       )

       if not address_proofs:
           raise HTTPException(
                status_code=400,
                detail="No address proofs found"
           )
       case = bpm_service.get_cases(db=db , case_no= case_no)
       if case:
           metadata = {"medallion_owner_id": case_entity.identifier_value}
           audit_trail_service.create_audit_trail(
               db=db,
               case=case,
               description=f"Validated medallion owner address proofs for medallion owner {case_entity.identifier_value}",
               meta_data=metadata
           )
       return "Ok"
    except Exception as e:
        logger.error("Error in process_medallion_documents: %s", str(e), exc_info=True)
        raise e
    
@step(step_id="115", name="Fetch - Medallion Owner Address", operation="fetch")
def fetch_owner_address(db: Session, case_no, case_params=None) -> dict:
    """Fetch the medallion owner address"""
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        owner = None
        address_details = {
            "address_line_1": "",
            "address_line_2": "",
            "city": "",
            "state": "",
            "zip": ""
        }

        if case_entity:
            owner = medallion_service.get_medallion_owner(db=db , medallion_owner_id=int(case_entity.identifier_value))
        elif case_params:
            owner = medallion_service.get_medallion_owner(db, medallion_owner_id=case_params['object_lookup'])

        if not owner:
            return {}

        owner_data = format_medallon_owner(owner)
        address_info = owner_data.get("primary_address", None)
        address_proofs = upload_service.get_documents(
            db, object_type="medallion_owner",
            object_id=owner.id,
            document_type="owner_address_proof"
        )

        if address_info:
            address_details["address_line_1"] = address_info.address_line_1 or ""
            address_details["address_line_2"] = address_info.address_line_2 or ""
            address_details["city"] = address_info.city or ""
            address_details["state"] = address_info.state or ""
            address_details["zip"] = address_info.zip or ""

        extracted_data = {}
        metadata = {}

        is_address_saved = False

        if case_params:
            is_address_saved = case_params.get("isAddressSaved", False)
            
        if is_address_saved:
            logger.info("Address is already saved, skipping extraction")
        else:
            if address_info and address_proofs and address_proofs.get("document_path", None):
                metadata = s3_utils.get_file_metadata(address_proofs["document_path"])
                metadata = metadata if metadata else {}
                metadata = metadata.get("extracted_data", {}).get("extracted_data", {})
                extracted_data.update(metadata)
            
            logger.info("#######Extracted data from address proof: %s", extracted_data)

            if extracted_data.get("address" , None):

                address_details["address_line_1"] = ""
                address_details["address_line_2"] = ""
                address_details["city"] = ""
                address_details["state"] = ""
                address_details["zip"] = ""

                address = usaddress.tag(extracted_data["address"])[0]
                logger.info(f"extracted Address : {address}")

                parts = [
                    address.get("AddressNumber", ""),
                    address.get("StreetName", ""),
                    address.get("StreetNamePostType", "")
                ]
                street = " ".join(filter(None, parts))
                if address.get("OccupancyIdentifier", None):
                    street = f"{street}, {address['OccupancyIdentifier']}"
                
                address_details["address_line_1"] = street or None
                if address.get("PlaceName" , None):
                    address_details["city"] = address.get("PlaceName" , None)
                if address.get("StateName" , None):
                    address_details["state"] = address.get("StateName" , None)
                if address.get("ZipCode" , None):
                    address_details["zip"] = address.get("ZipCode" , None)
                
        return {
            "owner_id": owner.id,
            "owner_name": owner_data["owner_name"],
            "primary_contact_number": owner_data["primary_contact_number"],
            "primary_email_address": owner_data["primary_email_address"],
            "additional_phone_number_1": owner_data.get("additional_phone_number_1" , None),
            "additional_phone_number_2": owner_data.get("additional_phone_number_2" , None),
            "ssn": owner_data.get("ssn") or None,
            "ein": owner_data.get("ein") or None,
            "passport": owner_data.get("passport") or None,
            "medallion_owner_address_info": address_details,
            "secondary_address_info": owner_data.get("secondary_address", {}),
            "is_mailing_address_same": owner_data["is_mailing_address_same"],
            "medallion_owner_address_proofs": [address_proofs],
            "correspondence_info":{
            #    "document":upload_service.get_documents(db=db , object_id=owner.id , object_type="medallion_owner" , document_type="update_address")
            }
        }
    except Exception as e:
        logger.error("Error in fetch_owner_address: %s", str(e), exc_info=True)
        raise e
    
@step(step_id="115", name="Process - Medallion Owner Address", operation="process")
def process_owner_address(db: Session, case_no, step_data) -> dict:
    """Process the medallion owner address"""
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        primary_address = {}
        secondary_address = {}
        owner = None

        if case_entity:
            owner = medallion_service.get_medallion_owner(db=db , medallion_owner_id=int(case_entity.identifier_value))

        if not owner:
            raise HTTPException(
                status_code=400,
                detail="Medallion owner not found"
            )
        
        update_address_from = upload_service.get_documents(db=db , object_id=owner.id , object_type="medallion_owner" , document_type="update_address")
        
        owner_data = format_medallon_owner(owner)

        primary_address_data = step_data.get("primary_address", {})
        secondary_address_data = step_data.get("secondary_address", {})

        address_update_data = prepare_address_update_payload(primary_address_data, secondary_address_data ,owner_data, owner_data,step_data)

        invalid_characters_pattern = re.compile(r"[^a-zA-Z0-9\s\-,]")
        for field in ["address_line_1", "address_line_2", "city", "state", "zip" , "po_box"]:
            primary_address[field] = primary_address_data.get(field, "")
            secondary_address[field] = secondary_address_data.get(field, "")

            if invalid_characters_pattern.search(primary_address[field]) or invalid_characters_pattern.search(secondary_address[field]):
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid characters in {field}"
                )
            
        primary_add = owner_data.get("primary_address", None)
        secondary_add = owner_data.get("secondary_address", None)

        address_1 = entity_service.upsert_address(
            db, {
                "id": primary_add.id if primary_add else None,
                **primary_address
            }
        )

        address_data = secondary_address if step_data.get("is_mailing_address_same") is False else primary_address

        address_2 = entity_service.upsert_address(
            db, {
                "id": secondary_add.id if secondary_add else None,
                **address_data
            }
        )
        logger.info("Addresses upserted: %s , %s", address_1, address_2)

        medallion_service.upsert_medallion_owner(db=db , medallion_owner_data={
            "id": owner.id,
            "primary_address_id": address_1.id,
            "primary_email_address": step_data.get("email") or owner.primary_email_address,
            "primary_phone": step_data.get("phone_1") or owner.primary_phone,
            "is_mailing_address_same": step_data.get("is_mailing_address_same", True)
        })

        if owner.medallion_owner_type == "I" and owner.individual:
            individual = owner.individual
            entity_service.upsert_individual(
                db=db , individual_data={
                    "id": individual.id,
                    "primary_address_id": address_1.id,
                    "secondary_address_id": address_2.id,
                    "primary_email_address": step_data.get("email") or individual.primary_email_address,
                    "primary_contact_number": step_data.get("phone_1") or individual.primary_contact_number,
                    "additional_phone_number_1": step_data.get("phone_2") or individual.additional_phone_number_1
                }
            )
        elif owner.medallion_owner_type == "C" and owner.corporation:
            corporation = owner.corporation
            entity_service.upsert_corporation(
                db=db , corporation_data={
                    "id": corporation.id,
                    "primary_address_id": address_1.id,
                    "secondary_address_id": address_2.id,
                    "primary_email_address": step_data.get("email") or corporation.primary_email_address,
                    "primary_contact_number": step_data.get("phone_1") or corporation.primary_contact_number
                }
            )

        payload = {
            "data": address_update_data,
            "identifier": f"form_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
            "template_id": settings.address_update_template_id,
            "bucket": settings.s3_bucket_name
        }

        logger.info("Calling lambda function with payload %s", payload)

        response = invoke_lambda_function(
            function_name="pdf_filler",
            payload=payload
        )

        logger.info("Response from lambda function %s", response)
        response_body = json.loads(response.get("body", None))
        if not response_body:
            raise HTTPException(
                status_code=400,
                detail="Failed to fill the form"
            )
        s3_key = response_body.get("s3_key")

        file = ("storage_receipt.pdf",s3_utils.download_file(s3_key))
        email = getattr(owner, "primary_email_address", "") if owner else ""
        

        if update_address_from and update_address_from.get("document_path", None):
            update_address_from = upload_service.update_document(
                db=db , document_dict=update_address_from ,
                new_filename="Update_Address_Form.pdf",
                original_extension="PDF", file_size_kb=0,
                document_path=s3_key, notes="",
                document_type="update_address", object_type="medallion_owner",
                object_id=owner.id, document_date=datetime.now().strftime('%Y-%m-%d')   
            )
        else:
            upload_service.create_document(
                db, new_filename="Update_Address_Form.pdf",
                original_extension="PDF", file_size_kb=0,
                document_path=s3_key, notes="",
                document_type="update_address", object_type="medallion_owner",
                object_id=owner.id, document_date=datetime.now().strftime('%Y-%m-%d')
            )

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Updated medallion owner address for owner {owner.id}",
                meta_data={"medallion_owner_id": owner.id}
            )

        return "Ok"
    except Exception as e:
        logger.error("Error in process_owner_address: %s", str(e), exc_info=True)
        raise e
    
@step(step_id="190" , name="Verify - Update Address Documents" , operation="fetch")
def verify_address_documents(db: Session, case_no, case_params=None) -> dict:
    """Fetch the medallion owner address documents for verification"""
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        owner = None
        if case_entity:
            owner = medallion_service.get_medallion_owner(db=db , medallion_owner_id=int(case_entity.identifier_value))
        if not owner:
            return {}
        
        owner_data = format_medallon_owner(owner)
        address_info = owner_data.get("primary_address", None)
        
        address_proof = upload_service.get_documents(
            db, object_type="medallion_owner",
            object_id=owner.id,
            document_type="owner_address_proof"
        )

        signed_update_address_form = upload_service.get_documents(
            db, object_type="medallion_owner",
            object_id=owner.id,
            document_type="signed_update_address"
        )
        return {
            "owner_id": owner.id,
            "owner_name": owner_data["owner_name"],
            "primary_contact_number": owner_data["primary_contact_number"],
            "primary_email_address": owner_data["primary_email_address"],
            "ssn": owner_data.get("ssn") or None,
            "ein": owner_data.get("ein") or None,
            "passport": owner_data.get("passport") or None,
            "medallion_owner_address_info": address_info,
            "medallion_secondary_address_info": owner_data.get("secondary_address", {}),
            "is_mailing_address_same": owner_data["is_mailing_address_same"],
            "documents" :[signed_update_address_form , address_proof]
        }
    except Exception as e:
        logger.error("Error in verify_address_documents: %s", str(e), exc_info=True)
        raise e
@step(step_id="190" , name="Verify - Update Address Documents" , operation="process")
def process_verify_address_documents(db: Session, case_no, step_data) -> dict:
    """Process the medallion owner address documents for verification"""
    try:
        logger.info("Process - Verify Address Documents")
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}
        
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Document Verified for medallion owner {case_entity.identifier_value}",
                meta_data={"medallion_owner_id": case_entity.identifier_value}
            )
        return "Ok"
    except Exception as e:
        logger.error("Error in process_verify_address_documents: %s", str(e), exc_info=True)
        raise e