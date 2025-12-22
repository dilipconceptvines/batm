import json
from datetime import datetime
import requests
import usaddress

from sqlalchemy.orm import Session
from fastapi import HTTPException

from app.utils.logger import get_logger
from app.bpm.step_info import step
from app.bpm.services import bpm_service
from app.uploads.services import upload_service
from app.entities.services import entity_service
from app.entities.utils import format_individual_details
from app.medallions.services import medallion_service
from app.utils.s3_utils import s3_utils
from app.audit_trail.services import audit_trail_service
from app.utils.general import fill_if_missing
from app.bpm_flows.update_medallion_address.utils import prepare_address_update_payload
from app.medallions.utils import format_medallon_owner
from app.utils.lambda_utils import invoke_lambda_function
from app.core.config import settings

logger = get_logger(__name__)
entity_mapper = {
    "INDIVIDUAL_OWNER": "update_individual_owner",
    "OWNER_IDENTIFIER": "id"
}

@step(step_id="185" , name="Fetch -Uploded update individual Documents", operation="fetch")
def fetch_individual_uploaded_documents(
    db: Session, case_no, case_params = None
):
    try:
        logger.info("Fetch the uploaded documents")
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        individual = None

        if case_params and case_params.get("object_name") == "Medallion_Owner":
            owner = medallion_service.get_medallion_owner(db=db , medallion_owner_id=case_params.get("object_lookup"))
            individual = entity_service.get_individual(db=db , individual_id= owner.individual_id)

        if case_entity:
            individual = entity_service.get_individual(db=db , individual_id=int(case_entity.identifier_value))
            
        if not individual:
            return {}

        ssn_document = upload_service.get_documents(
                db, object_type="individual_owner", object_id=individual.id, document_type="ssn"
            )

        license_document = upload_service.get_documents(
            db, object_type="individual_owner", object_id=individual.id, document_type="driving_license"
        )

        passport_document = upload_service.get_documents(
            db, object_type="individual_owner", object_id=individual.id, document_type="passport"
        )

        payee_proof = upload_service.get_documents(
            db, object_type="individual_owner", object_id=individual.id, document_type="payee_proof"
        )

        individual_data = format_individual_details(individual)
        
        if not case_entity:
            case_entity = bpm_service.create_case_entity(
                db=db, case_no=case_no, entity_name=entity_mapper["INDIVIDUAL_OWNER"],
                identifier=entity_mapper["OWNER_IDENTIFIER"],
                identifier_value=individual.id
            )
        
        individual_data["individual_info"]["ssn"] = f"xxx-xx-{individual_data['individual_info']['ssn'][-4:]}" if individual_data.get("individual_info", {}).get("ssn", None) else None
        return {
            "individual_details": individual_data,
            "documents":[ssn_document, license_document , payee_proof , passport_document],
            "required_documents":["ssn" , "driving_license" , "payee_proof"]
            }
    except Exception as e:
        logger.error(f"Error fetching uploaded documents: {e}")
        raise e
@step(step_id="185", name="Process - Uploaded Documents", operation="process")
def process_uploaded_documents(
    db: Session, case_no, case_params = None
):
    try:
        logger.info("Process the uploaded documents")
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            owner = medallion_service.get_medallion_owner(db=db , individual_id=case_entity.identifier_value)
            if owner:
                meta_data = {
                    "medallion_owner_id": owner.id
                }
                audit_trail_service.create_audit_trail(
                    db=db,
                    case=case,
                    description=f"Document uploaded for individual owner {owner.id}",
                    meta_data=meta_data
                )
        logger.info("Nothing To Do Here")
        return "Ok"
    except Exception as e:
        logger.error(f"Error processing uploaded documents: {e}")
        raise e

@step(step_id="186", name="Fetch - Update Individual Owner Details", operation="fetch")
def fetch_update_individual_owner_details(
    db: Session, case_no, case_params = None
):
    """Fetch Update individual owner details"""
    try:
        logger.info("Fetch the Update individual owner information")
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        individual = None
        if case_entity and case_entity.identifier_value:
            individual = entity_service.get_individual(
                db, individual_id=int(case_entity.identifier_value)
            )

        if not individual:
            logger.info("No individual owner found for the case")
            return {}

        orc_results = {}
        # Get OCR Data from the Documents

        all_docs = upload_service.get_documents(db=db , object_type="individual_owner", object_id=individual.id , multiple=True , sort_order="asc")

        if all_docs:
            for doc in all_docs:
                metadata = {}
                if doc and doc.get("document_path"):
                    metadata = s3_utils.get_file_metadata(doc["document_path"])
                    metadata = metadata if metadata else {}
                    metadata = metadata.get("extracted_data" , {})
                else:
                    metadata = {}
                
                orc_results[doc.get("document_type")] = metadata

        
        logger.info(f"####-- OCR Results - = {orc_results} ######")
        individual_data = format_individual_details(individual)
        individual_data["individual_info"]["ssn"] = f"xxx-xx-{individual_data['individual_info']['ssn'][-4:]}" if individual_data.get("individual_info", {}).get("ssn", None) else None

        payee_proof = orc_results.get("payee_proof", {})
        license = orc_results.get("driving_license", {})
        ssn = orc_results.get("ssn" , {})
        passport = orc_results.get("passport" , {})

        if license:
            info = individual_data.setdefault("individual_info", {})
            fill_if_missing(info, "first_name", license, "first_name")
            fill_if_missing(info, "last_name", license, "last_name")
            fill_if_missing(info, "driving_license", license, "license_number")
            fill_if_missing(info, "driving_license_expiry_date", license, "expiration_date")
            fill_if_missing(info, "dob", license, "date_of_birth")
            if license.get("address"):
                try:
                    address = usaddress.tag(license.get("address"))[0]

                    parts = [

                        address.get("AddressNumber", ""),

                        address.get("StreetName", ""),

                        address.get("StreetNamePostType", "")

                    ]
                    
                    street = " ".join(filter(None, parts))
                    if address.get("OccupancyIdentifier", None):
                        street = f"{street}, {address['OccupancyIdentifier']}"
                    addr = individual_data.setdefault("primary_address", {})

                    if not addr.get("address_line_1"):
                        addr["address_line_1"] = street
                    fill_if_missing(addr, "city", address, "PlaceName")
                    fill_if_missing(addr, "state", address, "StateName")
                    fill_if_missing(addr, "zip", address, "ZipCode")

                except Exception as e:
                    logger.info(f"Address parsing failed: {e}")
        if ssn:
            info = individual_data.setdefault("individual_info", {})
            fill_if_missing(info, "ssn", ssn, "social_security_number")
        if payee_proof:
            payee_info = individual_data.setdefault("payee_info", {})
            data = payee_info.setdefault("data", {})
            if payee_info.get("pay_to_mode") != "Check":
                individual_data["payee_info"]["pay_to_mode"] = "ACH"
                fill_if_missing(data, "bank_name", payee_proof, "bank_name")
                fill_if_missing(data, "bank_account_name", payee_proof, "account_holder_name")
                fill_if_missing(data, "bank_account_number", payee_proof, "account_number")
        if passport:
            info = individual_data.setdefault("individual_info", {})
            fill_if_missing(info, "passport", passport, "passport_number")
        
        owner = medallion_service.get_medallion_owner(db=db , individual_id=individual.id)
        update_address_from = None
        if owner:
            update_address_from = upload_service.get_documents(db=db , object_id=owner.id , object_type="medallion_owner" , document_type="update_address")

        individual_data["documents"] = all_docs + ([update_address_from] if update_address_from and update_address_from.get("document_path", None) else [])

        return individual_data
    except Exception as e:
        logger.error("Error fetching individual owner information: %s", str(e), exc_info=True)
        raise e
    
@step(step_id="186", name="Process - Individual Owner Details", operation="process")
def process_individual_owner_details(
    db: Session, case_no, step_data: dict
):
    """Process individual owner details"""
    try:
        logger.info("Step 1: Fetch the case entity")
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        

        individual_entity = None
        owner_info = {}
        if case_entity and case_entity.identifier_value:
            logger.info("**** Case identifier value: %s", case_entity.identifier_value)
            individual_entity = entity_service.get_individual(
                db, individual_id=int(case_entity.identifier_value)
            )
            if not individual_entity:
                raise ValueError("Individual owner not found")

        logger.info("Step 2: Prepare the individual owner information")

        unique_fields = {
            "driving_license": "Driving license",
            "passport_number": "Passport number",
            "primary_contact_number": "Phone",
            "primary_email_address": "Email",
            "bank_account_number": "Bank account number"
        }

        for field, label in unique_fields.items():
            value = step_data.get(field)
            if value:
                existing = entity_service.get_individual(db=db, **{field: value})
                if existing and existing.id != individual_entity.id:
                    raise ValueError(f"{label} already exists for another individual owner")
                
        owner_info = {
            "id": individual_entity.id,
            "dob": datetime.strptime(step_data["dob"], "%Y-%m-%d") if step_data.get("dob") else None,
            "passport": step_data.get("passport_number", None),
            "primary_email_address": step_data.get("primary_email_address", None),
            "primary_contact_number": step_data.get("primary_contact_number", None),
            "additional_phone_number_1": step_data.get("additional_phone_number_1", None),
            "additional_phone_number_2": step_data.get("additional_phone_number_2", None),
            "driving_license": step_data.get("driving_license", ""),
        }

        logger.info("**** Owner info: %s", owner_info)
        individual_entity = entity_service.upsert_individual(db, individual_data=owner_info)

        if not individual_entity:
            raise ValueError("Failed to create individual owner")
        
        individual_update_data = {"id": individual_entity.id}
        
        logger.info("Step 3: Prepare and insert the primary address")
        primary_address_data = {
            "address_line_1": step_data.get("address_line_1"),
            "address_line_2": step_data.get("address_line_2", ""),
            "city": step_data.get("city"),
            "state": step_data.get("state"),
            "zip": step_data.get("zip")
        }

        secondary_address_data = {
            "address_line_1": step_data.get("secondary_address_line_1", ""),
            "address_line_2": step_data.get("secondary_address_line_2", ""),
            "city": step_data.get("secondary_city", ""),
            "state": step_data.get("secondary_state", ""),
            "zip": step_data.get("secondary_zip", "")
        }

        medallion_owner = medallion_service.get_medallion_owner(db=db , individual_id=individual_entity.id)
        medallion_owner_data = format_medallon_owner(medallion_owner) if medallion_owner else {}

        update_address_from = upload_service.get_documents(db=db , object_id=medallion_owner.id , object_type="medallion_owner" , document_type="update_address") if medallion_owner else {}

        contact_data = {
            "email": step_data.get("primary_email_address", None),
            "phone_1": step_data.get("primary_contact_number", None)
        }
        update_address_data = {}

        if primary_address_data and secondary_address_data:
            if step_data.get("is_update_address" , False):
                update_address_data = prepare_address_update_payload(primary_address_data , secondary_address_data , medallion_owner_data , contact_data) 

        primary_address = entity_service.upsert_address(db, address_data=primary_address_data)

        if not primary_address:
            raise ValueError("Failed to create primary address")

        secondary_address = entity_service.upsert_address(db, address_data=secondary_address_data)

        if not secondary_address:
            raise ValueError("Failed to create secondary address")
        
        logger.info("Step 5: Prepare and insert the bank account details")
        bank_account_entity = None
        if step_data.get("pay_to") == "ACH":
            account_number = step_data.get("bank_account_number", "")
            bank_account_data = {
                "bank_routing_number": step_data.get("bank_routing_number", ""),
                "bank_account_number": int(account_number) if account_number else None,
                "bank_name": step_data.get("bank_name", ""),
                "bank_account_name": step_data.get("bank_account_name", ""),
                "bank_account_type": step_data.get("bank_account_type", ""),
                "effective_from": datetime.strptime(step_data["effective_from"], "%Y-%m-%d") if step_data.get("effective_from") else None,
            }
            bank_account_entity = entity_service.upsert_bank_account(db, bank_account_data=bank_account_data)
            if bank_account_entity:
                individual_update_data["bank_account_id"] = bank_account_entity.id
                individual_update_data["pay_to_mode"] = "ACH"

        else:
            individual_update_data["payee"] = step_data.get("bank_account_name", None)
            individual_update_data["pay_to_mode"] = "Check"

        if primary_address:
            individual_update_data["primary_address_id"] = primary_address.id
        if secondary_address:
            individual_update_data["secondary_address_id"] = secondary_address.id
        individual_entity = entity_service.upsert_individual(db, individual_data=individual_update_data)

        owner = medallion_service.upsert_medallion_owner(
            db, medallion_owner_data={
                "id": medallion_owner.id if medallion_owner else None,
                "primary_address_id": primary_address.id,
                "medallion_owner_status": "Y",
                "primary_email_address": individual_entity.primary_email_address,
                "primary_phone": individual_entity.primary_contact_number
            }
        )

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Updated individual owner details for owner {owner.id}",
                meta_data={"medallion_owner_id": owner.id}
            )
        
        if update_address_data and step_data.get("is_update_address" , False):
            signed_update_address_form = upload_service.get_documents(
                db, object_type="medallion_owner",
                object_id=owner.id,
                document_type="signed_update_address"
            )

            if signed_update_address_form and signed_update_address_form.get("document_id", None):
                upload_service.delete_document(db=db , document_id= signed_update_address_form.get("document_id", None))

            payload = {
                "data": update_address_data,
                "identifier": f"form_{datetime.now().strftime('%Y%m%d%H%M%S')}",
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

        return "Ok"
    except Exception as e:
        logger.error("Error processing individual owner creation: %s", str(e), exc_info=True)
        raise e
    

@step(step_id="193", name="Fetch - Individual Owner Documents", operation="fetch")
def fetch_individual_owner_documents(
    db: Session, case_no, case_params = None
):
    """Fetch individual owner documents"""
    try:
        logger.info("Fetch the individual owner documents")
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}
        
        individual_entity = entity_service.get_individual(
            db, individual_id=int(case_entity.identifier_value)
        )

        if not individual_entity:
            logger.info("No individual owner found for the case")
            return {}
        
        ssn_document = upload_service.get_documents(
            db, object_type="individual_owner", object_id=individual_entity.id, document_type="ssn"
        )

        license_document = upload_service.get_documents(
            db, object_type="individual_owner", object_id=individual_entity.id, document_type="driving_license"
        )

        passport_document = upload_service.get_documents(
            db, object_type="individual_owner", object_id=individual_entity.id, document_type="passport"
        )

        payee_proof = upload_service.get_documents(
            db, object_type="individual_owner", object_id=individual_entity.id, document_type="payee_proof"
        )

        owner = medallion_service.get_medallion_owner(db=db , individual_id=individual_entity.id)
        update_address_from = None
        signed_update_address_form = None
        if owner:
            update_address_from = upload_service.get_documents(db=db , object_id=owner.id , object_type="medallion_owner" , document_type="update_address")
            signed_update_address_form = upload_service.get_documents(
                db, object_type="medallion_owner",
                object_id=owner.id,
                document_type="signed_update_address"
            )

        individual_details = format_individual_details(individual_entity)
        individual_details["individual_info"]["ssn"] = f"xxx-xx-{individual_details['individual_info']['ssn'][-4:]}" if individual_details.get("individual_info", {}).get("ssn", None) else None

        documents = [ssn_document, license_document, payee_proof, passport_document]
        document_types = ["ssn", "driving_license", "payee_proof", "passport"]

        if update_address_from and update_address_from.get("document_path"):
            documents.insert(-1, signed_update_address_form)
            document_types.insert(-1, "signed_update_address")

        return {
            "individual_details": individual_details,
            "documents": documents,
            "document_types": document_types,
            "object_type": "individual_owner",
            "object_id": individual_entity.id,
        }
    except Exception as e:
        logger.error("Error fetching individual owner documents: %s", str(e), exc_info=True)
        raise e
    

@step(step_id="193", name="Process - Individual Owner Documents", operation="process")
def process_individual_owner_documents(
    db: Session, case_no, step_data: dict
):
    """Process individual owner documents"""
    try:
        logger.info("Step 1: Fetch the case entity")
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if not case_entity:
            return {}

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            owner = medallion_service.get_medallion_owner(db=db , individual_id=case_entity.identifier_value)
            if owner:
                audit_trail_service.create_audit_trail(
                    db=db,
                    case=case,
                    description=f"Document Verified for individual owner {owner.id}",
                    meta_data={"medallion_owner_id": owner.id}
                )

        logger.info("Nothing To Do Here")        
        return "Ok"
    except Exception as e:
        logger.error("Error processing individual owner documents: %s", str(e), exc_info=True)
        raise e