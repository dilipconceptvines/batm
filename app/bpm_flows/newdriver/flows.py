# app/bpm_flows/newdriver/flows.py
 
from datetime import datetime
import json 
from fastapi import HTTPException
import usaddress
 
from app.bpm.step_info import step
from app.utils.logger import get_logger
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.drivers.services import driver_service
from app.uploads.services import upload_service
from app.entities.services import entity_service
from app.drivers.schemas import DriverStatus
from app.utils.general import generate_random_6_digit
from app.drivers.utils import format_driver_response
from app.utils.document_processor import document_processor
from app.utils.general import fill_if_missing
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)
entity_mapper = {
    "DRIVER": "drivers",
    "DRIVER_IDENTIFIER": "id",
}
 
@step(step_id="142", name="Fetch - Return search driver", operation='fetch')
def search_driver_information(db, case_no, case_params=None):
    """
    Fetch the driver information for the new driver step
    """
    try:
        if case_params:
            if not set(case_params.keys()).intersection(['ssn', 'tlc_license_number', 'dmv_license_number']):
                logger.warning("No valid search parameters provided.")
                return {"error": "At least one of SSN, TLC License Number, or DMV License Number must be provided."}
        
            if case_params.get("ssn"):
                logger.info("ssn is not given in params")
                raise ValueError("ssn is not given in params")
       
 
            driver = driver_service.get_drivers(
                db, ssn=case_params.get("ssn", None), tlc_license_number=case_params.get("tlc_license_number", None),
                dmv_license_number=case_params.get("dmv_license_number", None), driver_status=DriverStatus.INACTIVE
            )
        
            if not driver:
                raise ValueError(
                    "No Inactive driver found matching the provided criteria.")
        
            driver_documents = upload_service.get_documents(
                db, object_type="driver", object_id=driver.id, multiple=True
            )
 
            return {
                "driver_id": driver.id,
                "driver_lookup_id": driver.driver_id,
                "first_name": driver.first_name,
                "last_name": driver.last_name,
                "full_name": driver.full_name,
                "driver_type": driver.driver_type,
                "driver_ssn": driver.ssn,
                "lease_info": {
                    "lease_type": "Weekly",  # TODO: Add the appropriate query
                },
                "tlc_license_number": driver.tlc_license.tlc_license_number,
                "dmv_license_number": driver.dmv_license.dmv_license_number,
                "ssn": driver.ssn,
                "has_documents": bool(driver_documents),
                "has_vehicle": False,  # TODO: Correct query once clear
                "matched_on": [],
            }
        
        return {}
    except Exception as e:
        logger.error("Error searching driver information: %s", e, exc_info=True)
        raise e
 
 
@step(step_id="142", name="Process - Create case with new driver", operation='process')
def set_driver_information(db, case_no, step_data):
    """
    Process the driver information for the new driver step
    """
    try:
        # If a case already exists for this step then we should not process it
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if case_entity:
            raise ValueError("Driver cannot be reselected for this case")
 
        driver = None
        # If no driver id is passed through step data the we create an empty
        driver_data = {
            "driver_id": generate_random_6_digit(), # Temporary unique driver ID
            "driver_status": DriverStatus.IN_PROGRESS,
        }
        if not step_data.get('driverId'):
            driver = driver_service.upsert_driver(db, driver_data)
        else:
            driver = driver_service.get_drivers(db, driver_id=step_data.get('driverId'))
 
        # Create case entity if not exists
        if not case_entity:
            case_entity =bpm_service.create_case_entity(
                db=db, case_no=case_no,
                entity_name=entity_mapper['DRIVER'],
                identifier=entity_mapper['DRIVER_IDENTIFIER'],
                identifier_value=str(driver.id)
            )
 
        logger.info("Case entity %s created for driver %s", case_entity.id, driver.id)
        
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Created case with new driver {driver.driver_id}",
                meta_data={"driver_id": driver.id}
            )
        return "Ok"
    except Exception as e:
        logger.error("Error creating case entity for driver: %s", e)
        raise e
@step(step_id="143", name="Fetch - Upload driver documents", operation="fetch")
def fetch_upload_driver_documents(db, case_no, case_params=None):
    """
    Fetches the list of required documents and checks which ones are already
    uploaded for the driver associated with the case
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}
    
        driver_id = int(case_entity.identifier_value)
    
        # Define mandatory documents for a new driver registration
        mandatory_docs = ["dmv_license", "tlc_license" , "dmv_abstract"]
        optional_docs = ["driver_ssn","payee_proof"]
    
        document_status = []
        others_documents = upload_service.get_documents(db=db , object_type="driver" , object_id=driver_id , like_document_type= "others",multiple=True , sort_order="asc")
    
        # Check status for all required documents
        for doc_type in mandatory_docs + optional_docs:
            doc = upload_service.get_documents(
                db, object_type="driver", object_id=driver_id,
                document_type=doc_type
            )
            document_status.append({
                "document_type": doc_type,
                "is_mandatory": doc_type in mandatory_docs,
                "uploaded_document": doc
            })
            
        # Check status for other documents

        if others_documents:
            for doc in others_documents:
                document_status.append({
                    "document_type": doc.get("document_type"),
                    "is_mandatory": False,
                    "uploaded_document": doc
                })
    
        return {
            "documents": document_status,
            "object_type":"driver",
            "object_id":driver_id
            }
    except Exception as e:
        logger.error("Error fetching driver documents: %s", e)
        raise e

@step(step_id="143", name="Process - Upload driver documents", operation="process")
def process_upload_driver_documents(db, case_no, step_data):
    """
    Verifies that all mandatory documents for the associated driver have been
    uploaded. The actual file upload is handled by the dedicated/upload-document
    endpoint.
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}
    
        driver_id = int(case_entity.identifier_value)
    
        mandatory_docs = ["dmv_license", "tlc_license", "driver_ssn", "payee_proof"]
    
        for doc_type in mandatory_docs:
            doc = upload_service.get_documents(
                db, object_type="driver", object_id=driver_id, document_type=doc_type
            )
            if not doc:
                raise HTTPException(status_code=400, detail=f"Mandatory document {doc_type} not uploaded")
        
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Verified mandatory documents for driver {driver_id}",
                meta_data={"driver_id": driver_id}
            )
        return "Ok"
    except Exception as e:
        logger.error("Error processing driver documents: %s", e)
        raise e

@step(step_id="144", name="Fetch - Return driver information", operation='fetch')
def fetch_driver_information(db, case_no, case_params=None):
    """
    Fetch the driver information for the new driver step
    """
    try:
        logger.info("Fetch driver information")
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}
       
        driver_id = int(case_entity.identifier_value)
 
        driver = driver_service.get_drivers(db, id=driver_id)
       
        # Start with existing data from the database
        driver_data = driver.to_dict()
 
        # Fetch relevant documents for OCR
        ocr_docs = ["dmv_license", "tlc_license", "driver_ssn", "payee_proof"]
        
        ocr_results = {}
        # Get the latest state of the case to check which step is active    
        all_docs = upload_service.get_documents(
            db, object_type="driver", object_id=driver_id, multiple=True
        )

        if all_docs:
            for doc in all_docs:
                metadata = {}
                if doc and doc.get("document_path"):
                    metadata = s3_utils.get_file_metadata(doc["document_path"])
                    metadata = metadata if metadata else {}
                    metadata = metadata.get("extracted_data" , {})
                else:
                    metadata = {}
                
                logger.info("-*-*-*- OCR metadata: -*-*-******************** %s", metadata)
                ocr_results[doc.get("document_type")] = metadata
                
        logger.info("-*-*-*- OCR results: -*-*-******************** %s", ocr_results)
        driver_data = format_driver_response(driver, False)
        # Merge OCR results into the driver data (OCR data takes precedence)
        
        address = {}
        payee_proof = ocr_results.get("payee_proof", {})
        driver_ssn = ocr_results.get("driver_ssn", {})
        tlc_license = ocr_results.get("tlc_license", {})
        dmv_license = ocr_results.get("dmv_license" , {})
        
        
        if dmv_license:
            dmv_data = dmv_license
            info = driver_data.setdefault("dmv_license_details", {})
            driver_info = driver_data.setdefault("driver_details", {})
            address_info = driver_data.setdefault("primary_address_details", {})
            
            fill_if_missing(info,"dmv_license_number",dmv_data ,"license_number")
            fill_if_missing(info,"dmv_license_expiry_date",dmv_data ,"expiration_date")
            fill_if_missing(driver_info,"first_name",dmv_data ,"first_name")
            fill_if_missing(driver_info,"last_name",dmv_data ,"last_name")
            fill_if_missing(driver_info,"dob",dmv_data ,"date_of_birth")

            if dmv_data.get("address", None):
                try:
                    address, _ = usaddress.tag(dmv_data.get("address", None))
                    
                    parts = [

                        address.get("AddressNumber", ""),

                        address.get("StreetName", ""),

                        address.get("StreetNamePostType", "")

                    ]
                    
                    street = " ".join(filter(None, parts))
                    if address.get("OccupancyIdentifier", None):
                        street = f"{street}, {address['OccupancyIdentifier']}"
                    
                    driver_data["primary_address_details"]["address_line_1"] = address_info.get("address_line_1") if address_info.get("address_line_1") else street
                    fill_if_missing(address_info,"city",address ,"PlaceName")
                    fill_if_missing(info,"dmv_license_issued_state",address ,"StateName")
                    fill_if_missing(address_info,"state",address ,"StateName")
                    fill_if_missing(address_info,"zip",address ,"ZipCode")

                except Exception as e:
                    print(f"Address parsing failed: {e}")

        if tlc_license:
            tlc_data = tlc_license
            info = driver_data.setdefault("tlc_license_details", {})
            fill_if_missing(info,"tlc_license_expiry_date",tlc_data ,"expiration_date")
            fill_if_missing(info,"tlc_license_number",tlc_data ,"tlc_license_number")


        if driver_ssn:
            ssn_data = driver_ssn
            info = driver_data.setdefault("driver_details", {})
            fill_if_missing(info,"driver_ssn",ssn_data ,"social_security_number")

        if payee_proof:
            payee_data = payee_proof
            info = driver_data.setdefault("payee_details", {})
            logger.info(f'@$$#$#%   payee info : {info} %$##$#$')
            if payee_data.get("bank_name" , None):
                info["pay_to_mode"] = "ACH"
                bank_info = info.setdefault("data", {})
                fill_if_missing(bank_info,"bank_name",payee_data ,"bank_name")
                fill_if_missing(bank_info,"bank_account_number",payee_data ,"account_number")
                fill_if_missing(bank_info,"bank_routing_number",payee_data ,"bank_routing_number")
                fill_if_missing(bank_info,"bank_account_name",payee_data ,"account_holder_name")
                fill_if_missing(bank_info,"effective_from",payee_data ,"effective_from")

        photo = upload_service.get_documents(
            db , object_type="driver" , object_id=driver.id , document_type="photo"
        )
        
        all_docs.extend([photo])
        
        return {"driver_data": driver_data, "driver_documents": all_docs}
    except Exception as e:
        logger.error("Error fetching driver information: %s", e)
        raise e


@step(step_id="144", name="Process - Create new driver or update existing driver", operation='process')
def create_or_update_driver_information(db, case_no, step_data):
    """
    Process the driver information for the new driver step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}
 
        driver = driver_service.get_drivers(db, id=int(case_entity.identifier_value))
 
        # If the driver id passed is different from the one in the db there is a problem
        if driver.driver_id != step_data['driver_details'].get("driver_id"):
            raise ValueError("The driver being updated does not have the correct driver id")
        

        unique_persional_feilds = {
            "ssn": step_data['driver_details'].get("ssn"),
            "email_address": step_data['driver_details'].get("email_address"),
            "phone_number": step_data['driver_details'].get("phone_number_1"),
        }

        for field, value in unique_persional_feilds.items():
            if not value:
                continue
            existing_driver = driver_service.get_drivers(db=db , **{field: value})
            if existing_driver and existing_driver.id != driver.id:
                raise ValueError(f"The driver with the same {field.replace('_', ' ')} already exists")

 
        driver_details = step_data.get("driver_details", {})
        # Update the driver basic details
        full_name = " ".join(filter(None, [part.strip() if part else None for part in [driver_details.get("first_name"),driver_details.get("middle_name"),driver_details.get("last_name")]]))
        driver = driver_service.upsert_driver(db, {
            "id": driver.id,
            "full_name": full_name,
            **driver_details
        })
 
        driver_data = {}

        # Check the DMV license details
        dmv_license_details = step_data.get("dmv_license_details", {})
        dmv_number = dmv_license_details.get("dmv_license_number")
        dmv_info = driver_service.get_dmv_license(db, dmv_license_number=dmv_number)

        if dmv_info and driver.dmv_license_number_id != dmv_info.id:
            raise ValueError("DMV number already exists")
        if not dmv_license_details.get("is_dmv_license_active"):
            raise ValueError("DMV license is not active")
 
        # Check the TLC license details
        tlc_license_details = step_data.get("tlc_license_details", {})
        tlc_number = tlc_license_details.get("tlc_license_number")
        tlc_info = driver_service.get_tlc_license(db, tlc_license_number=tlc_number)

        if tlc_info and driver.tlc_license_number_id != tlc_info.id:
            raise ValueError("TLC number already exists")
        if not tlc_number or not str(tlc_number).isdigit() or len(str(tlc_number)) > 7 or len(str(tlc_number)) < 6:
            raise ValueError("Invalid TLC License Number: must be exactly 6 to 7 digits")
        if not tlc_license_details.get("is_tlc_license_active"):
            raise ValueError("TLC license is not active")
       
        # Check the payee details
        payee_details = step_data.get("payee_details", {})
        account_number = payee_details.get("bank_account_number" , None)
        if account_number:
            ac_number = entity_service.get_bank_account(db=db , bank_account_number=account_number)
            if ac_number and driver.bank_account_id != ac_number.id:
                raise ValueError("Bank account number already exists")

        # Create or update the related entities
        dmv_license_data = {
            "id": driver.dmv_license_number_id,
            **dmv_license_details
        }
        dmv_license = driver_service.upsert_dmv_license(db, dmv_license_data)
        driver_data["dmv_license_number_id"] = dmv_license.id

        tlc_data = {
            "id": driver.tlc_license_number_id,
            **tlc_license_details,
        }
        tlc_license = driver_service.upsert_tlc_license(db, tlc_data)
        driver_data["tlc_license_number_id"] = tlc_license.id
        driver_data["driver_id"] = tlc_license.tlc_license_number
 
         # Handle bank account details
        if driver.driver_bank_account and payee_details.get("pay_to_mode") == "ACH":
            bank_account = driver.driver_bank_account
            bank_account_data = {}
            if payee_details.get("bank_account_number"):
                bank_account_data["bank_account_number"] = payee_details.get("bank_account_number")
            if payee_details.get("bank_name"):
                bank_account_data["bank_name"] = payee_details.get("bank_name")
            if payee_details.get("bank_routing_number"):
                bank_account_data["bank_routing_number"] = payee_details.get("bank_routing_number")
            if payee_details.get("bank_account_name"):
                bank_account_data["bank_account_name"] = payee_details.get("bank_account_name")
            if payee_details.get("effective_from"):
                bank_account_data["effective_from"] = datetime.fromisoformat(payee_details.get("effective_from"))
            
            bank_account = entity_service.upsert_bank_account(db, {
                "id": bank_account.id,
                **bank_account_data
            })
            driver_data["pay_to_mode"] = "ACH"
        elif payee_details.get("pay_to_mode") == "ACH":
            bank_data = {
                "bank_account_number": payee_details.get("bank_account_number"),
                "bank_name": payee_details.get("bank_name"),
                "bank_routing_number": payee_details.get("bank_routing_number"),
                "bank_account_name": payee_details.get("bank_account_name")
            }
            if payee_details.get("effective_from"):
                bank_data["effective_from"] = datetime.fromisoformat(payee_details.get("effective_from"))
            bank_account = entity_service.upsert_bank_account(db=db , bank_account_data=bank_data)
            driver_data["bank_account_id"] = bank_account.id
            driver_data["pay_to_mode"] = "ACH"
        else:
            driver_data["pay_to_mode"] = "Check"
            driver_data["pay_to"] = payee_details.get("payee")

        # Handle Primary and Secondary Address
        primary_address_details = step_data.get("primary_address_details", {})
        if primary_address_details:
            if driver.primary_driver_address:
                primary_address_details["id"] = driver.primary_driver_address.id
            primary_address = entity_service.upsert_address(db=db , address_data=primary_address_details)
            driver_data["primary_address_id"] = primary_address.id if primary_address else None
 
        secondary_address_details = step_data.get("secondary_address_details", {})
        if secondary_address_details:
            if driver.secondary_driver_address:
                secondary_address_details["id"] = driver.secondary_driver_address.id
            secondary_address = entity_service.upsert_address(db=db , address_data=secondary_address_details)
            driver_data["secondary_address_id"] = secondary_address.id if secondary_address else None

        # Finally update the driver with all related entity ids
        driver = driver_service.upsert_driver(db, {
            "id": driver.id,
            **driver_data
        })
        
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Created/updated driver {driver.driver_id} with complete information",
                meta_data={"driver_id": driver.id}
            )
   
        return "Ok"
    except Exception as e:
        logger.error("Error creating or updating driver information: %s", e, exc_info=True)
        raise e
@step(step_id="145", name="Fetch - Approve Driver", operation='fetch')
def fetch_driver_approval_information(db, case_no, case_params=None):
    """
    Fetch the driver approval information for the new driver step
    """
    try:
        logger.info("Fetch driver information")
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}
 
        driver = driver_service.get_drivers(db, id=int(case_entity.identifier_value))
        driver_data = format_driver_response(driver, False)
        return {
            'driver_details': {
                "personal_details": {
                    **driver_data["driver_details"],
                    "driver_seq_id": driver.id,
                    "dmv_license": driver_data["dmv_license_details"]["dmv_license_number"] if driver_data["dmv_license_details"]["dmv_license_number"] else "",
                    "tlc_license": driver_data["tlc_license_details"]["tlc_license_number"] if driver_data["tlc_license_details"]["tlc_license_number"] else "",
                },
                "driver_primary_address_details": driver_data["primary_address_details"],
                "driver_secondary_address_details": driver_data["secondary_address_details"],
                "payee_details": driver_data["payee_details"],
                "dmv_license_info": driver_data["dmv_license_details"],
                "tlc_license_info": driver_data["tlc_license_details"],
            },
            "driver_documents": upload_service.get_documents(
                db, object_type="driver", object_id=case_entity.identifier_value, multiple=True
            )
        }
    except Exception as e:
        logger.error("Error fetching driver approval information: %s", e, exc_info=True)
        raise e
 
@step(step_id="145", name="Process - Approve", operation='process')
def process_driver_approval_information(db, case_no, step_data):
    """
    Process the driver approval information for the new driver step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}
       
        driver = driver_service.get_drivers(db, id=int(case_entity.identifier_value))
        # If the driver id passed is different from the one in the db there is a problem
        if driver.driver_id != step_data['driver_details'].get("driver_id"):
            raise ValueError(
                "The driver being approved does not have the correct driver id")
        
        driver_service.upsert_driver(
            db=db , driver_data={
                "id": driver.id,
                "driver_status": DriverStatus.REGISTERED
            }
        )
        # Approve the driver by updating the status
        
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Approved driver {driver.driver_id} with Status {driver.driver_status}",
                meta_data={"driver_id": driver.id}
            )
        return "Ok"
    except Exception as e:
        logger.error("Error processing driver approval information: %s", e, exc_info=True)
        raise e
 
 