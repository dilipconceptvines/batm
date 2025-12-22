# app/bpm_flows/update_driver_address/flows.py

import json

from fastapi import HTTPException
from sqlalchemy.orm import Session
import usaddress

from app.bpm.step_info import step
from app.utils.logger import get_logger
from app.audit_trail.services import audit_trail_service
from app.core.config import settings
from app.utils.lambda_utils import invoke_lambda_function
from app.bpm.services import bpm_service
from app.drivers.services import driver_service
from app.entities.services import entity_service
from app.uploads.services import upload_service
from app.drivers.utils import format_driver_response
from app.utils.document_processor import document_processor
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)

entity_mapper = {
    "DRIVER_UPDATE_ADDRESS": "driver",
    "DRIVER_UPDATE_ADDRESS_IDENTIFIER": "id"
}

@step(step_id="152", name="Fetch Driver Address Documents", operation="fetch")
def fetch_address_documents(db: Session, case_no: str, case_params: dict):
    """Fetch the address documents for the driver"""
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        return_data = {
            "driver_info": {},
        }

        # Fetch the driver details
        driver = None
        if case_params and case_params.get("object_name") == "driver":
            driver = driver_service.get_drivers(db, driver_id=case_params.get("object_lookup"))
        if case_entity:
            driver = driver_service.get_drivers(db, id=int(case_entity.identifier_value))
        if not driver:
            return return_data

        driver_data = format_driver_response(driver, False)

        address_document = upload_service.get_documents(
                db,
                object_type="driver",
                object_id=driver.id,
                document_type="driver_address_proof"
            )

        # Create the case entity if not exists
        if not case_entity:
            bpm_service.create_case_entity(
                db,
                case_no=case_no,
                entity_name=entity_mapper['DRIVER_UPDATE_ADDRESS'],
                identifier=entity_mapper['DRIVER_UPDATE_ADDRESS_IDENTIFIER'],
                identifier_value=str(driver.id)
            )
        return {
            "driver_info": {
                **driver_data["driver_details"],
                "driver_seq_id": driver.id,
                "tlc_license": driver_data['tlc_license_details']['tlc_license_number'] if driver_data['tlc_license_details']['tlc_license_number'] else "",
                "dmv_license": driver_data['dmv_license_details']['dmv_license_number'] if driver_data['dmv_license_details']['dmv_license_number'] else "",
            },
            "driver_document_info": {
                'object_type': 'driver',
                'document_type': 'driver_address_proof',
                'object_id': driver.id
        },
            "driver_info_address_proofs": [address_document]
        }
    except Exception as e:
        logger.error("Error fetching address documents: %s", str(e), exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e

@step(step_id="152", name="Process Driver Address Documents", operation="process")
def process_address_documents(db: Session, case_no: str, step_data: dict):
    """Process the address documents for the driver"""
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        # Fetch the driver details
        driver = driver_service.get_drivers(db, driver_id=step_data.get("driver_id"))
        if not driver:
            raise HTTPException(status_code=404, detail="Driver not found")
            
        driver_address_proofs = upload_service.get_documents(
            db,
            object_type="driver",
            object_id=driver.id,
            document_type="driver_address_proof",
            multiple=True
        )

        if not driver_address_proofs:
            raise HTTPException(status_code=404, detail="Driver address proof not found")

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Processed driver address documents for driver {driver.driver_id}",
                meta_data={"driver_id": driver.id}
            )
        return "Ok"
    except Exception as e:
        logger.error("Error processing address documents: %s", str(e), exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e
    
@step(step_id="153", name="Fetch - Driver Address", operation="fetch")
def fetch_driver_address(db: Session, case_no: str, case_params: dict):
    """Fetch the driver address"""
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        
        driver = None
        if case_entity:
            driver = driver_service.get_drivers(db, id=int(case_entity.identifier_value))
        if not driver:
            return {
                "driver_info": {},
                "driver_info_address_proofs": []
            }
        
        driver_address_proofs = upload_service.get_documents(
            db,
            object_type="driver",
            object_id=driver.id,
            document_type="driver_address_proof"
        )
        driver_data = format_driver_response(driver, False)
        extracted_data = {}
        metadata = {}

        if driver_address_proofs and driver_address_proofs.get("document_path"):
            metadata = s3_utils.get_file_metadata(driver_address_proofs["document_path"])
            metadata = metadata if metadata else {}
            metadata = metadata.get("extracted_data" , {}).get("extracted_data" ,{})
            extracted_data.update(metadata)

        
        logger.info(f"#### Update Address OCR : {extracted_data}&*&&")
        
        if extracted_data.get("address" , None):
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

            driver_data["primary_address_details"]["address_line_1"] = street
            if address.get("PlaceName" , None):
                driver_data["primary_address_details"]["city"] = address.get("PlaceName" , None)
            if address.get("StateName" , None):
                driver_data["primary_address_details"]["state"] = address.get("StateName" , None)
            if address.get("ZipCode" , None):
                driver_data["primary_address_details"]["zip"] = address.get("ZipCode" , None)

        # Map the extracted data to address fields
        return {
            "driver_info": {
                **driver_data["driver_details"],
                "driver_seq_id": driver.id,
                "tlc_license": driver_data['tlc_license_details']['tlc_license_number'] if driver_data['tlc_license_details']['tlc_license_number'] else "",
                "dmv_license": driver_data['dmv_license_details']['dmv_license_number'] if driver_data['dmv_license_details']['dmv_license_number'] else "",
            },
            "primary_driver_address_info" : {
                "address_line_1": driver_data["primary_address_details"]["address_line_1"] or None, 
                "address_line_2": driver_data["primary_address_details"]["address_line_2"] or None,
                "city": driver_data["primary_address_details"]["city"] or None,
                "state": driver_data["primary_address_details"]["state"] or None,
                "zip": driver_data["primary_address_details"]["zip"] or None
            },
            "secondary_driver_address_info" : driver_data["secondary_address_details"],
            "driver_address_proof": [driver_address_proofs]
        }
    except Exception as e:
        logger.error("Error fetching driver address: %s", str(e), exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e
    
@step(step_id="153", name="Process - Driver Address", operation="process")
def process_driver_address(db: Session, case_no: str, step_data: dict):
    """Process the driver address"""
    try:
        _ = bpm_service.get_case_entity(db, case_no=case_no)
        
        driver = driver_service.get_drivers(db, driver_id=step_data.get("driver_id"))
        if not driver:
            raise HTTPException(status_code=404, detail="Driver not found")
        
        primary_address_detail = step_data.get('primary_address_detail', {})
        secondary_address_detail = step_data.get('secondary_address_detail', {})
        
        if driver.primary_address_id:
            primary_address = entity_service.upsert_address(db, {
                "id": driver.primary_address_id,
                **primary_address_detail,
            })
        else:
            primary_address = entity_service.upsert_address(db, address_data={**primary_address_detail})

        if driver.secondary_address_id:
            secondary_address = entity_service.upsert_address(db, {
                "id": driver.secondary_address_id,
                **secondary_address_detail,
            })
        else:
            secondary_address = entity_service.upsert_address(db, address_data={**secondary_address_detail})

        driver = driver_service.upsert_driver(db, {
            "id": driver.id,
            "primary_address_id": primary_address.id,
            "secondary_address_id": secondary_address.id
        })

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Updated driver address for driver {driver.driver_id}",
                meta_data={"driver_id": driver.id}
            )

        return "Ok"
    except Exception as e:
        logger.error("Error processing driver address: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e