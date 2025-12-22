## app/bpm_flows/updatedmv/flows.py

# Third party imports
from sqlalchemy.orm import Session
import usaddress

# Local imports
from app.utils.logger import get_logger
from app.bpm.step_info import step
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.drivers.services import driver_service
from app.uploads.services import upload_service
from app.drivers.utils import format_driver_response
from app.utils.document_processor import document_processor
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)

entity_mapper = {
    "DRIVER": "drivers",
    "DRIVER_IDENTIFIER": "id",
}


@step(step_id="146", name="Fetch - DMV Document", operation='fetch')
def fetch_dmv_document(db: Session, case_no, case_params=None):
    """
    Fetch the dmv document
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        driver = None
        if case_params:
            driver = driver_service.get_drivers(db, driver_id=case_params['object_lookup'])

        if case_entity:
            driver = driver_service.get_drivers(db, id=int(case_entity.identifier_value))

        if not driver:
            return {}
        
        driver_data = format_driver_response(driver, False)
        dmv_document = upload_service.get_documents(db, object_type="driver", object_id=driver.id, document_type="dmv_license")
        if not case_entity:
            case_entity = bpm_service.create_case_entity(
                db=db, case_no=case_no,
                entity_name=entity_mapper['DRIVER'],
                identifier=entity_mapper['DRIVER_IDENTIFIER'],
                identifier_value=str(driver.id)
            )

        return {
            "driver_info": {
                **driver_data["driver_details"],
                "driver_seq_id": driver.id,
                "tlc_license": driver_data['tlc_license_details']['tlc_license_number'] if driver_data['tlc_license_details']['tlc_license_number'] else "",
                "dmv_license": driver_data['dmv_license_details']['dmv_license_number'] if driver_data['dmv_license_details']['dmv_license_number'] else "",
            },
            "dmv_license_info": driver_data['dmv_license_details'],
            "dmv_license_document": dmv_document
        }
    except Exception as e:
        logger.error("Error fetching dmv document: %s", e, exc_info=True)
        raise e
            

@step(step_id="146", name="Process - DMV Document", operation='process')
def process_dmv_document(db: Session, case_no, step_data):
    """
    Process the dmv document
    """
    try:
        logger.info("Process - Nothing TO Do Here")
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}

        driver = driver_service.get_drivers(db, driver_id=case_entity.identifier_value)
        if not driver:
            return {}

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Processed DMV document for driver {driver.driver_id}",
                meta_data={"driver_id": driver.id}
            )
        return "Ok"
    except Exception as e:
        logger.error("Error processing dmv document: %s", e, exc_info=True)
        raise e

@step(step_id="147", name="Fetch - Return DMV License", operation='fetch')
def fetch_dmv_license(db: Session, case_no, case_params=None):
    """
    Fetch the dmv license for the update dmv step
    """
    try:
        logger.info("Return DMV License")
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        driver_info = {
            'driver_info': {},
            'dmv_license_info': {},
            'dmv_license_document': {}
        }
        driver = None
        if case_params:
            driver = driver_service.get_drivers(db, driver_id=case_params['object_lookup'])

        if case_entity:
            driver = driver_service.get_drivers(db, id=int(case_entity.identifier_value))

        if not driver:
            return driver_info

        driver_data = format_driver_response(driver, False)
        dmv_document = upload_service.get_documents(db, object_type="driver", object_id=driver.id, document_type="dmv_license")

        extracted_data ={}
        metadata = {}

        if dmv_document and dmv_document.get("document_path"):
            metadata = s3_utils.get_file_metadata(dmv_document["document_path"])
            metadata = metadata if metadata else {}
            metadata = metadata.get("extracted_data" , {})
            extracted_data[dmv_document.get("document_type")] = metadata

        logger.info(f"##$#### DMV Data OCR: {extracted_data}")

        license_number = driver_data["dmv_license_details"]["dmv_license_number"]

        if extracted_data.get("dmv_license" , {}):
            dmv_data = extracted_data.get("dmv_license" , {})

            if dmv_data:
                if dmv_data.get("license_number", None):
                    driver_data["dmv_license_details"]["dmv_license_number"] = dmv_data.get("license_number", None)

                if dmv_data.get("expiration_date", None):
                    driver_data["dmv_license_details"]["dmv_license_expiry_date"] = dmv_data.get("expiration_date", None)
                if dmv_data.get("address", None):
                    try:
                        address, address_type = usaddress.tag(dmv_data.get("address", ""))

                        if address.get("StateName"):
                            driver_data["dmv_license_details"]["dmv_license_issued_state"] = address["StateName"]

                    except Exception as e:
                        logger.info(f"Address parsing failed: {e}")
        return {
            "driver_info": {
                **driver_data["driver_details"],
                "driver_seq_id": driver.id,
                "tlc_license": driver_data['tlc_license_details']['tlc_license_number'] if driver_data['tlc_license_details']['tlc_license_number'] else "",
                "dmv_license": license_number if license_number else "",
            },
            "dmv_license_info": driver_data['dmv_license_details'],
            "dmv_license_document": dmv_document
        }
    except Exception as e:
        logger.error("Error fetching dmv license: %s", e, exc_info=True)
        return driver_info


@step(step_id="147", name="Process - Update DMV License", operation='process')
def set_dmv_license_information(db: Session, case_no, step_data):
    """
    Process the dmv license information for the update dmv step
    """
    try:
        logger.info("Process - Update DMV License")
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        dmv_data = step_data.get("dmv_license_details" , {})

        driver_id = dmv_data.get("driver_id" , None)
        dmv_number = dmv_data.get("dmv_license_number" , None)
        driver = driver_service.get_drivers(db, driver_id=driver_id)
        if not driver:
            raise ValueError("Driver not found for the driver id passed")

        if case_entity and driver.id != int(case_entity.identifier_value):
            raise ValueError("The driver id passed is not relevant to this case")
        
        license_number = driver.dmv_license.dmv_license_number if driver.dmv_license else None

        #if license_number and license_number == dmv_number:
        #   raise ValueError("New DMV Licensence Number is Same as Old DMV Licensence Number")

        dmv_license = driver_service.get_dmv_license(db, dmv_license_number=dmv_number)
        if dmv_license and dmv_license.id != driver.dmv_license_number_id:
            raise ValueError(f"DMV number {dmv_number} already exists")
        
        del dmv_data["driver_id"]

        if not dmv_license:
            dmv_license = driver_service.upsert_dmv_license(db, {
                **dmv_data
            })
        else:
            dmv_license = driver_service.upsert_dmv_license(db, {
                "id": dmv_license.id,
                **dmv_data
            })

        driver_service.upsert_driver(db, {
            "id": driver.id,
            "dmv_license_number_id": dmv_license.id
        })

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Updated DMV license for driver {driver.driver_id} with DMV license {dmv_license.dmv_license_number}",
                meta_data={"driver_id": driver.id}
            )

        return "Ok"
    except Exception as e:
        logger.error("Error processing dmv license: %s", e, exc_info=True)
        raise e