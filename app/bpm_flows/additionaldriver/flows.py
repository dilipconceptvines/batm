## app/bpm_flows/additionaldriver/flows.py

# Standard imports
from datetime import datetime, timezone

# Local imports
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.bpm.step_info import step
from app.bpm_flows.additionaldriver import utils as additionaldriver_utils
from app.core.config import settings
from app.core.dependencies import get_db_with_current_user
from app.drivers.services import driver_service
from app.leases.search_service import format_lease_response
from app.leases.services import lease_service
from app.uploads.services import upload_service
from app.utils.logger import get_logger

logger = get_logger(__name__)

entity_mapper = {
    "LEASE_DRIVERS": "lease_drivers",
    "LEASE_DRIVER_ID": "driver_id",
}


@step(step_id="163", name="Fetch - Search Driver Information", operation="fetch")
def choose_additional_driver(db, case_no, case_params=None):
    """
    Fetch the driver information for the driver lease step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        lease = None
        if case_params.get("object_lookup"):
            lease = lease_service.get_lease(
                db=db, lease_id=case_params.get("object_lookup")
            )

        selected_driver_id = ""
        if case_entity:
            lease_driver = lease_service.get_lease_drivers(
                db=db, driver_id=case_entity.identifier_value
            )
            if lease_driver:
                lease = lease_driver.lease
            selected_driver_id = case_entity.identifier_value
        if not lease:
            return {
                "lease_case_details": {},
                "driver_info": {},
                "selected_driver": selected_driver_id,
            }

        lease_case_details = format_lease_response(db, lease)

        if not set(case_params.keys()).intersection(
            ["ssn", "tlc_license_number", "dmv_license_number"]
        ):
            return {
                "lease_case_details": lease_case_details,
                "selected_driver": selected_driver_id,
            }

        if not case_params.get("ssn"):
            raise ValueError("SSN is required")

        driver_details = driver_service.get_drivers(
            db=db,
            ssn=case_params.get("ssn", None),
            tlc_license_number=case_params.get("tlc_license_number", None),
            dmv_license_number=case_params.get("dmv_license_number", None),
        )

        # Check if driver has already signed a valid lease
        if not driver_details:
            return {
                "lease_case_details": lease_case_details,
                "driver_info": {},
                "selected_driver": selected_driver_id,
            }
        if additionaldriver_utils.has_driver_signed_lease(db, lease, driver_details):
            return {
                "lease_case_details": lease_case_details,
                "driver_info": {},
                "selected_driver": selected_driver_id,
            }

        return {
            "lease_case_details": lease_case_details,
            "driver_info": {
                "driver_id": driver_details.id,
                "driver_lookup_id": driver_details.driver_id,
                "first_name": driver_details.first_name,
                "last_name": driver_details.last_name,
                "driver_type": driver_details.driver_type,
                "driver_ssn": driver_details.ssn,
                "tlc_license_number": driver_details.tlc_license.tlc_license_number,
                "dmv_license_number": driver_details.dmv_license.dmv_license_number,
                "contact_number": driver_details.phone_number_1,
            },
            "selected_driver": selected_driver_id,
        }
    except Exception as e:
        logger.error("Error fetching driver information: %s", str(e), exc_info=True)
        raise e


@step(
    step_id="163", name="Process - Add additional drivers to lease", operation="process"
)
def set_additional_driver(db, case_no, step_data):
    """
    Process the driver information for the additional driver step.
    Adds the additional driver and generates their document.
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        case = bpm_service.get_cases(db=db, case_no=case_no)

        driver = driver_service.get_drivers(
            db=db, driver_id=step_data["selected_driver"]["driver_id"]
        )
        if not driver:
            raise ValueError(
                f"The driver id {step_data['selected_driver']['driver_id']} is not present"
            )

        lease = lease_service.get_lease(db=db, lease_id=step_data["lease_id"])

        # If case entity exists and the driver is different from the one being selected,
        # mark the previous driver as inactive
        if (
            case_entity
            and case_entity.identifier_value
            != step_data["selected_driver"]["driver_id"]
        ):
            previous_driver_id = case_entity.identifier_value
            logger.info(
                f"Marking previous additional driver {previous_driver_id} as inactive"
            )

            # Find and mark the previous additional driver as inactive
            previous_lease_driver = lease_service.get_lease_drivers(
                db=db, lease_id=lease.id, driver_id=previous_driver_id
            )

            if previous_lease_driver and previous_lease_driver.is_additional_driver:
                previous_lease_driver.is_active = False
                previous_lease_driver.updated_on = datetime.now(timezone.utc)
                db.add(previous_lease_driver)
                db.flush()
                logger.info(
                    f"Previous additional driver {previous_driver_id} marked as inactive"
                )

            # Update case entity with new driver
            case_entity.identifier_value = step_data["selected_driver"]["driver_id"]
            db.add(case_entity)
            db.flush()

        if not case_entity:
            case_entity = bpm_service.create_case_entity(
                db=db,
                case_no=case_no,
                entity_name=entity_mapper["LEASE_DRIVERS"],
                identifier=entity_mapper["LEASE_DRIVER_ID"],
                identifier_value=step_data["selected_driver"]["driver_id"],
            )

        # Check if driver has already signed a lease document (not as additional driver)
        for lease_driver in lease.lease_driver:
            if (
                lease_driver.is_active
                and lease_driver.driver.id == driver.id
                and not lease_driver.is_additional_driver
            ):
                raise ValueError(
                    f"Driver {driver.full_name} has already signed this document lease"
                )

        # Check if driver is already and additional driver for the lease
        for lease_driver in lease.lease_driver:
            if (
                lease_driver.is_active
                and lease_driver.driver.id == driver.id
                and lease_driver.is_additional_driver
            ):
                raise ValueError(
                    f"Driver {driver.full_name} is an active additional driver for the lease"
                )

        # Add the additional driver to the lease (or reuse if already exists) and get the lease_driver record
        new_lease_driver = additionaldriver_utils.add_additional_driver(
            db, lease, step_data["selected_driver"]
        )

        if not new_lease_driver:
            raise ValueError(
                "Failed to create lease driver record for additional driver"
            )

        # Generate document for the additional driver
        authorized_agent = settings.bat_authorized_agent
        logger.info(
            f"Generating additional driver document for driver {driver.driver_id}"
        )

        logger.info("*********** Generating additional driver documents ***********")
        document_info = additionaldriver_utils.generate_additional_driver_document(
            db, lease, new_lease_driver, authorized_agent
        )
        logger.info("*********** Generated additional driver documents ***********")

        # Create the document record
        upload_service.create_document(
            db,
            new_filename=document_info["document_name"],
            original_extension=document_info["document_format"],
            document_path=document_info["document_path"],
            object_type=document_info["object_type"],
            object_id=document_info["object_lookup_id"],
            notes=document_info["document_note"],
            document_type=document_info["document_type"],
            document_date=document_info["document_date"],
            file_size_kb=0,
        )

        logger.info("Additional driver document generated successfully")

        # Create audit trail
        metadata = {
            "driver_id": driver.id,
            "lease_id": lease.id,
            "vehicle_id": lease.vehicle.id,
        }

        description = f"Additional Driver {driver.full_name} - {driver.driver_id} added to the lease {lease.lease_id}"

        audit_trail_service.create_audit_trail(
            db=db, case=case, meta_data=metadata, description=description
        )

        logger.info("Audit trail created successfully for Additional Driver Flow")
        return "Ok"
    except Exception as e:
        logger.error("Error processing driver information: %s", str(e), exc_info=True)
        raise e


@step(
    step_id="200", name="Fetch - Send Driver Details for Signature", operation="fetch"
)
def send_driver_details_for_sign(db, case_no, case_params=None):
    """
    Fetch documents for additional driver signature
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if not case_entity:
            return {}

        # Get the lease driver from case entity
        lease_driver = lease_service.get_lease_drivers(
            db=db, driver_id=case_entity.identifier_value
        )

        if not lease_driver:
            return {}

        lease = lease_driver.lease
        lease_case_details = format_lease_response(db, lease)

        # Fetch documents for this additional driver
        documents = additionaldriver_utils.fetch_additional_driver_documents(
            db, lease_driver
        )

        return {"lease_case_details": lease_case_details, "documents": documents}
    except Exception as e:
        logger.error(
            "Error fetching signature details for additional driver: %s",
            str(e),
            exc_info=True,
        )
        raise e


@step(
    step_id="200",
    name="Process - Send Driver Details for Signature",
    operation="process",
)
async def process_driver_details_for_sign(db, case_no, step_data):
    """
    Process document signature for additional driver (supports in-person e-sign and print modes)
    """
    try:
        from sqlalchemy import desc

        from app.esign.models import ESignEnvelope
        from app.uploads.models import Document
        from app.utils.docusign_utils import Signer, docusign_client

        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if not case_entity:
            return {}

        # Get the lease driver from case entity
        lease_driver = lease_service.get_lease_drivers(
            db=db, driver_id=case_entity.identifier_value
        )

        if not lease_driver:
            return {}

        lease = lease_driver.lease
        signature_mode = step_data.get("signature_mode")
        joined_date = step_data.get("joined_date")

        # Update date_added if joined_date is provided
        if joined_date:
            lease_driver = lease_service.upsert_lease_driver(
                db=db,
                lease_driver_data={
                    "id": lease_driver.id,
                    "date_added": joined_date,
                },
            )
            logger.info(
                f"Updated date_added for additional driver {lease_driver.driver_id} to {joined_date}"
            )

        # Handle print/wet signature mode
        if signature_mode == "print":
            logger.info(
                f"Processing print signature for additional driver {lease_driver.driver_id}"
            )

            # import pdb

            # pdb.set_trace()
            # Update signature status for wet signature
            additionaldriver_utils.upsert_additional_driver_document_for_wet_signature(
                db=db,
                lease_driver=lease_driver,
                signature_mode=signature_mode,
                print_document_details=step_data.get("print_document_details", []),
            )

            logger.info(
                f"Print signature processed successfully for additional driver {lease_driver.driver_id}"
            )

        # Handle in-person e-sign mode
        if signature_mode == "in-person":
            driver = lease_driver.driver

            # Get the latest document for this additional driver
            latest_document = (
                db.query(Document)
                .filter(
                    Document.object_lookup_id == str(lease_driver.id),
                    Document.document_type == "additional-driver",
                )
                .order_by(desc(Document.created_on))
                .first()
            )

            if not latest_document:
                raise ValueError(
                    f"No document found for additional driver {driver.driver_id}"
                )

            # Check if envelope already exists for this document
            existing_envelope = (
                db.query(ESignEnvelope)
                .filter(
                    ESignEnvelope.object_id == lease_driver.id,
                    ESignEnvelope.object_type == "additional_driver",
                )
                .first()
            )

            if existing_envelope:
                logger.warning(
                    f"DocuSign envelope {existing_envelope.envelope_id} already exists for additional driver {driver.driver_id}. Skipping."
                )
                return "Ok"

            # Get primary driver for the lease
            primary_lease_driver = None
            for ld in lease.lease_driver:
                if ld.is_active and not ld.is_additional_driver:
                    primary_lease_driver = ld
                    break

            if not primary_lease_driver:
                raise ValueError("No primary driver found for the lease")

            primary_driver = primary_lease_driver.driver

            # Check and log if email addresses are missing
            additional_driver_email = driver.email_address
            if not additional_driver_email:
                logger.warning(
                    f"Additional driver {driver.full_name} (ID: {driver.driver_id}) does not have an email address. Using placeholder email from settings."
                )
                additional_driver_email = settings.docusign_placeholder_email

            primary_driver_email = primary_driver.email_address
            if not primary_driver_email:
                logger.warning(
                    f"Primary driver {primary_driver.full_name} (ID: {primary_driver.driver_id}) does not have an email address. Using placeholder email from settings."
                )
                primary_driver_email = settings.docusign_placeholder_email

            # Create signers for DocuSign - both additional driver and primary driver
            # Order matters: recipient_id will be assigned sequentially starting from 1
            # - Signer 1 (Additional Driver) → recipient_id "1" → has_driver_signed
            # - Signer 2 (Primary Driver) → recipient_id "2" → has_frontend_signed
            signers = [
                Signer(
                    name=driver.full_name,
                    email=additional_driver_email,
                    signer_id="additional_driver",
                ),
                Signer(
                    name=primary_driver.full_name,
                    email=primary_driver_email,
                    signer_id="primary_driver",
                ),
            ]

            # Send document to DocuSign for in-person signing
            logger.info(
                f"Sending envelope for additional driver {driver.driver_id} and primary driver {primary_driver.driver_id} with in-person signature mode"
            )

            envelope_response = await docusign_client.send_envelope_async(
                source_s3_key=latest_document.document_path,
                document_name=f"Additional Driver Agreement for {driver.full_name}",
                signers=signers,
                signature_mode=signature_mode,
                project_name="additionaldriver",
                case_no=case_no,
                signing_position_info={
                    "additional_driver": {
                        "signHereTabs": [
                            {
                                "documentId": "1",
                                "pageNumber": "5",
                                "xPosition": "350",
                                "yPosition": "185",
                                "required": "true",
                                "tabLabel": "Additional Driver Signature",
                            }
                        ],
                        "fullNameTabs": [
                            {
                                "documentId": "1",
                                "pageNumber": "5",
                                "xPosition": "350",
                                "yPosition": "210",
                                "required": "true",
                                "tabLabel": "Additional Driver Print Name",
                            }
                        ],
                    },
                    "primary_driver": {
                        "signHereTabs": [
                            {
                                "documentId": "1",
                                "pageNumber": "5",
                                "xPosition": "350",
                                "yPosition": "280",
                                "required": "true",
                                "tabLabel": "Primary Driver Signature",
                            }
                        ],
                        "fullNameTabs": [
                            {
                                "documentId": "1",
                                "pageNumber": "5",
                                "xPosition": "350",
                                "yPosition": "305",
                                "required": "true",
                                "tabLabel": "Primary Driver Print Name",
                            }
                        ],
                        "dateSignedTabs": [
                            {
                                "documentId": "1",
                                "pageNumber": "5",
                                "xPosition": "100",
                                "yPosition": "360",
                                "required": "true",
                                "tabLabel": "Date",
                            }
                        ],
                    }
                },
            )

            # Create envelope record
            envelope_id = envelope_response["envelope_id"]
            new_envelope = ESignEnvelope(
                envelope_id=envelope_id,
                status=envelope_response["status"],
                object_type="additional_driver",
                object_id=lease_driver.id,
            )
            db.add(new_envelope)
            db.flush()
            db.refresh(new_envelope)

            logger.info(
                f"Envelope {envelope_id} created for additional driver {driver.driver_id}"
            )

            # Update lease driver document record
            additionaldriver_utils.upsert_additional_driver_document(
                db=db,
                lease_driver=lease_driver,
                signature_mode=signature_mode,
                envelope_id=envelope_id,
            )

        # Create audit trail at the end
        case = bpm_service.get_cases(db=db, case_no=case_no)
        driver = lease_driver.driver

        # Conditional description based on signature mode
        if signature_mode == "print":
            description = f"Additional Driver {driver.full_name} - {driver.driver_id} documents sent for print/wet signature"
        elif signature_mode == "in-person":
            description = f"Additional Driver {driver.full_name} - {driver.driver_id} documents sent for in-person e-signature"
        else:
            description = f"Additional Driver {driver.full_name} - {driver.driver_id} documents sent for signature"

        metadata = {
            "driver_id": driver.id,
            "lease_id": lease.id,
            "lease_drivers_id": lease_driver.id,
            "vehicle_id": lease.vehicle.id if lease.vehicle else None,
            "signature_mode": signature_mode,
        }

        audit_trail_service.create_audit_trail(
            db=db, case=case, meta_data=metadata, description=description
        )
        logger.info(
            f"Audit trail created for step 200 - additional driver signature process"
        )

        return "Ok"
    except Exception as e:
        logger.error(
            "Error processing additional driver signature: %s", str(e), exc_info=True
        )
        raise e


@step(
    step_id="201",
    name="Fetch - Complete Driver Details for Signature",
    operation="fetch",
)
def complete_driver_details_for_sign(db, case_no, case_params=None):
    """
    Fetch documents for completing additional driver signature
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if not case_entity:
            return {}

        # Get the lease driver from case entity
        lease_driver = lease_service.get_lease_drivers(
            db=db, driver_id=int(case_entity.identifier_value)
        )

        if not lease_driver:
            return {}

        lease = lease_driver.lease
        lease_case_details = format_lease_response(db, lease)

        # Fetch documents for this additional driver
        documents = additionaldriver_utils.fetch_additional_driver_documents(
            db, lease_driver
        )

        return {"lease_case_details": lease_case_details, "documents": documents}
    except Exception as e:
        logger.error(
            "Error fetching documents for completing additional driver signature: %s",
            str(e),
            exc_info=True,
        )
        raise e


@step(
    step_id="201",
    name="Process - Complete Driver Details for Signature",
    operation="process",
)
def process_complete_driver_details_for_sign(db, case_no, step_data):
    """
    Process completing additional driver signature
    """
    try:
        from app.drivers.schemas import DriverStatus

        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if not case_entity:
            return "Ok"

        # Get the lease driver from case entity
        lease_driver = lease_service.get_lease_drivers(
            db=db, driver_id=int(case_entity.identifier_value)
        )

        if not lease_driver:
            return "Ok"

        lease = lease_driver.lease
        driver = lease_driver.driver
        case = bpm_service.get_cases(db=db, case_no=case_no)

        # Mark the additional driver as ACTIVE
        if driver and driver.driver_status != DriverStatus.ACTIVE:
            driver = driver_service.upsert_driver(
                db, {"id": driver.id, "driver_status": DriverStatus.ACTIVE}
            )
            logger.info(
                f"Additional driver {driver.driver_id} status updated to ACTIVE"
            )
        else:
            logger.info(
                f"Additional driver {driver.driver_id} is already ACTIVE"
            )

        # Create audit trail
        metadata = {
            "driver_id": driver.id,
            "lease_id": lease.id,
            "lease_drivers_id": lease_driver.id,
            "vehicle_id": lease.vehicle.id if lease.vehicle else None,
        }

        description = f"Additional Driver {driver.full_name} - {driver.driver_id} signature process completed"

        audit_trail_service.create_audit_trail(
            db=db, case=case, meta_data=metadata, description=description
        )
        logger.info(
            f"Audit trail created for step 201 - additional driver signature completion"
        )

        return "Ok"
    except Exception as e:
        logger.error(
            "Error completing additional driver signature: %s", str(e), exc_info=True
        )
        raise e
