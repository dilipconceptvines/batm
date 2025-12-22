## app/bpm_flows/additionaldriver/utils.py

# Standard imports
import json
from datetime import date, datetime, timezone

# Third-party imports
from sqlalchemy import and_, desc, exists
from sqlalchemy.orm import Session

# Local imports
from app.core.config import settings
from app.drivers.schemas import DriverStatus
from app.drivers.services import driver_service
from app.drivers.utils import extract_driver_info
from app.leases.models import Lease, LeaseDriver, LeaseDriverDocument
from app.leases.services import lease_service
from app.medallions.utils import extract_medallion_info
from app.uploads.models import Document
from app.utils.lambda_utils import LambdaInvocationError, invoke_lambda_function
from app.utils.logger import get_logger
from app.vehicles.utils import extract_vehicle_info

logger = get_logger(__name__)


def add_additional_driver(db: Session, lease: Lease, driver_update_info: dict):
    driver_id = driver_update_info.get("driver_id")
    is_day_night_shift = driver_update_info.get("is_day_night_shift")

    valid_driver = driver_service.get_drivers(db=db, driver_id=driver_id)

    if is_day_night_shift is None:
        driver_role = "L"
    elif is_day_night_shift:
        driver_role = "DL"
    else:
        driver_role = "NL"

    lease_driver = (
        db.query(LeaseDriver)
        .filter(
            LeaseDriver.driver_id == driver_id,
            LeaseDriver.lease_id == lease.id,
            LeaseDriver.is_active,
        )
        .first()
    )

    data = {}

    if lease_driver:
        data = {
            "id": lease_driver.id,
            "is_day_night_shift": is_day_night_shift,
            "is_additional_driver": True,
        }
    else:
        data = {
            "driver_id": driver_id,
            "lease_id": lease.id,
            "driver_role": driver_role,
            "is_day_night_shift": is_day_night_shift,
            "date_added": datetime.utcnow(),
            "is_additional_driver": True,
        }

    updated_lease_driver = lease_service.upsert_lease_driver(
        db=db, lease_driver_data=data
    )

    logger.info(
        f"Additional Driver {driver_id} added successfully for lease {lease.lease_id}."
    )

    return updated_lease_driver


def has_driver_signed_lease(db: Session, lease: Lease, driver):
    """Check if a driver has already signed a lease for the given lease."""
    return db.query(
        exists().where(
            and_(
                LeaseDriver.driver_id == driver.driver_id,
                LeaseDriver.lease_id == lease.id,
                LeaseDriverDocument.lease_driver_id == LeaseDriver.id,
            )
        )
    ).scalar()


def fetch_additional_driver_documents(db: Session, lease_driver: LeaseDriver):
    """
    Fetch all documents and signature details for a specific additional driver.
    Returns a list of document dictionaries with all required details.
    """
    # Query documents for this specific additional driver
    latest_docs = (
        db.query(Document)
        .filter(
            Document.object_lookup_id == str(lease_driver.id),
            Document.document_type == "additional-driver",
        )
        .order_by(desc(Document.created_on))
        .all()
    )

    # Check if there's a LeaseDriverDocument for this driver
    lease_driver_document = (
        db.query(LeaseDriverDocument)
        .filter(
            LeaseDriverDocument.lease_driver_id == lease_driver.id,
            LeaseDriverDocument.is_active,
        )
        .first()
    )

    documents = []
    for latest_document in latest_docs:
        documents.append(
            {
                "document_id": latest_document.id,
                "driver_id": lease_driver.driver_id,
                "driver_name": lease_driver.driver.full_name
                if lease_driver.driver
                else "N/A",
                "driver_email": lease_driver.driver.email_address
                if lease_driver.driver
                else "N/A",
                "document_name": latest_document.document_name,
                "is_sent_for_signature": True if lease_driver_document else False,
                "has_front_desk_signed": lease_driver_document.has_frontend_signed
                if lease_driver_document
                else False,
                "has_driver_signed": lease_driver_document.has_driver_signed
                if lease_driver_document
                else False,
                "document_envelope_id": lease_driver_document.document_envelope_id
                if lease_driver_document
                else None,
                "document_date": latest_document.document_date,
                "file_size": latest_document.document_actual_size
                if latest_document.document_actual_size
                else 0,
                "comments": latest_document.document_note,
                "document_type": latest_document.document_type,
                "object_type": latest_document.object_type,
                "presigned_url": latest_document.presigned_url,
                "document_format": latest_document.document_format,
                "document_created_on": latest_document.created_on,
                "object_lookup_id": lease_driver.id,
                "signing_type": lease_driver_document.signing_type
                if lease_driver_document
                else "",
            }
        )

    return documents


def prepare_additional_driver_document(
    db: Session, lease: Lease, lease_driver: LeaseDriver, authorized_agent: str
) -> dict:
    """
    Prepare additional driver document with dynamic data from the lease and driver.
    This document includes:
    - Primary driver info (driver_*): from the primary/non-additional driver in the lease
    - Additional driver info (additional_driver_*): from the lease_driver parameter (additional driver)
    - Vehicle/medallion info: from the lease
    """
    # Additional driver (passed as parameter)
    additional_driver = lease_driver.driver if lease_driver else None

    # Find primary driver (non-additional driver) from the lease
    primary_driver = None
    if lease and lease.lease_driver:
        for ld in lease.lease_driver:
            if ld.is_active and not ld.is_additional_driver:
                primary_driver = ld.driver
                break

    # Extract information using helper functions
    primary_driver_info = extract_driver_info(primary_driver)
    additional_driver_info = extract_driver_info(additional_driver)
    vehicle_info = extract_vehicle_info(lease.vehicle if lease else None)
    medallion_info = extract_medallion_info(lease.medallion if lease else None)

    # Build the document info
    additional_driver_document_info = {
        # Primary driver fields (from non-additional driver)
        "driver_name": primary_driver_info["name"],
        "date_of_agreement": date.today().strftime(settings.common_date_format),
        "driver_address": primary_driver_info["address"],
        "driver_ssn": primary_driver_info["ssn"],
        "driver_dmv_license": primary_driver_info["dmv_license"],
        "driver_tlc_license": primary_driver_info["tlc_license"],
        "driver_primary_phone": primary_driver_info["primary_phone"],
        # Vehicle and medallion fields
        "medallion_number": medallion_info["number"],
        "plate_number": vehicle_info["plate_number"],
        "vehicle_make": vehicle_info["make"],
        "vehicle_model": vehicle_info["model"],
        "vehicle_vin": vehicle_info["vin"],
        # Additional driver fields (from lease_driver parameter)
        "additional_driver_name": additional_driver_info["name"],
        "additional_date_of_agreement": date.today().strftime(settings.common_date_format),
        "additional_driver_address": additional_driver_info["address"],
        "additional_primary_phone": additional_driver_info["primary_phone"],
        "additional_driver_ssn": additional_driver_info["ssn"],
        "additional_dmv_driver_license": additional_driver_info["dmv_license"],
        "additional_driver_tlc": additional_driver_info["tlc_license"],
    }
    return additional_driver_document_info


def upsert_additional_driver_document(
    db: Session,
    lease_driver: LeaseDriver,
    signature_mode: str,
    envelope_id: str = None,
):
    """
    Create or update LeaseDriverDocument record for additional driver signature.

    Args:
        db: Database session
        lease_driver: The LeaseDriver record
        signature_mode: Signature mode (in-person, email, print)
        envelope_id: DocuSign envelope ID (optional for print mode)
    """
    # Check if document already exists
    existing_document = (
        db.query(LeaseDriverDocument)
        .filter(
            LeaseDriverDocument.lease_driver_id == lease_driver.id,
            LeaseDriverDocument.is_active,
        )
        .first()
    )

    if existing_document:
        # Mark existing as inactive
        existing_document.is_active = False
        db.add(existing_document)
        db.flush()

    # Create new lease driver document
    # Do NOT mark as signed initially - webhooks will update signature status
    new_document = LeaseDriverDocument(
        lease_driver_id=lease_driver.id,
        document_envelope_id=envelope_id,
        signing_type=signature_mode,
        has_frontend_signed=False,
        has_driver_signed=False,
        frontend_signed_date=None,
        driver_signed_date=None,
        is_active=True,
        created_on=datetime.utcnow(),
        updated_on=datetime.utcnow(),
    )
    db.add(new_document)
    db.flush()
    db.refresh(new_document)

    logger.info(
        f"Created lease driver document for additional driver {lease_driver.driver_id} with envelope {envelope_id}. Signature status will be updated via webhooks."
    )

    return new_document


def upsert_additional_driver_document_for_wet_signature(
    db: Session,
    lease_driver: LeaseDriver,
    signature_mode: str,
    print_document_details: list,
):
    """
    Create or update LeaseDriverDocument records for additional driver wet/print signature.

    Args:
        db: Database session
        lease_driver: The LeaseDriver record for the additional driver
        signature_mode: Signature mode (should be "print")
        print_document_details: List of document details with signature status
            Example: [{"document_id": 123, "has_front_desk_signed": True, "has_driver_signed": True}]

    Returns:
        List of created document records
    """
    try:
        documents = []
        for document_detail in print_document_details:
            # Check if document already exists
            existing_document = (
                db.query(LeaseDriverDocument)
                .filter(
                    LeaseDriverDocument.lease_driver_id == lease_driver.id,
                    LeaseDriverDocument.is_active,
                )
                .first()
            )

            if existing_document:
                # Mark existing as inactive
                existing_document.is_active = False
                db.add(existing_document)
                db.flush()

            # Create new lease driver document for wet signature
            lease_document = LeaseDriverDocument(
                lease_driver_id=lease_driver.id,
                document_envelope_id=None,  # No envelope for wet signature
                document_id=document_detail.get("document_id", None),
                has_frontend_signed=document_detail.get("has_front_desk_signed", False),
                has_driver_signed=document_detail.get("has_driver_signed", False),
                frontend_signed_date=datetime.now(timezone.utc)
                if document_detail.get("has_front_desk_signed")
                else None,
                driver_signed_date=datetime.now(timezone.utc)
                if document_detail.get("has_driver_signed")
                else None,
                signing_type=signature_mode,
                is_active=True,
                created_on=datetime.now(timezone.utc),
                updated_on=datetime.now(timezone.utc),
            )
            db.add(lease_document)
            db.flush()
            db.refresh(lease_document)

            documents.append(
                {
                    "lease_driver_id": lease_driver.id,
                    "driver_id": lease_driver.driver_id,
                    "document_id": document_detail.get("document_id"),
                    "lease_driver_document_id": lease_document.id,
                    "document_envelope_id": lease_document.document_envelope_id,
                    "has_frontend_signed": lease_document.has_frontend_signed,
                    "has_driver_signed": lease_document.has_driver_signed,
                }
            )

        logger.info(
            f"Created {len(documents)} wet signature document(s) for additional driver {lease_driver.driver_id}"
        )
        return documents
    except Exception as e:
        logger.error(
            "Error upserting additional driver document for wet signature: %s",
            str(e),
            exc_info=True,
        )
        raise e


def generate_additional_driver_document(
    db: Session, lease: Lease, lease_driver: LeaseDriver, authorized_agent: str
):
    """Generate additional driver document"""
    try:
        # Prepare payload for Lambda function
        payload = {
            "data": prepare_additional_driver_document(
                db, lease, lease_driver, authorized_agent
            ),
            "bucket": settings.s3_bucket_name,
            "identifier": f"form_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            "template_id": settings.additional_driver_template_id,
        }

        logger.info(
            "Calling Lambda function with payload for additional driver document: %s",
            payload,
        )

        response = invoke_lambda_function(function_name="pdf_filler", payload=payload)

        # Extract s3_key from response
        logger.info("Response from Lambda: %s", response)
        response_body = json.loads(response["body"])
        s3_key = response_body.get("s3_key")

        return {
            "document_name": f"Additional Driver Document for Lease ID {lease.lease_id} for Driver ID {lease_driver.driver_id}",
            "document_format": "PDF",
            "document_path": s3_key,
            "document_type": "additional-driver",
            "object_type": f"ad-{lease_driver.driver_id}",
            "object_lookup_id": str(lease_driver.id),
            "document_note": "Additional driver agreement document created",
            "document_date": datetime.now(timezone.utc).isoformat().split("T")[0],
        }
    except LambdaInvocationError as e:
        logger.error(
            "Lambda error generating additional driver document: %s (Status: %s)",
            e.message,
            e.status_code,
        )
        raise
    except Exception as e:
        logger.error(
            "Error generating additional driver document: %s", str(e), exc_info=True
        )
        raise
