### app/leases/search_service.py

# Third party imports
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.sql import desc, func

# Local imports
from app.bpm.models import CaseEntity
from app.core.config import settings
from app.leases.models import Lease, LeaseDriver, LeaseDriverDocument
from app.medallions.utils import format_medallion_response
from app.uploads.models import Document
from app.utils.general import format_us_phone_number


def get_active_leases(db: Session):
    """Get all active leases"""
    return (
        db.query(Lease)
        .options(
            joinedload(Lease.medallion),
            joinedload(Lease.vehicle).joinedload("registrations"),
            joinedload(Lease.lease_driver)
            .joinedload(LeaseDriver.driver)
            .joinedload("tlc_license"),
            joinedload(Lease.lease_driver)
            .joinedload(LeaseDriver.driver)
            .joinedload("dmv_license"),
            joinedload(Lease.lease_driver).joinedload("documents"),
        )
        .filter(Lease.is_active.is_(True))
        .all()
    )


def format_lease_response(db: Session, lease):
    """Format a lease response"""
    from app.bpm.models import Case, CaseEntity
    from app.leases.models import LeaseConfiguration

    # Get lease amount from configurations
    lease_amount = 0.0
    lease_config = (
        db.query(LeaseConfiguration)
        .filter(
            LeaseConfiguration.lease_id == lease.id,
            LeaseConfiguration.lease_breakup_type == "lease_amount",
        )
        .first()
    )

    if lease_config and lease_config.lease_limit:
        lease_amount = float(lease_config.lease_limit)

    # Get driver lease case (DRVLEA) - regardless of status
    case_detail = None
    lease_termination_case_no = None

    from app.bpm.models import Case, CaseStatus, CaseType

    # Get DRIVERLEASE case (prefix DRVLEA) - regardless of status
    driverlease_case_entity = (
        db.query(CaseEntity)
        .join(Case, CaseEntity.case_no == Case.case_no)
        .join(CaseType, Case.case_type_id == CaseType.id)
        .filter(
            CaseEntity.entity_name == "lease",
            CaseEntity.identifier == "id",
            CaseEntity.identifier_value == str(lease.id),
            CaseType.prefix == "DRVLEA",
        )
        .order_by(CaseEntity.created_on.desc())
        .first()
    )

    if driverlease_case_entity:
        case = (
            db.query(Case)
            .filter(Case.case_no == driverlease_case_entity.case_no)
            .first()
        )
        if case:
            case_detail = {
                "case_no": case.case_no,
                "case_status": case.case_status.name if case.case_status else None,
            }

    # Check for TERMINATELEASE case (prefix TERMLEA) - only OPEN/IN_PROGRESS
    terminate_case_entity = (
        db.query(CaseEntity)
        .join(Case, CaseEntity.case_no == Case.case_no)
        .join(CaseType, Case.case_type_id == CaseType.id)
        .join(CaseStatus, Case.case_status_id == CaseStatus.id)
        .filter(
            CaseEntity.entity_name == "lease",
            CaseEntity.identifier == "id",
            CaseEntity.identifier_value == str(lease.id),
            CaseType.prefix == "TERMLEA",
            CaseStatus.name.in_(["OPEN", "IN_PROGRESS"]),
        )
        .order_by(CaseEntity.created_on.desc())
        .first()
    )

    if terminate_case_entity:
        lease_termination_case_no = terminate_case_entity.case_no

    # Determine shift information for shift-lease types
    shift_type = ""
    if lease.lease_type == "shift-lease":
        if lease.is_day_shift and lease.is_night_shift:
            shift_type = "Full"
        elif lease.is_day_shift:
            shift_type = "Day"
        elif lease.is_night_shift:
            shift_type = "Night"

    lease_details = {
        "lease_id": lease.lease_id,
        "lease_id_pk": lease.id,
        "medallion_number": lease.medallion.medallion_number
        if lease.medallion
        else None,
        "vehicle_vin_number": lease.vehicle.vin if lease.vehicle else None,
        "vehicle_plate_number": lease.vehicle.registrations[0].plate_number
        if lease.vehicle and lease.vehicle.registrations
        else "",
        "lease_date": lease.lease_start_date.strftime("%Y-%m-%d")
        if lease.lease_start_date
        else None,
        "lease_type": lease.lease_type,
        "lease_status": lease.lease_status,
        "lease_end_date": lease.lease_end_date.strftime("%Y-%m-%d")
        if lease.lease_end_date
        else None,
        "last_renewed_date": lease.last_renewed_date.strftime("%Y-%m-%d")
        if lease.last_renewed_date
        else None,
        "current_segment": lease.current_segment,
        "total_segments": lease.total_segments,
        "lease_amount": f"{lease_amount:,.2f}",
        "case_detail": case_detail,
        "lease_termination_case_no": lease_termination_case_no,
        "shift_type": shift_type,
        "driver": [],
        "removed_drivers": [],
        "has_documents": False,
    }

    medallion = format_medallion_response(lease.medallion) if lease.medallion else None
    lease_details["medallion_owner"] = (
        medallion["medallion_owner"] if medallion else None
    )

    for lease_driver in lease.lease_driver:
        if not lease_driver.is_active:
            continue

        documents_count = len(lease_driver.documents)
        if documents_count > 0:
            lease_details["has_documents"] = True

        driver = lease_driver.driver

        # Fetch all documents for this driver from lease_driver.documents relationship
        driver_documents = []

        # Use the lease_driver.documents relationship which contains LeaseDriverDocument records
        for lease_driver_doc in lease_driver.documents:
            # Fetch the actual document from the Document table using document_id
            if lease_driver_doc.document_id:
                document = (
                    db.query(Document)
                    .filter(Document.id == lease_driver_doc.document_id)
                    .first()
                )
                if document:
                    driver_documents.append(
                        {
                            "document_id": document.id,
                            "document_name": document.document_name,
                            "document_type": document.document_type,
                            "document_format": document.document_format,
                            "document_date": document.document_date.strftime("%Y-%m-%d")
                            if document.document_date
                            else None,
                            "document_size": document.document_actual_size,
                            "document_note": document.document_note,
                            "presigned_url": document.presigned_url,
                            "object_type": document.object_type,
                            "created_on": document.created_on.strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                            if document.created_on
                            else None,
                            "has_frontend_signed": lease_driver_doc.has_frontend_signed,
                            "has_driver_signed": lease_driver_doc.has_driver_signed,
                            "document_envelope_id": lease_driver_doc.document_envelope_id,
                            "signing_type": lease_driver_doc.signing_type,
                        }
                    )

        # Get case details for additional driver
        case_detail = None
        if lease_driver.is_additional_driver:
            case_entity = (
                db.query(CaseEntity)
                .filter(
                    CaseEntity.entity_name == "lease_drivers",
                    CaseEntity.identifier == "driver_id",
                    CaseEntity.identifier_value == driver.driver_id,
                )
                .order_by(CaseEntity.created_on.desc())
                .first()
            )
            if case_entity:
                from app.bpm.models import Case

                case = (
                    db.query(Case)
                    .filter(Case.case_no == case_entity.case_no)
                    .order_by(Case.created_on.desc())
                    .first()
                )
                if case:
                    case_detail = {
                        "case_no": case.case_no,
                        "case_status": case.case_status.name
                        if case.case_status
                        else None,
                    }
        # MIGRATION: This is a migration issue where lease driver is present but driver is not present
        if not driver:
            continue

        lease_details["driver"].append(
            {
                "driver_id_pk": driver.id,
                "tlc_license_no": driver.tlc_license.tlc_license_number
                if driver.tlc_license
                else None,
                "dmv_license_no": driver.dmv_license.dmv_license_number
                if driver.dmv_license
                else None,
                "ssn": driver.ssn,
                "phone_number": format_us_phone_number(driver.phone_number_1),
                "driver_id": driver.driver_id,
                "driver_name": f"{driver.first_name} {driver.last_name}",
                "driver_status": driver.driver_status,
                "is_driver_manager": bool(lease_driver.documents),
                "driver_lease_id": lease_driver.id,
                "is_additional_driver": True
                if lease_driver.is_additional_driver
                else False,
                "joined_date": lease_driver.date_added.strftime(
                    settings.common_date_format
                )
                if lease_driver.date_added and settings.common_date_format
                else None,
                "case_detail": case_detail,
                "documents": driver_documents,
            }
        )

    # Add removed additional drivers
    for lease_driver in lease.lease_driver:
        if lease_driver.is_active or not lease_driver.is_additional_driver:
            continue

        driver = lease_driver.driver

        # Get case details for removed additional driver
        case_entity = (
            db.query(CaseEntity)
            .filter(
                CaseEntity.entity_name == "lease_drivers",
                CaseEntity.identifier == "driver_id",
                CaseEntity.identifier_value == driver.driver_id,
            )
            .first()
        )

        case_detail = None
        if case_entity:
            from app.bpm.models import Case

            case = db.query(Case).filter(Case.case_no == case_entity.case_no).first()
            if case:
                case_detail = {
                    "case_no": case.case_no,
                    "case_status": case.case_status.name if case.case_status else None,
                }

        lease_details["removed_drivers"].append(
            {
                "driver_id_pk": driver.id,
                "tlc_license_no": driver.tlc_license.tlc_license_number
                if driver.tlc_license
                else None,
                "dmv_license_no": driver.dmv_license.dmv_license_number
                if driver.dmv_license
                else None,
                "ssn": driver.ssn,
                "phone_number": format_us_phone_number(driver.phone_number_1),
                "driver_id": driver.driver_id,
                "driver_name": f"{driver.first_name} {driver.last_name}",
                "driver_status": driver.driver_status,
                "driver_lease_id": lease_driver.id,
                "is_additional_driver": True,
                "joined_date": lease_driver.date_added.strftime(
                    settings.common_date_format
                )
                if lease_driver.date_added and settings.common_date_format
                else None,
                "removed_date": lease_driver.date_terminated.strftime(
                    settings.common_date_format
                )
                if lease_driver.date_terminated and settings.common_date_format
                else None,
                "case_detail": case_detail,
            }
        )

    return lease_details


def format_lease_export(db: Session, lease):
    """Format a lease response"""
    import json
    from app.bpm.models import Case, CaseEntity
    from app.leases.models import LeaseConfiguration

    # Get lease amount from configurations
    lease_amount = 0.0
    lease_config = (
        db.query(LeaseConfiguration)
        .filter(
            LeaseConfiguration.lease_id == lease.id,
            LeaseConfiguration.lease_breakup_type == "lease_amount",
        )
        .first()
    )

    if lease_config and lease_config.lease_limit:
        lease_amount = float(lease_config.lease_limit)

    # Determine shift information for shift-lease types
    shift_type = None
    if lease.lease_type == "shift-lease":
        if lease.is_day_shift and lease.is_night_shift:
            shift_type = "Full"
        elif lease.is_day_shift:
            shift_type = "Day"
        elif lease.is_night_shift:
            shift_type = "Night"

    temp_lease_drivers = [] 

    lease_details = {
        "lease_id": lease.lease_id,
        "medallion_number": lease.medallion.medallion_number
        if lease.medallion
        else "N/A",
        "vehicle_vin_number": lease.vehicle.vin if lease.vehicle else "N/A",
        "vehicle_plate_number": lease.vehicle.registrations[0].plate_number
        if lease.vehicle and lease.vehicle.registrations
        else "N/A",
        "lease_date": lease.lease_start_date.strftime("%Y-%m-%d")
        if lease.lease_start_date
        else "N/A",
        "lease_type": lease.lease_type if lease.lease_type else "N/A",
        "lease_amount": f"{lease_amount:,.2f}",
        "shift_type": shift_type,
        # The key 'lease_drivers' will be populated via temp_lease_drivers and serialized
        "has_documents": False,
    }

    for lease_driver in lease.lease_driver:
        documents_count = len(lease_driver.documents)
        if documents_count > 0:
            lease_details["has_documents"] = True

        driver = lease_driver.driver

        # MIGRATION: Handle case where lease driver is present but driver is not present
        if not driver:
            continue

        temp_lease_drivers.append(
            {
                "driver_id": driver.driver_id,
                "tlc_license_no": driver.tlc_license.tlc_license_number
                if driver.tlc_license and driver.tlc_license.tlc_license_number
                else None,
                "dmv_license_no": driver.dmv_license.dmv_license_number
                if driver.dmv_license and driver.dmv_license.dmv_license_number
                else None,
                "ssn": f"xxx-xx-{driver.ssn[-4:]}" if driver.ssn and len(driver.ssn) >= 4 else None,
                # Ensure phone number is passed as a string/None
                "phone_number": format_us_phone_number(str(driver.phone_number_1) if driver.phone_number_1 else None),
                "driver_name": f"{driver.first_name} {driver.last_name}",
                "is_driver_manager": bool(lease_driver.documents),
                "is_active": lease_driver.is_active,
                "is_additional_driver": lease_driver.is_additional_driver
            }
        )

    # --- FIX: Convert unhashable types (list/dict) to hashable strings (JSON) ---
    
    # 1. Serialize the list of driver dictionaries
    lease_details["lease_drivers"] = json.dumps(temp_lease_drivers)
    
    return lease_details