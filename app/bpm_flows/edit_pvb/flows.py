from sqlalchemy.orm import Session
from fastapi import HTTPException

from decimal import Decimal

from app.utils.logger import get_logger
from app.bpm.services import bpm_service
from app.audit_trail.services import audit_trail_service
from app.bpm.step_info import step
from app.leases.services import lease_service
from app.leases.schemas import LeaseStatus
from app.drivers.services import driver_service
from app.vehicles.services import vehicle_service
from app.medallions.services import medallion_service
from app.uploads.services import upload_service
from app.pvb.services import PVBService
from app.pvb.models import PVBDisposition
from app.ledger.services import LedgerService
from app.ledger.repository import LedgerRepository
from app.ledger.models import PostingCategory , EntryType
from app.medallions.utils import format_medallion_response
from app.bpm_flows.edit_pvb.utils import format_pvb_violation

logger = get_logger(__name__)

ENTITY_MAPPER = {
    "PVB": "pvb_violation",
    "PVB_IDENTIFIER": "id",
}

@step(step_id="225", name="Fetch - pvb details", operation="fetch")
def fetch_pvb_details(db: Session, case_no: str, case_params: dict = None):
    """
    Fetches driver and associated active lease information for the TLC violation workflow.
    """

    try:
        pvb_service = PVBService(db=db)
        violation = None
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if case_entity:
            violation = pvb_service.repo.get_violation_by_id(int(case_entity.identifier_value))
        if not violation and case_params and case_params.get("object_name") == "pvb":
            violation = pvb_service.repo.get_violation_by_id(int(case_params.get("object_lookup")))

        if  not violation:
            return {}
        
        if not case_entity:
            bpm_service.create_case_entity(
                db, case_no, ENTITY_MAPPER["PVB"], ENTITY_MAPPER["PVB_IDENTIFIER"], str(violation.id)
            )

        pvb_document = upload_service.get_documents(
            db=db,
            object_type="pvb",
            object_id=violation.id,
            document_type="pvb_invoice"
        )

        altered_documents = upload_service.get_documents(
            db=db,
            object_type="pvb",
            object_id=violation.id,
            like_document_type="additional_document",
            multiple=True
        )

        if not altered_documents:
            altered_documents = [
                 {
                "document_id": "",
                "document_name": "",
                "document_note": "",
                "document_path": "",
                "document_type": "additional_document_1",
                "document_date": "",
                "document_object_type": "pvb",
                "document_object_id": violation.id,
                "document_size": "",
                "document_uploaded_date": "",
                "presigned_url": "",
            }
            ]
            

        logger.info("Successfully fetched driver and lease details for PVB case", case_no=case_no)

        pvb_data = format_pvb_violation(violation)

        driver = driver_service.get_drivers(db, id=violation.driver_id) if violation.driver_id else None
        active_lease = lease_service.get_lease(db,lookup_id=violation.lease_id, status= LeaseStatus.ACTIVE.value) if violation.lease_id else None

        if not driver:
            logger.info("No driver found for TLC case", case_no=case_no)
            return {
                "driver": None,
                "leases": [],
                "pvb_violation": pvb_data,
                "pvb_document": pvb_document,
                "additional_documents": altered_documents
            }
        
        if not active_lease:
            logger.warning("No active lease found for driver", driver_id=driver.id)
            return {
                "driver": {
                    "id": driver.id,
                    "driver_id": driver.driver_id,
                    "full_name": driver.full_name,
                    "status": driver.driver_status.value if hasattr(driver.driver_status, 'value') else str(driver.driver_status),
                    "tlc_license": driver.tlc_license.tlc_license_number if driver.tlc_license else "N/A",
                    "phone": driver.phone_number_1 or "N/A",
                    "email": driver.email_address or "N/A",
                },
                "lease": {},
                "pvb_violation": pvb_data,
                "pvb_document": pvb_document,
                "additional_documents": altered_documents
            }

        medallion_owner = format_medallion_response(active_lease.medallion).get("medallion_owner") if active_lease.medallion else None

        lease_confis = active_lease.lease_configuration
        lease_amount = 0

        if lease_confis:
            config = config = next(
                (c for c in lease_confis if c.lease_breakup_type == "lease_amount"),
                None
            )
            if config and config.lease_limit:
                lease_amount = float(config.lease_limit)

        format_data = {}
        format_data["lease"] = {
                "id": active_lease.id,
                "lease_id": active_lease.lease_id,
                "lease_type": active_lease.lease_type if active_lease.lease_type else "N/A",
                "status": active_lease.lease_status if active_lease.lease_status else "N/A",
                "start_date": active_lease.lease_start_date if active_lease.lease_start_date else "N/A",
                "end_date": active_lease.lease_end_date if active_lease.lease_end_date else "N/A",
                "amount": f"{lease_amount:,.2f}",
            }
        format_data["medallion"]= {
            "medallion_id": active_lease.medallion_id if active_lease.medallion_id else None,
            "medallion_number": active_lease.medallion.medallion_number if active_lease.medallion else "N/A",
            "medallion_owner": medallion_owner,
        }

        format_data["vehicle"] = {
            "vehicle_id": active_lease.vehicle_id if active_lease.vehicle_id else None,
            "plate_no": active_lease.vehicle.registrations[0].plate_number if active_lease.vehicle and active_lease.vehicle.registrations else "N/A",
            "vin": active_lease.vehicle.vin if active_lease.vehicle else "N/A",
            "vehicle": " ".join(filter(None , [active_lease.vehicle.make, active_lease.vehicle.model, active_lease.vehicle.year]))
        }
        
        format_data["driver"] = {
            "id": driver.id,
            "driver_id": driver.driver_id,
            "full_name": driver.full_name,
            "status": driver.driver_status.value if hasattr(driver.driver_status, 'value') else str(driver.driver_status),
            "tlc_license": driver.tlc_license.tlc_license_number if driver.tlc_license else "N/A",
            "phone": driver.phone_number_1 or "N/A",
            "email": driver.email_address or "N/A",
        }

        return {
            "data": format_data,
            "pvb_document": pvb_document,
            "pvb_violation": pvb_data,
            "additional_documents": altered_documents
        }
    except Exception as e:
        logger.error("Error in TLC choose_driver_fetch: %s", e, exc_info=True)
        raise e

@step(step_id="225", name="Process - pvb details", operation="process")
def process_pvb_details(db: Session, case_no: str, step_data: dict):
    """
    Edit the PVB violation record.
    """
    try:
        logger.info("Processing PVB violation", case_no=case_no)

        pvb_service = PVBService(db=db)

        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if not case_entity:
            raise HTTPException(status_code=404, detail="Case entity not found")
        
        violation = pvb_service.repo.get_violation_by_id(int(case_entity.identifier_value))

        if not violation:
            raise HTTPException(status_code=404, detail="PVB violation not found")
        
        amount_due = Decimal(str(step_data.get("amount_due")))
        disposition = step_data.get("disposition")
        reduce_by = Decimal(str(step_data.get("reduced_by")))

        ledger_repo = LedgerRepository(db)
        ledger_service = LedgerService(ledger_repo)

        if disposition == PVBDisposition.REDUCED.value:
            amount = violation.amount_due - amount_due

            ledger_service.create_obligation(
                category=PostingCategory.PVB,
                amount=abs(amount),
                reference_id=violation.summons,
                driver_id=violation.driver_id,
                entry_type=EntryType.CREDIT if amount > 0 else EntryType.DEBIT,
                lease_id=violation.lease_id,
                medallion_id=violation.medallion_id,
                vehicle_id=violation.vehicle_id
            )
        elif disposition == PVBDisposition.DISMISSED.value:
            amount_due = 0
            ledger_service.create_obligation(
                category=PostingCategory.PVB,
                amount=abs(violation.amount_due),
                reference_id=violation.summons,
                driver_id=violation.driver_id,
                entry_type=EntryType.CREDIT if violation.amount_due > 0 else EntryType.DEBIT,
                lease_id=violation.lease_id,
                medallion_id=violation.medallion_id,
                vehicle_id=violation.vehicle_id
            )

        violation.amount_due = amount_due
        violation.reduce_to = reduce_by
        violation.disposition = disposition
        violation.state = step_data.get("state")
        violation.type = step_data.get("type")
        violation.summons = step_data.get("summons")
        violation.issue_date = step_data.get("issue_date")
        violation.issue_time = step_data.get("issue_time")
        violation.violation_code = step_data.get("violation_code")
        violation.disposition_change_date = step_data.get("disposition_change_date")
        violation.note = step_data.get("note")

        db.add(violation)
        db.commit()
        db.refresh(violation)

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"PVB violation summons {violation.summons} has been {disposition}. For Driver {violation.driver.driver_id} And Lease {violation.lease.lease_id}",
                meta_data={"vehicle_id": violation.vehicle_id, "driver_id": violation.driver_id, "lease_id": violation.lease_id, "medallion_id": violation.medallion_id}
            )

        logger.info("Successfully processed PVB violation", case_no=case_no)

        return "Ok"
    except Exception as e:
        logger.error("Error in TLC choose_driver_process: %s", e, exc_info=True)
        raise e