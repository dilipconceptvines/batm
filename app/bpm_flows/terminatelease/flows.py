## app/bpm_flows/terminatelease/flows.py

# Local imports
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.bpm.step_info import step
from app.bpm_flows.terminatelease import utils as terminate_lease_utils
from app.core.config import settings
from app.drivers.schemas import DriverStatus
from app.drivers.services import driver_service
from app.drivers.utils import format_driver_response
from app.leases.schemas import LeaseStatus
from app.leases.services import lease_service
from app.ledger.models import PostingCategory
from app.ledger.repository import LedgerRepository
from app.ledger.services import LedgerService
from app.utils.logger import get_logger
from app.vehicles.schemas import VehicleStatus

logger = get_logger(__name__)

entity_mapper = {"TERMINATE_LEASE": "lease", "TERMINATE_LEASE_IDENTIFIER": "id"}


@step(step_id="155", name="Fetch - Terminate Lease", operation="fetch")
def fetch_lease_details(db, case_no, case_params=None):
    """
    Fetch lease details for termination
    """
    try:
        from datetime import timedelta

        from app.medallions.services import medallion_service
        from app.medallions.utils import format_medallion_response
        from app.notes.services import note_service

        # Get or create case entity
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        # If case entity doesn't exist, create it with the lease_id from case_params
        if not case_entity:
            if not case_params or not case_params.get("object_lookup"):
                return {}

            lease = lease_service.get_lease(
                db, lease_id=case_params.get("object_lookup")
            )
            if not lease:
                return {}

            # Create case entity
            case_entity = bpm_service.create_case_entity(
                db=db,
                case_no=case_no,
                entity_name=entity_mapper["TERMINATE_LEASE"],
                identifier=entity_mapper["TERMINATE_LEASE_IDENTIFIER"],
                identifier_value=str(lease.id),
            )
        else:
            # Get lease from existing case entity
            lease = lease_service.get_lease(
                db, lookup_id=int(case_entity.identifier_value)
            )
            if not lease:
                return {}

        # Get medallion data for lease_case_details
        medallion = medallion_service.get_medallion(
            db=db, medallion_id=lease.medallion_id
        )
        medallion_data = format_medallion_response(medallion=medallion)

        # Determine vehicle availability
        vehicle_availability = "full"
        if lease.is_day_shift and not lease.is_night_shift:
            vehicle_availability = "day"
        elif lease.is_night_shift and not lease.is_day_shift:
            vehicle_availability = "night"

        # Build lease_case_details (following driver lease pattern)
        lease_case_details = {
            "lease_id": lease.lease_id,
            "lease_id_pk": lease.id,
            "vehicle_vin": lease.vehicle.vin if lease.vehicle else None,
            "plate_number": lease.vehicle.registrations[0].plate_number
            if lease.vehicle and lease.vehicle.registrations
            else None,
            "vehicle_type": lease.vehicle.vehicle_type if lease.vehicle else None,
            "lease_type": lease.lease_type,
            "vehicle_availability": vehicle_availability,
            "medallion_number": lease.medallion.medallion_number
            if lease.medallion
            else None,
            "medallion_type": lease.medallion.medallion_type
            if lease.medallion
            else None,
            "medallion_owner": medallion_data["medallion_owner"]
            if medallion_data
            else None,
            "make": lease.vehicle.make if lease.vehicle else None,
            "model": lease.vehicle.model if lease.vehicle else None,
            "year": lease.vehicle.year if lease.vehicle else None,
        }

        # Get notes for the lease
        notes_data = note_service.get_notes(
            db=db, entity_type="lease", entity_id=lease.id, multiple=True
        )

        notes = notes_data.get("items", []) if notes_data else []

        # Get termination_date and termination_reason from lease
        termination_date = lease.termination_date
        termination_reason = lease.termination_reason

        # Calculate deposit_release_date (termination_date + deposit_release_days from config)
        # If termination_date is not set, we can't calculate it
        deposit_release_date = None
        deposit_release_days = settings.lease_deposit_release_days

        if termination_date:
            deposit_release_date = (
                termination_date + timedelta(days=deposit_release_days)
            ).strftime("%Y-%m-%d")

        # Get termination reasons from config
        termination_reasons_list = []
        if settings.lease_termination_reasons:
            termination_reasons_list = [
                reason.strip()
                for reason in settings.lease_termination_reasons.split(",")
            ]

        return {
            "lease_case_details": lease_case_details,
            "termination_date": termination_date.strftime("%Y-%m-%d")
            if termination_date
            else "",
            "termination_reason": termination_reason if termination_reason else "",
            "termination_reasons": termination_reasons_list,
            "notes": notes,
            "cancellation_fee": float(lease.cancellation_fee)
            if lease.cancellation_fee
            else 0.00,
            "deposit_amount": float(lease.deposit_amount_paid)
            if lease.deposit_amount_paid
            else 0.00,
            "deposit_release_date": deposit_release_date
            if deposit_release_date
            else "",
            "deposit_release_days": deposit_release_days,
        }
    except Exception as e:
        logger.error("Error fetching lease details: %s", e)
        raise e


@step(step_id="155", name="Process - Terminate Lease", operation="process")
def process_lease_termination(db, case_no, step_data):
    """
    Terminate lease - set lease status to TERMINATED
    """
    try:
        from datetime import datetime

        from app.notes.services import note_service

        # Get case entity
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            raise ValueError("Case entity not found for this case")

        # Get lease
        lease = lease_service.get_lease(db, lookup_id=int(case_entity.identifier_value))
        if not lease:
            raise ValueError("Lease not found")

        # Prepare lease update data
        lease_data = {
            "id": lease.id,
        }

        # Update termination_date if provided in step_data
        if step_data.get("termination_date"):
            lease_data["termination_date"] = datetime.strptime(
                step_data["termination_date"], "%Y-%m-%d"
            ).date()

        # Update termination_reason if provided in step_data
        if step_data.get("termination_reason"):
            lease_data["termination_reason"] = step_data["termination_reason"]

        if step_data.get("cancellation_fee") <= 0:
            raise ValueError("Cancellation fee must be greater than 0")

        lease_data["cancellation_fee"] = step_data.get("cancellation_fee", 0)
        # Update the lease
        lease = lease_service.upsert_lease(db, lease_data)

        ledger_repo = LedgerRepository(db)
        ledger_service = LedgerService(ledger_repo)

        main_driver = next(
            (
                ld.driver.id
                for ld in lease.lease_driver
                if ld.is_additional_driver is False
            ),
            None,
        )

        logger.info(f"Posting cancellation fee to ledger for lease {lease.lease_id}")
        ledger = ledger_service.create_obligation(
            category=PostingCategory.CANCELLATION_FEE,
            amount=step_data.get("cancellation_fee", 0),
            reference_id=lease.lease_id,
            driver_id=main_driver,
            lease_id=lease.id,
            vehicle_id=lease.vehicle_id,
            medallion_id=lease.medallion_id,
        )

        # Handle notes if provided
        if step_data.get("notes"):
            notes = step_data.get("notes", [])
            for note_data in notes:
                # Check if it's an existing note (has note_id) or a new note
                if note_data.get("note_id"):
                    # Update existing note
                    note_service.upsert_lease_note(
                        db=db,
                        lease_id=lease.id,
                        note_id=note_data["note_id"],
                        note_data={
                            "note": note_data.get("note"),
                            "note_type": note_data.get("note_type", "termination"),
                        },
                    )
                else:
                    # Create new note
                    note_service.upsert_lease_note(
                        db=db,
                        lease_id=lease.id,
                        note_data={
                            "note": note_data.get("note"),
                            "note_type": note_data.get("note_type", "termination"),
                        },
                    )
            logger.info(f"Processed {len(notes)} note(s) for lease {lease.lease_id}")

        # Create audit trail for lease termination
        case = bpm_service.get_case_obj(db, case_no=case_no)

        # Build description with termination reason if available
        description = f"Lease {lease.lease_id} terminated"
        if lease.termination_reason:
            description += f" - Reason: {lease.termination_reason}"

        audit_trail_service.create_audit_trail(
            db=db,
            description=description,
            case=case,
            meta_data={
                "lease_id": lease.id,
                "vehicle_id": lease.vehicle_id,
                "medallion_id": lease.medallion_id,
                "termination_date": lease.termination_date.strftime("%Y-%m-%d")
                if lease.termination_date
                else None,
                "termination_reason": lease.termination_reason,
            },
            audit_type="AUTOMATED",
        )

        logger.info(f"Lease {lease.lease_id} terminated successfully")
        return "Ok"
    except Exception as e:
        logger.error("Error processing lease termination: %s", e)
        raise e
