# app/bpm_flows/driverlease/flows.py

import asyncio
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.audit_trail.schemas import AuditTrailType
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.bpm.step_info import step
from app.core.config import settings
from app.drivers.schemas import DOVLease, DriverStatus
from app.drivers.services import driver_service

# Docusign imports and tracking
from app.esign.models import ESignEnvelope
from app.leases.schemas import (
    LeaseStatus,
    LeaseType,
    LongTermLease,
    MedallionOnlyLease,
    ShiftLease,
    ShortTermLease,
)
from app.leases.services import lease_service
from app.leases.utils import (
    generate_dov_vehicle_lease_document,
    generate_long_term_lease_document,
    generate_medallion_lease_document,
)
from app.medallions.services import medallion_service
from app.medallions.utils import format_medallion_response
from app.uploads.services import upload_service
from app.utils.docusign_utils import Signer, docusign_client
from app.utils.general import generate_random_6_digit
from app.utils.lambda_utils import LambdaInvocationError
from app.utils.logger import get_logger
from app.vehicles.schemas import VehicleStatus
from app.vehicles.services import vehicle_service

logger = get_logger(__name__)

entity_mapper = {
    "LEASE": "lease",
    "LEASE_IDENTIFIER": "id",
}


@step(step_id="130", name="Fetch - Return vehicle information", operation="fetch")
def search_vehicle_information(db: Session, case_no, case_params=None):
    """
    Fetch the vehicle information for the driver lease step
    """
    try:
        if not case_params:
            return {}

        if not any(
            [
                case_params["medallion_number"],
                case_params["vin"],
                case_params["plate_number"],
            ]
        ):
            raise ValueError(
                "At least one of medallion_number, vin, or plate_number must be provided."
            )

        vehicle = vehicle_service.get_vehicles(
            db,
            vin=case_params["vin"],
            medallion_number=case_params["medallion_number"],
            plate_number=case_params["plate_number"],
        )

        if not vehicle or vehicle.vehicle_status != VehicleStatus.HACKED_UP:
            return {}

        hackup = vehicle_service.get_vehicle_hackup(
            db, vehicle_id=vehicle.id, hackup_status=VehicleStatus.ACTIVE
        )

        return {
            "vin": vehicle.vin,
            "make": vehicle.make,
            "model": vehicle.model,
            "year": vehicle.year,
            "vehicle_type": vehicle.vehicle_type,
            "plate_number": vehicle.registrations[0].plate_number
            if vehicle.registrations
            else "",
            "medallion_number": vehicle.medallions.medallion_number
            if vehicle.medallions
            else "",
            "entity_name": vehicle.vehicle_entity.entity_name
            if vehicle.vehicle_entity
            else None,
            "is_hacked_up": bool(hackup),
        }
    except Exception as e:
        logger.error("Error in search_vehicle_information: %s", e)
        raise e


@step(
    step_id="130", name="Process - Select vehicle for this lease", operation="process"
)
def set_vehicle_information(db: Session, case_no, step_data):
    """
    Process the vehicle information for the driver lease step
    """
    try:
        # If a case already exists for this step then we should not process it
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if case_entity:
            raise ValueError(
                "Cannot process this step for because a vehicle cannot be reselected for this case"
            )

        vehicle = vehicle_service.get_vehicles(db, vin=step_data["vehicle_vin"])
        if not vehicle:
            raise ValueError("Vehicle does not exist with this vin number")

        lease = lease_service.upsert_lease(
            db,
            {
                "lease_id": f"{vehicle.medallions.medallion_number}-{datetime.today().strftime('%d-%m-%y')}-{generate_random_6_digit()}",
                "medallion_id": vehicle.medallion_id,
                "is_active": False,
                "lease_status": LeaseStatus.IN_PROGRESS,
                "vehicle_id": vehicle.id,
            },
        )

        # Create case entity if it doesn't exists
        if not case_entity:
            case_entity = bpm_service.create_case_entity(
                db=db,
                case_no=case_no,
                entity_name=entity_mapper["LEASE"],
                identifier=entity_mapper["LEASE_IDENTIFIER"],
                identifier_value=str(lease.id),
            )

        logger.info("Case entity %s created for lease %s", case_entity.id, lease.id)

        # Create audit trail for vehicle selection
        case = bpm_service.get_case_obj(db, case_no=case_no)
        audit_trail_service.create_audit_trail(
            db=db,
            description=f"Vehicle selected for lease: VIN {vehicle.vin}, Medallion {vehicle.medallions.medallion_number if vehicle.medallions else 'N/A'}",
            case=case,
            meta_data={
                "lease_id": lease.id,
                "vehicle_id": vehicle.id,
                "medallion_id": vehicle.medallion_id,
            },
            audit_type=AuditTrailType.AUTOMATED,
        )

        return "Ok"
    except Exception as e:
        logger.error("Error in set_vehicle_information: %s", e)
        raise e


@step(step_id="131", name="Fetch - Return Lease Details", operation="fetch")
def get_lease_details(db: Session, case_no, case_params=None):
    """
    Fetch the lease details for the driver lease step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if not case_entity:
            return {}

        lease = lease_service.get_lease(db, lookup_id=int(case_entity.identifier_value))
        if not lease:
            return {}

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

        return {
            "lease_case_details": {
                "lease_id": lease.lease_id,
                "vehicle_vin": lease.vehicle.vin if lease.vehicle else None,
                "plate_number": lease.vehicle.registrations[0].plate_number
                if lease.vehicle.registrations
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
                "medallion_owner": medallion_data["medallion_owner"],
                "make" : lease.vehicle.make if lease.vehicle else None,
                "model" : lease.vehicle.model if lease.vehicle else None,
                "year" : lease.vehicle.year if lease.vehicle else None,
            },
            "lease_info": {
                "lease_id": lease.lease_id,
                "total_weeks": lease.duration_in_weeks,
                "lease_start_date": lease.lease_start_date,
                "lease_end_date": lease.lease_end_date,
                "pay_day": lease.lease_pay_day,
                "is_auto_renewal": lease.is_auto_renewed,
                "is_day_shift": lease.is_day_shift,
                "is_night_shift": lease.is_night_shift,
                "deposit_amount_paid": lease.deposit_amount_paid,
                "payments": lease.lease_payments_type,
                "cancellation_fee": lease.cancellation_fee,
                "current_segment": lease.current_segment,
                "total_segments": lease.total_segments,
            },
        }
    except Exception as e:
        logger.error("Error in get_lease_details: %s", e)
        raise e


@step(step_id="131", name="Process - Save Lease Details", operation="process")
def set_lease_details(db: Session, case_no, step_data):
    """
    Process the lease details for the driver lease step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        lease = lease_service.get_lease(db, lookup_id=int(case_entity.identifier_value))

        if lease.lease_type != step_data.get("lease_type"):
            existing_configs = lease_service.get_lease_configurations(
                db, lease_id=lease.id, multiple=True
            )

            if existing_configs:
                for config in existing_configs:
                    lease_service.delete_lease_configurations(db, config.id)
        lease_data = {
            "id": lease.id,
            "lease_id": step_data.get("lease_id", lease.lease_id),
            "lease_type": step_data.get("lease_type", lease.lease_type),
            "duration_in_weeks": step_data.get("total_weeks", lease.duration_in_weeks),
            "lease_start_date": (
                datetime.strptime(step_data["lease_start_date"], "%Y-%m-%d").date()
                if step_data.get("lease_start_date")
                else lease.lease_start_date
            ),
            "lease_end_date": (
                datetime.strptime(step_data["lease_end_date"], "%Y-%m-%d").date()
                if step_data.get("lease_end_date")
                else lease.lease_end_date
            ),
            "lease_pay_day": step_data.get("pay_day", lease.lease_pay_day),
            "is_auto_renewed": step_data.get("is_auto_renewal", lease.is_auto_renewed),
            "is_day_shift": step_data.get("is_day_shift", lease.is_day_shift),
            "is_night_shift": step_data.get("is_night_shift", lease.is_night_shift),
            "deposit_amount_paid": step_data.get(
                "deposit_amount_paid", lease.deposit_amount_paid
            ),
            "lease_payments_type": step_data.get("payments", lease.lease_payments_type),
            "cancellation_fee": step_data.get(
                "cancellation_fee", lease.cancellation_fee
            ),
            "lease_remark": step_data.get("lease_remark", lease.lease_remark),
            "lease_status": step_data.get("lease_status", lease.lease_status),
            "is_active": step_data.get("is_active", lease.is_active),
            "current_segment": 1,
            # Set total_segments only for DOV leases (8 segments = 4 years at 6 months each)
            # For all other lease types, set to None (no segment limit)
            "total_segments": 8 if step_data.get("lease_type") == LeaseType.DOV.value else None,
        }

        lease = lease_service.upsert_lease(db, lease_data)

        # Create audit trail for lease details
        case = bpm_service.get_case_obj(db, case_no=case_no)
        audit_trail_service.create_audit_trail(
            db=db,
            description=f"Lease details saved: {lease.lease_type} lease, Start: {lease.lease_start_date}, End: {lease.lease_end_date}",
            case=case,
            meta_data={
                "lease_id": lease.id,
                "vehicle_id": lease.vehicle_id,
                "medallion_id": lease.medallion_id,
            },
            audit_type=AuditTrailType.AUTOMATED,
        )

        return "Ok"
    except Exception as e:
        logger.error("Error in set_lease_details: %s", e)
        raise e


@step(step_id="132", name="Fetch - Enter Financial Information", operation="fetch")
def get_financial_information(db, case_no, case_params=None):
    """
    Fetch the financial information for the driver lease step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}

        lease = lease_service.get_lease(db, lookup_id=int(case_entity.identifier_value))
        if not lease:
            return {}

        # medallion_data = medallion_service.get_medallion(
        #     db=db, medallion_id=lease.medallion_id
        # )

        # medallion_data = format_medallion_response(medallion=medallion)

        medallion = medallion_service.get_medallion(
            db=db, medallion_id=lease.medallion_id
        )

        medallion_data = format_medallion_response(medallion=medallion)

        configurations = lease_service.get_lease_configurations(
            db, lease_id=lease.id, multiple=True
        )
        payment_config = lease_service.fetch_lease_payment_configuration(
            db, multiple=True
        )

        # if not payment_config:
        #     return {}

        # Lease Caps from the TLC - Defaults
        TLC_VEHICLE_CAP_TOTAL = 0.00
        TLC_VEHICLE_WEEKLY_CAP = 0.00
        TLC_MEDALLION_WEEKLY_CAP = 0.00
        TLC_INSPECTION_FEES = settings.tlc_inspection_fees
        TAX_STAMPS = settings.tax_stamps
        REGISTRATION = settings.registration
        DOV_SECURITY_DEPOSIT_CAP = 0.00

        lease_type = lease.lease_type
        total_weeks = lease.duration_in_weeks or 0

        # Extract from lease configuration
        # def get_limit(key):
        #     """Extract limit from lease configuration"""
        #     return next((config.lease_limit for config in configurations if config.lease_breakup_type == key), 0.00)

        med_lease = 0
        veh_lease = 0
        day_shift = 0
        night_shift = 0

        # Default values
        management_recommendation = 0.00
        lease_amount = 0.00

        # Lease type based configuration
        if lease_type == LeaseType.DOV.value:
            for config in payment_config:
                if config.config_type == "dov_med_lease":
                    med_lease = config.total_amount
                elif config.config_type == "dov_veh_lease":
                    veh_lease = config.total_amount
            lease_amount = med_lease + veh_lease
            management_recommendation = lease_amount
            vehicle_lifetime_cap = lease.vehicle.vehicle_lifetime_cap or 0.00
            if vehicle_lifetime_cap:
                TLC_VEHICLE_WEEKLY_CAP = round(
                    float(lease.vehicle.vehicle_lifetime_cap) / 208, 2
                )
            DOV_SECURITY_DEPOSIT_CAP = settings.dov_security_deposit_cap
            if "hybrid" in lease.vehicle.vehicle_type.lower():
                TLC_MEDALLION_WEEKLY_CAP = settings.tlc_medallion_weekly_cap_hybrid
            else:
                TLC_MEDALLION_WEEKLY_CAP = settings.tlc_medallion_weekly_cap_regular
        elif lease_type == LeaseType.LONG_TERM.value:
            for config in payment_config:
                if config.config_type == "long_term_lease":
                    day_shift = config.day_shift_amount
                    night_shift = config.night_shift_amount
            lease_amount = day_shift + night_shift
            management_recommendation = lease_amount
            if "hybrid" in lease.vehicle.vehicle_type.lower():
                TLC_VEHICLE_WEEKLY_CAP = 0.00

                if lease.is_day_shift and lease.is_night_shift:
                    TLC_MEDALLION_WEEKLY_CAP = (
                        settings.long_term_medallion_weekly_cap_hybrid_full
                    )
                elif lease.is_day_shift:
                    TLC_MEDALLION_WEEKLY_CAP = (
                        settings.long_term_medallion_weekly_cap_hybrid_day
                    )
                else:
                    TLC_MEDALLION_WEEKLY_CAP = (
                        settings.long_term_medallion_weekly_cap_hybrid_night
                    )

            else:
                TLC_VEHICLE_WEEKLY_CAP = 0.00

                if lease.is_day_shift and lease.is_night_shift:
                    TLC_MEDALLION_WEEKLY_CAP = (
                        settings.long_term_medallion_weekly_cap_regular_full
                    )
                elif lease.is_day_shift:
                    TLC_MEDALLION_WEEKLY_CAP = (
                        settings.long_term_medallion_weekly_cap_regular_day
                    )
                else:
                    TLC_MEDALLION_WEEKLY_CAP = (
                        settings.long_term_medallion_weekly_cap_regular_night
                    )

        elif lease_type == LeaseType.SHIFT.value:
            # Shift lease - either day OR night shift
            for config in payment_config:
                if config.config_type == "shift_lease":
                    day_shift = config.day_shift_amount
                    night_shift = config.night_shift_amount
            lease_amount = day_shift + night_shift
            management_recommendation = lease_amount
            if "hybrid" in lease.vehicle.vehicle_type.lower():
                TLC_VEHICLE_WEEKLY_CAP = 0.00
                if lease.is_day_shift:
                    TLC_MEDALLION_WEEKLY_CAP = (
                        settings.shift_lease_medallion_weekly_cap_hybrid_day
                    )
                else:
                    TLC_MEDALLION_WEEKLY_CAP = (
                        settings.shift_lease_medallion_weekly_cap_hybrid_night
                    )
            else:
                TLC_VEHICLE_WEEKLY_CAP = 0.00
                if lease.is_day_shift:
                    TLC_MEDALLION_WEEKLY_CAP = (
                        settings.shift_lease_medallion_weekly_cap_regular_day
                    )
                else:
                    TLC_MEDALLION_WEEKLY_CAP = (
                        settings.shift_lease_medallion_weekly_cap_regular_night
                    )

        elif lease_type == LeaseType.SHORT_TERM.value:
            for config in payment_config:
                if config.config_type == "short_term_lease":
                    day_shift = config.day_shift_amount
                    night_shift = config.night_shift_amount
            lease_amount = day_shift + night_shift
            management_recommendation = lease_amount
        elif lease_type == LeaseType.MEDALLION.value:
            for config in payment_config:
                if config.config_type == "medallion_only":
                    lease_amount = config.total_amount
            management_recommendation = med_lease
            if "hybrid" in lease.vehicle.vehicle_type.lower():
                TLC_MEDALLION_WEEKLY_CAP = settings.tlc_medallion_weekly_cap_hybrid
            else:
                TLC_MEDALLION_WEEKLY_CAP = settings.tlc_medallion_weekly_cap_regular
        else:
            lease_amount = med_lease + veh_lease
            management_recommendation = lease_amount

        # Determine vehicle availability
        vehicle_availability = "full"
        if lease.is_day_shift and not lease.is_night_shift:
            vehicle_availability = "day"
        elif lease.is_night_shift and not lease.is_day_shift:
            vehicle_availability = "night"

        return {
            "lease_case_details": {
                "lease_id": lease.lease_id,
                "vehicle_vin": lease.vehicle.vin if lease.vehicle else None,
                "plate_number": lease.vehicle.registrations[0].plate_number
                if lease.vehicle.registrations
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
                "medallion_owner": medallion_data["medallion_owner"],
                "make" : lease.vehicle.make if lease.vehicle else None,
                "model" : lease.vehicle.model if lease.vehicle else None,
                "year" : lease.vehicle.year if lease.vehicle else None,
            },
            "financials": {
                "tlc_max_vehicle_cap": lease.vehicle.vehicle_lifetime_cap,
                "tlc_vehicle_cap": TLC_VEHICLE_WEEKLY_CAP,
                "tlc_medallion_cap": TLC_MEDALLION_WEEKLY_CAP,
                "tlc_inspection_fees": TLC_INSPECTION_FEES,
                "tax_stamps": TAX_STAMPS,
                "registration": REGISTRATION,
                "security_deposit_cap": DOV_SECURITY_DEPOSIT_CAP,
                "sales_tax": 0.00
                if lease_type == LeaseType.LONG_TERM.value
                else round(float(lease.vehicle.sales_tax) / 208, 2),
                "management_recommendation": round(management_recommendation, 2),
                "day_shift_amount": round(day_shift, 2) if day_shift else 0,
                "night_shift_amount": round(night_shift, 2) if night_shift else 0,
                "lease_amount": round(lease_amount, 2),
                "med_lease": round(med_lease, 2),
                "veh_lease": round(veh_lease, 2),
                "total_vehicle_cost": lease.vehicle.vehicle_total_price,
                "vehicle_true_cost": lease.vehicle.vehicle_true_cost,
                # "is_over_vehicle_cap": is_over_vehicle_cap,
                # "is_weekly_over_cap": is_weekly_over_cap,
                "vehicle_hack_up_cost": lease.vehicle.vehicle_hack_up_cost,
                "cancellation_amount": lease.cancellation_fee,
                "security_deposit": lease.deposit_amount_paid,
                "additional_balance_due": lease.additional_balance_due
                if lease.additional_balance_due
                else 0.00,
                "current_segment": lease.current_segment,
                "total_segments": lease.total_segments,
            },
            "lease_configuration": {
                "lease_id": lease.lease_id,
                "lease_type": lease.lease_type if lease.lease_type else "",
                "total_weeks": lease.duration_in_weeks,
                "medallion_id": lease.medallion.medallion_number
                if lease.medallion
                else "",
                "vehicle_id": lease.vehicle.vin if lease.vehicle else "",
                "lease_start_date": lease.lease_start_date.isoformat()
                if lease.lease_start_date
                else "",
                "lease_end_date": lease.lease_end_date.isoformat()
                if lease.lease_end_date
                else "",
                "is_auto_renewed": lease.is_auto_renewed,
                "is_day_shift": lease.is_day_shift,
                "lease_remark": lease.lease_remark,
                "configurations": [
                    {
                        "lease_breakup_type": config.lease_breakup_type,
                        "lease_limit": config.lease_limit,
                    }
                    for config in configurations
                ],
            },
        }
    except Exception as e:
        logger.error("Error in get_financial_information: %s", e)
        raise e


@step(step_id="132", name="Process - Save Financial Information", operation="process")
def set_financial_information(db, case_no, step_data):
    """
    Process the financial information for the driver lease step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if not case_entity:
            raise ValueError("Step cannot be executed because there is no valid case")

        lease = lease_service.get_lease(db, lookup_id=int(case_entity.identifier_value))

        if lease.lease_type != step_data.get("lease_type"):
            raise ValueError(
                f"Lease type doesn't match with this lease id {lease.lease_type}"
            )

        lease_type = step_data.get("lease_type")
        if lease_type == LeaseType.DOV.value:
            data = DOVLease(**step_data)
            lease_service.handle_dov_lease(db, lease.id, data)
        elif lease_type == LeaseType.LONG_TERM.value:
            data = DOVLease(**step_data)  # TODO: Change after requiremements come
            lease_service.handle_dov_lease(db, lease.id, data)
        elif lease_type == LeaseType.SHIFT.value:
            data = DOVLease(**step_data)
            lease_service.handle_dov_lease(db, lease.id, data)
        elif lease_type == LeaseType.SHORT_TERM.value:
            data = ShortTermLease(**step_data)
            lease_service.handle_short_term_lease(db, lease.id, data)
        elif lease_type == LeaseType.MEDALLION.value:
            data = DOVLease(**step_data)  # TODO: Change after requiremements come
            lease_service.handle_dov_lease(db, lease.id, data)
        else:
            raise ValueError(f"Invalid lease type: {lease.lease_type}")

        lease_data = {
            "id": lease.id,
            "lease_id": step_data.get("lease_id", lease.lease_id),
            "deposit_amount_paid": step_data.get("financial_information", {}).get(
                "security_deposit", lease.deposit_amount_paid
            ),
            "additional_balance_due": step_data.get("financial_information", {}).get(
                "additional_balance_due", lease.additional_balance_due
            ),
            "cancellation_fee": step_data.get("financial_information", {}).get(
                "cancellation_charge", lease.cancellation_fee
            ),
        }

        lease = lease_service.upsert_lease(db, lease_data)

        # Get the lease configurations to extract amounts for schedule
        configs = lease_service.get_lease_configurations(
            db, lease_id=lease.id, multiple=True
        )

        def get_config_value(key: str) -> float:
            """Helper to get configuration value"""
            config = next((c for c in configs if c.lease_breakup_type == key), None)
            return float(config.lease_limit) if config and config.lease_limit else 0.0

        # Calculate weekly amounts based on lease type
        vehicle_weekly = 0.0
        medallion_weekly = 0.0

        vehicle_weekly = get_config_value("total_vehicle_lease")
        medallion_weekly = get_config_value("total_medallion_lease_payment")

        # Create/update lease schedule
        lease_service.create_or_update_lease_schedule(
            db=db,
            lease=lease,
            vehicle_weekly_amount=vehicle_weekly,
            medallion_weekly_amount=medallion_weekly,
        )
        logger.info(f"Lease schedule created/updated for lease {lease.id}")

        # Create audit trail for financial information
        total_lease_amount = vehicle_weekly + medallion_weekly
        case = bpm_service.get_case_obj(db, case_no=case_no)
        audit_trail_service.create_audit_trail(
            db=db,
            description=f"Financial information saved: Total weekly lease amount ${total_lease_amount:.2f} (Vehicle: ${vehicle_weekly:.2f}, Medallion: ${medallion_weekly:.2f})",
            case=case,
            meta_data={
                "lease_id": lease.id,
                "vehicle_id": lease.vehicle_id,
                "medallion_id": lease.medallion_id,
            },
            audit_type=AuditTrailType.AUTOMATED,
        )

        return "Ok"
    except Exception as e:
        logger.error("Error in set_financial_information: %s", e, exc_info=True)
        raise e


@step(step_id="133", name="Fetch - Search Driver Information", operation="fetch")
def choose_driver(db, case_no, case_params=None):
    """
    Fetch the driver information for the driver lease step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}
        lease = lease_service.get_lease(db, lookup_id=int(case_entity.identifier_value))
        if not lease:
            return {}

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

        lease_case_details = {
            "lease_id": lease.lease_id,
            "vehicle_vin": lease.vehicle.vin if lease.vehicle else None,
            "plate_number": lease.vehicle.registrations[0].plate_number
            if lease.vehicle.registrations
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
            "medallion_owner": medallion_data["medallion_owner"],
            "make" : lease.vehicle.make if lease.vehicle else None,
            "model" : lease.vehicle.model if lease.vehicle else None,
            "year" : lease.vehicle.year if lease.vehicle else None,
        }

        # In case we need to show the drivers that were already selected
        selected_driver_info = []
        drivers_already_part_of_lease = lease.lease_driver

        if drivers_already_part_of_lease:
            for lease_driver in drivers_already_part_of_lease:
                # Only include active drivers
                if lease_driver.is_active:
                    selected_driver_info.append(lease_driver.to_dict())

        if not set(case_params.keys()).intersection(
            ["ssn", "tlc_license_number", "dmv_license_number"]
        ):
            return {
                "lease_case_details": lease_case_details,
                "selected_driver_info": selected_driver_info,
            }

        if not case_params.get("ssn"):
            logger.info("ssn is not given in params")
            raise ValueError("ssn is not given in params")

        driver = driver_service.get_drivers(
            db,
            ssn=case_params.get("ssn", None),
            tlc_license_number=case_params.get("tlc_license_number", None),
            dmv_license_number=case_params.get("dmv_license_number", None),
        )
        if not driver:
            return {
                "lease_case_details": lease_case_details,
                "selected_driver_info": selected_driver_info,
            }

        if (
            driver.driver_status != DriverStatus.REGISTERED
            and driver.driver_status != DriverStatus.ACTIVE
        ):
            return {
                "lease_case_details": lease_case_details,
                "selected_driver_info": selected_driver_info,
            }

        return {
            "lease_case_details": lease_case_details,
            "driver_info": {
                "driver_id": driver.id,
                "driver_lookup_id": driver.driver_id,
                "first_name": driver.first_name,
                "last_name": driver.last_name,
                "driver_type": driver.driver_type,
                "driver_ssn": driver.ssn,
                "tlc_license_number": driver.tlc_license.tlc_license_number,
                "dmv_license_number": driver.dmv_license.dmv_license_number,
                "contact_number": driver.phone_number_1,
            },
            "selected_driver_info": selected_driver_info,
        }
    except Exception as e:
        logger.error("Error in choose_driver: %s", e, exc_info=True)
        raise e


@step(step_id="133", name="Process - Add drivers to lease", operation="process")
def set_driver(db, case_no, step_data):
    """
    Process the driver information for the driver lease step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if not case_entity:
            return {}

        lease = lease_service.get_lease(db, lookup_id=int(case_entity.identifier_value))
        lease_drivers = lease_service.get_lease_drivers(
            db, lease_id=lease.id, multiple=True
        )
        driver_ids = set(
            [
                str(lease_driver.driver_id)
                for lease_driver in lease_drivers
                if lease_driver.driver_id
            ]
        )

        # Remove existing drivers
        lease_service.remove_drivers_from_lease(db, lease.id, driver_ids)

        # Add the drivers that have been passed
        passed_driver_ids = set()
        for driver_info in step_data["select_driver"]:
            passed_driver_ids.add(driver_info["driver_id"])
            message = lease_service.update_lease_driver_info(db, lease.id, driver_info)
            logger.info(message)

        # lease_id = f"{lease.medallion.medallion_number}-{datetime.today().date()}-{generate_random_6_digit()}"

        # lease_service.upsert_lease(
        #     db=db, lease_data={"id": case_entity.identifier_value, "lease_id": lease_id}
        # )

        # driver_diff = driver_ids.difference(passed_driver_ids)
        # lease_service.remove_drivers_from_lease(db, lease.id, driver_diff)
        authorized_agent = step_data.get("authorized_agent", "")

        logger.info("Generate documents for all the drivers selected")

        if lease.lease_type == LeaseType.DOV.value:
            logger.info("\n")
            logger.info("********** Generating dov vehicle document **********\n")
            vehicle_document_info = generate_dov_vehicle_lease_document(
                db, lease, authorized_agent
            )
            upload_service.create_document(
                db,
                new_filename=vehicle_document_info["document_name"],
                original_extension=vehicle_document_info["document_format"],
                document_path=vehicle_document_info["document_path"],
                object_type=vehicle_document_info["object_type"],
                object_id=vehicle_document_info["object_lookup_id"],
                notes=vehicle_document_info["document_note"],
                document_type=vehicle_document_info["document_type"],
                document_date=datetime.now(timezone.utc).isoformat().split("T")[0],
                file_size_kb=0,
            )
            logger.info("********** Generated dov vehicle document **********")
            logger.info("\n")
            logger.info("********** Generating dov medallion document **********")
            medallion_document_info = generate_medallion_lease_document(
                db, lease, authorized_agent
            )
            upload_service.create_document(
                db,
                new_filename=medallion_document_info["document_name"],
                original_extension=medallion_document_info["document_format"],
                document_path=medallion_document_info["document_path"],
                object_type=medallion_document_info["object_type"],
                object_id=medallion_document_info["object_lookup_id"],
                notes=medallion_document_info["document_note"],
                document_type=medallion_document_info["document_type"],
                document_date=datetime.now(timezone.utc).isoformat().split("T")[0],
                file_size_kb=0,
            )
            logger.info("********** Generated dov medallion document **********")
            logger.info("\n")
        elif lease.lease_type == LeaseType.LONG_TERM.value:
            logger.info("\n")
            logger.info("********** Generating long term document **********\n")
            long_term_document_info = generate_long_term_lease_document(
                db, lease, authorized_agent
            )
            upload_service.create_document(
                db,
                new_filename=long_term_document_info["document_name"],
                original_extension=long_term_document_info["document_format"],
                document_path=long_term_document_info["document_path"],
                object_type=long_term_document_info["object_type"],
                object_id=long_term_document_info["object_lookup_id"],
                notes=long_term_document_info["document_note"],
                document_type=long_term_document_info["document_type"],
                document_date=datetime.now(timezone.utc).isoformat().split("T")[0],
                file_size_kb=0,
            )
            logger.info("********** Generated long term document **********")
        elif lease.lease_type == LeaseType.SHIFT.value:
            logger.info("\n")
            logger.info("********** Generating shift lease document **********\n")
            long_term_document_info = generate_long_term_lease_document(
                db, lease, authorized_agent
            )
            upload_service.create_document(
                db,
                new_filename=long_term_document_info["document_name"],
                original_extension=long_term_document_info["document_format"],
                document_path=long_term_document_info["document_path"],
                object_type=long_term_document_info["object_type"],
                object_id=long_term_document_info["object_lookup_id"],
                notes=long_term_document_info["document_note"],
                document_type=long_term_document_info["document_type"],
                document_date=datetime.now(timezone.utc).isoformat().split("T")[0],
                file_size_kb=0,
            )
            logger.info("********** Generated shift lease document **********")
        elif lease.lease_type == LeaseType.MEDALLION.value:
            logger.info("********** Generating dov medallion document **********")
            medallion_document_info = generate_medallion_lease_document(
                db, lease, authorized_agent
            )
            upload_service.create_document(
                db,
                new_filename=medallion_document_info["document_name"],
                original_extension=medallion_document_info["document_format"],
                document_path=medallion_document_info["document_path"],
                object_type=medallion_document_info["object_type"],
                object_id=medallion_document_info["object_lookup_id"],
                notes=medallion_document_info["document_note"],
                document_type=medallion_document_info["document_type"],
                document_date=datetime.now(timezone.utc).isoformat().split("T")[0],
                file_size_kb=0,
            )
            logger.info("********** Generated dov medallion document **********")
            logger.info("\n")

        # Create audit trail for driver addition
        # Collect all driver names for the description
        driver_names = []
        driver_ids_list = []
        for driver_info in step_data["select_driver"]:
            driver = driver_service.get_drivers(db, driver_id=driver_info["driver_id"])
            if driver:
                driver_names.append(driver.full_name)
                driver_ids_list.append(driver.id)

        case = bpm_service.get_case_obj(db, case_no=case_no)
        # Add driver_id to metadata (use first driver for single driver, or list for multiple)
        metadata = {
            "lease_id": lease.id,
            "vehicle_id": lease.vehicle_id,
            "medallion_id": lease.medallion_id,
        }
        if driver_ids_list:
            metadata["driver_id"] = (
                driver_ids_list[0] if len(driver_ids_list) == 1 else driver_ids_list[0]
            )

        audit_trail_service.create_audit_trail(
            db=db,
            description=f"Driver(s) added to lease: {', '.join(driver_names)}",
            case=case,
            meta_data=metadata,
            audit_type=AuditTrailType.AUTOMATED,
        )

        return "Ok"
    except LambdaInvocationError as lambda_error:
        # Log detailed error information for debugging
        logger.error(
            "Lambda error in set_driver for lease %s: %s (Status: %s, Details: %s)",
            lease.id,
            lambda_error.message,
            lambda_error.status_code,
            lambda_error.error_details,
        )
        # Raise ValueError with only the message - router will convert to HTTPException
        raise ValueError(lambda_error.message)
    except Exception as e:
        logger.error("Error in set_driver: %s", e, exc_info=True)
        raise


@step(step_id="134", name="Fetch - Document for signature", operation="fetch")
def fetch_document_for_signature(db, case_no, case_params=None):
    """Fetch the document for signature for the driver lease step"""
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if not case_entity:
            return {}

        lease = lease_service.get_lease(db, lookup_id=int(case_entity.identifier_value))
        if not lease:
            return {}

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

        lease_case_details = {
            "lease_id": lease.lease_id,
            "vehicle_vin": lease.vehicle.vin if lease.vehicle else None,
            "plate_number": lease.vehicle.registrations[0].plate_number
            if lease.vehicle.registrations
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
            "medallion_owner": medallion_data["medallion_owner"],
            "make" : lease.vehicle.make if lease.vehicle else None,
            "model" : lease.vehicle.model if lease.vehicle else None,
            "year" : lease.vehicle.year if lease.vehicle else None,
        }

        # documents = upload_service.get_documents(db, object_type="lease", object_id=lease.id, multiple=True)
        documents = lease_service.fetch_latest_driver_document_status_by_lease(
            db, lease=lease
        )

        return {"lease_case_details": lease_case_details, "documents": documents}
    except Exception as e:
        logger.error("Error in fetch_document_for_signature: %s", e, exc_info=True)
        raise e


@step(step_id="134", name="Process - Send document for signature", operation="process")
async def send_document_for_signature(db, case_no, step_data):
    """Send the document for signature for the driver lease step"""
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if not case_entity:
            return {}

        lease = lease_service.get_lease(db, lookup_id=int(case_entity.identifier_value))
        if not lease:
            return {}

        # Invalidate all the documents that belong to the lease driver
        lease_service.invalidate_lease_driver_documents(db, lease)

        signature_mode = step_data["signature_mode"]
        # Check the type of the signature, if print we dont need to worry about the envelopes.
        if signature_mode == "print":
            # Update signature status'es on the driver lease table
            lease_service.upsert_lease_drive_document_for_wet_signature(
                db,
                lease,
                signature_mode=signature_mode,
                print_document_details=step_data.get("print_document_details", []),
            )
            return "Ok"

        existing_envelope = (
            db.query(ESignEnvelope)
            .filter(
                ESignEnvelope.object_id == lease.id,
                ESignEnvelope.object_type == "lease",
            )
            .first()
        )
        if existing_envelope:
            logger.warning(
                f"DocuSign envelope {existing_envelope.envelope_id} already exists for lease {lease.id}. Skipping."
            )
            return {"message": "An envelope has already been sent for this lease."}

        logger.info(
            f"Generating vehicle lease document for lease {lease.id} using legacy utility."
        )
        authorised_agent = step_data.get("authorised_agent", "")

        # --- DocuSign Integration (Using NEW System) ---
        # Create signers with email null checks
        signers = []
        for ld in lease.lease_driver:
            if ld.is_active:
                driver_email = ld.driver.email_address
                if not driver_email:
                    logger.warning(
                        f"Driver {ld.driver.full_name} (ID: {ld.driver.driver_id}) does not have an email address. Using placeholder email from settings - {settings.docusign_placeholder_email}"
                    )
                    driver_email = settings.docusign_placeholder_email
                signers.append(
                    Signer(
                        name=ld.driver.full_name,
                        email=driver_email,
                        # signing_type="embedded",
                        # client_user_id=ld.driver.email_address,
                    )
                )

        signature_mode = step_data["signature_mode"]

        if lease.lease_type == LeaseType.DOV.value:
            vehicle_doc_data = generate_dov_vehicle_lease_document(
                db, lease, authorised_agent
            )
            upload_service.upsert_document(db, vehicle_doc_data)
            vehicle_lease_s3_key = vehicle_doc_data.get("document_path")
            logger.info("Sending envelope for Vehicle Lease")
            envelope_response = await docusign_client.send_envelope_async(
                source_s3_key=vehicle_lease_s3_key,
                document_name=f"Vehicle Lease Agreement for {lease.lease_id}",
                signers=signers,
                signature_mode=signature_mode,
                project_name="driverlease",
                case_no=case_no,
                signing_position_info={
                    "driver_positions": {
                        "signHereTabs": [
                            {
                                "documentId": "1",
                                "pageNumber": "16",
                                "xPosition": "280",
                                "yPosition": "545",
                                "required": "true",
                                "tabLabel": "Driver Signature",
                            }
                        ],
                        "dateSignedTabs": [
                            {
                                "documentId": "1",
                                "pageNumber": "16",
                                "xPosition": "83",
                                "yPosition": "460",
                                "required": "true",
                                "tabLabel": "Driver Date",
                            }
                        ],
                    }
                },
            )

            vehicle_agreement_envelope_id = envelope_response["envelope_id"]
            new_envelope = ESignEnvelope(
                envelope_id=envelope_response["envelope_id"],
                status=envelope_response["status"],
                object_type="vehicle_lease",
                object_id=lease.id,
            )
            db.add(new_envelope)
            db.flush()
            db.refresh(new_envelope)

            logger.info("Envelope for Vehicle Lease sent")
            lease_service.upsert_lease_driver_documents(
                db,
                lease,
                signature_mode=signature_mode,
                document_types=["driver_vehicle_lease"],
                envelope_ids=[
                    vehicle_agreement_envelope_id,
                ],
            )

        if lease.lease_type == LeaseType.LONG_TERM.value or lease.lease_type == LeaseType.SHIFT.value:
            if lease.lease_type == LeaseType.LONG_TERM.value:
                document_name = f"Long Term Lease Agreement for {lease.lease_id}"
            else:
                document_name = f"Shift Lease Agreement for {lease.lease_id}"
            long_term_lease_doc_data = generate_long_term_lease_document(
                db, lease, authorised_agent
            )
            upload_service.upsert_document(db, long_term_lease_doc_data)
            long_term_lease_s3_key = long_term_lease_doc_data.get("document_path")
            logger.info(f"Sending envelope for {lease.lease_type} Lease")
            envelope_response = await docusign_client.send_envelope_async(
                source_s3_key=long_term_lease_s3_key,
                document_name=document_name,
                signers=signers,
                signature_mode=signature_mode,
                project_name="driverlease",
                case_no=case_no,
                signing_position_info={
                    "driver_positions": {
                        "signHereTabs": [
                            {
                                "documentId": "1",
                                "pageNumber": "14",
                                "xPosition": "280",
                                "yPosition": "545",
                                "required": "true",
                                "tabLabel": "Driver Signature",
                            }
                        ],
                        "dateSignedTabs": [
                            {
                                "documentId": "1",
                                "pageNumber": "14",
                                "xPosition": "83",
                                "yPosition": "460",
                                "required": "true",
                                "tabLabel": "Driver Date",
                            }
                        ],
                    }
                },
            )

            long_term_lease_agreement_envelope_id = envelope_response["envelope_id"]
            new_envelope = ESignEnvelope(
                envelope_id=envelope_response["envelope_id"],
                status=envelope_response["status"],
                object_type="long_term_lease",
                object_id=lease.id,
            )
            db.add(new_envelope)
            db.flush()
            db.refresh(new_envelope)

            logger.info("Envelope for Vehicle Lease sent")
            lease_service.upsert_lease_driver_documents(
                db,
                lease,
                signature_mode=signature_mode,
                document_types=["driver_long_term_lease"],
                envelope_ids=[
                    long_term_lease_agreement_envelope_id,
                ],
            )

        if lease.lease_type == LeaseType.DOV.value or lease.lease_type == LeaseType.MEDALLION.value:
            medallion_doc_data = generate_medallion_lease_document(
                db, lease, authorised_agent
            )
            upload_service.upsert_document(db, medallion_doc_data)
            medallion_lease_s3_key = medallion_doc_data.get("document_path")
            logger.info("Sending envelope for Medallion Lease")

            envelope_response = await docusign_client.send_envelope_async(
                source_s3_key=medallion_lease_s3_key,
                document_name=f"Medallion Lease Agreement for {lease.lease_id}",
                signers=signers,
                project_name="driverlease",
                case_no=case_no,
                signature_mode=signature_mode,
                signing_position_info={
                    "driver_positions": {
                        "signHereTabs": [
                            {
                                "documentId": "1",
                                "pageNumber": "13",
                                "xPosition": "285",
                                "yPosition": "645",
                                "required": "true",
                                "tabLabel": "Driver Signature",
                            }
                        ],
                        "dateSignedTabs": [
                            {
                                "documentId": "1",
                                "pageNumber": "13",
                                "xPosition": "83",
                                "yPosition": "690",
                                "required": "true",
                                "tabLabel": "Driver Date",
                            }
                        ],
                    }
                },
            )
            medallion_lease_envelope_id = envelope_response["envelope_id"]

            new_medallion_envelope = ESignEnvelope(
                envelope_id=envelope_response["envelope_id"],
                status=envelope_response["status"],
                object_type="medallion_lease",
                object_id=lease.id,
            )
            db.add(new_medallion_envelope)
            db.flush()
            db.refresh(new_medallion_envelope)
            logger.info("Envelope for Medallion Lease sent")
            lease_service.upsert_lease_driver_documents(
                db,
                lease,
                signature_mode=signature_mode,
                envelope_ids=[
                    medallion_lease_envelope_id,
                ],
                document_types=["driver_medallion_lease"],
            )

        # Mark the bat manager signature as done as the image is generated
        lease_service.mark_bat_manager_as_signed(db, lease)

        # Create audit trail for document sending
        driver_count = len([ld for ld in lease.lease_driver if ld.is_active])
        case = bpm_service.get_case_obj(db, case_no=case_no)

        # Get first active driver for metadata
        first_driver_id = None
        for ld in lease.lease_driver:
            if ld.is_active:
                first_driver_id = ld.driver.id
                break

        metadata = {
            "lease_id": lease.id,
            "vehicle_id": lease.vehicle_id,
            "medallion_id": lease.medallion_id,
        }
        if first_driver_id:
            metadata["driver_id"] = first_driver_id

        audit_trail_service.create_audit_trail(
            db=db,
            description=f"Documents sent for signature ({signature_mode} mode) to {driver_count} driver(s)",
            case=case,
            meta_data=metadata,
            audit_type=AuditTrailType.AUTOMATED,
        )

        return "Ok"
    except Exception as e:
        logger.error("Error in send_document_for_signature: %s", e)
        raise e


@step(step_id="135", name="Fetch - Documents for completing Lease", operation="fetch")
def fetch_documents_for_complete_lease(db, case_no, case_params=None):
    """
    Fetch the documents for completing Lease for the driver lease step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if not case_entity:
            return {}

        lease = lease_service.get_lease(db, lookup_id=int(case_entity.identifier_value))
        if not lease:
            return {}

        vehicle_envelope = (
            db.query(ESignEnvelope)
            .filter(
                ESignEnvelope.object_id == lease.id,
                ESignEnvelope.object_type == "vehicle_lease",
            )
            .first()
        )
        medallion_envelope = (
            db.query(ESignEnvelope)
            .filter(
                ESignEnvelope.object_id == lease.id,
                ESignEnvelope.object_type == "medallion_lease",
            )
            .first()
        )

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

        lease_case_details = {
            "lease_id": lease.lease_id,
            "vehicle_vin": lease.vehicle.vin if lease.vehicle else None,
            "plate_number": lease.vehicle.registrations[0].plate_number
            if lease.vehicle.registrations
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
            "medallion_owner": medallion_data["medallion_owner"],
            "make" : lease.vehicle.make if lease.vehicle else None,
            "model" : lease.vehicle.model if lease.vehicle else None,
            "year" : lease.vehicle.year if lease.vehicle else None,
        }

        documents = None
        if lease.lease_driver:
            documents = lease_service.fetch_latest_driver_document_status_by_lease(
                db, lease=lease
            )

        if not lease:
            return {}

        if not vehicle_envelope or not medallion_envelope:
            return {"lease_case_details": lease_case_details, "documents": documents}
        else:
            return {
                "lease_case_details": lease_case_details,
                "documents": documents,
                "vehicle_signature_status": vehicle_envelope.status,
                "vehicle_can_complete": vehicle_envelope.status == "completed",
                "vehicle_envelope_id": vehicle_envelope.envelope_id,
                "vehicle_message": f"Signature status is '{vehicle_envelope.status}'. You can complete the lease once all parties have signed.",
                "medallion_signature_status": medallion_envelope.status,
                "medallion_can_complete": medallion_envelope.status == "completed",
                "medallion_envelope_id": medallion_envelope.envelope_id,
                "medallion_message": f"Signature status is '{medallion_envelope.status}'. You can complete the lease once all parties have signed.",
            }
    except Exception as e:
        logger.error("Error in fetch_documents_for_complete_lease: %s", e)
        raise e


@step(step_id="135", name="Process - Completing Lease", operation="process")
def complete_lease(db, case_no, step_data):
    """
    Process the completing Lease for the driver lease step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if not case_entity:
            return {}

        lease = lease_service.get_lease(db, lookup_id=int(case_entity.identifier_value))
        vehicle = vehicle_service.get_vehicles(db, vehicle_id=lease.vehicle_id)

        if not lease or not vehicle:
            return {}

        lease_drivers = lease_service.get_lease_drivers(
            db, lease_id=lease.id, multiple=True
        )

        if step_data["signature_mode"] != "print":
            for lease_driver in lease_drivers:
                for lease_driver_document in lease_driver.documents:
                    if lease_driver_document.envelope.status != "envelope-completed":
                        raise ValueError(
                            "Lease Documents have not been fully signed yet."
                        )

        for lease_driver in lease_drivers:
            if not lease_driver or not lease_driver.is_active:
                raise ValueError("No active driver found for this lease")

            driver = driver_service.get_drivers(db, driver_id=lease_driver.driver_id)

            if not driver:
                raise ValueError("No driver found with this lease")

            driver = driver_service.upsert_driver(
                db, {"id": driver.id, "driver_status": DriverStatus.ACTIVE}
            )

        cases = bpm_service.get_cases(db=db, case_no=case_no, multiple=True)
        if cases:
            case_ids = [case.id for case in cases]
            audits = audit_trail_service.get_audit_trail_by_case_ids(db, case_ids)
            for audit in audits:
                audit_trail_service.update_audit_trail(
                    db=db,
                    audit_id=audit.id,
                    update_data={
                        "meta_data": {
                            "medallion_id": vehicle.medallions.id,
                            "vehicle_id": vehicle.id,
                            "driver_id": lease_drivers[0].driver.id,
                            "lease_id": lease.id,
                        }
                    },
                )

        vehicle = vehicle_service.upsert_vehicle(
            db, {"id": vehicle.id, "vehicle_status": VehicleStatus.ACTIVE}
        )

        lease = lease_service.upsert_lease(
            db, {"id": lease.id, "lease_status": LeaseStatus.ACTIVE, "is_active": True}
        )

        # Create audit trail for lease completion
        case = bpm_service.get_case_obj(db, case_no=case_no)
        driver_count = len([ld for ld in lease_drivers if ld.is_active])

        # Get first active driver for metadata
        first_driver_id = None
        if lease_drivers:
            first_driver_id = lease_drivers[0].driver.id

        audit_trail_service.create_audit_trail(
            db=db,
            description=f"Lease completed and activated: {lease.lease_id} with {driver_count} driver(s)",
            case=case,
            meta_data={
                "lease_id": lease.id,
                "vehicle_id": vehicle.id,
                "medallion_id": vehicle.medallions.id if vehicle.medallions else None,
                "driver_id": first_driver_id,
            },
            audit_type=AuditTrailType.AUTOMATED,
        )

        return "Ok"
    except Exception as e:
        logger.error("Error in complete_lease: %s", e, exc_info=True)
        raise e
