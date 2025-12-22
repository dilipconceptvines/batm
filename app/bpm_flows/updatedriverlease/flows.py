## app/bpm_flows/updatedriverlease/flows.py

# Local imports
from datetime import datetime

from app.utils.logger import get_logger
from app.bpm.step_info import step
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.drivers.services import driver_service
from app.leases.services import lease_service
from app.medallions.services import medallion_service
from app.drivers.utils import format_driver_response
from app.medallions.utils import format_medallion_response
from app.drivers.schemas import DOVLease
from app.leases.schemas import LongTermLease, ShortTermLease, MedallionOnlyLease

logger = get_logger(__name__)

entity_mapper = {
    "LEASE": "lease",
    "LEASE_IDENTIFIER": "id",
}


@step(step_id="156", name="Fetch - Leases associated with the driver", operation='fetch')
def fetch_driver_lease_details(db, case_no, case_params=None):
    """
    Fetch the driver leases for the update driver lease step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        driver = None
        if case_params:
            driver = driver_service.get_drivers(db, driver_id=case_params['object_lookup'])

        if not driver:
            return {"driver_info": {}}
        
        driver_data = format_driver_response(driver, False)
        return {
            "driver_info": {
                "driver_seq_id": driver.id,
                **driver_data["driver_details"],
                **driver_data["dmv_license_details"],
                **driver_data["tlc_license_details"],
            },
            "lease_info": lease_service.fetch_lease_information_for_driver(db, driver.driver_id)
        }
    except Exception as e:
        logger.error("Error in fetch_driver_lease_details: %s", e, exc_info=True)
        raise e


@step(step_id="156", name="Process - Driver Lease Creation", operation='process')
def process_lease_termination(db, case_no, step_data):
    """
    Select driver lease
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        driver_lease_id = step_data.get("lease_id")
        driver_lease = lease_service.get_lease_drivers(db=db , lease_driver_id=driver_lease_id) if driver_lease_id else None
        lease = lease_service.get_lease(db, lookup_id=driver_lease.lease_id) if driver_lease else None

        if not lease:
            raise ValueError("Lease not found for the driver id passed")

        if case_entity and lease.id != int(case_entity.identifier_value):
            raise ValueError("The lease id passed is not relevant to this case")

        if not case_entity:
            case_entity = bpm_service.create_case_entity(
                db=db, case_no=case_no,
                entity_name=entity_mapper['LEASE'],
                identifier=entity_mapper['LEASE_IDENTIFIER'],
                identifier_value=str(lease.id)
            )
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Created lease for driver {driver_lease.driver_id} with lease {lease.lease_id}",
                meta_data={"lease_id": lease.id}
            )
        return "Ok"
    except Exception as e:
        logger.error("Error in process_lease_termination: %s", e, exc_info=True)
        raise e


@step(step_id="157", name="Fetch - Return Lease Details", operation='fetch')
def get_lease_details(db, case_no, case_params=None):
    """
    Fetch the lease details for the driver lease step
    """
    try:
        lease = None
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if not case_entity:
            return {}

        lease = lease_service.get_lease(db, lookup_id=int(case_entity.identifier_value))
        if not lease:
            return {}
        
        medallion_data = format_medallion_response(medallion=lease.medallion)

        return {
            'lease_case_details': {
                "vehicle_vin": lease.vehicle.vin if lease.vehicle else None,
                "plate_number": lease.vehicle.registrations[0].plate_number if lease.vehicle.registrations else None,
                "vehicle_type": lease.vehicle.vehicle_type if lease.vehicle else None,
                "lease_type": lease.lease_type,
                "medallion_number": lease.medallion.medallion_number if lease.medallion else None,
                "medallion_owner": medallion_data["medallion_owner"]
            },
            'lease_info': {
                'lease_id': lease.lease_id ,
                'total_weeks': lease.duration_in_weeks,
                'lease_start_date': lease.lease_start_date,
                'lease_end_date': lease.lease_end_date,
                'pay_day': lease.lease_pay_day,
                'is_auto_renewal': lease.is_auto_renewed,
                'is_day_shift': lease.is_day_shift,
                "is_night_shift": lease.is_night_shift,
                "deposit_amount_paid": lease.deposit_amount_paid,
                'payments': lease.lease_payments_type,
                'cancellation_fee': lease.cancellation_fee
            }
        }
    except Exception as e:
        logger.error("Error in get_lease_details: %s", e, exc_info=True)
        raise e


@step(step_id="157", name="Process - Save Lease Details", operation='process')
def set_lease_details(db, case_no, step_data):
    """
    Process the lease details for the driver lease step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
 
        if not case_entity:
            raise ValueError(
                "Step cannot be executed because there is no valid case")
 
        lease = lease_service.get_lease(db, lookup_id=int(case_entity.identifier_value))

        lease_data = {
            "id": lease.id,
            "lease_id": step_data.get("lease_id", lease.lease_id),
            "lease_type": step_data.get("lease_type", lease.lease_type),
            "duration_in_weeks": step_data.get("total_weeks", lease.duration_in_weeks),
            "lease_start_date": (
                datetime.strptime(step_data["lease_start_date"], "%Y-%m-%d").date()
                if step_data.get("lease_start_date") else lease.lease_start_date
            ),
            "lease_end_date": (
                datetime.strptime(step_data["lease_end_date"], "%Y-%m-%d").date()
                if step_data.get("lease_end_date") else lease.lease_end_date
            ),
            "lease_pay_day": step_data.get("pay_day", lease.lease_pay_day),
            "is_auto_renewed": step_data.get("is_auto_renewal", lease.is_auto_renewed),
            "is_day_shift": step_data.get("is_day_shift", lease.is_day_shift),
            "is_night_shift": step_data.get("is_night_shift", lease.is_night_shift),
            "deposit_amount_paid": step_data.get("deposit_amount_paid", lease.deposit_amount_paid),
            "lease_payments_type": step_data.get("payments", lease.lease_payments_type),
            "cancellation_fee": step_data.get("cancellation_fee", lease.cancellation_fee),
            "lease_remark": step_data.get("lease_remark", lease.lease_remark),
            "lease_status": step_data.get("lease_status", lease.lease_status),
            "is_active": step_data.get("is_active", lease.is_active),
        }
 
        lease_service.upsert_lease(db , lease_data={"id": lease.id, **lease_data})
        
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Updated lease details for lease {lease.lease_id}",
                meta_data={"lease_id": lease.id}
            )
        return "Ok"
    except Exception as e:
        logger.error("Error in set_lease_details: %s", e, exc_info=True)
        raise e


@step(step_id="158", name="Fetch - Enter Driver Financial Information", operation='fetch')
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
        
        medallion_data = format_medallion_response(medallion=lease.medallion)

        configurations = lease_service.get_lease_configurations(db, lease_id=lease.lease_id, multiple=True)
        payment_config = lease_service.fetch_lease_payment_configuration(db, multiple=True)

        # if not payment_config:
        #     return {}

        # Lease Caps from the TLC
        TLC_VEHICLE_CAP_TOTAL = 42900.00
        TLC_VEHICLE_WEEKLY_CAP = 275.00
        TLC_MEDALLION_WEEKLY_CAP = 994.00

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
        if lease_type == "dov":
            for config in payment_config:
                if config.config_type == "dov_med_lease":
                    med_lease = config.total_amount
                elif config.config_type == "dov_veh_lease":
                    veh_lease = config.total_amount
            lease_amount = med_lease + veh_lease
            management_recommendation = lease_amount
        elif lease_type == "long-term":
            for config in payment_config:
                if config.config_type == "long_term_lease":
                    day_shift = config.day_shift_amount
                    night_shift = config.night_shift_amount
            lease_amount = day_shift + night_shift
            management_recommendation = lease_amount
        elif lease_type == "short-term":
            for config in payment_config:
                if config.config_type == "short_term_lease":
                    day_shift = config.day_shift_amount
                    night_shift = config.night_shift_amount
            lease_amount = day_shift + night_shift
            management_recommendation = lease_amount
        elif lease_type == "medallion-only":
            for config in payment_config:
                if config.config_type == "medallion_only":
                    lease_amount = config.total_amount
            management_recommendation = med_lease
        else:
            lease_amount = med_lease + veh_lease
            management_recommendation = lease_amount

        # Flags
        is_over_vehicle_cap = (veh_lease * total_weeks > TLC_VEHICLE_CAP_TOTAL) if total_weeks else False
        is_weekly_over_cap = (veh_lease > TLC_VEHICLE_WEEKLY_CAP)

        return {
            'lease_case_details': {
                "vehicle_vin": lease.vehicle.vin if lease.vehicle else None,
                "plate_number": lease.vehicle.registrations[0].plate_number if lease.vehicle.registrations else None,
                "vehicle_type": lease.vehicle.vehicle_type if lease.vehicle else None,
                "lease_type": lease.lease_type,
                "medallion_number": lease.medallion.medallion_number if lease.medallion else None,
                "medallion_owner": medallion_data["medallion_owner"]
            },
            "financials": {
                "tlc_max_vehicle_cap": TLC_VEHICLE_CAP_TOTAL,
                "tlc_weekly_cap": TLC_VEHICLE_WEEKLY_CAP,
                "tlc_medallion_cap": TLC_MEDALLION_WEEKLY_CAP,
                "management_recommendation": round(management_recommendation, 2),
                "day_shift_amount": round(day_shift, 2) if day_shift else 0,
                "night_shift_amount": round(night_shift, 2) if night_shift else 0,
                "lease_amount": round(lease_amount, 2),
                "med_lease": round(med_lease, 2),
                "veh_lease": round(veh_lease, 2),
                "is_over_vehicle_cap": is_over_vehicle_cap,
                "is_weekly_over_cap": is_weekly_over_cap,
            },
            "lease_configuration": {
                "lease_id": lease.lease_id,
                "lease_type": lease.lease_type if lease.lease_type else "",
                "total_weeks": lease.duration_in_weeks,
                "medallion_id": lease.medallion.medallion_number if lease.medallion else "",
                "vehicle_id": lease.vehicle.vin if lease.vehicle else "",
                "lease_start_date": lease.lease_start_date.isoformat() if lease.lease_start_date else "",
                "lease_end_date": lease.lease_end_date.isoformat() if lease.lease_end_date else "",
                "is_auto_renewed": lease.is_auto_renewed,
                "is_day_shift": lease.is_day_shift,
                "lease_remark": lease.lease_remark,
                "configurations": [
                    {
                        "lease_breakup_type": config.lease_breakup_type,
                        "lease_limit": config.lease_limit
                    } for config in configurations
                ]
            }
        }
    except Exception as e:
        logger.error("Error in get_financial_information: %s", e, exc_info=True)
        raise e


@step(step_id="158", name="Process - Save Driver Financial Information", operation='process')
def set_financial_information(db, case_no, step_data):
    """
    Process the financial information for the driver lease step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if not case_entity:
            raise ValueError(
                "Step cannot be executed because there is no valid case")

        lease = lease_service.get_lease(db, lookup_id=int(case_entity.identifier_value))

        if lease.lease_type != step_data.get("leaseType"):
            raise ValueError(f"Lease type doesn't match with this lease id {lease.lease_type}")
        
        lease_type = step_data.get("leaseType")
        if lease_type == "dov":
            data = DOVLease(**step_data)
            lease_service.handle_dov_lease(db, lease.id, data)
        elif lease_type == "long-term":
            data = LongTermLease(**step_data)
            lease_service.handle_long_term_lease(db, lease.id, data)
        elif lease_type == "short-term":
            data = ShortTermLease(**step_data)
            lease_service.handle_short_term_lease(db, lease.id, data)
        elif lease_type == "medallion-only":
            data = MedallionOnlyLease(**step_data)
            lease_service.handle_medallion_lease(db, lease.id, data)
        else:
            raise ValueError(f"Invalid lease type: {lease.lease_type}")
        
        lease_drivers = lease_service.get_lease_drivers(db=db , lease_id=lease.id)

        driver = driver_service.get_drivers(db=db , driver_id= lease_drivers.driver_id)

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Updated financial information for lease {lease.lease_id}",
                meta_data={"driver_id": driver.id, "vehicle_id": lease.vehicle_id, "medallion_id": lease.medallion_id, "lease_id": lease.id}
            )

        return "Ok"
    except Exception as e:
        logger.error("Error in set_financial_information: %s", e, exc_info=True)
        raise e
