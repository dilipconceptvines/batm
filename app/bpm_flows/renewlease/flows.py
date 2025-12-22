## app/bpm_flows/renewlease/flows.py

# Standard library imports
from datetime import datetime, timezone

# Local imports
from app.utils.logger import get_logger
from app.bpm.step_info import step
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.leases.services import lease_service
from app.drivers.services import driver_service
from app.medallions.utils import format_medallion_response
from app.uploads.services import upload_service
from app.vehicles.services import vehicle_service
from app.vehicles.schemas import VehicleStatus
from app.leases.schemas import LeaseStatus
from app.leases.utils import generate_medallion_lease_document, generate_dov_vehicle_lease_document
from app.drivers.schemas import DOVLease, DriverStatus
from app.leases.schemas import LongTermLease, ShortTermLease, MedallionOnlyLease

logger = get_logger(__name__)

entity_mapper = {
    "LEASE": "lease",
    "LEASE_IDENTIFIER": "id",
}

@step(step_id="136", name="Fetch - Return Lease Details", operation='fetch')
def get_lease_details(db, case_no, case_params=None):
    """
    Fetch the lease details for the driver lease step
    """
    try:
        lease = None
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        
        if "object_lookup" in case_params:
            lease = lease_service.get_lease(db, lease_id=case_params["object_lookup"])
        elif case_entity:
            lease = lease_service.get_lease(db, lookup_id=case_entity.identifier_value)            
        if not lease:
            return {}
        
        medallion = format_medallion_response(lease.medallion) if lease.medallion else None
        
        return {
            'lease_case_details': {
                "vehicle_vin": lease.vehicle.vin if lease.vehicle else None,
                "plate_number": lease.vehicle.registrations[0].plate_number if lease.vehicle.registrations else None,
                "vehicle_type": lease.vehicle.vehicle_type if lease.vehicle else None,
                "lease_type": lease.lease_type,
                "medallion_number": lease.medallion.medallion_number if lease.medallion else None,
                "medallion_owner": medallion["medallion_owner"] if medallion else None
            },
            'lease_info': {
                'lease_id': lease.lease_id,
                'total_weeks': lease.duration_in_weeks,
                'lease_start_date': lease.lease_start_date,
                'lease_end_date': lease.lease_end_date,
                'pay_day': lease.lease_pay_day,
                'is_auto_renewal': lease.is_auto_renewed,
                'is_day_shift': lease.is_day_shift,
                "is_night_shift": lease.is_night_shift,
                "deposit_amount_paid": lease.deposit_amount_paid if lease.deposit_amount_paid else 0,
                'payments': lease.lease_payments_type,
                'cancellation_fee': lease.cancellation_fee
            }
        }
    except Exception as e:
        logger.error("Error in get_lease_details: %s", e, exc_info=True)
        raise e


@step(step_id="136", name="Process - Save Lease Details", operation='process')
def set_lease_details(db, case_no, step_data):
    """
    Process the lease details for the driver lease step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        lease = lease_service.get_lease(db, lease_id=step_data.get("lease_id"))
        if lease.lease_type != step_data.get("lease_type"):
            existing_configs = lease_service.get_lease_configurations(db, lease_id=lease.id, multiple=True)

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

        lease = lease_service.upsert_lease(db, lease_data)

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
                description=f"Updated lease details for lease {lease.lease_id}",
                meta_data={"lease_id": lease.id}
            )

        return "Ok"
    except Exception as e:
        logger.error("Error in set_lease_details: %s", e, exc_info=True)
        raise e


@step(step_id="137", name="Fetch - Enter Financial Information", operation='fetch')
def get_financial_information(db, case_no, case_params=None):
    """
    Fetch the financial information for the driver lease step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}

        lease = lease_service.get_lease(db, lookup_id=case_entity.identifier_value)
        if not lease:
            return {}
        
        medallion_data = format_medallion_response(medallion= lease.medallion) if lease.medallion else None

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


@step(step_id="137", name="Process - Save Financial Information", operation='process')
def set_financial_information(db, case_no, step_data):
    """
    Process the financial information for the driver lease step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if not case_entity:
            raise ValueError(
                "Step cannot be executed because there is no valid case")

        lease = lease_service.get_lease(db, lookup_id=case_entity.identifier_value)

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

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Updated financial information for lease {lease.lease_id}",
                meta_data={"lease_id": lease.id}
            )
        
        return "Ok"
    except Exception as e:
        logger.error("Error in set_financial_information: %s", e, exc_info=True)
        raise e


# @step(step_id="138", name="Fetch - Search Driver Information", operation='fetch')
# def choose_driver(db, case_no, case_params=None):
#     """
#     Fetch the driver information for the driver lease step
#     """
#     try:
#         case_entity = bpm_service.get_case_entity(db, case_no=case_no)

#         if not case_entity:
#             return {}

#         lease = lease_service.get_lease(db, lookup_id=case_entity.identifier_value)
#         lease_case_details = {
#                 "vehicle_vin": lease.vehicle.vin if lease.vehicle else None,
#                 "plate_number": lease.vehicle.registrations[0].plate_number if lease.vehicle.registrations else None,
#                 "vehicle_type": lease.vehicle.vehicle_type if lease.vehicle else None,
#                 "lease_type": lease.lease_type,
#                 "medallion_number": lease.medallion.medallion_number if lease.medallion else None,
#             }

#         if not set(case_params.keys()).intersection(
#                 ['ssn', 'tlc_license_number', 'dmv_license_number']):
#             return {"lease_case_details": lease_case_details}

#         driver = driver_service.get_drivers(
#             db, ssn=case_params.get("ssn", None),
#             tlc_license_number=case_params.get('tlc_license_number', None),
#             dmv_license_number=case_params.get('dmv_license_number', None)
#         )
#         if not driver:
#             return {"lease_case_details": lease_case_details}

#         if driver.driver_status != DriverStatus.REGISTERED and driver.driver_status != DriverStatus.ACTIVE:
#             return {"lease_case_details": lease_case_details}

#         return {
#             "lease_case_details": lease_case_details,
#             "driver_info": {
#                 "driver_id": driver.id,
#                 "driver_lookup_id": driver.driver_id,
#                 "first_name": driver.first_name,
#                 "last_name": driver.last_name,
#                 "driver_type": driver.driver_type,
#                 "driver_ssn": driver.ssn,
#                 "tlc_license_number": driver.tlc_license.tlc_license_number,
#                 "dmv_license_number": driver.dmv_license.dmv_license_number,
#                 "contact_number": driver.phone_number_1
#             }
#         }
#     except Exception as e:
#         logger.error("Error in choose_driver: %s", e, exc_info=True)
#         raise e


# @step(step_id="138", name="Process - Add drivers to lease", operation='process')
# def set_driver(db, case_no, step_data):
#     """
#     Process the driver information for the driver lease step
#     """
#     try:
#         case_entity = bpm_service.get_case_entity(db, case_no=case_no)

#         if not case_entity:
#             return {}

#         lease = lease_service.get_lease(db, lookup_id=int(case_entity.identifier_value))
#         lease_drivers = lease_service.get_lease_drivers(db, lease_id=lease.lease_id, multiple=True)
#         driver_ids = set([str(lease_driver.driver_id) for lease_driver in lease_drivers if lease_drivers.driver_id])

#         passed_driver_ids = set()
#         for driver_info in step_data['select_driver']:
#             passed_driver_ids.add(driver_info['driver_id'])
#             message = lease_service.update_lease_driver_info(db, lease.id, driver_info)
#             logger.info(message)

#         driver_diff = driver_ids.difference(passed_driver_ids)
#         lease_service.remove_drivers_from_lease(db, lease.id, driver_diff)

#         logger.info("Generate documents for all the drivers selected")
#         medallion_document_info = generate_medallion_lease_document(lease)
#         vehicle_document_info = generate_vehicle_lease_document(lease)
#         upload_service.create_document(
#             db,
#             new_filename=medallion_document_info['document_name'],
#             original_extension=medallion_document_info['document_format'],
#             document_path=medallion_document_info['document_path'],
#             object_type=medallion_document_info['object_type'],
#             object_id=medallion_document_info['object_lookup_id'],
#             notes=medallion_document_info['document_note'],
#             document_type=medallion_document_info['document_type'],
#             document_date=datetime.now(timezone.utc).isoformat().split('T')[0],
#             file_size_kb=0,
#         )
#         upload_service.create_document(
#             db,
#             new_filename=vehicle_document_info['document_name'],
#             original_extension=vehicle_document_info['document_format'],
#             document_path=vehicle_document_info['document_path'],
#             object_type=vehicle_document_info['object_type'],
#             object_id=vehicle_document_info['object_lookup_id'],
#             notes=vehicle_document_info['document_note'],
#             document_type=vehicle_document_info['document_type'],
#             document_date=datetime.now(timezone.utc).isoformat().split('T')[0],
#             file_size_kb=0,
#         )
#         return "Ok"
#     except Exception as e:
#         logger.error("Error in set_driver: %s", e, exc_info=True)
#         raise e


@step(step_id="138", name="Fetch - Document for signature", operation='fetch')
def fetch_document_for_signature(db, case_no, case_params=None):
    """
    Fetch the document for signature for the driver lease step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if not case_entity:
            return {}

        lease = lease_service.get_lease(db, lookup_id=int(case_entity.identifier_value))
        if not lease:
            return {}
        
        medallion = format_medallion_response(lease.medallion) if lease.medallion else None

        lease_case_details = {
            "vehicle_vin": lease.vehicle.vin if lease.vehicle else None,
            "plate_number": lease.vehicle.registrations[0].plate_number if lease.vehicle.registrations else None,
            "vehicle_type": lease.vehicle.vehicle_type if lease.vehicle else None,
            "lease_type": lease.lease_type,
            "medallion_number": lease.medallion.medallion_number if lease.medallion else None,
            "medallion_owner": medallion["medallion_owner"] if medallion else None
        }


        documents = lease_service.fetch_latest_driver_document_status_by_lease(db, lease)
        # documents = upload_service.get_documents(db, object_type="lease", object_id=lease.id, multiple=True)

        return {'lease_case_details': lease_case_details, 'documents': documents}
    except Exception as e:
        logger.error("Error in fetch_document_for_signature: %s", e, exc_info=True)
        raise e


@step(step_id="138", name="Process - Send document for signature", operation='process')
def send_document_for_signature(db, case_no, step_data):
    """Send the document for signature for the driver lease step"""
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if not case_entity:
            return {}

        lease = lease_service.get_lease(db, lookup_id=int(case_entity.identifier_value))
        if not lease:
            return {}

        lease_service.upsert_lease_driver_documents(db, lease)
        
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Prepared documents for signature for lease {lease.lease_id}",
                meta_data={"lease_id": lease.id}
            )
        
        return "Ok"
    except Exception as e:
        logger.error("Error in send_document_for_signature: %s", e)
        raise e

@step(step_id="139", name="Fetch - Documents for completing Lease", operation='fetch')
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
        medallion = format_medallion_response(lease.medallion) if lease.medallion else None

        lease_case_details = {
            "vehicle_vin": lease.vehicle.vin if lease.vehicle else None,
            "plate_number": lease.vehicle.registrations[0].plate_number if lease.vehicle.registrations else None,
            "vehicle_type": lease.vehicle.vehicle_type if lease.vehicle else None,
            "lease_type": lease.lease_type,
            "medallion_number": lease.medallion.medallion_number if lease.medallion else None,
            "medallion_owner": medallion["medallion_owner"] if medallion else None
        }

        lease_service.upsert_lease_driver_documents(db, lease)
        documents = lease_service.fetch_latest_driver_document_status_by_lease(db, lease=lease)
        
        if not lease:
            return {}

        
        return {'lease_case_details': lease_case_details, 'documents': documents}
    except Exception as e:
        logger.error("Error in fetch_documents_for_complete_lease: %s", e)
        raise e


@step(step_id="139", name="Process - Completing Lease", operation='process')
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

        lease_drivers = lease_service.get_lease_drivers(db, lease_id=lease.id , multiple=True)

        if lease_drivers:
            for lease_driver in lease_drivers:
                if not lease_driver or lease_driver.is_active == False:
                    raise ValueError("No active driver found for this lease")
                
                driver = driver_service.get_drivers(db, driver_id=lease_driver.driver_id)

                if not driver:
                    raise ValueError("No driver found with this lease")
                
                driver = driver_service.upsert_driver(db, {
                    "id": driver.id,
                    "driver_status": DriverStatus.ACTIVE
                })
        
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Completed lease renewal for lease {lease.lease_id}",
                meta_data={"medallion_id": vehicle.medallions.id, "vehicle_id": vehicle.id, "driver_id": lease_drivers[0].driver.id, "lease_id": lease.id}
            )

        vehicle = vehicle_service.upsert_vehicle(db, {
            "id": vehicle.id,
            "vehicle_status": VehicleStatus.ACTIVE
        })

        lease = lease_service.upsert_lease(db, {
            "id": lease.id,
            "lease_status": LeaseStatus.ACTIVE,
            "is_active": True
        })
        
        return "Ok"
    except Exception as e:
        logger.error("Error in complete_lease: %s", e)
        raise e

# @step(step_id="854", name="Fetch - Documents for completing Lease", operation='fetch')
# def fetch_documents_for_complete_lease(db, case_no, case_params=None):
#     """
#     Fetch the documents for completing Lease for the driver lease step
#     """
#     case_entity = utils.fetch_case_entity(db, case_no)

#     if not case_entity:
#         return {}

#     lease = driverlease_utils.get_lease_by_lease_id(
#         db, case_entity.identifier_value)

#     lease_case_details = driverlease_utils.get_lease_details(db, lease)

#     documents = driverlease_utils.fetch_latest_driver_document_status_by_lease(
#         db, lease)
#     return {'lease_case_details': lease_case_details, 'documents': documents}


# @step(step_id="854", name="Process - Completing Lease", operation='process')
# def complete_lease(db, case_no, step_data):
#     """
#     Process the completing Lease for the driver lease step
#     """
#     case_entity = utils.fetch_case_entity(db, case_no)

#     if not case_entity:
#         return {}

#     lease = driverlease_utils.get_lease_by_lease_id(db,
#                                               case_entity.identifier_value)
#     if not lease:
#         return {}
#     lease.is_active = True

    
#     driverlease_utils.update_lease_active(db, lease)
#     db.add(lease)
#     db.flush()
#     return "Ok"