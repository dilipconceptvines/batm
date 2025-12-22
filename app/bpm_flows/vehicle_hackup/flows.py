## app/bpm_flows/vehicle_hackup/flows.py

# Standard library imports
from datetime import datetime, timedelta

from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service

# Local imports
from app.bpm.step_info import step
from app.utils.logger import get_logger
from app.medallions.schemas import MedallionStatus
from app.medallions.services import medallion_service
from app.medallions.utils import format_medallion_response
from app.uploads.services import upload_service
from app.vehicles.schemas import HackupStatus, RegistrationStatus, VehicleStatus , ProcessStatusEnum , ExpensesAndComplianceSubType , ExpensesAndComplianceCategory
from app.vehicles.services import vehicle_service
from app.bpm_flows.allocate_medallion_vehicle.utils import format_vehicle_details

logger = get_logger(__name__)
entity_mapper = {
    "VEHICLE": "vehicles",
    "VEHICLE_IDENTIFIER": "id",
}


@step(step_id="125", name="Fetch - vehicle hackup details", operation="fetch")
def fetch_vehicle_hackup_information(db, case_no, case_params=None):
    """
    Fetch the vehicle hackup information for the vehicle hackup step
    """
    try:
        # Get the case entity
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        vehicle = None
        if not vehicle :
            if case_params and case_params.get("object_name") == "vehicle":
                vehicle = vehicle_service.get_vehicles(db=db, vehicle_id=case_params.get("object_lookup"))
            if case_entity:
                vehicle = vehicle_service.get_vehicles(db=db, vehicle_id=int(case_entity.identifier_value))

        if not vehicle:
            return {}
        
        vehicle_details = format_vehicle_details(vehicle)

        vehicle_details["medallion_details"] = {
            "medallion_number": vehicle.medallions.medallion_number if vehicle.medallions else None,
            "medallion_type": vehicle.medallions.medallion_type if vehicle.medallions else None,
            "medallion_status": vehicle.medallions.medallion_status if vehicle.medallions else None,
        }


        if not case_entity:
            case_entity = bpm_service.create_case_entity(
                db=db,
                case_no=case_no,
                entity_name=entity_mapper["VEHICLE"],
                identifier=entity_mapper["VEHICLE_IDENTIFIER"],
                identifier_value=str(vehicle.id),
            )
            logger.info("Case entity %s created ", case_entity.id)

        hackup = vehicle_service.get_vehicle_hackup(db=db , vehicle_id=vehicle.id)

        if not hackup:
            return {
                "vehicle_details": vehicle_details
            }

        not_hackup_task = {
            "drop_location": None,
            "drop_by": None,
            "completed_by": None,
            "drop_date" : None,
            "completed_date": None,
            "status": ProcessStatusEnum.pending ,
            "note": None,
            "is_task_done": False,
            "is_required": False
        }

        hackup_data = {
            "id":hackup.id if hackup else None,
            "vehicle_id": hackup.vehicle_id if hackup else None,
            "tpep_provider": hackup.tpep_provider if hackup else None,
            "configuration_type": hackup.configuration_type if hackup else None,
            "paint": hackup.paint_task.to_dict() if hackup and hackup.paint_task else not_hackup_task,
            "camera": hackup.camera_task.to_dict() if hackup and hackup.camera_task else not_hackup_task,
            "partition": hackup.partition_task.to_dict() if hackup and hackup.partition_task else not_hackup_task,
            "meter": hackup.meter_task.to_dict() if hackup and hackup.meter_task else not_hackup_task,
            "meter_serial_number": hackup.meter_serial_number if hackup else None,
            "rooftop": hackup.rooftop_task.to_dict() if hackup and hackup.rooftop_task else not_hackup_task,
            "dmv_registration": hackup.dmv_registration_task.to_dict() if hackup and hackup.dmv_registration_task else not_hackup_task,
            "tlc_inspection": hackup.tlc_inspection_task.to_dict() if hackup and hackup.tlc_inspection_task else not_hackup_task,
            "dealership": hackup.dealership_task.to_dict() if hackup and hackup.dealership_task else not_hackup_task,
            "bat_garage": hackup.bat_garage_task.to_dict() if hackup and hackup.bat_garage_task else not_hackup_task,
            "status" : hackup.status if hackup else None
        }
        
        return {
            "vehicle_details": vehicle_details,
            "hackup_details": hackup_data
        }

    except Exception as e:
        logger.error("Error fetching vehicle hackup information: %s", e)
        raise e

@step(step_id="125" , name="process - vehicle hackup details" , operation="process")
def process_vehicle_hackup_details(db, case_no, step_data):
    """
    Process the vehicle hackup details
    """
    try:
        vehicle = None

        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if case_entity:
            vehicle = vehicle_service.get_vehicles(db, vehicle_id=int(case_entity.identifier_value))
        if not vehicle:
            raise ValueError("Vehicle not found")
        
        required_status = [VehicleStatus.AVAILABLE_FOR_HACK_UP , VehicleStatus.HACK_UP_IN_PROGRESS]
        if vehicle.vehicle_status not in required_status or not vehicle.medallions:
            raise ValueError( f"Vehicle must be {VehicleStatus.AVAILABLE_FOR_HACK_UP.value} or {VehicleStatus.HACK_UP_IN_PROGRESS.value} and have a medallion")
        
        vehicle_hackup = vehicle_service.get_vehicle_hackup(db=db , vehicle_id=vehicle.id)

        hackup_data = {
            "id":vehicle_hackup.id if vehicle_hackup else None,
            "vehicle_id": vehicle.id if vehicle else None,
            "paint_task_id": None,
            "camera_task_id": None,
            "partition_task_id": None,
            "meter_task_id": None,
            "rooftop_task_id": None,
            "dmv_registration_task_id": None,
            "tlc_inspection_task_id": None,
            "dealership_task_id": None,
            "bat_garage_task_id": None,
            "status": HackupStatus.ACTIVE
        }

        tasks =  step_data.get("tasks" , {})
        for key , value in tasks.items():
            if value:
                existing_task_id = getattr(vehicle_hackup, f"{key}_task_id", None) if vehicle_hackup else None
        
                task = vehicle_service.upsert_hackup_tasks(
                    db=db,
                    hackup_tasks={
                        "id": existing_task_id,
                        "task_name" : key,
                        **value
                        }
                )
                hackup_data[f"{key}_task_id"] = task.id if task else None

        vehicle_hackup = vehicle_service.upsert_vehicle_hackup(db=db, vehicle_hackup_data=hackup_data)

        vehicle_service.upsert_vehicle(
            db=db , vehicle_data={
                "id": vehicle.id,
                "vehicle_status": VehicleStatus.HACK_UP_IN_PROGRESS
            }
        )

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Processed HackUp Details For vehicle {vehicle.vin} with status {vehicle.vehicle_status}",
                meta_data={"vehicle_id": vehicle.id , "medallion_id": vehicle.medallion_id if vehicle.medallion_id else None}
            )

        return "Ok"
    except Exception as e:
        logger.error("Error processing vehicle hackup details: %s", e)
        raise e

@step(step_id="126" , name = "Fetch - additional hackup information" , operation="fetch")
def fetch_additional_hackup_information(db, case_no, case_params=None):
    """
    Fetch the additional hackup information for the vehicle hackup step
    """
    try:
        vehicle = None
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if case_entity:
            vehicle = vehicle_service.get_vehicles(db=db , vehicle_id=case_entity.identifier_value)
        
        if not vehicle:
            return {}
        
        vehicle_details = format_vehicle_details(vehicle)
        vehicle_details["medallion_details"] = {
            "medallion_number": vehicle.medallions.medallion_number if vehicle.medallions else None,
            "medallion_type": vehicle.medallions.medallion_type if vehicle.medallions else None,
            "medallion_status": vehicle.medallions.medallion_status if vehicle.medallions else None,
        }

        vehicle_hackup = vehicle_service.get_vehicle_hackup(db=db , vehicle_id=vehicle.id)
        vehicle_registration = vehicle_service.get_vehicle_registration(db=db , vehicle_id=vehicle.id)

        if not vehicle_hackup:
            return {
                "vehicle_details": vehicle_details
            }
        
        liability_insurance = vehicle_service.get_vehicle_insurance(
            db=db, vehicle_id=vehicle.id, insurance_type=ExpensesAndComplianceSubType.Liability_Insurance.value
        )
        worker_compensation_insurance = vehicle_service.get_vehicle_insurance(
            db=db, vehicle_id=vehicle.id, insurance_type=ExpensesAndComplianceSubType.Worker_Compensation_Insurance.value
        )

        additional_details = {
            "plate_number": vehicle_registration.plate_number if vehicle_registration else None,
            "meter_serial_number": vehicle_hackup.meter_serial_number if vehicle_hackup else None,
            "lib_insurance_number": liability_insurance.insurance_number if liability_insurance else None,
            "lib_insurance_start_date": liability_insurance.insurance_start_date if liability_insurance else None,
            "lib_insurance_end_date": liability_insurance.insurance_end_date if liability_insurance else None,
            "wc_insurance_number": worker_compensation_insurance.insurance_number if worker_compensation_insurance else None,
            "wc_insurance_start_date": worker_compensation_insurance.insurance_start_date if worker_compensation_insurance else None,
            "wc_insurance_end_date": worker_compensation_insurance.insurance_end_date if worker_compensation_insurance else None,
        }

        return {
            "vehicle_details": vehicle_details,
            "additional_details": additional_details
        }
    except Exception as e:
        logger.error("Error fetching additional hackup information: %s", e)
        raise e


@step(step_id="126" , name = "Process - additional hackup information" , operation="process")
def process_additional_hackup_information(db, case_no, step_data):
    """
    process additional hackup information
    """
    try:
        vehicle = None
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if case_entity:
            vehicle = vehicle_service.get_vehicles(db=db , vehicle_id=case_entity.identifier_value)
        
        if not vehicle:
            raise ValueError("Vehicle not found")

        vehicle_hackup = vehicle_service.get_vehicle_hackup(db=db , vehicle_id=vehicle.id)
        vehicle_registraion = vehicle_service.get_vehicle_registration(db=db , vehicle_id=vehicle.id)

        plate_number = step_data.get("plate_number" , None)
        meter_serial_number = step_data.get("meter_serial_number" , None)
        lib_insurance_number = step_data.get("lib_insurance_number" , None)
        lib_insurance_start_date = step_data.get("lib_insurance_start_date" , None)
        lib_insurance_end_date = step_data.get("lib_insurance_end_date" , None)
        wc_insurance_number = step_data.get("wc_insurance_number" , None)
        wc_insurance_start_date = step_data.get("wc_insurance_start_date" , None)
        wc_insurance_end_date = step_data.get("wc_insurance_end_date" , None)

        is_plate_number = vehicle_service.get_vehicles(db=db , plate_number=plate_number)
        is_meter_serial_number = vehicle_service.get_vehicles(db=db , meter_serial_number=meter_serial_number)
        is_lib_insurance_number = vehicle_service.get_vehicles(db=db , insurance_number=lib_insurance_number)
        is_wc_insurance_number = vehicle_service.get_vehicles(db=db , insurance_number=wc_insurance_number)

        if is_plate_number and is_plate_number.id != vehicle.id:
            raise ValueError(
                f"Plate number '{plate_number}' is already registered under vehicle VIN {is_plate_number.vin}."
            )
        if is_meter_serial_number and is_meter_serial_number.id != vehicle.id:
            raise ValueError(
                f"Meter serial number '{meter_serial_number}' is already linked to vehicle VIN {is_meter_serial_number.vin}."
            )
        if is_lib_insurance_number and is_lib_insurance_number.id != vehicle.id:
            raise ValueError(
                f"Insurance number '{lib_insurance_number}' is already associated with vehicle VIN {lib_insurance_number.vin}."
            )
        if is_wc_insurance_number and is_wc_insurance_number.id != vehicle.id:
            raise ValueError(
                f"Insurance number '{wc_insurance_number}' is already associated with vehicle VIN {wc_insurance_number.vin}."
            )
        
        
        if lib_insurance_start_date and lib_insurance_end_date and lib_insurance_start_date > lib_insurance_end_date:
            raise ValueError("Liability Insurance start date must be before insurance end date.")
        if wc_insurance_start_date and wc_insurance_end_date and wc_insurance_start_date > wc_insurance_end_date:
            raise ValueError("Worker Compensation Insurance start date must be before insurance end date.")
        

        registration = vehicle_service.upsert_registration(
            db=db,
            registration_data={
                "id": vehicle_registraion.id if vehicle_registraion else None,
                "vehicle_id": vehicle.id,
                "registration_date": datetime.now().date(),
                "registration_expiry_date": datetime.now().date() + timedelta(days=365),
                "plate_number": plate_number,
                "status": RegistrationStatus.ACTIVE
            }
        )

        additional_hackup = vehicle_service.upsert_vehicle_hackup(
            db=db,
            vehicle_hackup_data = {
                "id": vehicle_hackup.id if vehicle_hackup else None,
                "meter_serial_number": meter_serial_number,
                "status": HackupStatus.ACTIVE
            })
        
        insurances = [
            {
            "vehicle_id": vehicle.id,
            "insurance_type": ExpensesAndComplianceSubType.Liability_Insurance.value,
            "insurance_number": lib_insurance_number,
            "insurance_start_date": lib_insurance_start_date,
            "insurance_end_date": lib_insurance_end_date,
            "status": HackupStatus.ACTIVE
            },
            {
            "vehicle_id": vehicle.id,
            "insurance_type": ExpensesAndComplianceSubType.Worker_Compensation_Insurance.value,
            "insurance_number": wc_insurance_number,
            "insurance_start_date": wc_insurance_start_date,
            "insurance_end_date": wc_insurance_end_date,
            "status": HackupStatus.ACTIVE
            }
        ]

        for insurance in insurances:
            vehicle_service.upsert_vehicle_insurance(db=db , vehicle_insurance=insurance)

        expenses = [
            {
                "vehicle_id": vehicle.id,
                "category": ExpensesAndComplianceCategory.INSPECTIONS_AND_COMPLIANCE.value,
                "sub_type": ExpensesAndComplianceSubType.Liability_Insurance.value,
                "specific_info": lib_insurance_number,
                "issue_date": lib_insurance_start_date,
                "expiry_date" : lib_insurance_end_date
            },
            {
                "vehicle_id": vehicle.id,
                "category": ExpensesAndComplianceCategory.INSPECTIONS_AND_COMPLIANCE.value,
                "sub_type": ExpensesAndComplianceSubType.Worker_Compensation_Insurance.value,
                "specific_info": wc_insurance_number,
                "issue_date": wc_insurance_start_date,
                "expiry_date" : wc_insurance_end_date
            }
        ]

        for expense in expenses:
            vehicle_service.upsert_vehicle_expenses(db=db , vehicle_expenses=expense)
        
        vehicle_service.upsert_vehicle(db=db , vehicle_data={
            "id": vehicle.id,
            "vehicle_status": VehicleStatus.HACKED_UP
        })

        medallion_service.upsert_medallion(
            db=db , medallion_data={
                "id": vehicle.medallion_id,
                "medallion_status": MedallionStatus.ACTIVE
            }
        )

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Processed additional hackup information for vehicle {vehicle.vin} with status {vehicle.vehicle_status}",
                meta_data={"vehicle_id": vehicle.id , "medallion_id": vehicle.medallion_id if vehicle.medallion_id else None}
            )

        return "Ok"
    except Exception as e:
        logger.error("Error processing additional hackup information: %s", e)
        raise e
