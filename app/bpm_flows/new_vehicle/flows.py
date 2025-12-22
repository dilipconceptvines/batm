## app/bpm_flows/new_vehicle/flows.py

# Standard imports
from datetime import datetime

# Third party imports
from fastapi import HTTPException

# Local imports
from app.utils.logger import get_logger
from app.bpm.step_info import step
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.vehicles.services import vehicle_service
from app.vehicles.schemas import VehicleStatus
from app.uploads.services import upload_service
from app.bpm_flows.allocate_medallion_vehicle.utils import format_vehicle_details
from app.core.config import settings
from app.vehicles.schemas import ExpensesAndComplianceCategory , ExpensesAndComplianceSubType

logger = get_logger(__name__)
entity_mapper = {
    "VEHICLE": "vehicles",
    "VEHICLE_IDENTIFIER": "id",
}

@step(step_id="121", name="Fetch - Vehicle Documents", operation='fetch')
def fetch_vehicle_documents(db, case_no, case_params=None):
    """
    Fetch the vehicle information for the new vehicle step
    """
    try:
        logger.info("Fetch vehicle information")
        vehicle = None
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        if case_entity :
            vehicle = vehicle_service.get_vehicles(
                db, vehicle_id=int(case_entity.identifier_value)
            )
        
        if not vehicle:
            if case_params and case_params.get("object_name") == "entityId":
                vehicle = vehicle_service.upsert_vehicle(
                    db, {"entity_id": int(case_params['object_lookup'])}
                )
                
        if not vehicle:
            return {}
        
        vehicle_invoice = upload_service.get_documents(db=db , object_type="vehicle",
                                            object_id=vehicle.id,document_type="vehicle_invoice")
        
        if not case_entity:
            case_entity = bpm_service.create_case_entity(
                db=db, case_no=case_no,
                entity_name=entity_mapper["VEHICLE_IDENTIFIER"],
                identifier=entity_mapper["VEHICLE_IDENTIFIER"],
                identifier_value=str(vehicle.id)
            )
        
        return {
            "documents":[vehicle_invoice],
            "document_type":["vehicle_invoice"],
            "required_documents":["vehicle_invoice"],
            "object_type":"vehicle",
            "object_id":vehicle.id
        }
    except Exception as e:
        logger.error("Error fetching vehicle information: %s", e)
        raise e

@step(step_id="121", name="Process - Upload vehicle Documents", operation='process')
def upload_vehicle_documents(db, case_no, step_data):
    """
    Process the vehicle information for the new vehicle step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}

        vehicle = vehicle_service.get_vehicles(
            db, vehicle_id=int(case_entity.identifier_value)
        )
        if not vehicle:
            return {}
        
        logger.info("Nothing TO DO Here")

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Uploaded vehicle documents for vehicle {vehicle.vin} with vehicle owner {vehicle.vehicle_entity.entity_name}",
                meta_data={"vehicle_id": vehicle.id , "vehicle_owner_id": vehicle.vehicle_entity.id}
            )
    
        return "Ok"
    except Exception as e:
        logger.error("Error creating or updating vehicle information: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e

@step(step_id="122", name="Process - Enter Vehilce Details", operation='process')
def process_vehicle_details(db, case_no, step_data):
    """
    Process the vehicle delivery details for the new vehicle step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}

        vehicle = vehicle_service.get_vehicles(
            db, vehicle_id=int(case_entity.identifier_value)
        )

        if not vehicle:
            raise ValueError("Vehicle not found")
        
        is_vehicle = vehicle_service.get_vehicles(db=db , vin=step_data.get("vin"))

        if is_vehicle and is_vehicle.id != vehicle.id:
            raise ValueError("Vehicle with this VIN already exists")
        
        total_price = step_data.get("base_price",0) + step_data.get("sales_tax" , 0)
        true_cost = total_price + step_data.get("vehicle_hack_up_cost" , 0)
        vehicle_hack_up_cost = step_data.get("vehicle_hack_up_cost" , 0)
        life_cap = true_cost if true_cost < settings.tlc_vehicle_cap_total else settings.tlc_vehicle_cap_total

        is_invo_number = vehicle_service.get_vehicles(db=db , invoice_number=step_data.get("invoice_number"))

        if is_invo_number and is_invo_number.id != vehicle.id:
            raise ValueError("Vehicle with this invoice number already exists")
        
        step_data["vehicle_true_cost"] = true_cost
        step_data["vehicle_lifetime_cap"] = life_cap
        step_data["vehicle_hack_up_cost"] = vehicle_hack_up_cost

        vehicle_data = vehicle_service.upsert_vehicle(db=db , vehicle_data={
            "id": vehicle.id,
            **step_data
        })


        vehicle_invoice = upload_service.get_documents(db=db , object_type="vehicle",
                                            object_id=vehicle.id,document_type="vehicle_invoice")
        
        vehicle_expense = vehicle_service.get_vehicle_expenses(
            db=db , vehicle_id=vehicle.id , category=ExpensesAndComplianceCategory.VEHICLE_PURCHASE,
            sub_type=ExpensesAndComplianceSubType.INVOICE
        )

        vehicle_service.upsert_vehicle_expenses(
            db=db , vehicle_expenses={
                "id": vehicle_expense.id if vehicle_expense else None,
                "vehicle_id": vehicle.id ,
                "category": ExpensesAndComplianceCategory.VEHICLE_PURCHASE,
                "sub_type": ExpensesAndComplianceSubType.INVOICE,
                "amount": total_price,
                "issue_date": step_data.get("invoice_date" , None),
                "invoice_number": step_data.get("invoice_number" , None),
                "vendor_name": vehicle.dealer.dealer_name if vehicle.dealer else None,
                "document_id": vehicle_invoice.get("document_id" , None)
            }
        )

        if not vehicle_data:
            raise ValueError("Error updating vehicle")
        
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Created vehicle record for VIN {vehicle.vin} (Owner: {vehicle.vehicle_entity.entity_name if vehicle.vehicle_entity and vehicle.vehicle_entity.entity_name else 'Unknown'})",
                meta_data={"vehicle_id": vehicle.id , "vehicle_owner_id": vehicle.vehicle_entity.id if vehicle.vehicle_entity else None}
            )
        
        return "Ok"
    except Exception as e:
        logger.error("Error processing vehicle delivery details: %s", e)
        raise e

@step(step_id="122", name="fetch - Vehilce Details", operation='fetch')
def fetch_vehicle_details(db, case_no, step_data):
    """
    Fetch the vehicle delivery details for the new vehicle step
    """
    try:
        logger.info("Fetch vehicle delivery details")
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}

        vehicle = vehicle_service.get_vehicles(
            db, vehicle_id=int(case_entity.identifier_value)
        )
        if not vehicle:
            return {}
        
        vehicle_details = format_vehicle_details(vehicle)
        vehicle_invoice = upload_service.get_documents(db=db , object_type="vehicle",
                                            object_id=vehicle.id,document_type="vehicle_invoice")
        vehicle_details["invoice_document"] = vehicle_invoice

        return vehicle_details
    except Exception as e:
        logger.error("Error fetching vehicle delivery details: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e
    
@step(step_id="123", name="Process - vehicle delivery details", operation='process')
def process_vehicle_delivery_details(db, case_no, step_data):
    """
    Process the vehicle delivery details for the new vehicle step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}

        vehicle = vehicle_service.get_vehicles(
            db, vehicle_id=int(case_entity.identifier_value)
        )

        if not vehicle:
            raise ValueError("Vehicle not found")
        
        vehicle_data = vehicle_service.upsert_vehicle(db=db , vehicle_data={
            "id": vehicle.id,
            "vehicle_status": VehicleStatus.PENDING_DELIVERY,
            **step_data
        })

        if not vehicle_data:
            raise ValueError("Error updating vehicle")
        
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            metadata = {"vehicle_id": vehicle_data.id}
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Marked vehicle {vehicle_data.vin} as {VehicleStatus.PENDING_DELIVERY.value} with delivery details",
                meta_data=metadata
            )

        return "Ok"
    except Exception as e:
        logger.error("Error processing vehicle delivery details: %s", e)
        raise e

@step(step_id="123", name="Fetch - vehicle delivery details", operation='fetch')
def fetch_vehicle_delivery_details(db, case_no, step_data):
    """
    Fetch the vehicle delivery details for the new vehicle step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}

        vehicle = vehicle_service.get_vehicles(
            db, vehicle_id=int(case_entity.identifier_value)
        )

        if not vehicle:
            return {}
        
        return {
            "vehicle_details": format_vehicle_details(vehicle),
            "delivery_details": {
                "expected_delivery_date": vehicle.expected_delivery_date,
                "delivery_location": vehicle.delivery_location,
                "delivery_note": vehicle.delivery_note
            }
        }
    except Exception as e:
        logger.error("Error fetching vehicle documents: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e