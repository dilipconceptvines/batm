from pydantic import ValidationError

from app.utils.logger import get_logger
from app.bpm.step_info import step
from app.bpm.services import bpm_service
from app.audit_trail.services import audit_trail_service
from app.uploads.services import upload_service
from app.vehicles.services import vehicle_service
from app.bpm_flows.allocate_medallion_vehicle.utils import format_vehicle_details
from app.vehicles.utils import format_hackup_expense_expense
from app.vehicles.schemas import VehicleHackupExpenseSchema , ExpensesAndComplianceCategory , ExpensesAndComplianceSubType , VehicleStatus
from app.vehicles.schemas import ProcessStatusEnum

logger = get_logger(__name__)

enity_mapper = {
   "VEHICLE": "vehicles_expenses",
   "VEHICLE_IDENTIFIER": "id",
}

@step(step_id="213" , name="Fetch - Vehicle Hackup Expenses", operation='fetch')
def fetch_vehicle_hackup_expenses(db, case_no, case_params=None):
    try:
        logger.info("Fetch vehicle hackup expenses")
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        vehicle = None
        expense = None

        if case_params and case_params.get("object_name") == "vehicle":
            vehicle = vehicle_service.get_vehicles(
                db=db , vehicle_id=int(case_params['object_lookup'])
            )
            
        if case_entity:
            expense = vehicle_service.get_vehicle_expenses(
                db=db , lookup_id=int(case_entity.identifier_value)
            )
            vehicle = vehicle_service.get_vehicles(
                db=db , vehicle_id= expense.vehicle_id
            )
        

        if not vehicle:
            return {}
        
        if vehicle.vehicle_status in [VehicleStatus.PENDING_DELIVERY , VehicleStatus.IN_PROGRESS , VehicleStatus.ARCHIVED]:
            raise ValueError("Vehicle is not in a valid status for adding purchase expenses.")
        
        if not expense:
            expense = vehicle_service.upsert_vehicle_expenses(
                db=db , vehicle_expenses={
                    "vehicle_id": vehicle.id,
                    "category": ExpensesAndComplianceCategory.VEHICLE_HACKUP.value,
                    "amount": 0.0
                }
            )

        vehicle_details = format_vehicle_details(vehicle)
        expense_details = format_hackup_expense_expense(expense)

        invoice = upload_service.get_documents(
            db=db , object_type="vehicle_expenses" , object_id=expense.id , document_type="hackup_invoice"
        )

        expense_details["invoice"] = invoice

        if not case_entity:
            case_entity = bpm_service.create_case_entity(
                db=db,
                case_no=case_no,
                entity_name=enity_mapper["VEHICLE"],
                identifier=enity_mapper["VEHICLE_IDENTIFIER"],
                identifier_value=str(expense.id)
            )

        expected_expenses_and_compliance_values = {
            ExpensesAndComplianceCategory.VEHICLE_HACKUP.value: [
                ExpensesAndComplianceSubType.PAINT.value,
                ExpensesAndComplianceSubType.METER.value,
                ExpensesAndComplianceSubType.ROOFTOP.value,
                ExpensesAndComplianceSubType.CAMERA.value,
                ExpensesAndComplianceSubType.PARTITION.value
            ]
        }


        return {
            "vehicle_details": vehicle_details,
            "expense_details": expense_details,
            "expenses_types": expected_expenses_and_compliance_values
        }
    except Exception as e:
        logger.error("Error fetching vehicle hackup expenses: %s", str(e))
        raise e
    
@step(step_id="213" , name="process - Vehicle Hackup Expenses", operation="process")
def process_vehicle_hackup_expenses(db, case_no, step_data):
    """
    Process the vehicle expenses hackup data.
    """
    try:
        logger.info("Process vehicle hackup expenses")

        expense = None

        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}
        
        expense = vehicle_service.get_vehicle_expenses(
            db=db , lookup_id=int(case_entity.identifier_value)
        )

        vehicle = vehicle_service.get_vehicles(
            db=db , vehicle_id= expense.vehicle_id
        )
        
        if not vehicle:
            raise ValueError("Vehicle not found")
        
        if not expense:
            raise ValueError("Vehicle expense not found")
        
        invoice = upload_service.get_documents(
            db=db , object_type="vehicle_expenses" , object_id=expense.id , document_type="hackup_invoice"
        )
        expn_data = {**step_data}
        expn_data["id"] = expense.id
        expn_data["vehicle_id"] = expense.vehicle_id

        if invoice and invoice.get("document_path" , None):
            expn_data["document_id"] = invoice.get("id")

        try:
            expense_data = VehicleHackupExpenseSchema(**expn_data)
        except ValidationError as ve:
            logger.error("Validation failed for case %s: %s", case_no, ve.errors())
            raise ValueError(f"Invalid data: {ve.errors()}")

        vehicle_service.upsert_vehicle_expenses(
            db=db, vehicle_expenses=expense_data.dict()
        )

        hackup = vehicle_service.get_vehicle_hackup(db=db , vehicle_id=vehicle.id)
        if not hackup:
            hackup = vehicle_service.upsert_vehicle_hackup(
                db=db,
                vehicle_id=vehicle.id,
                status="pending"
            )
        sub_type = step_data.get("sub_type")
        task_id = getattr(hackup, f"{sub_type}_task_id", None)
        task_data = vehicle_service.upsert_hackup_tasks(
            db=db, hackup_tasks={
                "id": task_id if task_id else None,
                "task_name": sub_type,
                "completed_date": step_data.get("issue_date" , None),
                "status": ProcessStatusEnum.completed,
                "amount": step_data.get("amount" , 0.0),
                "note": step_data.get("note" , None),
                "is_task_done": True
            }
        )

        meter_serial_number = step_data.get("meter_serial_no" , None)

        data = {
            "id": hackup.id,
            f"{sub_type}_task_id": task_data.id,
        }

        if sub_type == "meter" and meter_serial_number:
            data["meter_serial_number"] = meter_serial_number

        vehicle_service.upsert_vehicle_hackup(
            db=db , vehicle_hackup_data=data
        )

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f'Vehicle Hackup expenses for vehicle {expense.vehicle.vin if expense.vehicle else "N/A"}',
                meta_data={"vehicle_id":expense.vehicle_id}
            )

        return "Ok"
    except Exception as e:
        logger.error("Error processing vehicle hackup expenses: %s", str(e))
        raise e