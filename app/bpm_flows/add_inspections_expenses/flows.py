from pydantic import ValidationError

from app.utils.logger import get_logger
from app.bpm.step_info import step
from app.bpm.services import bpm_service
from app.audit_trail.services import audit_trail_service
from app.uploads.services import upload_service
from app.vehicles.services import vehicle_service
from app.bpm_flows.allocate_medallion_vehicle.utils import format_vehicle_details
from app.vehicles.utils import format_inspection_expense
from app.vehicles.schemas import VehicleInspectionsComplianceExpenseSchema , ExpensesAndComplianceCategory , ExpensesAndComplianceSubType , VehicleStatus

logger = get_logger(__name__)

enity_mapper = {
   "VEHICLE": "vehicles_expenses",
   "VEHICLE_IDENTIFIER": "id",
}

@step(step_id="217" , name="Fetch - Vehicle Inspection Expenses", operation='fetch')
def fetch_vehicle_inspection_expenses_and_compliance(db, case_no, case_params=None):
    try:
        logger.info("Fetch vehicle inspection expenses and compliance")
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
        
        if not case_entity:
            expense = vehicle_service.upsert_vehicle_expenses(
                db=db , vehicle_expenses={
                    "vehicle_id": vehicle.id,
                    "category": ExpensesAndComplianceCategory.INSPECTIONS_AND_COMPLIANCE.value,
                    "amount": 0.0
                }
            )

        vehicle_details = format_vehicle_details(vehicle)
        expense_details = format_inspection_expense(expense)

        invoice = upload_service.get_documents(
            db=db , object_type="vehicle_expenses" , object_id=expense.id , document_type="certificate"
        )

        additional_docs = upload_service.get_documents(
            db=db , object_type="vehicle_expenses" , object_id=expense.id , document_type="additional_document"
        )


        expense_details["certificate"] = invoice
        expense_details["additional_documents"] = additional_docs

        medallion = vehicle.medallions if vehicle else None

        vehicle_details["medallion_number"] = medallion.medallion_number if medallion else None

        if not case_entity:
            case_entity = bpm_service.create_case_entity(
                db=db,
                case_no=case_no,
                entity_name=enity_mapper["VEHICLE"],
                identifier=enity_mapper["VEHICLE_IDENTIFIER"],
                identifier_value=str(expense.id)
            )

        expected_expenses_and_compliance_values = {
            ExpensesAndComplianceCategory.INSPECTIONS_AND_COMPLIANCE.value: [
                ExpensesAndComplianceSubType.TLC_INSPECTION.value,
                ExpensesAndComplianceSubType.MILE_RUN_INSPECTION.value,
                ExpensesAndComplianceSubType.DMV_INSPECTION.value,
                ExpensesAndComplianceSubType.Liability_Insurance.value,
                ExpensesAndComplianceSubType.Worker_Compensation_Insurance.value,
                ExpensesAndComplianceSubType.OTHERS.value
            ]
        }


        return {
            "vehicle_details": vehicle_details,
            "expense_details": expense_details,
            "expenses_types": expected_expenses_and_compliance_values
        }
    except Exception as e:
        logger.error("Error fetching vehicle inspection expenses and compliance: %s", str(e))
        raise e
    
@step(step_id="217" , name="process - Vehicle Inspection Expenses and Compliance", operation="process")
def process_vehicle_inspection_expenses_and_compliance(db, case_no, step_data):
    """
    Process the vehicle inspection expenses and compliance for the new vehicle step
    """
    try:
        logger.info("Process vehicle inspection expenses and compliance")

        expense = None

        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if not case_entity:
            return {}
        
        expense = vehicle_service.get_vehicle_expenses(
            db=db , lookup_id=int(case_entity.identifier_value)
        )

        if not expense:
            raise ValueError("Vehicle expense not found")
        
        expn_data = {**step_data}
        expn_data["id"] = expense.id
        expn_data["vehicle_id"] = expense.vehicle_id

        try:
            expense_data = VehicleInspectionsComplianceExpenseSchema(**expn_data)
        except ValidationError as ve:
            logger.error("Validation failed for case %s: %s", case_no, ve.errors())
            raise ValueError(f"Invalid data: {ve.errors()}")

        expense =vehicle_service.upsert_vehicle_expenses(
            db=db, vehicle_expenses=expense_data.dict()
        )

        allowed_inspection_types = [
            ExpensesAndComplianceSubType.TLC_INSPECTION.value,
            ExpensesAndComplianceSubType.MILE_RUN_INSPECTION.value,
            ExpensesAndComplianceSubType.DMV_INSPECTION.value
        ]

        allowed_insurance_types = [
            ExpensesAndComplianceSubType.Liability_Insurance.value,
            ExpensesAndComplianceSubType.Worker_Compensation_Insurance.value,
            ]

        if step_data.get("sub_type") in allowed_inspection_types:
            inspection_data = {
                "vehicle_id": expense.vehicle_id,
                "inspection_type": step_data.get("sub_type"),
                "inspection_date": step_data.get("issue_date"),
                "next_inspection_due_date": step_data.get("expiry_date"),
                "inspection_fee": step_data.get("amount"),
                "status": "completed"
            }

            if expense.sub_type == "mile run inspection":
                inspection_data["mile_run"] = True

            vehicle_service.upsert_inspection(
                db=db, inspection_data=inspection_data
            )

        elif expense.sub_type in allowed_insurance_types:
        
            insurance_data = {
                "vehicle_id": expense.vehicle_id,
                "insurance_type": step_data.get("sub_type"),
                "insurance_start_date": step_data.get("issue_date"),
                "insurance_end_date": step_data.get("expiry_date"),
                "amount": step_data.get("amount"),
                "status": "Active"
            }

            vehicle_service.upsert_vehicle_insurance(
                db=db, vehicle_insurance=insurance_data
            )

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f'Vehicle Inspctions expenses and compliance processed for vehicle {expense.vehicle.vin if expense.vehicle else "N/A"}',
                meta_data={"vehicle_id":expense.vehicle_id}
            )

        return "Ok"
    except Exception as e:
        logger.error("Error processing vehicle inspection expenses and compliance: %s", str(e))
        raise e