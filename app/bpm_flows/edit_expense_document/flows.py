from pydantic import ValidationError

from app.utils.logger import get_logger
from app.bpm.step_info import step
from app.bpm.services import bpm_service
from app.audit_trail.services import audit_trail_service
from app.uploads.services import upload_service
from app.vehicles.services import vehicle_service
from app.bpm_flows.allocate_medallion_vehicle.utils import format_vehicle_details
from app.vehicles.utils import format_vehicle_other_document
from app.vehicles.schemas import VehicleOtherDocumentsExpenseSchema , ExpensesAndComplianceCategory , ExpensesAndComplianceSubType , VehicleStatus

logger = get_logger(__name__)

enity_mapper = {
   "VEHICLE": "vehicles_expenses",
   "VEHICLE_IDENTIFIER": "id",
}

@step(step_id="221" , name="Fetch - Vehicle Expenses other documents", operation='fetch')
def fetch_vehicle_expense_other_document(db, case_no, case_params=None):
    try:
        logger.info("Fetch vehicle expenses other document for case_no: %s", case_no)
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)

        expense = None 
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        if case_entity:
            expense = vehicle_service.get_vehicle_expenses(
                db=db , lookup_id=int(case_entity.identifier_value)
            )
        if not expense and case_params and case_params.get("object_name") == "vehicle_expense":
            expense = vehicle_service.get_vehicle_expenses(
                db=db , lookup_id=int(case_params['object_lookup'])
            
            )         

        if not expense:
            return {}
        
        vehicle = vehicle_service.get_vehicles(
            db=db , vehicle_id= expense.vehicle_id
        )
        if not vehicle:
            return {}
        
        if vehicle.vehicle_status in [VehicleStatus.PENDING_DELIVERY , VehicleStatus.IN_PROGRESS , VehicleStatus.ARCHIVED]:
            raise ValueError("Vehicle is not in a valid status for Edit purchase expenses.")
        
        vehicle_details = format_vehicle_details(vehicle)
        expense_details = format_vehicle_other_document(expense)

        invoice = upload_service.get_documents(
            db=db , object_type="vehicle_expenses" , object_id=expense.id , document_type="invoice"
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
            ExpensesAndComplianceCategory.OTHER_VHICLE_DOCUMENTS.value: [
                ExpensesAndComplianceSubType.ACCIDENT_INCIDENT_REPORT.value,
                ExpensesAndComplianceSubType.INSURANCE_RIDER_ENDORSEMENT.value,
                ExpensesAndComplianceSubType.LOAN_FINANCE_DOCUMENT.value,
                ExpensesAndComplianceSubType.VEHICLE_PHOTO_DAMAGE_DOCUMENTION.value,
                ExpensesAndComplianceSubType.WARRANTY_DOCUMENT.value,
                ExpensesAndComplianceSubType.OTHERS.value
            ]
        }

        return {
            "vehicle_details": vehicle_details,
            "expense_details": expense_details,
            "expenses_types": expected_expenses_and_compliance_values
        }
    except Exception as e:
        logger.error("Error fetching vehicle document expenses: %s", str(e))
        raise e
    
@step(step_id="221" , name="process -Edit Vehicle expenses documents", operation="process")
def process_vehicle_expenses_documents(db, case_no, step_data):
    """
    Process the vehicle expenses docuements.
    """
    try:
        logger.info("Process edit vehicle expenses document for case_no: %s", case_no)

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
            db=db , object_type="vehicle_expenses" , object_id=expense.id , document_type="invoice"
        )
        expn_data = {**step_data}
        expn_data["id"] = expense.id
        expn_data["vehicle_id"] = expense.vehicle_id

        if invoice and invoice.get("document_path" , None):
            expn_data["document_id"] = invoice.get("id")

        try:
            expense_data = VehicleOtherDocumentsExpenseSchema(**expn_data)
        except ValidationError as ve:
            logger.error("Validation failed for case %s: %s", case_no, ve.errors())
            raise ValueError(f"Invalid data: {ve.errors()}")

        vehicle_service.upsert_vehicle_expenses(
            db=db, vehicle_expenses=expense_data.dict()
        )

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Edit Vehicle Expense Document for vehicle vin:{expense.vehicle.vin if expense.vehicle else 'N/A'}",
                meta_data={"vehicle_id":expense.vehicle_id}
            )

        return "Ok"
    except Exception as e:
        logger.error("Error processing edit vehicle expenses document: %s", str(e))
        raise e