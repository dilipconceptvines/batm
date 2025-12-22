from pydantic import ValidationError

from app.utils.logger import get_logger
from app.bpm.step_info import step
from app.bpm.services import bpm_service
from app.audit_trail.services import audit_trail_service
from app.uploads.services import upload_service
from app.vehicles.services import vehicle_service
from app.bpm_flows.allocate_medallion_vehicle.utils import format_vehicle_details
from app.vehicles.utils import format_vehicle_purchase_expense
from app.vehicles.schemas import VehiclePurchaseExpenseSchema , ExpensesAndComplianceCategory , ExpensesAndComplianceSubType , VehicleStatus
from app.core.config import settings

logger = get_logger(__name__)

enity_mapper = {
   "VEHICLE": "vehicles_expenses",
   "VEHICLE_IDENTIFIER": "id",
}

@step(step_id="202" , name="Fetch - Vehicle Purchase Expenses", operation='fetch')
def fetch_vehicle_purchase_expenses(db, case_no, case_params=None):
    try:
        logger.info("Fetch vehicle purchase expenses for case_no: %s", case_no)
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
                    "category": ExpensesAndComplianceCategory.VEHICLE_PURCHASE.value,
                    "sub_type": ExpensesAndComplianceSubType.INVOICE.value,
                    "amount": 0.0
                }
            )

        vehicle_details = format_vehicle_details(vehicle)
        expense_details = format_vehicle_purchase_expense(expense)

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
            ExpensesAndComplianceCategory.VEHICLE_PURCHASE.value: [
                ExpensesAndComplianceSubType.INVOICE.value
            ]
        }


        return {
            "vehicle_details": vehicle_details,
            "expense_details": expense_details,
            "expenses_types": expected_expenses_and_compliance_values
        }
    except Exception as e:
        logger.error("Error fetching vehicle purchase expenses: %s", str(e))
        raise e
    
@step(step_id="202" , name="process - Vehicle purchase expenses", operation="process")
def process_vehicle_purchase_expenses(db, case_no, step_data):
    """
    Process the vehicle purchase expenses and compliance data.
    """
    try:
        logger.info("Process vehicle purchase expenses for case_no: %s", case_no)

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
            expense_data = VehiclePurchaseExpenseSchema(**expn_data)
        except ValidationError as ve:
            logger.error("Validation failed for case %s: %s", case_no, ve.errors())
            raise ValueError(f"Invalid data: {ve.errors()}")

        vehicle_service.upsert_vehicle_expenses(
            db=db, vehicle_expenses=expense_data.dict()
        )

        total_price = step_data.get("base_price",0) + step_data.get("sales_tax" , 0)
        true_cost = total_price + vehicle.vehicle_hack_up_cost
        life_cap = true_cost if true_cost < settings.tlc_vehicle_cap_total else settings.tlc_vehicle_cap_total

        vehicel_data = {
                "id": expense.vehicle_id,
                "base_price": expn_data.get("base_price" , 0.0),
                "sales_tax": expn_data.get("sales_tax" , 0.0),
                "invoice_number": expn_data.get("invoice_number" , None),
                "invoice_date": expn_data.get("issue_date" , None),
                "vehicle_total_price": total_price,
                "vehicle_true_cost": true_cost,
                "vehicle_lifetime_cap": life_cap
            }
        
        if step_data.get("vendor_name" , None):
            delear = vehicle_service.get_dealer(
                db=db , dealer_name=step_data.get("vendor_name")
            )
            if delear:
                vehicel_data["dealer_id"] = delear.id


        vehicle_service.upsert_vehicle(
            db=db , vehicle_data=vehicel_data
        )

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Vehicle purchase expenses updated for vehicle vin:{expense.vehicle.vin if expense.vehicle else 'N/A'}",
                meta_data={"vehicle_id":expense.vehicle_id}
            )

        return "Ok"
    except Exception as e:
        logger.error("Error processing vehicle purchase expenses: %s", str(e))
        raise e