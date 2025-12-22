### app/bpm_flows/new_repair/flows.py

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.audit_trail.schemas import AuditTrailType
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.bpm.step_info import step
from app.drivers.services import driver_service
from app.leases.services import lease_service
from app.uploads.services import upload_service
from app.repairs.models import WorkshopType
from app.repairs.services import RepairService
from app.utils.logger import get_logger

logger = get_logger(__name__)

ENTITY_MAPPER = {
    "REPAIR": "repair_invoice",
    "REPAIR_IDENTIFIER": "id",
}

# Repair Payment Matrix (same as in RepairService)
REPAYMENT_MATRIX = [
    {"min": 0, "max": 200, "installment": "full"},
    {"min": 201, "max": 500, "installment": 100},
    {"min": 501, "max": 1000, "installment": 200},
    {"min": 1001, "max": 3000, "installment": 250},
    {"min": 3001, "max": float('inf'), "installment": 300},
]


def _get_next_sunday(from_date: datetime) -> datetime:
    """Helper function to get the next Sunday from a given date."""
    days_until_sunday = (6 - from_date.weekday()) % 7
    if days_until_sunday == 0:  # If today is Sunday
        days_until_sunday = 7
    return from_date + timedelta(days=days_until_sunday)


def _get_weekly_installment(total_amount: Decimal) -> Decimal:
    """Determines the weekly installment amount based on the repayment matrix."""
    for rule in REPAYMENT_MATRIX:
        if rule["min"] <= total_amount <= rule["max"]:
            if rule["installment"] == "full":
                return total_amount
            return Decimal(str(rule["installment"]))
    return Decimal("300")  # Default for amounts over the max


def _generate_payment_schedule_preview(
    total_amount: Decimal,
    start_week: datetime
) -> List[Dict[str, Any]]:
    """
    Generates a preview of the payment schedule without creating database records.
    This is used for the confirmation modal.
    """
    weekly_installment = _get_weekly_installment(total_amount)
    remaining_balance = total_amount
    schedule = []
    installment_counter = 1
    
    current_week_start = start_week
    
    while remaining_balance > 0:
        # Calculate this installment amount
        if remaining_balance <= weekly_installment:
            installment_amount = remaining_balance
        else:
            installment_amount = weekly_installment
        
        # Calculate week end date (Saturday)
        week_end = current_week_start + timedelta(days=6)
        
        # Calculate prior balance and new balance
        prior_balance = total_amount - sum(inst["installment"] for inst in schedule)
        new_balance = prior_balance - installment_amount
        
        schedule.append({
            "installment_id": f"RPR-PREVIEW-{str(installment_counter).zfill(4)}",
            "week_period": f"{current_week_start.strftime('%m/%d/%Y')}-{week_end.strftime('%m/%d/%Y')}",
            "installment": float(installment_amount),
            "prior_balance": float(prior_balance),
            "balance": float(new_balance),
            "status": "Scheduled"
        })
        
        remaining_balance -= installment_amount
        current_week_start += timedelta(days=7)
        installment_counter += 1
    
    return schedule


# @step(step_id="307", name="Fetch - Search Driver & Enter Invoice Details", operation="fetch")
@step(step_id="129", name="Fetch - Search Driver & Enter Invoice Details", operation="fetch")
def search_driver_and_invoice_fetch(db: Session, case_no: str, case_params: Dict[str, Any] = None):
    """
    Fetches driver and lease information based on TLC License search.
    
    Query Parameters:
        - tlc_license_no: TLC License number (e.g., "00504124")
    """
    try:
        logger.info("Fetching driver details for Repair Invoice case", case_no=case_no)
        
        if not case_params or not case_params.get("tlc_license_no"):
            return {
                "driver": None,
                "leases": []
            }
        
        tlc_license_no = case_params.get("tlc_license_no")
        
        # Search by TLC License Number
        driver = driver_service.get_drivers(db, tlc_license_number=tlc_license_no)
        
        if not driver:
            logger.info("No driver found for Repair Invoice case", case_no=case_no, tlc_license_no=tlc_license_no)
            return {
                "driver": None,
                "leases": []
            }
        
        # Fetch active leases for the driver
        active_leases = lease_service.get_lease(
            db, 
            driver_id=driver.driver_id, 
            status="Active", 
            exclude_additional_drivers=True, 
            multiple=True
        )
        
        # Handle the lease data structure
        if active_leases and active_leases[0]:
            if isinstance(active_leases[0], list):
                lease_list = active_leases[0]
            else:
                lease_list = [active_leases[0]]
        else:
            lease_list = []
        
        if not lease_list:
            logger.warning("No active leases found for driver", driver_id=driver.id)
            return {
                "driver": {
                    "id": driver.id,
                    "driver_id": driver.driver_id,
                    "full_name": driver.full_name,
                    "status": driver.driver_status.value if hasattr(driver.driver_status, 'value') else str(driver.driver_status),
                    "tlc_license": driver.tlc_license.tlc_license_number if driver.tlc_license else "N/A",
                    "phone": driver.phone_number_1 or "N/A",
                    "email": driver.email_address or "N/A",
                },
                "leases": []
            }
        
        # Format lease data for UI
        formatted_leases = []
        for lease in lease_list:
            formatted_leases.append({
                "id": lease.id,
                "lease_id": lease.lease_id,
                "medallion_number": lease.medallion.medallion_number if lease.medallion else "N/A",
                "medallion_id": lease.medallion_id if lease.medallion_id else None,
                "plate_no": lease.vehicle.registrations[0].plate_number if lease.vehicle and lease.vehicle.registrations else "N/A",
                "vin": lease.vehicle.vin if lease.vehicle else "N/A",
                "vehicle_id": lease.vehicle_id if lease.vehicle_id else None,
                "lease_type": lease.lease_type if lease.lease_type else "N/A",
                "weekly_lease": f"${lease.lease_weekly_amount:.2f}/week" if hasattr(lease, 'lease_weekly_amount') else "$1,200.00/week",
            })
        
        driver_data = {
            "id": driver.id,
            "driver_id": driver.driver_id,
            "full_name": driver.full_name,
            "status": driver.driver_status.value if hasattr(driver.driver_status, 'value') else str(driver.driver_status),
            "tlc_license": driver.tlc_license.tlc_license_number if driver.tlc_license else "N/A",
            "phone": driver.phone_number_1 or "N/A",
            "email": driver.email_address or "N/A",
        }

        repair_invoice = upload_service.get_documents(
            db=db , object_type="repair", object_id=case_no , document_type="repair_invoice"
        )
        
        logger.info("Successfully fetched driver and lease details for Repair", case_no=case_no, driver_id=driver.id)
        
        return {
            "driver": driver_data,
            "leases": formatted_leases,
            "repair_invoice": repair_invoice
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error fetching driver and lease details for Repair", case_no=case_no, error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while fetching driver details") from e


# @step(step_id="307", name="Process - Create Repair Invoice", operation="process")
@step(step_id="129", name="Process - Create Repair Invoice", operation="process")
def create_repair_invoice_process(db: Session, case_no: str, step_data: Dict[str, Any]):
    """
    Creates a repair invoice with automatic payment schedule generation.
    
    This step handles the entire repair invoice creation in one transaction:
    1. Validates all input data
    2. Generates payment schedule preview (if preview requested)
    3. Creates repair invoice and installments
    4. Links to ledger
    5. Creates audit trail
    
    Expected step_data:
        - driver_id: Driver primary key
        - lease_id: Lease primary key
        - vehicle_id: Vehicle primary key
        - medallion_id: Medallion primary key
        - total_amount: Total repair cost
        - invoice_number: Invoice number from vendor
        - invoice_date: Date of invoice (ISO format)
        - workshop_type: "BIG_APPLE_WORKSHOP" or "EXTERNAL_WORKSHOP"
        - start_week: Start date for repayment (ISO format, must be Sunday)
        - notes: Optional repair description
        - preview_only: Boolean flag to generate schedule preview without creating invoice
    """
    try:
        logger.info("Processing repair invoice creation", case_no=case_no)
        
        # Validate required fields
        required_fields = [
            "driver_id", "lease_id", "vehicle_id", "medallion_id",
            "total_amount", "invoice_number", "invoice_date", 
            "workshop_type", "start_week"
        ]
        missing_fields = [field for field in required_fields if not step_data.get(field)]
        if missing_fields:
            raise HTTPException(
                status_code=400, 
                detail=f"Missing required fields: {', '.join(missing_fields)}"
            )
        
        # Validate and parse total amount
        try:
            total_amount = Decimal(str(step_data["total_amount"]))
            if total_amount <= 0:
                raise ValueError("Amount must be positive")
        except (ValueError, TypeError) as e:
            raise HTTPException(status_code=400, detail=f"Invalid total_amount: {e}") from e
        
        # Parse and validate start_week
        try:
            start_week = datetime.fromisoformat(step_data["start_week"].replace('Z', '+00:00'))
            # Ensure start_week is a Sunday
            if start_week.weekday() != 6:  # 6 = Sunday
                raise ValueError("Start week must be a Sunday")
        except (ValueError, TypeError) as e:
            raise HTTPException(status_code=400, detail=f"Invalid start_week: {e}") from e
        
        # Validate workshop type
        try:
            workshop_type = WorkshopType(step_data["workshop_type"])
        except ValueError as e:
            raise HTTPException(
                status_code=400, 
                detail="Invalid workshop_type. Must be 'BIG_APPLE_WORKSHOP' or 'EXTERNAL_WORKSHOP'"
            ) from e
        
        # Check if this is a preview request
        if step_data.get("preview_only"):
            logger.info("Generating payment schedule preview", case_no=case_no)
            
            schedule = _generate_payment_schedule_preview(total_amount, start_week)
            
            return {
                "preview": True,
                "repair_amount": float(total_amount),
                "invoice_number": step_data["invoice_number"],
                "invoice_date": step_data["invoice_date"],
                "start_week": start_week.strftime("%m/%d/%Y"),
                "payment_schedule": schedule
            }
        
        # Validate driver existence
        driver = driver_service.get_drivers(db, id=step_data["driver_id"])
        if not driver:
            raise HTTPException(status_code=404, detail="Driver not found")
        
        # Validate lease existence
        lease = lease_service.get_lease(db, lookup_id=step_data["lease_id"], status="Active")
        if not lease:
            raise HTTPException(status_code=404, detail="Active lease not found")
        
        # Verify driver is the primary driver on the lease
        is_primary_driver = False
        for lease_driver in lease.lease_driver:
            if lease_driver.driver_id == driver.driver_id and not lease_driver.is_additional_driver:
                is_primary_driver = True
                break
        
        if not is_primary_driver:
            raise HTTPException(
                status_code=400, 
                detail="Driver is not the primary driver on the selected lease"
            )
        
        # Check for duplicate invoice number
        from app.repairs.models import RepairInvoice
        
        existing_invoice = db.query(RepairInvoice).filter(
            RepairInvoice.invoice_number == step_data["invoice_number"]
        ).first()
        
        if existing_invoice:
            raise HTTPException(
                status_code=400,
                detail=f"Invoice number {step_data['invoice_number']} already exists"
            )
        
        # Initialize repair service
        repair_service = RepairService(db)
        
        # Prepare invoice data
        invoice_data = {
            "driver_id": driver.id,
            "lease_id": lease.id,
            "vehicle_id": step_data["vehicle_id"],
            "medallion_id": step_data["medallion_id"],
            "total_amount": str(total_amount),
            "invoice_number": step_data["invoice_number"],
            "invoice_date": step_data["invoice_date"],
            "workshop_type": workshop_type.value,
            "start_week": start_week,
            "notes": step_data.get("notes", ""),
        }
        
        # Create repair invoice (this also generates payment schedule)
        repair_invoice = repair_service.create_repair_invoice(
            case_no=case_no,
            invoice_data=invoice_data,
            user_id=1  # TODO: Get actual user ID from context
        )


        repair_invoice_doc = upload_service.get_documents(
            db=db , object_type="repair", object_id=case_no , document_type="repair_invoice"
        )

        if repair_invoice_doc and repair_invoice_doc.get("document_path"):
            upload_service.update_document(
                db=db,document_dict={"document_id": repair_invoice_doc["document_id"]},
                object_id=repair_invoice.id,
                document_path=repair_invoice_doc.get("document_path"),
                object_type="repair",
                document_type="repair_invoice",
                new_filename=repair_invoice_doc.get("document_name"),
                file_size_kb=repair_invoice_doc.get("document_size"),
                original_extension=repair_invoice_doc.get("document_format"),
                document_date=datetime.now().strftime('%Y-%m-%d'),
                notes=repair_invoice_doc.get("document_notes")
            )
        receipt_s3_key, receipt_url = repair_service._generate_and_store_receipt(repair_invoice)
        
        # Create audit trail
        case = bpm_service.get_cases(db, case_no=case_no)
        audit_trail_service.create_audit_trail(
            db,
            description=f"Repair invoice created: {repair_invoice.repair_id}",
            case=case,
            meta_data={
                "repair_id": repair_invoice.repair_id,
                "invoice_number": repair_invoice.invoice_number,
                "total_amount": float(repair_invoice.total_amount),
                "driver_id": repair_invoice.driver_id,
                "lease_id": repair_invoice.lease_id,
                "workshop_type": repair_invoice.workshop_type.value,
                "installments_count": len(repair_invoice.installments),
            },
            audit_type=AuditTrailType.AUTOMATED,
        )
        
        logger.info(
            "Repair invoice created successfully", 
            repair_id=repair_invoice.repair_id,
            case_no=case_no,
            installments=len(repair_invoice.installments)
        )
        
        return {
            "message": "Repair invoice created successfully. Payment schedule generated.",
            "repair_id": repair_invoice.repair_id,
            "invoice_number": repair_invoice.invoice_number,
            "total_amount": float(repair_invoice.total_amount),
            "installments_count": len(repair_invoice.installments),
            "receipt_url": receipt_url
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error creating repair invoice", case_no=case_no, error=str(e), exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail=f"An error occurred while creating repair invoice: {str(e)}"
        ) from e