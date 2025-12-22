# app/bpm_flows/misc_expense/flows.py

from typing import Any, Dict

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.audit_trail.schemas import AuditTrailType
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.bpm.step_info import step
from app.drivers.services import driver_service
from app.leases.services import lease_service
from app.misc_expenses.schemas import MiscellaneousExpenseCreate
from app.misc_expenses.services import MiscellaneousExpenseService
from app.uploads.services import upload_service
from app.utils.logger import get_logger

logger = get_logger(__name__)

ENTITY_MAPPER = {
    "MISC_EXPENSE": "miscellaneous_expense",
    "MISC_EXPENSE_IDENTIFIER": "id",
}


@step(step_id="308", name="Fetch - Search Driver & Enter Expense Details", operation="fetch")
def search_driver_and_enter_expense_details_fetch(db: Session, case_no: str, case_params: Dict[str, Any] = None):
    """
    Fetches driver and associated active lease information to initiate a miscellaneous expense entry.

    This step allows searching for a driver by TLC License, Medallion Number, or VIN/Plate Number.
    It returns the driver's profile and all their currently active leases.

    Args:
        db: Database session
        case_no: BPM case number
        case_params: Query parameters containing search criteria
            - tlc_license_no: Driver's TLC License number
            - medallion_no: Medallion number associated with driver's lease
            - vin_or_plate: VIN or Plate number of the leased vehicle

    Returns:
        Dict containing driver information and list of active leases

    Raises:
        HTTPException: If driver not found or driver has no active leases
    """
    try:
        documents = upload_service.get_documents(db, object_type="misc_expense", object_id=case_no)
        if not documents:
            documents = {
                "document_id": "",
                "document_name": "",
                "document_note": "",
                "document_path": "",
                "document_type": "misc_expense",
                "document_date": "",
                "document_object_type": "misc_expense",
                "document_object_id": case_no,
                "document_size": "",
                "document_uploaded_date": "",
                "presigned_url": "",
            }

        if not case_params:
            return {"driver": None, "leases": [], "documents": documents}

        tlc_license_no = case_params.get("tlc_license_no")
        medallion_no = case_params.get("medallion_no")
        vin_or_plate = case_params.get("vin_or_plate")

        # Validate at least one search parameter is provided
        if not any([tlc_license_no, medallion_no, vin_or_plate]):
            return {"driver": None, "leases": [], "documents": documents}

        logger.info(
            "Searching for driver for Miscellaneous Expense",
            case_no=case_no,
            tlc_license=tlc_license_no,
            medallion=medallion_no,
            vin_or_plate=vin_or_plate
        )

        # Find the driver using provided search criteria
        driver = driver_service.get_drivers(
            db,
            tlc_license_number=tlc_license_no,
            medallion_number=medallion_no,
            vin=vin_or_plate,
        )

        if not driver:
            raise HTTPException(
                status_code=404,
                detail="No matching active driver found for the provided search criteria."
            )

        # Fetch all active leases for the driver
        leases_tuple = lease_service.get_lease(
            db,
            driver_id=driver.driver_id,
            status="Active",
            multiple=True
        )
        active_leases = leases_tuple[0] if leases_tuple else []

        if not active_leases:
            raise HTTPException(
                status_code=404,
                detail="Driver does not have an active lease. Cannot create miscellaneous expense."
            )

        # Format lease data for UI display
        formatted_leases = []
        for lease in active_leases:
            # Get the weekly lease amount from schedule (not config!)
            weekly_amount = None
            try:
                schedule_amount = lease_service.get_current_lease_amount(db, lease.id)
                if schedule_amount:
                    weekly_amount = f"${schedule_amount:.2f}/week"
                else:
                    # Only if no schedule exists, fall back to config
                    lease_config = lease_service.get_lease_configurations(
                        db,
                        lease_id=lease.id,
                        lease_breakup_type="lease_amount"
                    )
                    if lease_config:
                        weekly_amount = f"${float(lease_config.lease_limit):.2f}/week"
                    else:
                        weekly_amount = "Not Configured"

            except Exception as e:
                logger.error(
                    "Error fetching lease amount",
                    lease_id=lease.id,
                    error=str(e),
                    exc_info=True
                )
                weekly_amount = "Error Loading"


            formatted_leases.append({
                "lease_id_pk": lease.id,
                "lease_id": lease.lease_id,
                "vehicle_id": lease.vehicle.id,
                "medallion_no": lease.medallion.medallion_number if lease.medallion else "N/A",
                "medallion_id": lease.medallion.id if lease.medallion else "N/A",
                "plate_no": (
                    lease.vehicle.registrations[0].plate_number
                    if lease.vehicle and lease.vehicle.registrations
                    else "N/A"
                ),
                "vin": lease.vehicle.vin if lease.vehicle else "N/A",
                "vehicle_name": (
                    f"{lease.vehicle.year} {lease.vehicle.make} {lease.vehicle.model}"
                    if lease.vehicle
                    else "N/A"
                ),
                "lease_type": lease.lease_type,
                "lease_status": lease.lease_status,
                "weekly_lease": weekly_amount,
            })

        # Format driver data for UI display
        driver_data = {
            "id": driver.id,
            "driver_id": driver.driver_id,
            "full_name": driver.full_name,
            "status": (
                driver.driver_status.value
                if hasattr(driver.driver_status, 'value')
                else str(driver.driver_status)
            ),
            "tlc_license": (
                driver.tlc_license.tlc_license_number
                if driver.tlc_license
                else "N/A"
            ),
            "phone": driver.phone_number_1 or "N/A",
            "email": driver.email_address or "N/A",
        }

        logger.info(
            "Successfully fetched driver and lease details for Miscellaneous Expense",
            case_no=case_no,
            driver_id=driver.id,
            active_leases_count=len(formatted_leases)
        )

        return {
            "driver": driver_data,
            "leases": formatted_leases,
            "documents": documents
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "Error fetching driver and lease details for Miscellaneous Expense",
            case_no=case_no,
            error=str(e),
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while fetching driver details: {str(e)}"
        ) from e
    
@step(step_id="308", name="Process - Create Miscellaneous Expense", operation="process")
def create_miscellaneous_expense_process(db: Session, case_no: str, step_data: Dict[str, Any]):
    """
    Processes the final submission of the miscellaneous expense, creating the master record
    and posting the charge immediately to the centralized ledger.
    
    This step handles:
    1. Data validation using Pydantic schema
    2. Creating the miscellaneous expense master record
    3. Immediate posting to the ledger as a DEBIT obligation
    4. Linking the expense to the BPM case
    5. Creating audit trail
    6. Closing the BPM case (single-step workflow)
    
    Args:
        db: Database session
        case_no: BPM case number
        step_data: Dictionary containing expense creation data
            - driver_id: ID of the driver
            - lease_id: ID of the associated lease
            - vehicle_id: ID of the vehicle
            - medallion_id: ID of the medallion
            - category: Expense category (e.g., "Cleaning", "Lost Key")
            - amount: Decimal amount of the expense (must be > 0)
            - expense_date: Date of the expense
            - reference_number: Optional reference number
            - notes: Optional notes
            - document_id: Optional uploaded document ID
    
    Returns:
        Dict with success message
    
    Raises:
        HTTPException: For validation errors or ledger posting failures
    """
    try:
        logger.info("Processing Miscellaneous Expense creation", case_no=case_no)
        
        # Validate the incoming data using Pydantic schema
        try:
            expense_create_data = MiscellaneousExpenseCreate(**step_data)
        except Exception as validation_error:
            logger.error(
                "Validation error for Miscellaneous Expense data",
                case_no=case_no,
                error=str(validation_error)
            )
            raise HTTPException(
                status_code=400,
                detail=f"Invalid expense data: {str(validation_error)}"
            ) from validation_error
        
        # Get the currently logged-in user from the DB session info
        current_user_id = db.info.get("current_user_id")
        if not current_user_id:
            logger.error("User context not found", case_no=case_no)
            raise HTTPException(
                status_code=403,
                detail="User context not found for audit trail."
            )
        
        # Create the miscellaneous expense and post to ledger
        misc_expense_service = MiscellaneousExpenseService(db)
        created_expense = misc_expense_service.create_misc_expense(
            case_no=case_no,
            expense_data=expense_create_data,
            user_id=current_user_id
        )
        
        # Handle optional document attachment
        document_id = step_data.get("document_id")
        if document_id:
            try:
                upload_service.upsert_document(db, {
                    "id": document_id,
                    "object_type": "miscellaneous_expense",
                    "object_lookup_id": str(created_expense.id)
                })
                logger.info(
                    "Linked document to Miscellaneous Expense",
                    expense_id=created_expense.expense_id,
                    document_id=document_id
                )
            except Exception as doc_error:
                logger.warning(
                    "Failed to link document to Miscellaneous Expense",
                    expense_id=created_expense.expense_id,
                    document_id=document_id,
                    error=str(doc_error)
                )
        
        # Mark the BPM case as closed (single-step workflow completes here)
        bpm_service.mark_case_as_closed(db, case_no)
        
        # Create audit trail entry
        case = bpm_service.get_cases(db, case_no=case_no)
        audit_trail_service.create_audit_trail(
            db,
            description=(
                f"Miscellaneous Expense created: {created_expense.expense_id} - "
                f"{expense_create_data.category} - ${expense_create_data.amount}"
            ),
            case=case,
            meta_data={
                "expense_id": created_expense.expense_id,
                "category": expense_create_data.category,
                "amount": float(expense_create_data.amount),
                "driver_id": expense_create_data.driver_id,
                "lease_id": expense_create_data.lease_id,
                "expense_date": expense_create_data.expense_date.isoformat(),
                "reference_number": expense_create_data.reference_number,
            },
            audit_type=AuditTrailType.AUTOMATED,
        )
        
        logger.info(
            "Miscellaneous Expense successfully created and posted to ledger",
            case_no=case_no,
            expense_id=created_expense.expense_id,
            amount=float(expense_create_data.amount)
        )
        
        return {
            "message": "Miscellaneous expense successfully created and posted to the ledger.",
            "expense_id": created_expense.expense_id,
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "Error processing Miscellaneous Expense creation",
            case_no=case_no,
            error=str(e),
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create miscellaneous expense: {str(e)}"
        ) from e