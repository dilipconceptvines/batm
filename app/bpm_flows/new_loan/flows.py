## app/bpm_flows/newloan/flows.py

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, Any, Optional

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.bpm.step_info import step
from app.drivers.services import driver_service
from app.leases.services import lease_service
from app.loans.services import LoanService
from app.medallions.services import medallion_service
from app.utils.logger import get_logger
from app.vehicles.services import vehicle_service

logger = get_logger(__name__)

# Entity mapper for case entity tracking
entity_mapper = {
    "DRIVER_LOAN": "driver_loans",
    "DRIVER_LOAN_ID": "id",
}

#@step(step_id="303", name="Fetch - Search Driver & Enter Loan Details", operation="fetch")
@step(step_id="219", name="Fetch - Search Driver & Enter Loan Details", operation="fetch")
def enter_loan_details_fetch(db: Session, case_no: str, case_params: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Fetches driver details and active leases for the driver loan workflow.
    User searches by TLC License number to find the driver and their associated leases.
    
    Args:
        db: Database session
        case_no: BPM case number
        case_params: Query parameters containing 'tlc_license_no'
    
    Returns:
        Dict containing driver details and list of active leases with full lease information
    """
    try:
        logger.info(f"Fetching driver and lease details for loan creation - case {case_no}")
        
        # Check if we already have a case entity (driver already selected)
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        selected_loan_id = None
        if case_entity:
            selected_loan_id = case_entity.identifier_value
            logger.info(f"Found existing loan entry: {selected_loan_id}")
        
        # If no search parameters provided, return empty state
        if not case_params or not case_params.get("tlc_license_no"):
            return {
                "driver": {},
                "leases": [],
                "selected_loan_id": selected_loan_id,
            }
        
        tlc_license_no = case_params.get("tlc_license_no")
        logger.info(f"Searching for driver with TLC License: {tlc_license_no}")
        
        # Search for driver by TLC License
        driver = driver_service.get_drivers(db, tlc_license_number=tlc_license_no)
        
        if not driver:
            logger.warning(f"No driver found with TLC License: {tlc_license_no}")
            raise HTTPException(
                status_code=404, 
                detail=f"No driver found with TLC License number: {tlc_license_no}"
            )
        
        # Validate driver status - only Active or Registered drivers can get loans
        from app.drivers.schemas import DriverStatus
        if driver.driver_status not in [DriverStatus.ACTIVE, DriverStatus.REGISTERED]:
            raise HTTPException(
                status_code=400,
                detail=f"Driver {driver.driver_id} is not active. Current status: {driver.driver_status.value}. Only Active or Registered drivers can receive loans."
            )
        
        # Fetch active leases for the driver (both as primary driver and co-lessee)
        active_leases = lease_service.get_lease(
            db, 
            driver_id=driver.id, 
            status="Active", 
            multiple=True
        )
        
        if not active_leases or not active_leases[0]:
            logger.warning(f"No active leases found for driver ID: {driver.id}")
            raise HTTPException(
                status_code=404, 
                detail="No active lease found for this driver. Driver must have an active lease to receive a loan."
            )
        
        # Format lease data for UI with comprehensive details
        formatted_leases = []
        for lease in active_leases[0]:
            lease_data = {
                "lease_id_pk": lease.id,
                "lease_id": lease.lease_id,
                "lease_type": lease.lease_type,
                "medallion_no": lease.medallion.medallion_number if lease.medallion else "N/A",
                "medallion_id": lease.medallion.id if lease.medallion else None,
                "vehicle_make_model": f"{lease.vehicle.make} {lease.vehicle.model} {lease.vehicle.year}" if lease.vehicle else "N/A",
                "vin": lease.vehicle.vin if lease.vehicle else "N/A",
                "vehicle_id": lease.vehicle.id if lease.vehicle else None,
                "plate_no": lease.vehicle.registrations[0].plate_number if lease.vehicle and lease.vehicle.registrations else "N/A",
                "lease_status": lease.lease_status,
                "weekly_lease_amount": float(lease.lease_amount) if lease.lease_amount else 0.0,
            }
            formatted_leases.append(lease_data)
        
        # Format driver data for UI
        driver_data = {
            "id": driver.id,
            "driver_id": driver.driver_id,
            "full_name": driver.full_name,
            "first_name": driver.first_name,
            "last_name": driver.last_name,
            "status": driver.driver_status.value if hasattr(driver.driver_status, 'value') else str(driver.driver_status),
            "tlc_license": driver.tlc_license.tlc_license_number if driver.tlc_license else "N/A",
            "dmv_license": driver.dmv_license.dmv_license_number if driver.dmv_license else "N/A",
            "phone": driver.phone_number_1 or "N/A",
            "email": driver.email_address or "N/A",
        }
        
        logger.info(f"Successfully fetched driver {driver.driver_id} with {len(formatted_leases)} active lease(s)")
        
        return {
            "driver": driver_data,
            "leases": formatted_leases,
            "selected_loan_id": selected_loan_id,
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching driver and lease details for case {case_no}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail=f"An error occurred while fetching driver details: {str(e)}"
        ) from e

#@step(step_id="303", name="Process - Create Driver Loan", operation="process")
@step(step_id="219", name="Process - Create Driver Loan", operation="process")
async def enter_loan_details_process(db: Session, case_no: str, step_data: Dict[str, Any], user_id: int = None) -> Dict[str, Any]:
    """
    Creates a new driver loan with installment schedule.
    This is the final step that:
    1. Validates all loan details
    2. Creates the loan record
    3. Generates the installment schedule
    4. Returns the schedule for user confirmation
    
    Args:
        db: Database session
        case_no: BPM case number
        step_data: Dictionary containing loan details from the form
                  Must conform to enter_details.json schema
    
    Returns:
        Dict with loan details and payment schedule for confirmation
    """
    try:
        logger.info(f"Processing driver loan creation for case {case_no}")
        
        # Extract and validate required fields
        driver_id = step_data.get("driver_id")
        lease_id = step_data.get("lease_id")
        vehicle_id = step_data.get("vehicle_id")
        medallion_id = step_data.get("medallion_id")
        loan_amount = step_data.get("loan_amount")
        interest_rate = step_data.get("interest_rate", 0)  # Default to 0%
        start_week_str = step_data.get("start_week")
        notes = step_data.get("notes")
        
        # Validate required fields
        if not all([driver_id, lease_id, vehicle_id, medallion_id, loan_amount, start_week_str]):
            raise HTTPException(
                status_code=400,
                detail="Missing required fields: driver_id, lease_id, vehicle_id, medallion_id, loan_amount, and start_week are required."
            )
        
        # Validate loan amount
        try:
            loan_amount_decimal = Decimal(str(loan_amount))
            if loan_amount_decimal <= 0:
                raise ValueError("Loan amount must be greater than zero")
        except (ValueError, TypeError) as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid loan amount: {loan_amount}. Must be a positive number."
            ) from e
        
        # Validate interest rate
        try:
            interest_rate_decimal = Decimal(str(interest_rate))
            if interest_rate_decimal < 0 or interest_rate_decimal > 100:
                raise ValueError("Interest rate must be between 0 and 100")
        except (ValueError, TypeError) as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid interest rate: {interest_rate}. Must be between 0 and 100."
            ) from e
        
        # Parse and validate start_week (must be a Sunday)
        try:
            start_week = datetime.strptime(start_week_str, "%Y-%m-%d").date()
            if start_week.weekday() != 6:  # 6 = Sunday
                raise ValueError("Start week must be a Sunday")
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid start week: {start_week_str}. Must be a Sunday date in YYYY-MM-DD format."
            ) from e
        
        # Validate driver exists and is active
        driver = driver_service.get_drivers(db, driver_id=driver_id)
        if not driver:
            raise HTTPException(status_code=404, detail=f"Driver with ID {driver_id} not found.")
        
        from app.drivers.schemas import DriverStatus
        if driver.driver_status not in [DriverStatus.ACTIVE, DriverStatus.REGISTERED]:
            raise HTTPException(
                status_code=400,
                detail=f"Driver {driver.driver_id} is not active. Current status: {driver.driver_status.value}"
            )
        
        # Validate lease exists and is active
        lease = lease_service.get_lease(db, lookup_id=lease_id)
        driver_lease = lease_service.get_lease_drivers(db=db , lease_id=lease.id, driver_id=driver.driver_id)
        if not lease:
            raise HTTPException(status_code=404, detail=f"Lease with ID {lease_id} not found.")
        
        if lease.lease_status != "Active":
            raise HTTPException(
                status_code=400,
                detail=f"Lease {lease.lease_id} is not active. Current status: {lease.lease_status}"
            )
        
        # Validate driver is associated with the lease
        # if driver.id != lease.driver_id:
        #     # Check if driver is a co-lessee
        #     lease_drivers = lease_service.get_lease_drivers(db, lease_id=lease.id, multiple=True)
        #     driver_ids = [ld.driver_id for ld in lease_drivers if ld.driver_id]
            
        #     if driver.id not in driver_ids:
        #         raise HTTPException(
        #             status_code=400,
        #             detail=f"Driver {driver.driver_id} is not associated with lease {lease.lease_id}."
        #         )
        if not driver_lease:
            raise HTTPException(
                status_code=400,
                detail=f"Driver {driver.driver_id} is not associated with lease {lease.lease_id}."
            )
        
        # Validate vehicle exists
        vehicle = vehicle_service.get_vehicles(db, vehicle_id=vehicle_id)
        if not vehicle:
            raise HTTPException(status_code=404, detail=f"Vehicle with ID {vehicle_id} not found.")
        
        # Validate medallion exists
        medallion = medallion_service.get_medallion(db, medallion_id=medallion_id)
        if not medallion:
            raise HTTPException(status_code=404, detail=f"Medallion with ID {medallion_id} not found.")
        
        # Create loan and generate installment schedule
        loan_svc = LoanService(db)
        
        logger.info(
            f"Creating loan for driver {driver.driver_id}: "
            f"Amount=${loan_amount_decimal}, Rate={interest_rate_decimal}%, Start={start_week}"
        )
        
        # Prepare loan data according to service method signature
        loan_data = {
            "driver_id": driver.id,
            "lease_id": lease.id,
            "vehicle_id": vehicle.id,
            "medallion_id": medallion.id,
            "loan_amount": loan_amount_decimal,
            "interest_rate": interest_rate_decimal,
            "start_week": start_week,
            "notes": notes,
        }
        
        # Get user_id from parameter, step_data, or use system user
        if user_id is None:
            user_id = step_data.get("created_by", 1)  # Default to system user ID 1
        
        # Create loan and schedule (this method is synchronous, not async)
        loan = loan_svc.create_loan_and_schedule(
            case_no=case_no,
            loan_data=loan_data,
            user_id=user_id
        )
        
        # Get the installments from the created loan
        installments = loan.installments if hasattr(loan, 'installments') else []
        
        logger.info(f"Created loan {loan.loan_id} with {len(installments)} installments")
        
        # Create case entity linking to this loan
        bpm_service.create_case_entity(
            db=db,
            case_no=case_no,
            entity_name=entity_mapper["DRIVER_LOAN"],
            identifier=entity_mapper["DRIVER_LOAN_ID"],
            identifier_value=str(loan.id)
        )
        
        db.commit()
        
        # Format installment schedule for UI confirmation
        formatted_installments = []
        for inst in installments:
            formatted_installments.append({
                "installment_id": inst.installment_id,
                "week_period": f"{inst.week_end_date.strftime('%m/%d/%Y')}-{(inst.week_end_date + timedelta(days=6)).strftime('%m/%d/%Y')}",
                "principle": float(inst.principal_amount),
                "interest": float(inst.interest_amount),
                "total_due": float(inst.total_due),
                "balance": float(inst.total_due-inst.principal_amount),
                "due_date": inst.week_end_date.isoformat(),
                "status": inst.status.value if hasattr(inst.status, 'value') else str(inst.status),
            })
        
        # Calculate totals
        total_interest = sum(float(inst.interest_amount) for inst in installments)
        total_to_repay = float(loan_amount_decimal) + total_interest
        
        # Format period
        first_due = installments[0].week_end_date
        last_due = installments[-1].week_end_date
        period = f"{first_due.strftime('%m/%d/%Y')}-{last_due.strftime('%m/%d/%Y')}"
        
        # Create audit trail
        case = bpm_service.get_cases(db=db, case_no=case_no)
        
        receipt_s3_key, receipt_url = loan_svc._generate_and_store_receipt(loan)
        
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Created driver loan {loan.loan_id} for ${loan_amount_decimal:.2f}",
                meta_data={
                    "loan_id": loan.id,
                    "loan_display_id": loan.loan_id,
                    "driver_id": driver.id,
                    "driver_display_id": driver.driver_id,
                    "lease_id": lease.id,
                    "lease_display_id": lease.lease_id,
                    "loan_amount": float(loan_amount_decimal),
                    "interest_rate": float(interest_rate_decimal),
                    "installments_count": len(installments),
                }
            )
        
        logger.info(f"Successfully created loan {loan.loan_id} for case {case_no}")
        
        # Return data for confirmation modal
        return {"installments": formatted_installments, "loan_id": loan.loan_id, "receipt_s3_key": receipt_s3_key, "receipt_url": receipt_url, "status": "Ok"}
        
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error creating driver loan for case {case_no}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to create driver loan: {str(e)}"
        ) from e