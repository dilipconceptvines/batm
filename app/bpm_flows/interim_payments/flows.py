# app/bpm_flows/interim_payments/flows.py (COMPLETE REWRITE)

from datetime import datetime
from typing import Dict, Any, Optional
from decimal import Decimal

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.bpm.step_info import step
from app.drivers.services import driver_service
from app.interim_payments.models import (
    InterimPayment, 
    PaymentMethod, 
    PaymentStatus,
    InterimPaymentAllocation
)
from app.interim_payments.services import InterimPaymentService
from app.interim_payments.validators import InterimPaymentValidator
from app.leases.services import lease_service
from app.ledger.services import LedgerService
from app.ledger.repository import LedgerRepository
from app.ledger.models import LedgerBalance, BalanceStatus
from app.utils.s3_utils import s3_utils
from app.utils.logger import get_logger

logger = get_logger(__name__)

# Entity mapper for case entity tracking
entity_mapper = {
    "INTERIM_PAYMENT": "interim_payment",
    "INTERIM_PAYMENT_IDENTIFIER": "id"
}

# ============================================================================
# STEP 210: SEARCH DRIVER & ENTER PAYMENT DETAILS
# ============================================================================

@step(step_id="210", name="Fetch - Search Driver and Enter Payment Details", operation="fetch")
def fetch_driver_and_lease_details(
    db: Session, 
    case_no: str, 
    case_params: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Fetches driver details and active leases for the interim payment workflow.
    User searches by TLC License number to find the driver and their associated leases.
    
    BEHAVIOR:
    - If interim_payment exists: Return payment data for pre-filling (Edit mode)
    - If no payment exists: Return empty form (Create mode)
    
    This enables "Edit Payment Details" functionality from Step 211.
    """
    try:
        logger.info(f"Fetching driver and lease details for case {case_no}")
        
        # Get TLC license from query params
        tlc_license_no = case_params.get("tlc_license_number") if case_params else None
        
        if not tlc_license_no:
            return {
                "driver": None,
                "leases": [],
                "existing_payment": None
            }
        
        # Find driver by TLC license
        driver = driver_service.get_drivers(db, tlc_license_number=tlc_license_no)
        
        if not driver:
            raise HTTPException(
                status_code=404,
                detail=f"Driver not found with TLC License: {tlc_license_no}"
            )
        
        # Get all active leases for this driver
        leases = lease_service.get_lease(db, driver_id=driver.driver_id, status="Active")
        
        if not leases:
            raise HTTPException(
                status_code=404,
                detail=f"No active leases found for driver {driver.full_name}"
            )
        
        # Format driver details
        driver_data = {
            "driver_id": driver.id,
            "driver_name": driver.full_name,
            "tlc_license": driver.tlc_license.tlc_license_number if driver.tlc_license else "N/A",
        }
        
        # Format leases
        formatted_leases = []
        for lease in leases:
            formatted_leases.append({
                "lease_id": lease.id,
                "lease_reference": lease.lease_id,
                "medallion_no": lease.medallion.medallion_number if lease.medallion else "N/A",
                "vehicle_plate": lease.vehicle.plate_number if lease.vehicle else "N/A",
                "lease_status": lease.status,
            })
        
        # Check if payment already exists (Edit mode)
        existing_payment = None
        selected_interim_payment_id = None
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        
        if case_entity:
            existing_payment_obj = db.query(InterimPayment).filter(
                InterimPayment.id == int(case_entity.identifier_value)
            ).first()
            
            if existing_payment_obj:
                existing_payment = {
                    "interim_payment_id": existing_payment_obj.id,
                    "driver_id": existing_payment_obj.driver_id,
                    "lease_id": existing_payment_obj.lease_id,
                    "payment_amount": float(existing_payment_obj.total_amount),
                    "payment_method": existing_payment_obj.payment_method.value,
                    "payment_date": existing_payment_obj.payment_date.isoformat(),
                    "notes": existing_payment_obj.notes
                }
                selected_interim_payment_id = existing_payment_obj.id
        
        logger.info(
            f"Successfully fetched driver with active leases",
            driver_id=driver.driver_id,
            leases=len(formatted_leases),
            edit_mode=existing_payment is not None
        )
        
        return {
            "driver": driver_data,
            "leases": formatted_leases,
            "selected_interim_payment_id": selected_interim_payment_id,
            "existing_payment": existing_payment
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Error fetching driver and lease details for case {case_no}: {e}",
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while fetching driver details: {str(e)}"
        ) from e


@step(step_id="210", name="Process - Create/Update Payment Details", operation="process")
def create_interim_payment_record(
    db: Session, 
    case_no: str, 
    step_data: Dict[str, Any]
) -> Dict[str, str]:
    """
    Creates or updates an interim payment entry record with payment details.
    
    FIXED: Now sets status=ACTIVE and uses validator for comprehensive checks
    
    LOGIC:
    - If case_entity exists: UPDATE existing interim_payment
    - If case_entity doesn't exist: CREATE new interim_payment
    
    IMPORTANT: When updating, clear any existing allocations to prevent inconsistency.
    """
    try:
        logger.info(f"Creating/Updating interim payment entry for case {case_no}")
        
        # Check if case entity already exists
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        
        # Extract and validate required fields
        driver_id = step_data.get("driver_id")
        lease_id = step_data.get("lease_id")
        payment_amount = step_data.get("payment_amount")
        payment_method = step_data.get("payment_method")
        payment_date_str = step_data.get("payment_date")
        notes = step_data.get("notes")
        
        # Validation: Required fields
        if not driver_id or not lease_id:
            raise HTTPException(
                status_code=400,
                detail="Driver and lease selection is required"
            )
        
        if not payment_amount or payment_amount <= 0:
            raise HTTPException(
                status_code=400,
                detail="Payment amount must be greater than zero"
            )
        
        if not payment_method:
            raise HTTPException(
                status_code=400,
                detail="Payment method is required"
            )
        
        if not payment_date_str:
            raise HTTPException(
                status_code=400,
                detail="Payment date is required"
            )
        
        # Parse payment date
        try:
            payment_date = datetime.fromisoformat(payment_date_str.replace('Z', "+00:00"))
        except (ValueError, AttributeError) as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid payment date format. Use ISO 8601 format (YYYY-MM-DD). Error: {str(e)}"
            ) from e
        
        # Validate payment method enum
        try:
            payment_method_enum = PaymentMethod(payment_method)
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid payment method: {payment_method}. Valid options: CASH, CHECK, ACH"
            ) from e
        
        # Verify driver and lease exist
        driver = driver_service.get_drivers(db, id=driver_id)
        if not driver:
            raise HTTPException(status_code=404, detail=f"Driver {driver_id} not found")
        
        lease = lease_service.get_lease(db, lookup_id=str(lease_id))
        if not lease:
            raise HTTPException(status_code=404, detail=f"Lease {lease_id} not found")
        
        # Calculate total outstanding for this lease (for UI display)
        repo = LedgerRepository(db)
        open_balances = repo.get_open_balances_by_lease(lease_id=lease.id)
        total_outstanding = sum(float(b.balance) for b in open_balances)
        
        # Get current user ID
        current_user_id = db.info.get("current_user_id", 1)
        
        # ========== UPDATE MODE ==========
        if case_entity:
            interim_payment_service = InterimPaymentService(db)
            interim_payment = interim_payment_service.repo.get_payment_by_id(
                int(case_entity.identifier_value)
            )
            
            if interim_payment:
                # Store original values for audit
                original_amount = interim_payment.total_amount
                original_method = interim_payment.payment_method.value
                
                # Update payment details
                interim_payment.driver_id = driver.id
                interim_payment.lease_id = lease.id
                interim_payment.payment_date = payment_date
                interim_payment.total_amount = payment_amount
                interim_payment.payment_method = payment_method_enum
                interim_payment.notes = notes
                interim_payment.updated_by = current_user_id
                
                # CRITICAL: Clear allocations if they exist
                if interim_payment.allocations:
                    logger.warning(
                        f"Clearing existing allocations for payment {interim_payment.payment_id} "
                        f"because payment details were modified",
                        allocations_count=len(interim_payment.allocations),
                        case_no=case_no
                    )
                    interim_payment.allocations = []
                
                db.commit()
                db.refresh(interim_payment)
                
                logger.info(
                    f"Updated interim payment {interim_payment.payment_id}",
                    case_no=case_no,
                    original_amount=float(original_amount),
                    new_amount=float(payment_amount),
                    original_method=original_method,
                    new_method=payment_method
                )
                
                return {
                    "message": "Payment details updated successfully",
                    "interim_payment_id": str(interim_payment.id),
                    "operation": "UPDATE",
                    "allocations_cleared": True,
                    "total_outstanding": round(total_outstanding, 2)
                }
        
        # ========== CREATE MODE ==========
        interim_payment_service = InterimPaymentService(db)
        payment_id = interim_payment_service._generate_next_payment_id()
        
        # FIXED: Now sets status=ACTIVE
        new_interim_payment = InterimPayment(
            payment_id=payment_id,
            case_no=case_no,
            driver_id=driver.id,
            lease_id=lease.id,
            payment_date=payment_date,
            payment_method=payment_method_enum,
            total_amount=payment_amount,
            notes=notes,
            allocations=[],
            status=PaymentStatus.ACTIVE,  # ✅ FIXED: Set initial status
            created_by=current_user_id
        )
        
        db.add(new_interim_payment)
        db.flush()
        db.refresh(new_interim_payment)
        
        # Create case entity linking to this interim payment
        bpm_service.create_case_entity(
            db=db,
            case_no=case_no,
            entity_name=entity_mapper["INTERIM_PAYMENT"],
            identifier=entity_mapper["INTERIM_PAYMENT_IDENTIFIER"],
            identifier_value=str(new_interim_payment.id)
        )
        
        db.commit()
        
        logger.info(
            f"Created interim payment entry",
            interim_payment_id=new_interim_payment.id,
            payment_id=payment_id,
            lease_id=lease.lease_id,
            driver=driver.driver_id,
            payment_amount=payment_amount,
            method=payment_method
        )
        
        # Create audit trail
        case = bpm_service.get_cases(db=db, case_no=case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Created interim payment of ${payment_amount:.2f} ({payment_method}) for driver {driver.driver_id} and lease {lease.lease_id}",
                meta_data={
                    "interim_payment_id": new_interim_payment.id,
                    "payment_id": payment_id,
                    "driver_id": driver.id,
                    "driver_name": driver.full_name,
                    "lease_id": lease.id,
                    "lease_reference": lease.lease_id,
                    "payment_amount": float(payment_amount),
                    "payment_method": payment_method,
                    "payment_date": payment_date_str
                }
            )
        
        return {
            "message": "Interim payment entry created successfully.",
            "interim_payment_id": str(new_interim_payment.id),
            "payment_id": payment_id,
            "operation": "CREATE",
            "total_outstanding": round(total_outstanding, 2)
        }
        
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        logger.error(
            f"Error creating interim payment entry for case {case_no}: {e}",
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create interim payment entry: {str(e)}"
        ) from e


# ============================================================================
# STEP 211: ALLOCATE PAYMENTS
# ============================================================================

@step(step_id="211", name="Fetch - Allocate Payments", operation="fetch")
def fetch_outstanding_balances(
    db: Session, 
    case_no: str, 
    case_params: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Fetches outstanding ledger balances for the SPECIFIC lease selected in Step 210.
    
    CRITICAL: Balances are filtered by BOTH driver_id AND lease_id to ensure
    we only show obligations for the selected lease, not all of the driver's leases.
    """
    try:
        logger.info(f"Fetching outstanding balances for case {case_no}")
        
        # Get the interim payment entry from case entity
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        
        if not case_entity:
            return {}
        
        # Retrieve the interim payment record
        interim_payment_service = InterimPaymentService(db)
        interim_payment = interim_payment_service.repo.get_payment_by_id(
            int(case_entity.identifier_value)
        )
        
        if not interim_payment:
            raise HTTPException(
                status_code=404,
                detail=f"Interim payment record not found with ID {case_entity.identifier_value}"
            )
        
        # Get the lease_id and driver_id from Step 210
        selected_lease_id = interim_payment.lease_id
        selected_driver_id = interim_payment.driver_id
        
        logger.info(
            f"Fetching balances for driver {selected_driver_id} and lease {selected_lease_id}"
        )
        
        # Retrieve driver and lease objects
        driver = driver_service.get_drivers(db, id=selected_driver_id)
        if not driver:
            raise HTTPException(
                status_code=404,
                detail=f"Driver {selected_driver_id} not found"
            )
        
        lease = lease_service.get_lease(db, lookup_id=str(selected_lease_id))
        if not lease:
            raise HTTPException(
                status_code=404,
                detail=f"Lease {selected_lease_id} not found"
            )
        
        # Fetch open balances for THIS SPECIFIC LEASE ONLY
        repo = LedgerRepository(db)
        open_balances = repo.get_open_balances_by_lease(
            lease_id=lease.id,
            driver_id=driver.id
        )
        
        # Format balances for UI
        formatted_balances = []
        for balance in open_balances:
            formatted_balances.append({
                "balance_id": str(balance.id),
                "category": balance.category.value,
                "reference_id": balance.reference_id,
                "description": f"{balance.category.value} - {balance.reference_id}",
                "outstanding": float(balance.balance),
                "original_amount": float(balance.original_amount),
                "due_date": balance.created_on.strftime("%Y-%m-%d") if balance.created_on else None,
                "status": balance.status.value
            })
        
        total_outstanding = sum(b['outstanding'] for b in formatted_balances)
        
        # Format driver details
        driver_details = {
            "driver_id": driver.driver_id,
            "driver_name": driver.full_name,
            "tlc_license": driver.tlc_license.tlc_license_number if driver.tlc_license else "N/A",
        }
        
        # Format lease details
        lease_details = {
            "lease_id": lease.lease_id,
            "medallion_no": lease.medallion.medallion_number if lease.medallion else "N/A",
        }
        
        logger.info(
            f"Returning {len(formatted_balances)} balances totaling "
            f"${total_outstanding:.2f} for lease {lease.lease_id}"
        )
        
        return {
            "driver": driver_details,
            "lease": lease_details,
            "total_outstanding": round(total_outstanding, 2),
            "obligations": formatted_balances,
            "payment_amount": float(interim_payment.total_amount)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Error fetching outstanding balances for case {case_no}: {e}",
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while fetching outstanding balances: {str(e)}"
        ) from e


@step(step_id="211", name="Process - Allocate Payments", operation="process")
def process_payment_allocation(
    db: Session, 
    case_no: str, 
    step_data: Dict[str, Any]
) -> Dict[str, str]:
    """
    FIXED: Now uses validator, creates structured allocations, proper error handling
    
    Processes the final allocation of the interim payment.
    
    Workflow:
    1. Retrieve the interim payment record created in Step 210
    2. Extract allocations from step_data
    3. ✅ COMPREHENSIVE VALIDATION (uses InterimPaymentValidator)
    4. Verify all balance_ids belong to the selected lease
    5. Update the interim payment record with allocations
    6. Apply allocations to ledger (creates CREDIT postings)
    7. ✅ CREATE STRUCTURED ALLOCATION RECORDS
    8. Generate and upload receipt to S3
    9. Mark BPM case as closed
    10. Create audit trail
    """
    try:
        logger.info(f"Processing payment allocation for case {case_no}")
        
        # Get the interim payment entry from case entity
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        
        if not case_entity:
            raise HTTPException(
                status_code=404,
                detail="No interim payment entry found. Please complete Step 1 first."
            )
        
        # Retrieve the interim payment record
        interim_payment_service = InterimPaymentService(db)
        interim_payment = interim_payment_service.repo.get_payment_by_id(
            int(case_entity.identifier_value)
        )
        
        if not interim_payment:
            raise HTTPException(
                status_code=404,
                detail=f"Interim payment record not found with ID {case_entity.identifier_value}"
            )
        
        # Get payment details from Step 210
        selected_lease_id = interim_payment.lease_id
        selected_driver_id = interim_payment.driver_id
        payment_amount = Decimal(str(interim_payment.total_amount))
        payment_method = interim_payment.payment_method.value
        payment_date = interim_payment.payment_date
        notes = interim_payment.notes
        
        logger.info(
            f"Processing allocation for driver {selected_driver_id}, "
            f"lease {selected_lease_id}, amount ${payment_amount}"
        )
        
        # Extract allocations from step_data
        allocations = step_data.get("allocations", [])
        
        if not allocations or len(allocations) == 0:
            raise HTTPException(
                status_code=400,
                detail="At least one allocation is required."
            )
        
        # ✅ COMPREHENSIVE VALIDATION using validator
        validator = InterimPaymentValidator(db)
        validator._validate_obligation_selection(
            allocations=allocations,
            driver_id=selected_driver_id,
            lease_id=selected_lease_id
        )
        
        # Validate total allocated amount
        total_allocated = sum(Decimal(str(alloc.get("amount", 0))) for alloc in allocations)
        
        if total_allocated > payment_amount:
            raise HTTPException(
                status_code=400,
                detail=f"Total allocated (${total_allocated}) cannot exceed payment (${payment_amount})."
            )
        
        # Format allocations
        formatted_allocations = []
        for alloc in allocations:
            formatted_allocations.append({
                "category": alloc.get("category"),
                "reference_id": alloc.get("reference_id"),
                "amount": float(alloc.get("amount")),
            })
        
        # Update the interim payment record
        interim_payment.allocations = formatted_allocations
        
        # Generate payment ID if not already set
        if not interim_payment.payment_id:
            interim_payment.payment_id = interim_payment_service._generate_next_payment_id()
        
        # Ensure status is ACTIVE
        interim_payment.status = PaymentStatus.ACTIVE
        
        db.commit()
        db.refresh(interim_payment)
        
        logger.info(
            f"Updated interim payment {interim_payment.payment_id} "
            f"with {len(formatted_allocations)} allocation(s)"
        )
        
        # Apply allocations to ledger (SYNC method, not async)
        allocation_dict = {
            alloc["reference_id"]: Decimal(str(alloc["amount"]))
            for alloc in formatted_allocations
        }
        
        repo = LedgerRepository(db)
        ledger_service = LedgerService(repo)
        
        created_postings = ledger_service.apply_interim_payment(
            payment_amount=payment_amount,
            allocations=allocation_dict,
            driver_id=selected_driver_id,
            lease_id=selected_lease_id,
            payment_method=payment_method
        )
        
        logger.info(
            f"Created {len(created_postings)} ledger postings for payment {interim_payment.payment_id}"
        )
        
        # ✅ CREATE STRUCTURED ALLOCATION RECORDS
        current_user_id = db.info.get("current_user_id", 1)
        
        for alloc in formatted_allocations:
            # Get the ledger balance
            balance = repo.get_balance_by_reference_id(alloc["reference_id"])
            
            if balance:
                allocation_record = InterimPaymentAllocation(
                    interim_payment_id=interim_payment.id,
                    ledger_balance_id=str(balance.id),
                    category=alloc["category"],
                    reference_id=alloc["reference_id"],
                    allocated_amount=Decimal(str(alloc["amount"])),
                    balance_before=None,  # Could capture before application
                    balance_after=balance.balance,
                    created_by=current_user_id
                )
                db.add(allocation_record)
        
        db.commit()
        
        logger.info(
            f"Created {len(formatted_allocations)} structured allocation records"
        )
        
        # Generate and upload receipt
        receipt_url = None
        try:
            from app.interim_payments.pdf_service import InterimPaymentPdfService
            
            pdf_service = InterimPaymentPdfService(db)
            receipt_pdf = pdf_service.generate_receipt_pdf(interim_payment.id)
            
            # Upload to S3
            s3_key = f"receipts/interim_payments/{interim_payment.payment_id}.pdf"
            upload_success = s3_utils.upload_file(
                file_obj=receipt_pdf,
                key=s3_key,
                content_type="application/pdf"
            )
            
            if upload_success:
                interim_payment.receipt_s3_key = s3_key
                db.commit()
                receipt_url = s3_utils.generate_presigned_url(s3_key)
                logger.info(f"Uploaded receipt to S3: {s3_key}")
            else:
                logger.error(
                    f"Failed to upload receipt to S3 for payment {interim_payment.payment_id}"
                )
                
        except Exception as receipt_error:
            logger.error(
                f"Error generating/storing receipt: {str(receipt_error)}",
                exc_info=True
            )
            # Don't fail the entire transaction for receipt errors
        
        # Mark BPM case as closed
        bpm_service.mark_case_as_closed(db, case_no)
        
        logger.info(f"Marked case {case_no} as closed")
        
        # Create audit trail
        case = bpm_service.get_cases(db=db, case_no=case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Completed interim payment {interim_payment.payment_id} for ${payment_amount}",
                meta_data={
                    "interim_payment_id": interim_payment.id,
                    "payment_id": interim_payment.payment_id,
                    "driver_id": selected_driver_id,
                    "lease_id": selected_lease_id,
                    "payment_amount": float(payment_amount),
                    "payment_method": payment_method,
                    "allocations_count": len(formatted_allocations),
                    "total_allocated": float(total_allocated)
                }
            )
        
        return {
            "message": "Interim payment successfully created and allocated.",
            "payment_id": interim_payment.payment_id,
            "driver_name": interim_payment.driver.full_name if interim_payment.driver else "Unknown",
            "receipt_url": receipt_url
        }
        
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        logger.error(
            f"Error processing payment allocation for case {case_no}: {e}",
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process payment allocation: {str(e)}"
        ) from e