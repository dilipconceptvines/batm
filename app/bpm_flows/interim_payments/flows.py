# app/bpm_flows/interim_payments/flows.py

from datetime import datetime
from typing import Dict, Any, Optional
from io import BytesIO

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.bpm.step_info import step
from app.drivers.services import driver_service
from app.interim_payments.models import InterimPayment, PaymentMethod
from app.interim_payments.services import InterimPaymentService
from app.leases.services import lease_service
from app.ledger.services import LedgerService
from app.ledger.models import LedgerBalance
from app.utils.s3_utils import s3_utils
from app.utils.logger import get_logger

logger = get_logger(__name__)

# Entity mapper for case entity tracking
entity_mapper = {
    "INTERIM_PAYMENT": "interim_payment",
    "INTERIM_PAYMENT_IDENTIFIER": "id"
}

@step(step_id="210", name="Fetch - Search Driver and Enter Payment Details", operation="fetch")
def fetch_driver_and_lease_details(db: Session, case_no: str, case_params: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Fetches driver details and active leases for the interim payment workflow.
    User searches by TLC License number to find the driver and their associated leases.
    
    1. Enters TLC License No
    2. Clicks Search
    3. Sees driver details and associated active leases
    4. Selects a lease for the payment
    """
    try:
        logger.info("Fetching driver and lease details for case", case_no=case_no)

        # Check if the case entity already exists
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        selected_interim_payment_id = None

        if case_entity:
            selected_interim_payment_id = str(case_entity.identifier_value)
            logger.info(
                "Found existing case entity for interim payment",
                case_no=case_no, interim_payment_id=selected_interim_payment_id
            )

        # If no search parameters are provided, return empty response
        # This handles the initial load of the page before user searches
        if not case_params or "tlc_license_no" not in case_params:
            return {
                "driver": None,
                "leases": [],
                "selected_interim_payment_id": selected_interim_payment_id
            }
        
        tlc_license_no = case_params["tlc_license_no"]
        logger.info("Searching for driver with TLC License no", tlc_license_no=tlc_license_no)

        # Search for driver by TLC License number
        driver = driver_service.get_drivers(db, tlc_license_number=tlc_license_no)

        if not driver:
            logger.info("No driver found with provided TLC License no", tlc_license_no=tlc_license_no)
            raise HTTPException(status_code=404, detail=f"Driver not found with TLC License No: {tlc_license_no}")
        
        # Fetch all active leases for driver
        active_leases = lease_service.get_lease(
            db,
            driver_id=driver.id,
            status="Active",
            exclude_additional_drivers=True,
            multiple=True
        )

        if not active_leases or not active_leases[0]:
            logger.info("No active leases found for driver", driver_id=driver.id)
            raise HTTPException(status_code=404, detail=f"No active leases found for the driver. {driver.full_name}")
        
        # Format the lease data for response
        formatted_leases = []
        for lease in active_leases:
            formatted_leases.append({
                "id": lease.id,
                "lease_id": lease.lease_id,
                "medallion_number": lease.medallion.medallion_number if lease.medallion else "N/A",
                "plate_no": lease.vehicle.registrations[0].plate_number if lease.vehcile and lease.vehicle.registrations else "N/A",
                "vin": lease.vehicle.vin if lease.vehicle else "N/A",
                "lease_type": lease.lease_type.value if hasattr(lease.lease_type, "value") else str(lease.lease_type),
                "status": lease.lease_status.value if hasattr(lease.lease_status, "value") else str(lease.lease_status)
            })

        # Format driver data for response
        driver_data = {
            "id": driver.id,
            "driver_id": driver.driver_id,
            "full_name": driver.full_name,
            "status": driver.driver_status.value if hasattr(driver.driver_status, "value") else str(driver.driver_status),
            "tlc_license": driver.tlc_license.tlc_license_number if driver.tlc_license else "N/A",
            "phone": driver.phone_number_1 or "N/A",
            "email": driver.email_address or "N/A"
        }

        logger.info("Successfully fetched driver with active leases", driver_id=driver.driver_id, leases=len(formatted_leases))

        return {
            "driver": driver_data,
            "leases": formatted_leases,
            "selected_interim_payment_id": selected_interim_payment_id
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error fetching driver and lease details for case", case_no=case_no, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while fetching driver details: {str(e)}"
        ) from e
    
@step(step_id="210", name="Process - Create Interim Payment Record", operation="process")
def create_interim_payment_record(db: Session, case_no: str, step_data: Dict[str, Any]) -> Dict[str, str]:
    """
    Creates or updates an interim payment entry record with the selected driver, lease,
    and complete payment details (amount, method, date, notes).

    1. Selects a lease from the list
    2. Enters Total payment amount
    3. Selects payment method (Cash/Check/ACH dropdown)
    4. Selects payment date
    5. Enters optional notes
    6. Clicks "Proceed to allocation"

    All payment details are captured HERE in step 210, so step 211 only needs
    to handle the allocation across outstanding balances.
    """
    try:
        logger.info("Creating/Updating interim payment entry for case", case_no=case_no)

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
                detail=f"Invalid payment method: {payment_method}. Must be one of: Cash, Check, ACh"
            ) from e
        
        # Validate driver existence
        driver = driver_service.get_drivers(db, id=driver_id)
        if not driver:
            raise HTTPException(
                status_code=404,
                detail=f"Driver with id {driver_id} not found"
            )
        
        # Validate lease existence and status
        lease = lease_service.get_lease(db, lookup_id=lease_id, status="Active")
        if not lease:
            raise HTTPException(
                status_code=404,
                detail=f"Active lease with id {lease_id} not found."
            )
        
        # Verify driver is the primary driver on the selected lease
        lease_driver_exists = False
        for lease_driver in lease.lease_driver:
            if lease_driver.driver_id == driver.driver_id and not lease_driver.is_additional_driver:
                lease_driver_exists = True
                break

        if not lease_driver_exists:
            raise HTTPException(
                status_code=400,
                detail=f"Driver {driver.full_name} is not the primary driver on the selected lease"
            )
        
        # Get total outstanding for this lease
        from app.ledger.repository import LedgerRepository
        repo = LedgerRepository(db)
        ledger_service = LedgerService(repo)
        open_balances = ledger_service.repo.get_open_balances_for_driver(
            driver_id=driver.driver_id,
            lease_id=lease.lease_id
        )
        total_outstanding = sum(float(b.balance) for b in open_balances) if open_balances else 0.0

        interim_payment_service = InterimPaymentService(db)

        if case_entity:
            # Entry already exists, update it with new payment details
            interim_payment = interim_payment_service.repo.get_payment_by_id(
                int(case_entity.identifier_value)
            )

            if interim_payment:
                # Update existing record with ALL payment details
                interim_payment.driver_id = driver.driver_id
                interim_payment.lease_id = lease.lease_id
                interim_payment.total_amount = payment_amount
                interim_payment.payment_method = payment_method_enum
                interim_payment.payment_date = payment_date
                interim_payment.notes = notes

                db.commit()
                db.refresh(interim_payment)

                logger.info(
                    "Updated existing interim payment with amount",
                    interim_payment_id=interim_payment.id, payment_amount=payment_amount,
                    payment_method=payment_method
                )

                return {
                    "message": "Interim payment entry updated successfully",
                    "interim_payment_id": str(interim_payment.id),
                    "total_outstanding": round(total_outstanding, 2)
                }
            
        interim_payment_service = InterimPaymentService(db)
        payment_id = interim_payment_service._generate_next_payment_id()
        # Create new interim payment entry with ALL payment details
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
            created_by=db.info.get("current_user_id", 1)
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
            "Created interim payment entry for driver, lease, amount, and method",
            interim_payment_id=new_interim_payment.id, lease_id=lease.lease_id,
            driver=driver.driver_id, payment_amount=payment_amount,
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

@step(step_id="211", name="Fetch - Allocate Payments", operation="fetch")
def fetch_outstanding_balances(db: Session, case_no: str, case_params: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Fetches outstanding ledger balances for the SPECIFIC lease selected in Step 210.
    
    This corresponds to Screen 3 in the Figma flow showing the "Allocate Payments" interface with:
    - Total Payment and Total Outstanding at the top
    - Table of outstanding obligations (Lease, Repairs, Loans, EZPass, PVB, Miscellaneous)
    - Each row shows: Category, Reference ID, Description, Outstanding, Payment Amount, Balance, Due Date
    
    CRITICAL: Balances are filtered by BOTH driver_id AND lease_id to ensure
    we only show obligations for the selected lease, not all of the driver's leases.
    
    Args:
        db: Database session
        case_no: BPM case number
        case_params: Optional query parameters (not used in this step)
    
    Returns:
        Dict containing:
        - driver: Driver details
        - lease: Lease details  
        - total_outstanding: Sum of all outstanding balances for this lease
        - obligations: List of all open ledger balances with details
    """
    try:
        logger.info(f"Fetching outstanding balances for case {case_no}")
        
        # Get the interim payment entry from case entity
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        
        if not case_entity:
            # raise HTTPException(
            #     status_code=404, 
            #     detail="No interim payment entry found. Please complete Step 1 first."
            # )
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
        
        # Get the lease_id and driver_id that were selected in Step 210
        selected_lease_id = interim_payment.lease_id
        selected_driver_id = interim_payment.driver_id
        
        logger.info(
            f"Fetching balances for driver {selected_driver_id} "
            f"and lease {selected_lease_id}"
        )
        
        # Retrieve driver and lease objects for response
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
        # This prevents showing obligations from other leases the driver may have
        from app.ledger.repository import LedgerRepository
        repo = LedgerRepository(db)
        ledger_service = LedgerService(repo)
        open_balances = ledger_service.repo.get_open_balances_for_driver(
            driver_id=selected_driver_id,
            lease_id=selected_lease_id  # Filter by lease_id
        )
        
        if not open_balances:
            logger.info(
                f"No open balances found for driver {selected_driver_id} "
                f"and lease {selected_lease_id}"
            )
            # Return empty obligations list - driver has no outstanding balances
            return {
                "driver": {
                    "driver_id": driver.driver_id,
                    "driver_name": driver.full_name,
                    "tlc_license": driver.tlc_license.tlc_license_number if driver.tlc_license else "N/A",
                },
                "lease": {
                    "lease_id": lease.lease_id,
                    "medallion_no": lease.medallion.medallion_number if lease.medallion else "N/A",
                },
                "total_outstanding": 0.0,
                "obligations": [],
            }
        
        # Format balances for UI display
        formatted_balances = []
        for balance in open_balances:
            formatted_balances.append({
                "balance_id": balance.id,  # Unique ledger balance ID
                "category": balance.category.value,
                "reference_id": balance.reference_id,
                "description": f"{balance.category.value} - {balance.reference_id}",
                "outstanding": float(balance.balance),
                "due_date": balance.created_on.date().isoformat() if balance.created_on else None,
            })
        
        # Calculate total outstanding for THIS LEASE ONLY
        total_outstanding = sum(b['outstanding'] for b in formatted_balances)
        
        # Format driver details for UI
        driver_details = {
            "driver_id": driver.driver_id,
            "driver_name": driver.full_name,
            "tlc_license": driver.tlc_license.tlc_license_number if driver.tlc_license else "N/A",
        }
        
        # Format lease details for UI
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
            "payment_amount": interim_payment.total_amount
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
async def process_payment_allocation(db: Session, case_no: str, step_data: Dict[str, Any]) -> Dict[str, str]:
    """
    Processes the final allocation of the interim payment (whose details were already
    captured in Step 210). Creates ledger postings and updates balances.
    
    This corresponds to Screen 4 (Confirmation Modal) and Screen 5 (Success) in the Figma flow:
    - User has already entered payment amount, method, date in Step 210
    - User has allocated the payment across outstanding balances
    - This step validates allocations and posts to ledger
    
    All allocations are verified to belong to the lease_id selected in Step 210.
    
    Workflow:
    1. Retrieve the interim payment record created in Step 210 (has payment details)
    2. Extract allocations from step_data
    3. Validate allocations don't exceed payment amount (from Step 210)
    4. Verify all balance_ids belong to the selected lease
    5. Update the interim payment record with allocations
    6. Generate unique payment_id
    7. Apply allocations to ledger (creates CREDIT postings)
    8. Mark BPM case as closed
    9. Create audit trail
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
        
        # Retrieve the interim payment record (has payment details from Step 210)
        interim_payment_service = InterimPaymentService(db)
        interim_payment = interim_payment_service.repo.get_payment_by_id(
            int(case_entity.identifier_value)
        )
        
        if not interim_payment:
            raise HTTPException(
                status_code=404, 
                detail=f"Interim payment record not found with ID {case_entity.identifier_value}"
            )
        
        # CRITICAL: Get payment details and lease/driver from Step 210
        selected_lease_id = interim_payment.lease_id
        selected_driver_id = interim_payment.driver_id
        payment_amount = float(interim_payment.total_amount)
        payment_method = interim_payment.payment_method.value
        payment_date = interim_payment.payment_date
        notes = interim_payment.notes
        
        logger.info(
            f"Processing allocation for driver {selected_driver_id}, "
            f"lease {selected_lease_id}, amount ${payment_amount:.2f}"
        )
        
        # Extract allocations from step_data
        allocations = step_data.get("allocations", [])
        
        # Validation: allocations required
        if not allocations or len(allocations) == 0:
            raise HTTPException(
                status_code=400, 
                detail="At least one allocation is required."
            )
        
        # Validate total allocated amount doesn't exceed payment amount
        total_allocated = sum(float(alloc.get("amount", 0)) for alloc in allocations)
        
        if total_allocated > payment_amount:
            raise HTTPException(
                status_code=400, 
                detail=f"Total allocated amount (${total_allocated:.2f}) cannot exceed payment amount (${payment_amount:.2f})."
            )
        
        # CRITICAL VALIDATION: Verify all allocations belong to the selected lease
        # This prevents applying payments to obligations from other leases
        for alloc in allocations:
            balance_id = alloc.get("balance_id")
            if not balance_id:
                raise HTTPException(
                    status_code=400,
                    detail="Each allocation must include a balance_id."
                )
            
            # Fetch the ledger balance to verify it belongs to the correct lease
            balance = db.query(LedgerBalance).filter(
                LedgerBalance.id == balance_id
            ).first()
            
            if not balance:
                raise HTTPException(
                    status_code=404,
                    detail=f"Ledger balance with ID {balance_id} not found."
                )
            
            # Verify balance belongs to the correct driver and lease
            if balance.driver_id != selected_driver_id or balance.lease_id != selected_lease_id:
                raise HTTPException(
                    status_code=400,
                    detail=f"Balance ID {balance_id} does not belong to the selected lease. Cannot allocate payment."
                )
        
        # Format allocations for the service layer
        formatted_allocations = []
        for alloc in allocations:
            formatted_allocations.append({
                "category": alloc.get("category"),
                "reference_id": alloc.get("reference_id"),
                "amount": float(alloc.get("amount")),
            })
        
        # Update the interim payment record with allocations
        interim_payment.allocations = formatted_allocations
        
        # Generate payment ID if not already set
        if not interim_payment.payment_id:
            interim_payment.payment_id = interim_payment_service._generate_next_payment_id()
        
        db.commit()
        db.refresh(interim_payment)
        
        logger.info(
            f"Updated interim payment {interim_payment.payment_id} "
            f"with {len(formatted_allocations)} allocation(s)"
        )
        
        # Apply allocations to ledger
        # CRITICAL: Pass the specific lease_id to ensure allocations are scoped correctly
        allocation_dict = {
            alloc["reference_id"]: alloc["amount"] 
            for alloc in formatted_allocations
        }
        
        
        from app.ledger.repository import LedgerRepository
        repo = LedgerRepository(db)
        ledger_service = LedgerService(repo)
        ledger_service.apply_interim_payment(
            payment_amount=payment_amount,
            allocations=allocation_dict,
            driver_id=selected_driver_id,
            lease_id=selected_lease_id,  # CRITICAL: Use the specific lease_id
            payment_method=payment_method,
        )
        
        logger.info(
            f"Successfully applied interim payment {interim_payment.payment_id} "
            f"to ledger for lease {selected_lease_id}"
        )

        # ===== NEW: Generate and store receipt =====
        try:
            from app.interim_payments.pdf_service import InterimPaymentPdfService
            from app.utils.s3_utils import s3_utils
            
            logger.info(f"Generating receipt PDF for payment {interim_payment.payment_id}")
            
            # Generate receipt PDF
            pdf_service = InterimPaymentPdfService(db)
            receipt_pdf = pdf_service.generate_receipt_pdf(interim_payment.id)
            
            # Prepare S3 key
            year = datetime.now().year
            month = datetime.now().strftime("%m")
            s3_key = f"interim_payments/receipts/{year}/{month}/{interim_payment.payment_id}.pdf"
            
            # Upload to S3
            pdf_buffer = BytesIO(receipt_pdf)
            upload_success = s3_utils.upload_file(
                file_obj=pdf_buffer,
                key=s3_key,
                content_type="application/pdf"
            )
            
            if upload_success:
                # Update interim payment with S3 key
                interim_payment.receipt_s3_key = s3_key
                db.commit()
                db.refresh(interim_payment)
                logger.info(f"Successfully stored receipt to S3: {s3_key}")
            else:
                logger.warning(f"Failed to upload receipt to S3 for payment {interim_payment.payment_id}")
                
        except Exception as receipt_error:
            # Log error but don't fail the entire transaction
            logger.error(f"Error generating/storing receipt: {str(receipt_error)}", exc_info=True)
        # ===== END: Receipt generation =====
        
        # Mark BPM case as closed
        bpm_service.mark_case_as_closed(db, case_no)
        
        logger.info(f"Marked case {case_no} as closed")
        
        # Create audit trail
        case = bpm_service.get_cases(db=db, case_no=case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Completed interim payment {interim_payment.payment_id} for ${payment_amount:.2f}",
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
            "driver_name": f"{interim_payment.driver.full_name if interim_payment.driver else 'Unknown'}",
            "receipt_url": s3_utils.generate_presigned_url(interim_payment.receipt_s3_key)
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



