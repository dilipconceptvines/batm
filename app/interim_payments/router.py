### app/interim_payments/router.py

import math
from datetime import date, datetime
from io import BytesIO
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.bpm.services import bpm_service
from app.core.db import get_db
from app.core.dependencies import get_db_with_current_user
from app.interim_payments.exceptions import InterimPaymentError
from app.interim_payments.schemas import (
    InterimPaymentResponse,
    PaginatedInterimPaymentResponse,
    InterimPaymentDetailResponse,
)
from app.interim_payments.models import PaymentMethod
from app.interim_payments.services import InterimPaymentService
from app.interim_payments.stubs import create_stub_interim_payments_response
from app.users.models import User
from app.ledger.models import BalanceStatus
from app.users.utils import get_current_user
from app.utils.exporter.excel_exporter import ExcelExporter
from app.utils.exporter.pdf_exporter import PDFExporter
from app.utils.s3_utils import s3_utils
from app.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/payments/interim-payments", tags=["Interim Payments"])

# Dependency to inject the InterimPaymentService
def get_interim_payment_service(db: Session = Depends(get_db)) -> InterimPaymentService:
    """Provides an instance of InterimPaymentService with the current DB session."""
    return InterimPaymentService(db)


def _enrich_with_ledger_status(
    db: Session, 
    allocations: List[dict]
) -> List[dict]:
    """
    Enrich allocation data with current ledger balance status.
    
    Args:
        db: Database session
        allocations: List of allocation dictionaries
        
    Returns:
        List of enriched allocations with ledger_balance_status and is_fully_paid
    """
    from app.ledger.repository import LedgerRepository
    
    ledger_repo = LedgerRepository(db)
    enriched = []
    
    for alloc in allocations:
        reference_id = alloc.get("reference_id")
        
        if reference_id:
            # Get current ledger balance
            balance = ledger_repo.get_balance_by_reference_id(reference_id)
            
            if balance:
                alloc["ledger_balance_status"] = balance.status.value
                alloc["is_fully_paid"] = balance.status == BalanceStatus.CLOSED
            else:
                alloc["ledger_balance_status"] = "NOT_FOUND"
                alloc["is_fully_paid"] = False
        
        enriched.append(alloc)
    
    return enriched


@router.post("/create-case", summary="Create a New Interim Payment Case", status_code=status.HTTP_201_CREATED)
def create_interim_payment_case(
    db: Session = Depends(get_db_with_current_user),
    current_user: User = Depends(get_current_user),
):
    """
    Initiates a new BPM workflow for creating an Interim Payment.
    """
    try:
        new_case = bpm_service.create_case(db, prefix="INTPAY", user=current_user)
        return {
            "message": "New Interim Payment case started successfully.",
            "case_no": new_case.case_no,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error("Error creating interim payment case: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Could not start a new interim payment case.") from e


@router.get("", response_model=PaginatedInterimPaymentResponse, summary="List Interim Payments with Receipts")
def list_interim_payments(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    sort_by: Optional[str] = Query("payment_date"),
    sort_order: str = Query("desc"),
    payment_id: Optional[str] = Query(None),
    driver_name: Optional[str] = Query(None),
    tlc_license: Optional[str] = Query(None),
    lease_id: Optional[str] = Query(None),
    medallion_no: Optional[str] = Query(None),
    payment_date: Optional[date] = Query(None),
    category: Optional[str] = Query(None, description="Filter by allocation category"),
    reference_id: Optional[str] = Query(None, description="Filter by allocation reference ID"),
    amount_from: Optional[float] = Query(None, ge=0, description="Filter by minimum amount"),
    amount_to: Optional[float] = Query(None, ge=0, description="Filter by maximum amount"),
    payment_date_from: Optional[date] = Query(None, description="Filter by payment date from"),
    payment_date_to: Optional[date] = Query(None, description="Filter by payment date to"),
    payment_method: Optional[str] = Query(None, description="Filter by payment method"),
    payment_service: InterimPaymentService = Depends(get_interim_payment_service),
    current_user: User = Depends(get_current_user),
):
    """
    List all interim payments with comprehensive filtering and sorting.
    Includes presigned URLs for receipts.
    """
    try:
        payments, total_items = payment_service.repo.list_payments(
            page=page, per_page=per_page, sort_by=sort_by, sort_order=sort_order,
            payment_id=payment_id, driver_name=driver_name, tlc_license=tlc_license,
            lease_id=lease_id, medallion_no=medallion_no, payment_date=payment_date,
            category=category, reference_id=reference_id,
            amount_from=amount_from, amount_to=amount_to,
            payment_date_from=payment_date_from, payment_date_to=payment_date_to,
            payment_method=payment_method
        )
        
        # Get available values for dropdowns
        available_categories = payment_service.repo.get_available_categories()
        available_payment_methods = [method.value for method in PaymentMethod]
        
        # Flatten the detailed allocation data for the list view
        response_items = []
        for payment in payments:
            if payment.allocations:
                for alloc in payment.allocations:
                    response_items.append(InterimPaymentResponse(
                        payment_id_display=payment.payment_id,
                        tlc_license=payment.driver.tlc_license.tlc_license_number if payment.driver and payment.driver.tlc_license else "N/A",
                        lease_id=payment.lease.lease_id,
                        category=alloc['category'],
                        reference_id=alloc['reference_id'],
                        amount=alloc['amount'],
                        payment_date=payment.payment_date,
                        payment_method=payment.payment_method,
                        receipt_url=s3_utils.generate_presigned_url(payment.receipt_s3_key) if payment.receipt_s3_key else None  # NEW: Include presigned URL
                    ))
        
        total_pages = math.ceil(total_items / per_page) if per_page > 0 else 0
        
        return PaginatedInterimPaymentResponse(
            items=response_items, total_items=total_items, page=page,
            per_page=per_page, total_pages=total_pages,
            available_categories=available_categories,
            available_payment_methods=available_payment_methods
        )
    except Exception as e:
        logger.error("Error fetching interim payments: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while fetching interim payments.")


@router.get("/export", summary="Export Interim Payments Data")
def export_interim_payments(
    export_format: str = Query("excel", enum=["excel", "pdf"]),
    # Pass through all filters from the list endpoint
    sort_by: Optional[str] = Query("payment_date"),
    sort_order: str = Query("desc"),
    payment_id: Optional[str] = Query(None),
    driver_name: Optional[str] = Query(None),
    tlc_license: Optional[str] = Query(None),
    lease_id: Optional[str] = Query(None),
    medallion_no: Optional[str] = Query(None),
    payment_date: Optional[date] = Query(None),
    # New filters
    category: Optional[str] = Query(None, description="Filter by allocation category"),
    reference_id: Optional[str] = Query(None, description="Filter by allocation reference ID"),
    amount_from: Optional[float] = Query(None, ge=0, description="Filter by minimum amount"),
    amount_to: Optional[float] = Query(None, ge=0, description="Filter by maximum amount"),
    payment_date_from: Optional[date] = Query(None, description="Filter by payment date from"),
    payment_date_to: Optional[date] = Query(None, description="Filter by payment date to"),
    payment_method: Optional[str] = Query(None, description="Filter by payment method"),
    payment_service: InterimPaymentService = Depends(get_interim_payment_service),
    current_user: User = Depends(get_current_user),
):
    """
    Exports filtered interim payment data to the specified format.
    """
    try:
        payments, _ = payment_service.repo.list_payments(
            page=1, per_page=10000, sort_by=sort_by, sort_order=sort_order,
            payment_id=payment_id, driver_name=driver_name, tlc_license=tlc_license,
            lease_id=lease_id, medallion_no=medallion_no, payment_date=payment_date,
            # New filters
            category=category, reference_id=reference_id,
            amount_from=amount_from, amount_to=amount_to,
            payment_date_from=payment_date_from, payment_date_to=payment_date_to,
            payment_method=payment_method
        )

        if not payments:
            raise HTTPException(status_code=404, detail="No interim payment data available for export with the given filters.")

        # Flatten the data for export
        export_data = []
        for payment in payments:
            if payment.allocations:
                for alloc in payment.allocations:
                    export_data.append({
                        "Payment ID": payment.payment_id,
                        "TLC License": payment.driver.tlc_license.tlc_license_number if payment.driver and payment.driver.tlc_license else "N/A",
                        "Lease ID": payment.lease.lease_id,
                        "Category": alloc['category'],
                        "Reference ID": alloc['reference_id'],
                        "Amount": float(alloc['amount']),
                        "Payment Date": payment.payment_date.strftime("%Y-%m-%d %H:%M:%S"),
                        "Payment Method": payment.payment_method.value,
                    })
        
        filename = f"interim_payments_{date.today()}.{'xlsx' if export_format == 'excel' else 'pdf'}"
        
        if export_format == "excel":
            exporter = ExcelExporter(export_data)
            file_content = exporter.export()
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        else: # PDF
            exporter = PDFExporter(export_data)
            file_content = exporter.export()
            media_type = "application/pdf"
        
        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        return StreamingResponse(file_content, media_type=media_type, headers=headers)

    except Exception as e:
        logger.error("Error exporting interim payment data: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred during the export process.") from e
    

@router.get("/{payment_id}", response_model=InterimPaymentDetailResponse, summary="Get Interim Payment Details")
def get_interim_payment_by_id(
    payment_id: str,
    payment_service: InterimPaymentService = Depends(get_interim_payment_service),
    current_user: User = Depends(get_current_user),
):
    """
    Get detailed information for a specific interim payment, including receipt URL.
    """
    try:
        payment = payment_service.repo.get_payment_by_payment_id(payment_id)
        
        if not payment:
            raise HTTPException(status_code=404, detail=f"Interim payment not found with ID {payment_id}")
        
        # Build detailed response
        response = InterimPaymentDetailResponse(
            id=payment.id,
            payment_id=payment.payment_id,
            case_no=payment.case_no,
            driver_id=payment.driver_id,
            driver_name=payment.driver.full_name if payment.driver else "N/A",
            tlc_license=payment.driver.tlc_license.tlc_license_number if payment.driver and payment.driver.tlc_license else "N/A",
            lease_id=payment.lease.lease_id if payment.lease else "N/A",
            medallion_number=payment.lease.medallion.medallion_number if payment.lease and payment.lease.medallion else "N/A",
            payment_date=payment.payment_date,
            total_amount=payment.total_amount,
            payment_method=payment.payment_method,
            notes=payment.notes,
            allocations=payment.allocations or [],
            receipt_url=s3_utils.generate_presigned_url(payment.receipt_s3_key) if payment.receipt_s3_key else None,  # NEW: Include presigned URL
            created_on=payment.created_on
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching interim payment {payment_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while fetching interim payment details.") from e
    

@router.get("/{payment_id}/receipt", summary="Download Interim Payment Receipt")
def download_interim_payment_receipt(
    payment_id: str,
    payment_service: InterimPaymentService = Depends(get_interim_payment_service),
    current_user: User = Depends(get_current_user),
):
    """
    Download the receipt PDF for a specific interim payment.
    This endpoint redirects to the presigned S3 URL or generates the PDF on-the-fly if not stored.
    """
    try:
        payment = payment_service.repo.get_payment_by_payment_id(payment_id)
        
        if not payment:
            raise HTTPException(status_code=404, detail=f"Interim payment not found with ID {payment_id}")
        
        # If receipt exists in S3, redirect to presigned URL
        if payment.receipt_s3_key:
            receipt_url = s3_utils.generate_presigned_url(payment.receipt_s3_key) if payment.receipt_s3_key else None
            return RedirectResponse(url=receipt_url) if receipt_url else None
        
        # Otherwise, generate PDF on-the-fly
        from app.interim_payments.pdf_service import InterimPaymentPdfService
        
        pdf_service = InterimPaymentPdfService(payment_service.repo.db)
        pdf_content = pdf_service.generate_receipt_pdf(payment.id)
        
        # Determine content type based on whether we generated PDF or fallback HTML
        is_pdf = pdf_content.startswith(b'%PDF')
        media_type = "application/pdf" if is_pdf else "text/html"
        ext = "pdf" if is_pdf else "html"
        
        filename = f"Receipt_{payment_id}_{datetime.now().strftime('%Y%m%d')}.{ext}"
        
        return StreamingResponse(
            BytesIO(pdf_content),
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading receipt for payment {payment_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to download interim payment receipt") from e


# --- Reset Allocation Endpoint ---

@router.post(
    "/case/{case_no}/reset-allocation",
    summary="Reset allocation to edit payment",
    status_code=status.HTTP_200_OK
)
def reset_allocation_step(
    case_no: str,
    db: Session = Depends(get_db_with_current_user),
    current_user: User = Depends(get_current_user)
):
    """
    Allow user to go back from Step 211 to Step 210 to edit payment details.
    
    **Effect:**
    - Clears any draft allocations (if user had started allocating)
    - Does NOT delete payment record
    - User can modify amount, method, date in Step 210
    - Can proceed to allocation again with new payment details
    
    **Use Case:**
    User entered $600 in Step 210, proceeded to Step 211, then realized
    they meant to enter $650. They click "Edit Payment Details" which
    calls this endpoint, then navigates back to Step 210.
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        
        if not case_entity:
            raise HTTPException(
                status_code=404,
                detail=f"Payment not found for case {case_no}"
            )
        
        # Get payment record
        service = InterimPaymentService(db)
        payment = service.repo.get_payment_by_id(int(case_entity.identifier_value))
        
        if not payment:
            raise HTTPException(
                status_code=404,
                detail="Payment record not found"
            )
        
        # Clear allocations (draft state)
        if payment.allocations:
            logger.info(
                f"Clearing {len(payment.allocations)} allocations for case {case_no}",
                payment_id=payment.payment_id,
                user_id=current_user.id
            )
            payment.allocations = []
            db.commit()
        
        return {
            "message": "Ready to edit payment details",
            "case_no": case_no,
            "payment_id": payment.payment_id,
            "next_step": "210"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resetting allocation for case {case_no}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to reset allocation"
        )


# --- Void Payment Endpoint ---

from pydantic import BaseModel, Field
from app.interim_payments.exceptions import InterimPaymentNotFoundError, InvalidOperationError


class VoidPaymentRequest(BaseModel):
    """Request schema for voiding a payment"""
    reason: str = Field(
        ...,
        min_length=10,
        max_length=500,
        description="Reason for voiding the payment (minimum 10 characters)",
        example="Payment entered for wrong driver - should be John Smith not John Doe"
    )


class VoidPaymentResponse(BaseModel):
    """Response schema after voiding a payment"""
    message: str
    payment_id: str
    status: str
    voided_at: str
    voided_by: int
    reason: str
    postings_reversed: int


@router.post(
    "/void/{payment_id}",
    summary="Void an Interim Payment",
    response_model=VoidPaymentResponse,
    status_code=status.HTTP_200_OK
)
def void_interim_payment(
    payment_id: str,
    void_request: VoidPaymentRequest,
    db: Session = Depends(get_db_with_current_user),
    current_user: User = Depends(get_current_user),
    service: InterimPaymentService = Depends(get_interim_payment_service)
):
    """
    Void an interim payment by reversing all associated ledger postings.

    **Required Permission:** `void_interim_payment`

    **Effect:**
    - All ledger postings reversed (creates opposite DEBIT/CREDIT postings)
    - Ledger balances reopened to original amounts
    - Payment status changed to VOIDED
    - Source modules notified (repair installments, loan installments revert to POSTED)
    - Audit trail created

    **Cannot be undone** - creates permanent reversal record

    **Example:**
    ```json
    {
      "reason": "Payment entered for wrong driver - should be Driver #123"
    }
    ```
    """
    # Check permissions
    if not current_user.has_permission("void_interim_payment"):
        raise HTTPException(
            status_code=403,
            detail="Insufficient permissions to void interim payments. Contact your administrator."
        )

    try:
        # Void the payment
        voided_payment = service.void_interim_payment(
            payment_id=payment_id,
            reason=void_request.reason,
            user_id=current_user.id
        )

        # Count reversed postings
        postings_count = len(voided_payment.allocations) if voided_payment.allocations else 0

        return VoidPaymentResponse(
            message="Interim payment voided successfully",
            payment_id=payment_id,
            status=voided_payment.status.value,
            voided_at=voided_payment.voided_at.isoformat(),
            voided_by=voided_payment.voided_by,
            reason=voided_payment.void_reason,
            postings_reversed=postings_count
        )

    except InterimPaymentNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"Payment {payment_id} not found"
        )
    except InvalidOperationError as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error voiding payment {payment_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to void payment. Please contact support."
        )