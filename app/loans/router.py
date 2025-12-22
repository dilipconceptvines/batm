### app/loans/router.py

import math
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional , List
from io import BytesIO

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.bpm.services import bpm_service
from app.core.db import get_db
from app.core.dependencies import get_db_with_current_user
from app.loans.exceptions import LoanNotFoundError
from app.loans.schemas import (
    DriverLoanDetailResponse,
    DriverLoanListResponse,
    LoanInstallmentResponse,
    PaginatedDriverLoanResponse,
    LoanInstallmentListResponse,
    PaginatedLoanInstallmentResponse,
    PostInstallmentRequest,
    PostInstallmentResponse,
    InstallmentPostingResult
)
from app.loans.repository import LoanRepository
from app.loans.services import LoanService
from app.loans.models import LoanInstallmentStatus
from app.loans.stubs import create_stub_loan_response, create_stub_installment_response
from app.leases.schemas import LeaseType
from app.users.models import User
from app.users.utils import get_current_user
from app.utils.exporter_utils import ExporterFactory
from app.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/payments/driver-loans", tags=["Driver Loans"])

# Dependency to inject the LoanService
def get_loan_service(db: Session = Depends(get_db)) -> LoanService:
    """Dependency to get LoanService instance."""
    return LoanService(db)

@router.get("", summary="List All Driver Loans", response_model=PaginatedDriverLoanResponse)
def list_driver_loans(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    sort_by: str = Query("loan_date", regex="^(loan_id|loan_date|start_week|principal_amount|status|driver_name|medallion_no)$"),
    sort_order: str = Query("desc", regex="^(asc|desc)$"),
    tlc_number: Optional[str] = Query(None),
    lease_id: Optional[str] = Query(None),
    loan_id: Optional[str] = Query(None),
    status: Optional[List[str]] = Query(None),
    driver_name: Optional[str] = Query(None),
    medallion_no: Optional[str] = Query(None),
    lease_type: Optional[str] = Query(None),
    start_week_from: Optional[date] = Query(None),
    start_week_to: Optional[date] = Query(None),
    min_principal: Optional[float] = Query(None),
    max_principal: Optional[float] = Query(None),
    min_interest_rate: Optional[float] = Query(None),
    max_interest_rate: Optional[float] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    List all driver loans with comprehensive filtering, sorting, and pagination.
    
    NEW: Includes receipt_url in response for direct access to loan receipts
    """
    try:
        repo = LoanRepository(db)
        
        loans, total_items = repo.list_loans(
            page=page, per_page=per_page, sort_by=sort_by, sort_order=sort_order,
            tlc_number=tlc_number, lease_id=lease_id, loan_id=loan_id,
            status=status, driver_name=driver_name, medallion_no=medallion_no,
            lease_type=lease_type, start_week_from=start_week_from,
            start_week_to=start_week_to, min_principal=min_principal,
            max_principal=max_principal, min_interest_rate=min_interest_rate,
            max_interest_rate=max_interest_rate,
        )

        # Build response items with enhanced details including receipt URL
        response_items = []
        for loan in loans:
            # Extract driver details
            driver_id = loan.driver.driver_id if loan.driver else None
            driver_name = loan.driver.full_name if loan.driver else None
            tlc_license = (
                loan.driver.tlc_license.tlc_license_number 
                if loan.driver and loan.driver.tlc_license 
                else None
            )
            
            # Extract medallion details
            medallion_no = loan.medallion.medallion_number if loan.medallion else None
            owner_name = None
            if loan.medallion and loan.medallion.owner:
                medallion_dict = loan.medallion.to_dict()
                owner_name = medallion_dict["owner_name"]
            medallion_owner = owner_name
            
            # Extract lease type
            lease_type_value = loan.lease.lease_type if loan.lease else None
            
            # NEW: Get fresh presigned URL for receipt
            receipt_url = loan.presigned_receipt_url
            
            item = DriverLoanListResponse(
                loan_id=loan.loan_id,
                status=loan.status,
                driver_id=driver_id,
                driver_name=driver_name,
                tlc_license=tlc_license,
                medallion_no=medallion_no,
                medallion_owner=medallion_owner,
                lease_type=lease_type_value,
                principal_amount=loan.principal_amount,
                interest_rate=loan.interest_rate,
                start_week=loan.start_week,
                receipt_url=receipt_url,  # NEW: Include receipt URL
            )
            response_items.append(item)
        
        total_pages = math.ceil(total_items / per_page) if per_page > 0 else 0

        # Get available lease types for frontend reference
        lease_types = LeaseType.values()

        return PaginatedDriverLoanResponse(
            items=response_items, 
            total_items=total_items, 
            page=page,
            per_page=per_page, 
            total_pages=total_pages,
            lease_types=lease_types
        )
    except Exception as e:
        logger.error("Error fetching driver loans: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail="An error occurred while fetching driver loans."
        ) from e

@router.post("/create-case", summary="Create a New Driver Loan Case", status_code=status.HTTP_201_CREATED)
def create_driver_loan_case(
    db: Session = Depends(get_db_with_current_user),
    current_user: User = Depends(get_current_user),
):
    """
    Initiates a new BPM workflow for manually creating a Driver Loan.
    """
    try:
        new_case = bpm_service.create_case(db, prefix="DRLNS", user=current_user)
        return {
            "message": "New Create Driver Loan case started successfully.",
            "case_no": new_case.case_no,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error("Error creating driver loan case: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Could not start a new driver loan case.") from e


@router.get("/{loan_id}", summary="Get Driver Loan Details", response_model=DriverLoanDetailResponse)
def get_driver_loan_details(
    loan_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Retrieve detailed information about a single driver loan, including its installment schedule.
    
    NEW: Includes receipt_url in response for direct access to loan receipt
    """
    try:
        repo = LoanRepository(db)
        loan = repo.get_loan_by_loan_id(loan_id)
        
        if not loan:
            raise HTTPException(status_code=404, detail=f"Driver loan not found with ID {loan_id}")
        
        # Format installments
        installment_list = []
        for inst in loan.installments:
            installment_list.append(LoanInstallmentResponse(
                installment_id=inst.installment_id,
                week_start_date=inst.week_start_date,
                week_end_date=inst.week_end_date,
                principal_amount=inst.principal_amount,
                interest_amount=inst.interest_amount,
                total_due=inst.total_due,
                status=inst.status,
                ledger_posting_id=inst.ledger_posting_ref,
            ))
        
        # Extract driver details
        driver_id = loan.driver.driver_id if loan.driver else None
        driver_name = loan.driver.full_name if loan.driver else None
        tlc_license = (
            loan.driver.tlc_license.tlc_license_number 
            if loan.driver and loan.driver.tlc_license 
            else None
        )
        
        # Extract medallion details
        medallion_no = loan.medallion.medallion_number if loan.medallion else None
        owner_name = None
        if loan.medallion and loan.medallion.owner:
            medallion_dict = loan.medallion.to_dict()
            owner_name = medallion_dict["owner_name"]
        medallion_owner = owner_name
        
        # Extract lease type
        lease_type_value = loan.lease.lease_type if loan.lease else None
        
        # NEW: Get fresh presigned URL for receipt
        receipt_url = loan.presigned_receipt_url
        
        return DriverLoanDetailResponse(
            loan_id=loan.loan_id,
            status=loan.status,
            driver_id=driver_id,
            driver_name=driver_name,
            tlc_license=tlc_license,
            medallion_no=medallion_no,
            medallion_owner=medallion_owner,
            lease_type=lease_type_value,
            principal_amount=loan.principal_amount,
            interest_rate=loan.interest_rate,
            loan_date=loan.loan_date,
            start_week=loan.start_week,
            notes=loan.notes,
            installments=installment_list,
            receipt_url=receipt_url,  # NEW: Include receipt URL
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error fetching driver loan details: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail="An error occurred while fetching driver loan details."
        ) from e
    

@router.get("/{loan_id}/receipt", summary="Download Driver Loan Receipt")
def download_loan_receipt(
    loan_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Download the receipt PDF for a specific driver loan.
    This endpoint redirects to the presigned S3 URL or generates the PDF on-the-fly if not stored.
    
    NEW ENDPOINT: Provides direct access to loan receipt PDFs
    """
    try:
        repo = LoanRepository(db)
        loan = repo.get_loan_by_loan_id(loan_id)
        
        if not loan:
            raise HTTPException(status_code=404, detail=f"Driver loan not found with ID {loan_id}")
        
        # If receipt exists in S3, redirect to presigned URL
        if loan.receipt_s3_key and loan.presigned_receipt_url:
            return RedirectResponse(url=loan.presigned_receipt_url)
        
        # Otherwise, generate PDF on-the-fly
        from app.loans.pdf_service import LoanPdfService
        
        pdf_service = LoanPdfService(db)
        pdf_content = pdf_service.generate_receipt_pdf(loan.id)
        
        # Determine content type based on whether we generated PDF or fallback HTML
        is_pdf = pdf_content.startswith(b'%PDF')
        media_type = "application/pdf" if is_pdf else "text/html"
        ext = "pdf" if is_pdf else "html"
        
        filename = f"Loan_Receipt_{loan_id}_{date.today()}.{ext}"
        
        return StreamingResponse(
            BytesIO(pdf_content),
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading receipt for loan {loan_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to download loan receipt") from e


@router.get("/loans/export", summary="Export Driver Loan Data")
def export_loans(
    format: str = Query("excel", enum=["excel", "pdf", "csv"]),
    sort_by: Optional[str] = Query("start_week"),
    sort_order: str = Query("desc"),
    tlc_number: Optional[str] = Query(None),
    lease_id: Optional[str] = Query(None),
    loan_id: Optional[str] = Query(None),
    status: Optional[List[str]] = Query(None),
    driver_name: Optional[str] = Query(None),
    medallion_no: Optional[str] = Query(None),
    lease_type: Optional[str] = Query(None),
    start_week_from: Optional[date] = Query(None, description="Export loans starting from this date"),
    start_week_to: Optional[date] = Query(None, description="Export loans starting up to this date"),
    min_principal: Optional[Decimal] = Query(None, ge=0),
    max_principal: Optional[Decimal] = Query(None, ge=0),
    min_interest_rate: Optional[Decimal] = Query(None, ge=0, le=100),
    max_interest_rate: Optional[Decimal] = Query(None, ge=0, le=100),
    loan_service: LoanService = Depends(get_loan_service),
    current_user: User = Depends(get_current_user),
):
    """
    Exports filtered driver loan data to the specified format (Excel, PDF, or CSV).
    
    Supports all the same filters as the list endpoint for consistent data export.
    """
    try:
        loans, _ = loan_service.repo.list_loans(
            page=1, 
            per_page=10000, 
            sort_by=sort_by, 
            sort_order=sort_order,
            tlc_number=tlc_number,
            lease_id=lease_id,
            loan_id=loan_id, 
            status=status, 
            driver_name=driver_name,
            medallion_no=medallion_no, 
            lease_type=lease_type, 
            start_week_from=start_week_from,
            start_week_to=start_week_to,
            min_principal=min_principal,
            max_principal=max_principal,
            min_interest_rate=min_interest_rate,
            max_interest_rate=max_interest_rate,
        )

        if not loans:
            raise HTTPException(
                status_code=404, 
                detail="No loan data available for export with the given filters."
            )

        # Build export data with enhanced details
        export_data = []
        for loan in loans:
            owner_name = None
            if loan.medallion.owner:
                medallion_dict = loan.medallion.to_dict()
                owner_name = medallion_dict["owner_name"]
            export_data.append({
                "Loan ID": loan.loan_id,
                "Status": loan.status.value if hasattr(loan.status, 'value') else str(loan.status),
                "Driver ID": loan.driver.driver_id if loan.driver else "",
                "Driver Name": loan.driver.full_name if loan.driver else "",
                "TLC License": (
                    loan.driver.tlc_license.tlc_license_number 
                    if loan.driver and loan.driver.tlc_license 
                    else ""
                ),
                "Medallion Number": loan.medallion.medallion_number if loan.medallion else "",
                "Medallion Owner": owner_name,
                "Lease Type": loan.lease.lease_type if loan.lease else "",
                "Principal Amount": float(loan.principal_amount),
                "Interest Rate (%)": float(loan.interest_rate),
                "Start Week": loan.start_week.isoformat() if loan.start_week else "",
            })
        
        # Use ExporterFactory to get the appropriate exporter
        exporter = ExporterFactory.get_exporter(format, export_data)
        file_content = exporter.export()
        
        # Set filename and media type based on format
        file_extensions = {
            "excel": "xlsx",
            "pdf": "pdf",
            "csv": "csv"
        }
        
        media_types = {
            "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "pdf": "application/pdf",
            "csv": "text/csv"
        }
        
        filename = f"driver_loans_{date.today()}.{file_extensions.get(format, 'xlsx')}"
        media_type = media_types.get(format, "application/octet-stream")
        
        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        return StreamingResponse(file_content, media_type=media_type, headers=headers)

    except ValueError as e:
        # Catch invalid format errors from ExporterFactory
        logger.error("Invalid export format: %s", e, exc_info=True)
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid export format: {str(e)}"
        ) from e
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logger.error("Error exporting loan data: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail="An error occurred during the export process."
        ) from e
    

@router.get("/fetch/installments", response_model=PaginatedLoanInstallmentResponse, summary="List Loan Installments")
def list_loan_installments(
    use_stubs: bool = Query(False, description="Return stubbed data for testing."),
    page: int = Query(1, ge=1, description="Page number for pagination."),
    per_page: int = Query(10, ge=1, le=100, description="Items per page."),
    sort_by: Optional[str] = Query("week_start_date", description="Field to sort by."),
    sort_order: str = Query("desc", enum=["asc", "desc"]),
    loan_id: Optional[str] = Query(None, description="Filter by Loan ID."),
    lease_id: Optional[int] = Query(None, description="Filter by Lease ID."),
    driver_id: Optional[int] = Query(None, description="Filter by Driver ID."),
    medallion_id: Optional[int] = Query(None, description="Filter by Medallion ID."),
    vehicle_id: Optional[int] = Query(None, description="Filter by Vehicle ID."),
    status: Optional[str] = Query(None, description="Filter by Installment Status."),
    loan_service: LoanService = Depends(get_loan_service),
    current_user: User = Depends(get_current_user),
):
    """
    Retrieves a paginated, sorted, and filtered list of loan installments.
    Supports filtering by loan_id, lease_id, driver_id, medallion_id, vehicle_id, or status.
    """
    logger.info("Using stubs: **** %s", use_stubs)
    if use_stubs:
        return create_stub_installment_response(page, per_page)
    
    try:
        installments, total_items = loan_service.repo.list_installments(
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order,
            loan_id=loan_id,
            lease_id=lease_id,
            driver_id=driver_id,
            medallion_id=medallion_id,
            vehicle_id=vehicle_id,
            status=status,
        )

        response_items = []
        for inst in installments:
            response_items.append(
                LoanInstallmentListResponse(
                    installment_id=inst.installment_id,
                    loan_id=inst.loan.loan_id,
                    driver_name=inst.loan.driver.full_name if inst.loan.driver else None,
                    medallion_no=inst.loan.medallion.medallion_number if inst.loan.medallion else None,
                    lease_id=inst.loan.lease.lease_id if inst.loan.lease else None,
                    vehicle_id=inst.loan.lease.vehicle_id if inst.loan.lease else None,
                    week_start_date=inst.week_start_date,
                    week_end_date=inst.week_end_date,
                    principal_amount=inst.principal_amount,
                    interest_amount=inst.interest_amount,
                    total_due=inst.total_due,
                    status=inst.status,
                    posted_on=inst.posted_on.date() if inst.posted_on else None,
                    ledger_posting_ref=inst.ledger_posting_ref,
                )
            )
        
        total_pages = math.ceil(total_items / per_page) if per_page > 0 else 0

        return PaginatedLoanInstallmentResponse(
            items=response_items,
            total_items=total_items,
            page=page,
            per_page=per_page,
            total_pages=total_pages
        )
    except Exception as e:
        logger.error("Error fetching loan installments: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while fetching loan installments.") from e
    

@router.post(
    "/installments/post-to-ledger",
    response_model=PostInstallmentResponse,
    status_code=status.HTTP_200_OK,
    summary="Post Loan Installments to Ledger"
)
def post_loan_installments_to_ledger(
    request: PostInstallmentRequest,
    loan_service: LoanService = Depends(get_loan_service),
    current_user: User = Depends(get_current_user),
):
    """
    Manually posts loan installments to the centralized ledger.

    **Example Requests:**
    
    ```json
    // Post specific installments
    {
      "installment_ids": ["DLN-2025-001-01", "DLN-2025-001-02", "DLN-2025-002-01"]
    }
    
    // Post all due installments
    {
      "post_all_due": true
    }
    ```
    """
    logger.info(
        f"User {current_user.id} initiated manual loan installment posting. "
        f"Mode: {'all_due' if request.post_all_due else 'specific_ids'}"
    )

    try:
        # Validate request
        if not request.installment_ids and not request.post_all_due:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Either provide installment_ids or set post_all_due=True"
            )
        
        if request.installment_ids and request.post_all_due:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot specify both installment_ids and post_all_due. Choose one mode."
            )
        
        # Post installments
        results, successful_count, failed_count = loan_service.post_installments_to_ledger(
            installment_ids=request.installment_ids,
            post_all_due=request.post_all_due,
        )

        # Build response message
        total_processed = successful_count + failed_count
        if failed_count == 0:
            message = f"Successfully posted all {successful_count} installments to ledger."
        elif successful_count == 0:
            message = f"Failed to post all {failed_count} installments"
        else:
            message = f"Posted {successful_count} out of {total_processed} installments. {failed_count} failed."

        logger.info(
            f"Loan installment posting completed. "
            f"Success: {successful_count}, Failed: {failed_count}"
        )

        return PostInstallmentResponse(
            total_processed=total_processed,
            successful_posts=successful_count,
            failed_posts=failed_count,
            results=results,
            message=message
        )
    
    except ValueError as ve:
        logger.warning(f"Validation error in loan installment posting: {ve}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(ve)
        ) from ve
    except Exception as e:
        logger.error(
            f"Unexpected error during loan installment posting: {e}",
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while posting loan installments to ledger."
        ) from e


@router.get(
    "/installments/postable",
    response_model=PaginatedLoanInstallmentResponse,
    summary="Get Postable Loan Installments"
)
def get_postable_installments(
    page: int = Query(1, ge=1, description="page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    loan_service: LoanService = Depends(get_loan_service),
    current_user: User = Depends(get_current_user),
):
    """
    Retrieves all loan installments that are eligible for posting to the ledger.
    """
    try:
        installments = loan_service.repo.get_due_installments_to_post(
            datetime.now(timezone.utc).date()
        )

        # Apply pagination
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_items = installments[start_idx:end_idx]

        response_items = []
        for inst in paginated_items:
            response_items.append(
                LoanInstallmentListResponse(
                    installment_id=inst.installment_id,
                    loan_id=inst.loan.loan_id,
                    driver_name=inst.loan.driver.full_name if inst.loan.driver else None,
                    medallion_no=inst.loan.medallion.medallion_number if inst.loan.medallion else None,
                    lease_id=inst.loan.lease.lease_id if inst.loan.lease else None,
                    vehicle_id=inst.loan.lease.vehicle_id if inst.loan.lease else None,
                    week_start_date=inst.week_start_date,
                    week_end_date=inst.week_end_date,
                    principal_amount=inst.principal_amount,
                    interest_amount=inst.interest_amount,
                    total_due=inst.total_due,
                    status=inst.status,
                    posted_on=inst.posted_on.date() if inst.posted_on else None,
                    ledger_posting_ref=inst.ledger_posting_ref,
                )
            )
        
        total_items = len(installments)
        total_pages = math.ceil(total_items / per_page) if per_page > 0 else 0
        
        return PaginatedLoanInstallmentResponse(
            items=response_items,
            total_items=total_items,
            page=page,
            per_page=per_page,
            total_pages=total_pages
        )
        
    except Exception as e:
        logger.error(f"Error fetching postable installments: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while fetching postable installments"
        ) from e
    