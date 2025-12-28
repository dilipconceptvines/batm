# app/deposits/router.py

import math
from datetime import date, datetime
from decimal import Decimal
from typing import Optional, List
from io import BytesIO

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.core.dependencies import get_db_with_current_user
from app.deposits.exceptions import (
    DepositError,
    DepositNotFoundError,
    DepositValidationError,
    InvalidDepositOperationError,
)
from app.deposits.schemas import (
    DepositResponse,
    DepositListResponse,
    PaginatedDepositResponse,
    DepositUpdateRequest,
    DepositRefundRequest,
    DepositApplicationSummary,
)
from app.deposits.services import DepositService
from app.deposits.models import DepositStatus, CollectionMethod
from app.ledger.services import LedgerService
from app.users.models import User
from app.users.utils import get_current_user
from app.utils.exporter_utils import ExporterFactory
from app.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/deposits", tags=["Security Deposits"])

# Dependency to inject the DepositService
def get_deposit_service(db: Session = Depends(get_db)) -> DepositService:
    """Provides an instance of DepositService with the current DB session."""
    return DepositService(db)


@router.get("", response_model=PaginatedDepositResponse, summary="List Security Deposits")
def list_deposits(
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    sort_by: str = Query("created_on", regex="^(deposit_id|lease_id|required_amount|collected_amount|deposit_status|created_on)$"),
    sort_order: str = Query("desc", enum=["asc", "desc"]),
    status: Optional[DepositStatus] = Query(None),
    lease_id: Optional[int] = Query(None),
    driver_tlc_license: Optional[str] = Query(None),
    deposit_service: DepositService = Depends(get_deposit_service),
    current_user: User = Depends(get_current_user),
):
    """
    Retrieves a paginated and filterable list of all security deposits.
    """
    try:
        # Get filtered deposits from repository
        deposits, total_items = deposit_service.repo.list_deposits(
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order,
            status=status,
            lease_id=lease_id,
            driver_tlc_license=driver_tlc_license,
        )

        # Convert to response models
        items = []
        for deposit in deposits:
            items.append(
                DepositListResponse(
                    deposit_id=deposit.deposit_id,
                    lease_id=deposit.lease_id,
                    driver_tlc_license=deposit.driver_tlc_license,
                    required_amount=deposit.required_amount,
                    collected_amount=deposit.collected_amount,
                    deposit_status=deposit.deposit_status,
                    lease_start_date=deposit.lease_start_date,
                    hold_expiry_date=deposit.hold_expiry_date,
                )
            )

        total_pages = math.ceil(total_items / per_page) if total_items > 0 else 0

        return PaginatedDepositResponse(
            items=items,
            total_items=total_items,
            page=page,
            per_page=per_page,
            total_pages=total_pages,
        )

    except DepositError as e:
        logger.warning("Business logic error in list_deposits: %s", e)
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error("Error listing deposits: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred while retrieving deposits.",
        ) from e


@router.get("/{deposit_id}", response_model=DepositResponse, summary="Get Deposit Details")
def get_deposit_details(
    deposit_id: str,
    deposit_service: DepositService = Depends(get_deposit_service),
    current_user: User = Depends(get_current_user),
):
    """
    Retrieve detailed information about a single security deposit.
    """
    try:
        deposit = deposit_service.repo.get_by_deposit_id(deposit_id)
        if not deposit:
            raise HTTPException(
                status_code=404,
                detail=f"Security deposit not found with ID {deposit_id}"
            )

        # Return the full deposit details (from_attributes=True will handle conversion)
        return deposit

    except HTTPException:
        raise
    except DepositError as e:
        logger.warning("Business logic error in get_deposit_details: %s", e)
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error("Error getting deposit details for %s: %s", deposit_id, e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred while retrieving deposit details.",
        ) from e


@router.get("/lease/{lease_id}", response_model=DepositResponse, summary="Get Deposit by Lease ID")
def get_deposit_by_lease(
    lease_id: int,
    deposit_service: DepositService = Depends(get_deposit_service),
    current_user: User = Depends(get_current_user),
):
    """
    Retrieve the security deposit associated with a specific lease.
    """
    try:
        deposit = deposit_service.repo.get_by_lease_id(lease_id)
        if not deposit:
            raise HTTPException(
                status_code=404,
                detail=f"No security deposit found for lease ID {lease_id}"
            )

        return deposit

    except HTTPException:
        raise
    except DepositError as e:
        logger.warning("Business logic error in get_deposit_by_lease: %s", e)
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error("Error getting deposit for lease %s: %s", lease_id, e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred while retrieving deposit for lease.",
        ) from e


@router.post("/{deposit_id}/collect", response_model=DepositResponse, summary="Record Deposit Collection")
def collect_deposit_payment(
    deposit_id: str,
    request: DepositUpdateRequest,
    db: Session = Depends(get_db_with_current_user),
    deposit_service: DepositService = Depends(get_deposit_service),
    current_user: User = Depends(get_current_user),
):
    """
    Record an additional payment/collection for an existing security deposit.
    Creates a ledger posting for the collection amount.
    """
    try:
        # Update deposit collection
        updated_deposit = deposit_service.update_deposit_collection(
            db=db,
            deposit_id=deposit_id,
            additional_amount=request.additional_amount,
            collection_method=request.collection_method,
            notes=request.notes,
        )

        logger.info(
            "Deposit collection recorded",
            deposit_id=deposit_id,
            amount=float(request.additional_amount),
            user_id=current_user.id
        )

        return updated_deposit

    except DepositNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except (DepositValidationError, InvalidDepositOperationError) as e:
        logger.warning("Validation error in collect_deposit_payment: %s", e)
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error("Error recording deposit collection for %s: %s", deposit_id, e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred while recording the deposit collection.",
        ) from e


@router.post("/{deposit_id}/apply", response_model=DepositApplicationSummary, summary="Apply Deposit to Obligations")
def apply_deposit_to_obligations(
    deposit_id: str,
    db: Session = Depends(get_db_with_current_user),
    deposit_service: DepositService = Depends(get_deposit_service),
    current_user: User = Depends(get_current_user),
):
    """
    Manually apply a held security deposit to outstanding obligations before hold expiry.
    Requires appropriate authorization.
    """
    try:
        # TODO: Add authorization check for manual application
        # This should typically require admin or manager role

        # Initialize ledger service for application
        ledger_service = LedgerService(db)

        # Apply deposit to obligations
        application_summary = deposit_service.auto_apply_deposit(
            db=db,
            deposit_id=deposit_id,
            ledger_service=ledger_service,
        )

        logger.info(
            "Deposit manually applied to obligations",
            deposit_id=deposit_id,
            total_applied=float(application_summary['total_applied']),
            user_id=current_user.id
        )

        return DepositApplicationSummary(**application_summary)

    except DepositNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except InvalidDepositOperationError as e:
        logger.warning("Operation error in apply_deposit_to_obligations: %s", e)
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error("Error applying deposit %s to obligations: %s", deposit_id, e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred while applying the deposit to obligations.",
        ) from e


@router.post("/{deposit_id}/refund", response_model=DepositResponse, summary="Process Deposit Refund")
def process_deposit_refund(
    deposit_id: str,
    request: DepositRefundRequest,
    db: Session = Depends(get_db_with_current_user),
    deposit_service: DepositService = Depends(get_deposit_service),
    current_user: User = Depends(get_current_user),
):
    """
    Process a refund for a held security deposit.
    Creates a ledger posting for the refund transaction.
    """
    try:
        # Initialize ledger service for refund
        ledger_service = LedgerService(db)

        # Process the refund
        updated_deposit = deposit_service.process_refund(
            db=db,
            deposit_id=deposit_id,
            refund_method=request.refund_method,
            refund_reference=request.refund_reference,
            ledger_service=ledger_service,
            user_id=current_user.id,
        )

        logger.info(
            "Deposit refund processed",
            deposit_id=deposit_id,
            refund_amount=float(updated_deposit.refund_amount),
            user_id=current_user.id
        )

        return updated_deposit

    except DepositNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except InvalidDepositOperationError as e:
        logger.warning("Operation error in process_deposit_refund: %s", e)
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error("Error processing refund for deposit %s: %s", deposit_id, e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred while processing the deposit refund.",
        ) from e


@router.get("/pending-reminders", response_model=List[DepositListResponse], summary="Get Deposits Needing Reminders")
def get_pending_reminders(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Retrieve security deposits that need reminder notifications.
    Admin access required.
    """
    try:
        # TODO: Add admin authorization check
        # if not current_user.has_role('admin'):
        #     raise HTTPException(status_code=403, detail="Admin access required")

        deposit_service = DepositService(db)

        # Get deposits that need reminders (unpaid deposits older than certain period)
        reminder_deposits = deposit_service.repo.get_unpaid_deposits_by_start_date(
            start_date=datetime.now().date()  # This would need to be adjusted for actual reminder logic
        )

        # Convert to response models
        items = []
        for deposit in reminder_deposits:
            items.append(
                DepositListResponse(
                    deposit_id=deposit.deposit_id,
                    lease_id=deposit.lease_id,
                    driver_tlc_license=deposit.driver_tlc_license,
                    required_amount=deposit.required_amount,
                    collected_amount=deposit.collected_amount,
                    deposit_status=deposit.deposit_status,
                    lease_start_date=deposit.lease_start_date,
                    hold_expiry_date=deposit.hold_expiry_date,
                )
            )

        return items

    except Exception as e:
        logger.error("Error getting pending reminders: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred while retrieving pending reminders.",
        ) from e


@router.get("/export", summary="Export Deposits Data")
def export_deposits(
    format: str = Query("excel", enum=["excel", "csv"]),
    status: Optional[DepositStatus] = Query(None),
    lease_id: Optional[int] = Query(None),
    driver_tlc_license: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Export security deposits data to Excel or CSV format.
    """
    try:
        deposit_service = DepositService(db)

        # Get filtered deposits (without pagination)
        deposits, _ = deposit_service.repo.list_deposits(
            page=1,
            per_page=10000,  # Large number to get all records
            sort_by="created_on",
            sort_order="desc",
            status=status,
            lease_id=lease_id,
            driver_tlc_license=driver_tlc_license,
        )

        if not deposits:
            raise HTTPException(
                status_code=404,
                detail="No deposit data available for export with the given filters."
            )

        # Build export data
        export_data = []
        for deposit in deposits:
            export_data.append({
                "Deposit ID": deposit.deposit_id,
                "Lease ID": deposit.lease_id,
                "Driver TLC License": deposit.driver_tlc_license or "",
                "Vehicle VIN": deposit.vehicle_vin or "",
                "Vehicle Plate": deposit.vehicle_plate or "",
                "Required Amount": float(deposit.required_amount),
                "Collected Amount": float(deposit.collected_amount),
                "Outstanding Amount": float(deposit.outstanding_amount),
                "Status": deposit.deposit_status.value,
                "Collection Method": deposit.collection_method.value if deposit.collection_method else "",
                "Lease Start Date": deposit.lease_start_date.isoformat() if deposit.lease_start_date else "",
                "Lease Termination Date": deposit.lease_termination_date.isoformat() if deposit.lease_termination_date else "",
                "Hold Expiry Date": deposit.hold_expiry_date.isoformat() if deposit.hold_expiry_date else "",
                "Refund Amount": float(deposit.refund_amount) if deposit.refund_amount else "",
                "Refund Date": deposit.refund_date.isoformat() if deposit.refund_date else "",
                "Created On": deposit.created_on.isoformat() if deposit.created_on else "",
                "Notes": deposit.notes or "",
            })

        # Use ExporterFactory to get the appropriate exporter
        exporter = ExporterFactory.get_exporter(format, export_data)
        file_content = exporter.export()

        # Set filename and media type based on format
        file_extensions = {
            "excel": "xlsx",
            "csv": "csv"
        }

        media_types = {
            "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "csv": "text/csv"
        }

        filename = f"security_deposits_{date.today()}.{file_extensions.get(format, 'xlsx')}"
        media_type = media_types.get(format, "application/octet-stream")

        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        return StreamingResponse(file_content, media_type=media_type, headers=headers)

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error exporting deposits: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred while exporting deposit data.",
        ) from e