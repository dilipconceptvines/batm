# app/ledger/router.py

import math
from datetime import date
from io import BytesIO
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse

from app.core.dependencies import get_db_with_current_user
from app.ledger.exceptions import LedgerError, PostingNotFoundError, InvalidLedgerOperationError
from app.ledger.models import BalanceStatus, EntryType, PostingCategory, PostingStatus
from app.ledger.schemas import (
    PaginatedLedgerBalanceResponse,
    PaginatedLedgerPostingResponse,
    VoidPostingRequest,
)
from app.ledger.services import LedgerService
from app.ledger.stubs import (
    create_stub_balance_response,
    create_stub_posting_response,
)
from app.users.models import User
from app.users.utils import get_current_user
from app.utils.exporter_utils import ExporterFactory
from app.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/ledger", tags=["Ledger"])


@router.get(
    "/balances",
    response_model=PaginatedLedgerBalanceResponse,
    summary="List Ledger Balances",
)
def list_ledger_balances(
    use_stubs: bool = Query(False, description="Return stubbed data for testing."),
    page: int = Query(1, ge=1, description="Page number for pagination."),
    per_page: int = Query(10, ge=1, le=100, description="Items per page."),
    sort_by: Optional[str] = Query(None, description="Field to sort by."),
    sort_order: str = Query("desc", enum=["asc", "desc"]),
    driver_name: Optional[str] = Query(None, description="Filter by Driver Name."),
    lease_id: Optional[int] = Query(None, description="Filter by Lease ID."),
    status: Optional[BalanceStatus] = Query(None, description="Filter by Balance Status."),
    category: Optional[PostingCategory] = Query(None, description="Filter by Category."),
    db_session=Depends(get_db_with_current_user),
    ledger_service: LedgerService = Depends(),
):
    """
    Retrieves a paginated, sorted, and filtered list of ledger balances.
    """
    if use_stubs:
        return create_stub_balance_response(page, per_page)

    try:
        balances, total_items = ledger_service.list_balances(
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order,
            driver_name=driver_name,
            lease_id=lease_id,
            status=status,
            category=category,
        )
        total_pages = math.ceil(total_items / per_page)
        return PaginatedLedgerBalanceResponse(
            items=balances,
            total_items=total_items,
            page=page,
            per_page=per_page,
            total_pages=total_pages,
        )

    except LedgerError as e:
        logger.warning("Ledger business logic error in list_ledger_balances: %s", e)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    except Exception as e:
        logger.error(
            "Unexpected error in list_ledger_balances: %s", e, exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while fetching ledger balances.",
        ) from e


@router.get(
    "/postings",
    response_model=PaginatedLedgerPostingResponse,
    summary="List Ledger Postings",
)
def list_ledger_postings(
    use_stubs: bool = Query(False, description="Return stubbed data for testing."),
    page: int = Query(1, ge=1, description="Page number for pagination."),
    per_page: int = Query(10, ge=1, le=100, description="Items per page."),
    sort_by: Optional[str] = Query(None, description="Field to sort by."),
    sort_order: str = Query("desc", enum=["asc", "desc"]),
    start_date: Optional[date] = Query(None, description="Filter from this date."),
    end_date: Optional[date] = Query(None, description="Filter to this date."),
    status: Optional[PostingStatus] = Query(None, description="Filter by Posting Status."),
    category: Optional[PostingCategory] = Query(None, description="Filter by Category."),
    entry_type: Optional[EntryType] = Query(
        None, description="Filter by Entry Type (DEBIT/CREDIT)."
    ),
    driver_name: Optional[str] = Query(None, description="Filter by Driver Name."),
    lease_id: Optional[int] = Query(None, description="Filter by Lease ID."),
    vehicle_vin: Optional[str] = Query(None, description="Filter by Vehicle VIN."),
    medallion_no: Optional[str] = Query(None, description="Filter by Medallion Number."),
    db_session=Depends(get_db_with_current_user),
    ledger_service: LedgerService = Depends(),
):
    """
    Retrieves a paginated, sorted, and filtered list of ledger postings.
    """
    if use_stubs:
        return create_stub_posting_response(page, per_page)

    try:
        postings, total_items = ledger_service.list_postings(
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order,
            start_date=start_date,
            end_date=end_date,
            status=status,
            category=category,
            entry_type=entry_type,
            driver_name=driver_name,
            lease_id=lease_id,
            vehicle_vin=vehicle_vin,
            medallion_no=medallion_no,
        )
        total_pages = math.ceil(total_items / per_page)
        return PaginatedLedgerPostingResponse(
            items=postings,
            total_items=total_items,
            page=page,
            per_page=per_page,
            total_pages=total_pages,
        )
    except Exception as e:
        logger.error("Unexpected error in list_ledger_postings: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while fetching ledger postings.",
        ) from e


@router.post("/postings/{posting_id}/void", status_code=status.HTTP_200_OK)
def void_ledger_posting(
    posting_id: str,
    payload: VoidPostingRequest,
    db_session=Depends(get_db_with_current_user),
    ledger_service: LedgerService = Depends(),
    current_user: User = Depends(get_current_user),
):
    """
    Voids a specific ledger posting by creating a reversal entry.
    """
    try:
        reversal_posting = ledger_service.void_posting(
            posting_id=posting_id, reason=payload.reason
        )
        return {
            "message": "Posting successfully voided.",
            "original_posting_id": posting_id,
            "reversal_posting_id": reversal_posting.id,
        }
    except PostingNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e)) from e
    except InvalidLedgerOperationError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e)) from e
    except LedgerError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    except Exception as e:
        logger.error("Unexpected error in void_ledger_posting: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while voiding the posting.",
        ) from e


@router.get("/export", summary="Export Ledger Data")
def export_ledger_data(
    export_type: str = Query("postings", enum=["postings", "balances"]),
    export_format: str = Query("excel", enum=["excel", "pdf"], alias="format"),
    sort_by: Optional[str] = Query(None),
    sort_order: str = Query("desc"),
    driver_name: Optional[str] = Query(None),
    lease_id: Optional[int] = Query(None),
    status: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    entry_type: Optional[str] = Query(None),
    vehicle_vin: Optional[str] = Query(None),
    medallion_no: Optional[str] = Query(None),
    db_session=Depends(get_db_with_current_user),
    ledger_service: LedgerService = Depends(),
    _current_user: User = Depends(get_current_user),
):
    """
    Exports filtered ledger data to the specified format (Excel or PDF).
    """
    try:
        data = []
        filename_prefix = ""
        
        if export_type == "postings":
            filename_prefix = "ledger_postings"
            postings, _ = ledger_service.list_postings(
                include_all=True,
                sort_by=sort_by, sort_order=sort_order,
                start_date=start_date, end_date=end_date, status=status,
                category=category, entry_type=entry_type, driver_name=driver_name,
                lease_id=lease_id, vehicle_vin=vehicle_vin, medallion_no=medallion_no
            )
            data = postings
        else:  # balances
            filename_prefix = "ledger_balances"
            balances, _ = ledger_service.list_balances(
                include_all=True,
                sort_by=sort_by, sort_order=sort_order, driver_name=driver_name,
                lease_id=lease_id, status=status, category=category
            )
            data = balances

        if not data:
            raise ValueError("No ledger data available for export with the given filters.")

        export_data = [item.model_dump() for item in data]
        
        filename = f"{filename_prefix}_{date.today()}.{'xlsx' if export_format == 'excel' else export_format}"

        exporter = ExporterFactory.get_exporter(export_format, export_data)
        file_content = exporter.export()

        media_types = {
            "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "pdf": "application/pdf"
        }
        media_type = media_types.get(export_format, "application/octet-stream")

        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        return StreamingResponse(file_content, media_type=media_type, headers=headers)

    except LedgerError as e:
        logger.warning("Business logic error during ledger export: %s", e)
        raise HTTPException(status_code=400, detail=str(e)) from e

    except Exception as e:
        logger.error("Error exporting ledger data: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred during the export process.",
        ) from e