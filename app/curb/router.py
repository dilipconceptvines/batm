### app/curb/router.py

import math
from datetime import date, datetime, timezone, timedelta
from io import BytesIO
from typing import Optional, Dict, Any

from celery.result import AsyncResult
from celery import chain
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.curb.exceptions import CurbError, TripProcessingError
from app.curb.schemas import CurbTripResponse, PaginatedCurbTripResponse
from app.curb.services import CurbService
from app.curb.stubs import create_stub_curb_trip_response
from app.users.models import User
from app.users.utils import get_current_user
from app.utils.exporter_utils import ExporterFactory
from app.curb.curb_sync_tasks import (
    curb_full_sync_chain_task,
    fetch_transactions_to_s3_task,
    fetch_trips_to_s3_task,
    parse_and_map_transactions_task,
    parse_and_map_trips_task,
)
from app.worker.app import app as celery_app
from app.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/trips", tags=["Trips"])


# Dependency to inject the CurbService
def get_curb_service(db: Session = Depends(get_db)) -> CurbService:
    """Dependency to get CurbService instance."""
    return CurbService(db)


@router.post(
    "/curb/import-by-medallion",
    summary="Import and Reconcile CURB Data by Medallion",
    status_code=status.HTTP_200_OK,
)
def import_curb_data(
    from_date: Optional[date] = Query(
        None,
        description="Start date for the import (YYYY-MM-DD). Defaults to yesterday.",
    ),
    to_date: Optional[date] = Query(
        None, description="End date for the import (YYYY-MM-DD). Defaults to today."
    ),
    medallion_no: Optional[str] = Query(
        None,
        description="A specific medallion number or a comma-separated list. If omitted, all active medallions are used.",
    ),
    curb_service: CurbService = Depends(get_curb_service),
    _current_user: User = Depends(get_current_user),
):
    """
    Triggers a direct, synchronous import of trip and transaction data from the
    CURB API. The import is filtered by active medallions in the system.

    - **medallion_no (optional)**: Provide a single medallion or a comma-separated list to limit the import. If not provided, the system will fetch data for ALL medallions currently marked as 'Active'.
    - **from_date (optional)**: The start of the date range to query. Defaults to yesterday.
    - **to_date (optional)**: The end of the date range to query. Defaults to today.
    """
    try:
        result = curb_service.import_and_reconcile_data(
            from_date=from_date,
            to_date=to_date,
            medallion_no=medallion_no,
        )
        return JSONResponse(content=result, status_code=status.HTTP_200_OK)
    except CurbError as e:
        logger.error(
            "A business logic error occurred during CURB import: %s", e, exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)
        ) from e
    except Exception as e:
        logger.error(
            "An unexpected server error occurred during CURB import: %s",
            e,
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred during the import process.",
        ) from e


@router.post(
    "/curb/map-trips",
    summary="Map Reconciled CURB Trips to Internal Entities",
    status_code=status.HTTP_200_OK,
)
def map_reconciled_curb_trips(
    curb_service: CurbService = Depends(get_curb_service),
    _current_user: User = Depends(get_current_user),
):
    """
    Finds all reconciled but unmapped CURB trips and attempts to associate them
    with the correct driver, lease, vehicle, and medallion records in the system.
    """
    try:
        result = curb_service.map_reconciled_trips()
        return JSONResponse(content=result, status_code=status.HTTP_200_OK)
    except Exception as e:
        logger.error(
            "An unexpected error occurred during trip mapping: %s", e, exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred during the mapping process.",
        ) from e


@router.post(
    "/curb/post-earnings-to-ledger",
    summary="Post Mapped CURB Earnings to Ledger",
    status_code=status.HTTP_200_OK,
)
async def post_earnings_to_ledger(
    start_date: date = Query(
        ..., description="Start date of the period to post earnings for (YYYY-MM-DD)."
    ),
    end_date: date = Query(
        ..., description="End date of the period to post earnings for (YYYY-MM-DD)."
    ),
    lease_id: Optional[int] = Query(
        None, description="Optional: Filter to post earnings for a specific lease ID."
    ),
    driver_id: Optional[int] = Query(
        None,
        description="Optional: Filter to post earnings for a specific driver ID (internal primary key).",
    ),
    curb_service: CurbService = Depends(get_curb_service),
    _current_user: User = Depends(get_current_user),
):
    """
    Finds all MAPPED (but not yet posted) credit card trips within a given date range
    and posts the aggregated earnings to the Centralized Ledger. This action is final
    for the selected trips and cannot be undone easily.

    - **start_date & end_date**: Defines the inclusive period for which to find and post earnings.
    - **lease_id (optional)**: Restricts the operation to a single lease.
    - **driver_id (optional)**: Restricts the operation to a single driver.
    """
    try:
        if start_date > end_date:
            raise HTTPException(
                status_code=400, detail="The start_date cannot be after the end_date."
            )

        result = curb_service.post_mapped_earnings_to_ledger(
            start_date=start_date,
            end_date=end_date,
            lease_id=lease_id,
            driver_id=driver_id,
        )

        # Check for partial failures
        if result.get("errors"):
            status_code = status.HTTP_207_MULTI_STATUS
            message = "Posting completed with some errors."
        else:
            status_code = status.HTTP_200_OK
            message = "Earnings posting process completed successfully."

        return JSONResponse(
            content={"message": message, "results": result},
            status_code=status_code,
        )
    except TripProcessingError as e:
        logger.error(
            "A critical error occurred during earnings posting: %s", e, exc_info=True
        )
        raise HTTPException(
            status_code=500, detail=f"A critical ledger error occurred: {e}"
        ) from e
    except Exception as e:
        logger.error(
            "An unexpected server error occurred during earnings posting: %s",
            e,
            exc_info=True,
        )
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred during the posting process.",
        ) from e


@router.get("/view", response_model=PaginatedCurbTripResponse, summary="View All Trips")
def view_all_trips(
    use_stubs: bool = Query(False, description="Use stub/demo data for testing."),
    page: int = Query(1, description="Page number for pagination."),
    per_page: int = Query(10, description="Number of items per page."),
    sort_by: str = Query("start_time", description="Field to sort by."),
    sort_order: str = Query("desc", description="Sort order: 'asc' or 'desc'."),
    trip_id: Optional[str] = Query(None, description="Filter by Trip ID."),
    driver_id: Optional[str] = Query(None, description="Filter by Driver ID / TLC No."),
    medallion_no: Optional[str] = Query(
        None, description="Filter by Medallion Number."
    ),
    start_date: Optional[date] = Query(None, description="Filter by trip start date."),
    end_date: Optional[date] = Query(None, description="Filter by trip end date."),
    curb_service: CurbService = Depends(get_curb_service),
    _current_user: User = Depends(get_current_user),
):
    """
    Endpoint to view a consolidated list of all trips from various sources.
    This view is powered by the imported CURB data.
    """
    if use_stubs:
        return create_stub_curb_trip_response(page, per_page)

    try:
        trips, total_items = curb_service.repo.list_trips(
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order,
            trip_id=trip_id,
            driver_id_tlc=driver_id,
            medallion_no=medallion_no,
            start_date=start_date,
            end_date=end_date,
        )

        response_items = [CurbTripResponse.model_validate(trip) for trip in trips]
        total_pages = math.ceil(total_items / per_page) if per_page > 0 else 0

        return PaginatedCurbTripResponse(
            items=response_items,
            total_items=total_items,
            page=page,
            per_page=per_page,
            total_pages=total_pages,
        )
    except Exception as e:
        logger.error("Error fetching trips view: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred while fetching trip data.",
        ) from e


@router.get(
    "/view-curb-data",
    response_model=PaginatedCurbTripResponse,
    summary="View Raw CURB Data",
)
def view_curb_data(
    use_stubs: bool = Query(False, description="Return stubbed data for testing."),
    page: int = Query(1, ge=1, description="Page number for pagination."),
    per_page: int = Query(10, ge=1, le=100, description="Items per page."),
    sort_by: Optional[str] = Query("start_time", description="Field to sort by."),
    sort_order: str = Query("desc", enum=["asc", "desc"]),
    trip_id: Optional[str] = Query(None, description="Filter by Trip ID."),
    driver_id: Optional[str] = Query(None, description="Filter by Driver ID / TLC No."),
    medallion_no: Optional[str] = Query(
        None, description="Filter by Medallion Number."
    ),
    start_date: Optional[date] = Query(None, description="Filter by trip start date."),
    end_date: Optional[date] = Query(None, description="Filter by trip end date."),
    transaction_date: Optional[date] = Query(
        None, description="Filter by transaction date."
    ),
    curb_service: CurbService = Depends(get_curb_service),
    current_user: User = Depends(get_current_user),
):
    """
    Endpoint to view the raw, imported data from the CURB system.
    This serves as the data source for the main 'View Trips' page.
    """
    # This endpoint behaves identically to /view for now, but is kept for logical separation
    # as per the UI design.

    # For transaction_date filtering, pass it through to the service layer
    try:
        trips, total_items = curb_service.repo.list_trips(
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order,
            trip_id=trip_id,
            driver_id_tlc=driver_id,
            medallion_no=medallion_no,
            start_date=start_date,
            end_date=end_date,
            transaction_date=transaction_date,
        )

        response_items = [CurbTripResponse.model_validate(trip) for trip in trips]
        total_pages = math.ceil(total_items / per_page) if per_page > 0 else 0

        return PaginatedCurbTripResponse(
            items=response_items,
            total_items=total_items,
            page=page,
            per_page=per_page,
            total_pages=total_pages,
        )
    except Exception as e:
        logger.error("Error fetching CURB data: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred while fetching CURB data.",
        ) from e


@router.get("/export", summary="Export Trip Data")
def export_trips(
    export_format: str = Query("excel", enum=["excel", "pdf"], alias="format"),
    sort_by: Optional[str] = Query("start_time"),
    sort_order: str = Query("desc"),
    trip_id: Optional[str] = Query(None),
    driver_id: Optional[str] = Query(None),
    medallion_no: Optional[str] = Query(None),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    curb_service: CurbService = Depends(get_curb_service),
    _current_user: User = Depends(get_current_user),
):
    """
    Exports filtered trip data to the specified format (Excel or PDF).
    """
    try:
        # For exports, fetch up to 1 lakh (100,000) records
        trips, _ = curb_service.repo.list_trips(
            page=1,
            per_page=100000,
            sort_by=sort_by,
            sort_order=sort_order,
            trip_id=trip_id,
            driver_id_tlc=driver_id,
            medallion_no=medallion_no,
            start_date=start_date,
            end_date=end_date,
        )

        if not trips:
            raise ValueError(
                "No trip data available for export with the given filters."
            )

        export_data = [
            CurbTripResponse.model_validate(trip).model_dump() for trip in trips
        ]

        filename = f"trips_export_{date.today()}.{'xlsx' if export_format == 'excel' else export_format}"

        # Use ExporterFactory to get the appropriate exporter
        exporter = ExporterFactory.get_exporter(export_format, export_data)
        file_content = exporter.export()

        # Set media type based on export format
        media_types = {
            "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "pdf": "application/pdf",
        }
        media_type = media_types.get(export_format, "application/octet-stream")

        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        return StreamingResponse(file_content, media_type=media_type, headers=headers)

    except CurbError as e:
        logger.warning("Business logic error during trip export: %s", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)
        ) from e
    except Exception as e:
        logger.error("Error exporting trip data: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during the export process.",
        ) from e
    

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def validate_date_format(date_string: str, param_name: str) -> str:
    """
    Validate date string format.
    
    Args:
        date_string: Date string to validate
        param_name: Parameter name for error messages
        
    Returns:
        Validated date string
        
    Raises:
        HTTPException: If date format is invalid
    """
    try:
        datetime.strptime(date_string, "%Y-%m-%d")
        return date_string
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid {param_name} format. Expected YYYY-MM-DD, got '{date_string}'"
        ) from e


def validate_date_range(from_date: Optional[str], to_date: Optional[str]) -> tuple:
    """
    Validate date range parameters.
    
    Args:
        from_date: Start date string or None
        to_date: End date string or None
        
    Returns:
        Tuple of (from_date, to_date) as strings
        
    Raises:
        HTTPException: If date range is invalid
    """
    # Set defaults
    if from_date is None:
        from_date = (date.today() - timedelta(days=1)).isoformat()
    else:
        from_date = validate_date_format(from_date, "from_date")
    
    if to_date is None:
        to_date = date.today().isoformat()
    else:
        to_date = validate_date_format(to_date, "to_date")
    
    # Validate range
    from_dt = datetime.strptime(from_date, "%Y-%m-%d").date()
    to_dt = datetime.strptime(to_date, "%Y-%m-%d").date()
    
    if from_dt > to_dt:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"from_date ({from_date}) cannot be after to_date ({to_date})"
        )
    
    # Check if range is too large (prevent abuse)
    days_difference = (to_dt - from_dt).days
    if days_difference > 90:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Date range too large ({days_difference} days). Maximum allowed is 90 days."
        )
    
    return from_date, to_date


def format_task_response(
    task_result: AsyncResult,
    message: str,
    from_date: str,
    to_date: str,
    task_type: str
) -> Dict[str, Any]:
    """
    Format a standardized task response.
    
    Args:
        task_result: Celery AsyncResult object
        message: Success message
        from_date: Start date
        to_date: End date
        task_type: Type of task initiated
        
    Returns:
        Formatted response dictionary
    """
    return {
        "status": "success",
        "message": message,
        "task_id": task_result.id,
        "task_type": task_type,
        "date_range": {
            "from": from_date,
            "to": to_date
        },
        "initiated_at": datetime.now(timezone.utc).isoformat(),
        "status_check_url": f"/api/curb/sync/status/{task_result.id}"
    }


# ============================================================================
# ENDPOINTS
# ============================================================================

@router.post("/full-sync", response_model=Dict[str, Any])
def trigger_full_sync(
    from_date: Optional[str] = Query(
        None,
        description="Start date in YYYY-MM-DD format (default: yesterday)",
        example="2025-01-01"
    ),
    to_date: Optional[str] = Query(
        None,
        description="End date in YYYY-MM-DD format (default: today)",
        example="2025-01-05"
    ),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
) -> JSONResponse:
    """
    Trigger the complete CURB synchronization chain.
    
    This endpoint initiates a full sync that:
    1. Fetches trip logs from CURB API and stores to S3
    2. Parses and maps trips from S3 to database
    3. Fetches transactions in 3-hour windows and stores to S3
    4. Parses and maps transactions from S3 to database
    
    All tasks run sequentially with result passing between tasks.
    
    **Date Range:**
    - If dates not provided, defaults to yesterday through today
    - Maximum range: 90 days
    - Format: YYYY-MM-DD
    
    **Returns:**
    - task_id: Use this to check task status
    - status_check_url: Endpoint to monitor progress
    
    **Example:**
    ```bash
    POST /api/curb/sync/full-sync?from_date=2025-01-01&to_date=2025-01-05
    ```
    """
    try:
        # Validate date range
        from_date, to_date = validate_date_range(from_date, to_date)
        
        logger.info(
            f"User {current_user.first_name} triggered full CURB sync: {from_date} to {to_date}"
        )
        
        # Trigger the full sync chain
        task = curb_full_sync_chain_task.apply_async(
            args=[from_date, to_date]
        )
        
        response = format_task_response(
            task_result=task,
            message="Full CURB synchronization chain initiated successfully",
            from_date=from_date,
            to_date=to_date,
            task_type="full_sync_chain"
        )
        
        logger.info(f"Full sync chain initiated: task_id={task.id}")
        
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content=response
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger full sync: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to initiate sync: {str(e)}"
        ) from e


@router.post("/trips", response_model=Dict[str, Any])
def trigger_trips_sync(
    from_date: Optional[str] = Query(
        None,
        description="Start date in YYYY-MM-DD format (default: yesterday)"
    ),
    to_date: Optional[str] = Query(
        None,
        description="End date in YYYY-MM-DD format (default: today)"
    ),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
) -> JSONResponse:
    """
    Trigger trips synchronization only.
    
    This endpoint chains two tasks:
    1. Fetches trip logs from CURB API and stores to S3
    2. Parses and maps trips from S3 to database
    
    **Use Case:**
    - When you only need to sync trip data
    - To retry failed trip imports
    - For testing trip sync independently
    
    **Example:**
    ```bash
    POST /api/curb/sync/trips?from_date=2025-01-01&to_date=2025-01-05
    ```
    """
    try:
        # Validate date range
        from_date, to_date = validate_date_range(from_date, to_date)
        
        logger.info(
            f"User {current_user.first_name} triggered trips sync: {from_date} to {to_date}"
        )
        
        # Chain: fetch trips -> parse trips
        trips_chain = chain(
            fetch_trips_to_s3_task.s(from_date, to_date),
            parse_and_map_trips_task.s(from_date, to_date)
        )
        
        task = trips_chain.apply_async()
        
        response = format_task_response(
            task_result=task,
            message="Trips synchronization chain initiated successfully",
            from_date=from_date,
            to_date=to_date,
            task_type="trips_sync_chain"
        )
        
        logger.info(f"Trips sync chain initiated: task_id={task.id}")
        
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content=response
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger trips sync: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to initiate trips sync: {str(e)}"
        ) from e


@router.post("/transactions", response_model=Dict[str, Any])
def trigger_transactions_sync(
    from_date: Optional[str] = Query(
        None,
        description="Start date in YYYY-MM-DD format (default: yesterday)"
    ),
    to_date: Optional[str] = Query(
        None,
        description="End date in YYYY-MM-DD format (default: today)"
    ),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
) -> JSONResponse:
    """
    Trigger transactions synchronization only.
    
    This endpoint chains two tasks:
    1. Fetches transactions in 3-hour windows from CURB API and stores to S3
    2. Parses and maps transactions from S3 to database
    
    **Use Case:**
    - When you only need to sync transaction data
    - To retry failed transaction imports
    - For testing transaction sync independently
    
    **Note:**
    - Transactions are fetched in 3-hour windows throughout each day
    - This ensures comprehensive coverage of all transaction times
    
    **Example:**
    ```bash
    POST /api/curb/sync/transactions?from_date=2025-01-01&to_date=2025-01-05
    ```
    """
    try:
        # Validate date range
        from_date, to_date = validate_date_range(from_date, to_date)
        
        logger.info(
            f"User {current_user.first_name} triggered transactions sync: {from_date} to {to_date}"
        )
        
        # Chain: fetch transactions -> parse transactions
        transactions_chain = chain(
            fetch_transactions_to_s3_task.s(from_date, to_date),
            parse_and_map_transactions_task.s(from_date, to_date)
        )
        
        task = transactions_chain.apply_async()
        
        response = format_task_response(
            task_result=task,
            message="Transactions synchronization chain initiated successfully",
            from_date=from_date,
            to_date=to_date,
            task_type="transactions_sync_chain"
        )
        
        logger.info(f"Transactions sync chain initiated: task_id={task.id}")
        
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content=response
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger transactions sync: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to initiate transactions sync: {str(e)}"
        ) from e


@router.post("/trips/fetch-only", response_model=Dict[str, Any])
def trigger_fetch_trips_only(
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
) -> JSONResponse:
    """
    Fetch trip logs and store to S3 only (no parsing/mapping).
    
    **Use Case:**
    - To quickly fetch and backup trip data
    - To retry failed S3 uploads
    - For data archival purposes
    """
    try:
        from_date, to_date = validate_date_range(from_date, to_date)
        
        logger.info(f"User {current_user.first_name} triggered fetch trips only: {from_date} to {to_date}")
        
        task = fetch_trips_to_s3_task.apply_async(args=[from_date, to_date])
        
        response = format_task_response(
            task_result=task,
            message="Trip fetch to S3 initiated successfully",
            from_date=from_date,
            to_date=to_date,
            task_type="fetch_trips_only"
        )
        
        return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=response)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger fetch trips: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        ) from e


@router.post("/trips/parse-only", response_model=Dict[str, Any])
def trigger_parse_trips_only(
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
) -> JSONResponse:
    """
    Parse existing trip XMLs from S3 (no fetching).
    
    **Use Case:**
    - To reprocess existing S3 data
    - To retry failed parsing
    - After fixing data mapping issues
    """
    try:
        from_date, to_date = validate_date_range(from_date, to_date)
        
        logger.info(f"User {current_user.first_name} triggered parse trips only: {from_date} to {to_date}")
        
        task = parse_and_map_trips_task.apply_async(args=[from_date, to_date])
        
        response = format_task_response(
            task_result=task,
            message="Trip parsing from S3 initiated successfully",
            from_date=from_date,
            to_date=to_date,
            task_type="parse_trips_only"
        )
        
        return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=response)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger parse trips: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        ) from e


@router.get("/status/{task_id}", response_model=Dict[str, Any])
def get_sync_status(
    task_id: str,
    current_user: User = Depends(get_current_user)
) -> JSONResponse:
    """
    Check the status of a sync task.
    
    **Task States:**
    - `PENDING`: Task is waiting to be executed
    - `STARTED`: Task has started execution
    - `SUCCESS`: Task completed successfully
    - `FAILURE`: Task failed with error
    - `RETRY`: Task is being retried
    - `REVOKED`: Task was cancelled
    
    **Example:**
    ```bash
    GET /api/curb/sync/status/abc123-def456-ghi789
    ```
    
    **Response includes:**
    - Current state
    - Result data (if completed)
    - Error information (if failed)
    - Task metadata
    """
    try:
        result = AsyncResult(task_id, app=celery_app)
        
        response = {
            "task_id": task_id,
            "state": result.state,
            "status": result.state,
        }
        
        # Add result data if available
        if result.ready():
            if result.successful():
                response["result"] = result.result
                response["completed_at"] = datetime.now(timezone.utc).isoformat()
            elif result.failed():
                response["error"] = str(result.result)
                response["failed_at"] = datetime.now(timezone.utc).isoformat()
        else:
            response["info"] = result.info
        
        # Add task metadata
        response["metadata"] = {
            "is_ready": result.ready(),
            "is_successful": result.successful() if result.ready() else None,
            "is_failed": result.failed() if result.ready() else None,
        }
        
        logger.info(f"Status check for task {task_id}: {result.state}")
        
        return JSONResponse(status_code=status.HTTP_200_OK, content=response)
        
    except Exception as e:
        logger.error(f"Failed to get task status for {task_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve task status: {str(e)}"
        ) from e


@router.get("/history", response_model=Dict[str, Any])
def get_sync_history(
    limit: int = Query(10, ge=1, le=100, description="Number of recent tasks to retrieve"),
    task_type: Optional[str] = Query(
        None,
        description="Filter by task type",
        enum=["full_sync_chain", "trips_sync_chain", "transactions_sync_chain"]
    ),
    current_user: User = Depends(get_current_user)
) -> JSONResponse:
    """
    Get recent sync task history.
    
    **Note:** This endpoint provides a basic history view.
    For production, consider implementing a database-backed task tracking system.
    
    **Query Parameters:**
    - `limit`: Number of recent tasks (1-100, default: 10)
    - `task_type`: Filter by specific task type
    
    **Example:**
    ```bash
    GET /api/curb/sync/history?limit=20&task_type=full_sync_chain
    ```
    """
    try:
        # Note: This is a simplified implementation
        # For production, you should store task metadata in database
        
        from celery.result import GroupResult
        
        response = {
            "message": "Task history endpoint",
            "note": "For comprehensive history, implement database-backed task tracking",
            "limit": limit,
            "task_type_filter": task_type,
            "suggestion": "Use Celery Flower for detailed task monitoring: http://localhost:5555"
        }
        
        logger.info(f"History requested by {current_user.first_name}")
        
        return JSONResponse(status_code=status.HTTP_200_OK, content=response)
        
    except Exception as e:
        logger.error(f"Failed to get sync history: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        ) from e


@router.post("/cancel/{task_id}", response_model=Dict[str, Any])
def cancel_sync_task(
    task_id: str,
    terminate: bool = Query(
        False,
        description="Force terminate task (use with caution)"
    ),
    current_user: User = Depends(get_current_user)
) -> JSONResponse:
    """
    Cancel a running sync task.
    
    **Warning:** Cancelling tasks may leave data in inconsistent state.
    Only use when absolutely necessary.
    
    **Parameters:**
    - `terminate`: If True, forcefully terminates the task
    
    **Example:**
    ```bash
    POST /api/curb/sync/cancel/abc123-def456?terminate=false
    ```
    """
    try:
        celery_app.control.revoke(
            task_id,
            terminate=terminate,
            signal='SIGTERM' if terminate else None
        )
        
        logger.warning(
            f"User {current_user.first_name} cancelled task {task_id} "
            f"(terminate={terminate})"
        )
        
        response = {
            "status": "success",
            "message": f"Task {task_id} has been {'terminated' if terminate else 'revoked'}",
            "task_id": task_id,
            "terminated": terminate,
            "warning": "Task cancellation may leave data in inconsistent state"
        }
        
        return JSONResponse(status_code=status.HTTP_200_OK, content=response)
        
    except Exception as e:
        logger.error(f"Failed to cancel task {task_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        ) from e


@router.get("/active-tasks", response_model=Dict[str, Any])
def get_active_tasks(
    current_user: User = Depends(get_current_user)
) -> JSONResponse:
    """
    Get list of currently active sync tasks.
    
    **Returns:**
    - List of active tasks across all workers
    - Task details including ID, name, and arguments
    
    **Example:**
    ```bash
    GET /api/curb/sync/active-tasks
    ```
    """
    try:
        # Get active tasks from all workers
        inspect = celery_app.control.inspect()
        active_tasks = inspect.active()
        
        # Filter for CURB sync tasks only
        curb_tasks = []
        if active_tasks:
            for worker, tasks in active_tasks.items():
                for task in tasks:
                    if 'curb' in task.get('name', '').lower():
                        curb_tasks.append({
                            "task_id": task.get('id'),
                            "task_name": task.get('name'),
                            "worker": worker,
                            "args": task.get('args'),
                            "kwargs": task.get('kwargs'),
                        })
        
        response = {
            "status": "success",
            "active_curb_tasks_count": len(curb_tasks),
            "active_tasks": curb_tasks,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"Active tasks requested by {current_user.first_name}")
        
        return JSONResponse(status_code=status.HTTP_200_OK, content=response)
        
    except Exception as e:
        logger.error(f"Failed to get active tasks: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        ) from e


@router.get("/scheduled-tasks", response_model=Dict[str, Any])
def get_scheduled_tasks(
    current_user: User = Depends(get_current_user)
) -> JSONResponse:
    """
    Get information about scheduled CURB sync tasks.
    
    **Returns:**
    - Beat schedule configuration for CURB tasks
    - Next scheduled execution times
    
    **Example:**
    ```bash
    GET /api/curb/sync/scheduled-tasks
    ```
    """
    try:
        # Get scheduled tasks from beat schedule
        scheduled = celery_app.control.inspect().scheduled()
        
        # Get beat schedule configuration
        from app.worker.config import beat_schedule
        
        curb_schedules = {}
        for task_name, config in beat_schedule.items():
            if 'curb' in task_name.lower():
                curb_schedules[task_name] = {
                    "task": config.get('task'),
                    "schedule": str(config.get('schedule')),
                    "options": config.get('options', {})
                }
        
        response = {
            "status": "success",
            "scheduled_curb_tasks": curb_schedules,
            "next_scheduled": scheduled,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"Scheduled tasks requested by {current_user.first_name}")
        
        return JSONResponse(status_code=status.HTTP_200_OK, content=response)
        
    except Exception as e:
        logger.error(f"Failed to get scheduled tasks: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        ) from e
