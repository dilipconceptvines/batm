### app/ezpass/router.py

import math
from datetime import date , time
from io import BytesIO
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi import status as fast_status
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.ezpass.exceptions import EZPassError
from app.ezpass.models import EZPassImportStatus
from app.ezpass.schemas import (
    EZPassTransactionResponse,
    PaginatedEZPassTransactionResponse,
    ManualAssociateRequest,
    ManualPostRequest,
    ReassignRequest,
    EZPassImportLogResponse,
    PaginatedEZPassImportLogResponse,
)
from app.ezpass.services import EZPassService, AVAILABLE_LOG_STATUSES, AVAILABLE_LOG_TYPES
from app.ezpass.stubs import create_stub_ezpass_response
from app.users.models import User
from app.users.utils import get_current_user
from app.utils.exporter_utils import ExporterFactory
from app.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/trips/ezpass", tags=["EZPass"])

# Dependency to inject the EZPassService
def get_ezpass_service(db: Session = Depends(get_db)) -> EZPassService:
    return EZPassService(db)

@router.post("/upload-csv", summary="Upload and Process EZPass CSV", status_code=fast_status.HTTP_202_ACCEPTED)
async def upload_ezpass_csv(
    file: UploadFile = File(...),
    ezpass_service: EZPassService = Depends(get_ezpass_service),
    current_user: User = Depends(get_current_user),
):
    """
    Accepts a CSV file of EZPass transactions, performs initial validation and parsing,
    stores the raw data, and triggers a background task for processing and association.
    """
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a CSV file.")

    try:
        file_stream = BytesIO(await file.read())
        result = ezpass_service.process_uploaded_csv(
            file_stream, file.filename, current_user.id
        )
        return JSONResponse(content=result, status_code=fast_status.HTTP_202_ACCEPTED)
    except EZPassError as e:
        logger.warning("Business logic error during EZPass CSV upload: %s", e)
        raise HTTPException(status_code=fast_status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error("Error processing EZPass CSV: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred during file processing.")

@router.get("", response_model=PaginatedEZPassTransactionResponse, summary="List EZPass Transactions")
def list_ezpass_transactions(
    use_stubs: bool = Query(False, description="Return stubbed data for testing."),
    page: int = Query(1, ge=1, description="Page number for pagination."),
    per_page: int = Query(10, ge=1, le=100, description="Items per page."),
    sort_by: Optional[str] = Query("transaction_date", description="Field to sort by."),
    sort_order: str = Query("desc", enum=["asc", "desc"]),
    from_transaction_date: Optional[date] = Query(None, description="Filter by a specific from transaction date."),
    to_transaction_date: Optional[date] = Query(None , description="Filter by a specific to transaction date."),
    from_transaction_time: Optional[time] = Query(None, description="Filter by a specific from transaction time."),
    to_transaction_time: Optional[time] = Query(None, description="Filter by a specific to transaction time."),
    from_posting_date: Optional[date] = Query(None, description="Filter by a specific from posting date."),
    to_posting_date: Optional[date] = Query(None, description="Filter by a specific to posting date."),
    from_amount: Optional[float] = Query(None, description="Filter by a specific from amount."),
    to_amount:Optional[float] = Query(None, description="Filter by a specific to amount."),
    transaction_id: Optional[str] = Query(None, description="Filter by transaction ID."),
    entry_plaza: Optional[str] = Query(None, description="Filter by entry plaza."),
    exit_plaza: Optional[str] = Query(None, description="Filter by exit plaza."),
    ezpass_class: Optional[str] = Query(None, description="Filter by EZPass Class."),
    vin: Optional[str] = Query(None, description="Filter by VIN."),
    agency: Optional[str] = Query(None, description="Filter by Agency."),
    medallion_no: Optional[str] = Query(None, description="Filter by Medallion Number."),
    driver_id: Optional[str] = Query(None, description="Filter by Driver ID."),
    plate_number: Optional[str] = Query(None, description="Filter by Plate Number."),
    status: Optional[str] = Query(None, description="Filter by transaction status."),
    ezpass_service: EZPassService = Depends(get_ezpass_service),
    current_user: User = Depends(get_current_user),
):
    """
    Provides a paginated and filterable view of all imported EZPass transactions,
    matching the UI requirements.
    """
    if use_stubs:
        return create_stub_ezpass_response(page, per_page)
    
    try:
        transactions, total_items = ezpass_service.repo.list_transactions(
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order,
            from_transaction_date=from_transaction_date,
            to_transaction_date=to_transaction_date,
            from_transaction_time=from_transaction_time,
            to_transaction_time=to_transaction_time,
            from_posting_date=from_posting_date,
            to_posting_date=to_posting_date,
            from_amount=from_amount,
            to_amount=to_amount,
            transaction_id=transaction_id,
            entry_plaza=entry_plaza,
            exit_plaza=exit_plaza,
            agency=agency,
            vin=vin,
            medallion_no=medallion_no,
            driver_id=driver_id,
            plate_number=plate_number,
            status=status,
        )

        response_items = [
            EZPassTransactionResponse(
                id=t.id,
                transaction_id=t.transaction_id,
                transaction_date= t.transaction_datetime,
                transaction_time=t.transaction_datetime.time(),
                entry_plaza=t.entry_plaza,
                exit_plaza=t.exit_plaza,
                ezpass_class=t.ezpass_class,
                medallion_no=t.medallion.medallion_number if t.medallion else (t.vehicle.medallions.medallion_number if t.vehicle and t.vehicle.medallions else ""),
                vin=t.vehicle.vin if t.vehicle else None,
                driver_id=t.driver.driver_id if t.driver else None,
                tag_or_plate=t.tag_or_plate,
                posting_date=t.posting_date,
                status=t.status,
                amount=t.amount,
                failure_reason=t.failure_reason,
                agency=t.agency,
                created_on=t.created_on
            )
            for t in transactions
        ]

        total_pages = math.ceil(total_items / per_page) if per_page > 0 else 0

        return PaginatedEZPassTransactionResponse(
            items=response_items,
            total_items=total_items,
            page=page,
            per_page=per_page,
            total_pages=total_pages
        )

    except Exception as e:
        logger.error("Error fetching EZPass transactions: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while fetching EZPass data.")


@router.get("/export", summary="Export EZPass Transaction Data")
def export_ezpass_transactions(
    export_format: str = Query("excel", enum=["excel", "pdf"], alias="format"),
    sort_by: Optional[str] = Query("transaction_date"),
    sort_order: str = Query("desc"),
    from_transaction_date: Optional[date] = Query(None),
    to_transaction_date: Optional[date] = Query(None),
    from_transaction_time: Optional[time] = Query(None),
    to_transaction_time: Optional[time] = Query(None),
    from_posting_date: Optional[date] = Query(None),
    to_posting_date: Optional[date] = Query(None),
    from_amount: Optional[float] = Query(None),
    to_amount: Optional[float] = Query(None),
    transaction_id: Optional[str] = Query(None),
    entry_plaza: Optional[str] = Query(None),
    exit_plaza: Optional[str] = Query(None),
    ezpass_class: Optional[str] = Query(None),
    vin: Optional[str] = Query(None),
    agency: Optional[str] = Query(None),
    medallion_no: Optional[str] = Query(None),
    driver_id: Optional[str] = Query(None),
    plate_number: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    ezpass_service: EZPassService = Depends(get_ezpass_service),
    _current_user: User = Depends(get_current_user),
):
    """
    Exports filtered EZPass transaction data to the specified format (Excel or PDF).
    """
    try:
        transactions, _ = ezpass_service.repo.list_transactions(
            page=1, per_page=10000, sort_by=sort_by, sort_order=sort_order,
            from_transaction_date=from_transaction_date,
            to_transaction_date=to_transaction_date,
            from_transaction_time=from_transaction_time,
            to_transaction_time=to_transaction_time,
            from_posting_date=from_posting_date,
            to_posting_date=to_posting_date,
            from_amount=from_amount,
            to_amount=to_amount,
            transaction_id=transaction_id,
            ezpass_class=ezpass_class,
            entry_plaza=entry_plaza,
            exit_plaza=exit_plaza,
            agency=agency,
            vin=vin,
            medallion_no=medallion_no,
            driver_id=driver_id,
            plate_number=plate_number,
            status=status,
        )

        if not transactions:
            raise ValueError("No EZPass data available for export with the given filters.")

        
        export_data = [
            EZPassTransactionResponse(
                posting_date=t.posting_date,
                tag_or_plate=t.tag_or_plate,
                created_on=t.created_on,
                entry_lane="",
                exit_lane="",  
                transaction_id=t.transaction_id,
                entry_plaza=t.entry_plaza,
                exit_plaza=t.exit_plaza,   
                transaction_date=t.transaction_datetime,
                transaction_time=t.transaction_datetime.time(),
                amount=t.amount,     
                agency=t.agency,
                ezpass_class=t.ezpass_class,
                vin=t.vehicle.vin if t.vehicle else None,
                medallion_no=t.medallion.medallion_number if t.medallion else (t.vehicle.medallions.medallion_number if t.vehicle and t.vehicle.medallions else "N/A"),
                driver_id=t.driver.driver_id if t.driver else None,
                status=t.status, 
                failure_reason=t.failure_reason,    
            ).model_dump(exclude={"id"})
            for t in transactions
        ]

        filename = f"ezpass_transactions_{date.today()}.{'xlsx' if export_format == 'excel' else export_format}"

        exporter = ExporterFactory.get_exporter(export_format, export_data)
        file_content = exporter.export()

        media_types = {
            "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "pdf": "application/pdf"
        }
        media_type = media_types.get(export_format, "application/octet-stream")

        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        return StreamingResponse(file_content, media_type=media_type, headers=headers)

    except EZPassError as e:
        logger.warning("Business logic error during EZPass export: %s", e)
        raise HTTPException(status_code=400, detail=str(e)) from e

    except Exception as e:
        logger.error("Error exporting EZPass data: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred during the export process.",
        ) from e
    

@router.post("/post-to-batm", summary="Manually post transactions to ledger", status_code=fast_status.HTTP_200_OK)
def manual_post_to_batm(
    request: ManualPostRequest,
    ezpass_service: EZPassService = Depends(get_ezpass_service),
    _current_user: User = Depends(get_current_user)
):
   """
    Manually post EZPass transactions to the centralized ledger.
    Used to force posting of ASSOCIATED transactions without waiting for automatic Celery task.
    
    This endpoint allows staff to:
    - Immediately post ASSOCIATED transactions to ledger
    - Retry failed postings (POSTING_FAILED status)
    - Process specific transactions urgently
    
    **Restrictions:**
    - Only ASSOCIATED status transactions can be posted
    - Cannot re-post transactions already POSTED_TO_LEDGER
    - Requires valid driver_id, lease_id, and positive amount
    
    **Process:**
    1. Validates transaction is ASSOCIATED
    2. Creates DEBIT obligation in centralized ledger (category: EZPASS)
    3. Updates transaction status to POSTED_TO_LEDGER
    4. Sets posting_date to current timestamp
   """
   try:
        result = ezpass_service.manual_post_to_ledger(
            transaction_ids=request.transaction_ids
        )
        return JSONResponse(content=result, status_code=fast_status.HTTP_200_OK)
   except EZPassError as e:
        logger.warning("Business logic error during manual posting: %s", e)
        raise HTTPException(status_code=fast_status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
   except Exception as e:
        logger.error("Error during manual posting: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred during manual posting.")  from e
   
@router.post("/reassign", summary="Reassign Transactions to Different Driver", status_code=fast_status.HTTP_200_OK)
def reassign_ezpass_transactions(
    request: ReassignRequest,
    ezpass_service: EZPassService = Depends(get_ezpass_service),
    current_user: User = Depends(get_current_user),
):
    """
    Reassign EZPass transactions from one driver/lease to another.
    Used to correct incorrect associations or handle driver changes.
    
    This endpoint allows staff to:
    - Move transactions between valid lease primary drivers
    - Correct misattributed tolls
    - Handle mid-lease driver changes
    
    **Restrictions:**
    - Cannot reassign transactions already POSTED_TO_LEDGER
    - New lease must be an active lease
    - New lease must belong to the specified new driver (valid primary driver)
    - Both new driver and new lease must exist in the system
    
    **Process:**
    1. Validates new driver and new lease exist
    2. Verifies new lease belongs to new driver
    3. Updates transaction with new associations
    4. Sets status to ASSOCIATED
    5. Clears any previous failure_reason
    
    **Use Cases:**
    - Driver X was incorrectly associated → reassign to correct Driver Y
    - Extra driver transactions → reassign to primary driver on lease
    - Toll occurred during lease transition → assign to appropriate lease
    """
    try:
        result = ezpass_service.reassign_transactions(
            transaction_ids=request.transaction_ids,
            new_driver_id=request.new_driver_id,
            new_lease_id=request.new_lease_id,
            new_medallion_id=request.new_medallion_id,
            new_vehicle_id=request.new_vehicle_id
        )
        return JSONResponse(content=result, status_code=fast_status.HTTP_200_OK)
    except EZPassError as e:
        logger.warning("Business logic error during reassignment: %s", e)
        raise HTTPException(status_code=fast_status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    except Exception as e:
        logger.error("Error during reassignment: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred during reassignment.") from e
    
@router.post("/associate-with-batm", summary="Retry Failed Associations", status_code=fast_status.HTTP_200_OK)
def retry_association_with_batm(
    request: ManualAssociateRequest,
    ezpass_service: EZPassService = Depends(get_ezpass_service),
    current_user: User = Depends(get_current_user),
):
    """
    Retry automatic association logic for failed or specific transactions.
    
    This endpoint does NOT manually assign to a driver - it retries the same
    automatic association logic (plate → vehicle → CURB trip → driver/lease).
    
    **Use Cases:**
    - Retry all ASSOCIATION_FAILED transactions (send empty request)
    - Retry specific transactions that failed (provide transaction_ids)
    - Re-run association after CURB data updates
    
    **Request Body:**
    - transaction_ids: Optional list of transaction IDs to retry
    - If null/empty: Retries ALL transactions with ASSOCIATION_FAILED status
    
    **Association Logic:**
    1. Extract plate number from tag_or_plate field
    2. Find Vehicle via plate registration
    3. Find CURB trip on that vehicle ±30 min of toll time
    4. If found: Associate with driver/lease/medallion from CURB trip
    5. Update status to ASSOCIATED or ASSOCIATION_FAILED
    
    **Example Requests:**
    
    Retry specific transactions:
    ```json
    {
        "transaction_ids": [123, 124, 125]
    }
    ```
    
    Retry all failed associations:
    ```json
    {}
    ```
    or
    ```json
    {
        "transaction_ids": null
    }
    ```
    """
    try:
        result = ezpass_service.retry_failed_associations(
            transaction_ids=request.transaction_ids
        )
        return JSONResponse(content=result, status_code=fast_status.HTTP_200_OK)
    except EZPassError as e:
        logger.warning("Business logic error during association retry: %s", e)
        raise HTTPException(status_code=fast_status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    except Exception as e:
        logger.error("Error during association retry: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred during association retry.") from e
    

@router.get("/imports", response_model=PaginatedEZPassImportLogResponse, summary="List EZPass Import Logs")
def list_ezpass_import_logs(
    page: int = Query(1, ge=1, description="Page number for pagination."),
    per_page: int = Query(10, ge=1, le=100, description="Items per page."),
    sort_by: str = Query("import_timestamp", description="Field to sort by (import_timestamp, file_name, status, total_records)."),
    sort_order: str = Query("desc", enum=["asc", "desc"], description="Sort order."),
    from_log_date: Optional[date] = Query(None, description="Filter from log date (inclusive)."),
    to_log_date: Optional[date] = Query(None, description="Filter to log date (inclusive)."),
    log_type: Optional[str] = Query(None, description="Filter by log type (Import, Associate, Post)."),
    log_status: Optional[str] = Query(None, description="Filter by log status (Success, Partial Success, Failure, Pending, Processing)."),
    file_name: Optional[str] = Query(None, description="Filter by file name (partial match)."),
    ezpass_service: EZPassService = Depends(get_ezpass_service),
    current_user: User = Depends(get_current_user),
):
    """
    Provides a paginated and filterable view of all EZPass import logs.
    
    This endpoint powers the "View EZPass Log" page showing:
    - Log Date: When the import occurred
    - Log Type: Type of operation (currently "Import" only)
    - Records Impacted: Total records in the CSV
    - Success: Number of successfully imported records
    - Unidentified: Number of failed records
    - Log Status: Overall status (Success/Partial Success/Failure/Pending/Processing)
    
    **Date Range Filtering:**
    Use `from_log_date` and `to_log_date` to filter by date range.
    Both parameters are optional and inclusive.
    
    Examples:
    - Last week: from_log_date=2024-12-01&to_log_date=2024-12-07
    - Before date: to_log_date=2024-12-01
    - After date: from_log_date=2024-12-01
    
    **Filter Metadata:**
    Response includes `available_log_types` and `available_log_statuses` 
    arrays to help frontend build filter dropdowns dynamically.
    
    **Filtering:**
    - from_log_date, to_log_date: Date range filter (inclusive)
    - log_type: Operation type (Import, Associate, Post)
    - log_status: Success, Partial Success, Failure, Pending, or Processing
    - file_name: Partial match on file name
    
    **Sorting:**
    - Supports sorting by any field
    - Default: Most recent first (import_timestamp desc)
    
    **Pagination:**
    - Default: 10 items per page
    - Maximum: 100 items per page
    """
    try:
        import_logs, total_items = ezpass_service.repo.list_import_logs(
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order,
            from_log_date=from_log_date,
            to_log_date=to_log_date,
            log_type=log_type,
            log_status=log_status,
            file_name=file_name,
        )
        
        # Transform to response schema
        response_items = []
        for log in import_logs:
            # Determine log status based on import status and record counts
            if log.status == EZPassImportStatus.COMPLETED:
                if log.failed_records == 0:
                    log_status_str = "Success"
                else:
                    log_status_str = "Partial Success"
            elif log.status == EZPassImportStatus.FAILED:
                log_status_str = "Failure"
            elif log.status == EZPassImportStatus.PENDING:
                log_status_str = "Pending"
            elif log.status == EZPassImportStatus.PROCESSING:
                log_status_str = "Processing"
            else:
                log_status_str = log.status.value
            
            response_items.append(
                EZPassImportLogResponse(
                    id=log.id,
                    log_date=log.import_timestamp,
                    log_type="Import",  # Currently all logs are import type
                    file_name=log.file_name,
                    records_impacted=log.total_records,
                    success=log.successful_records,
                    unidentified=log.failed_records,
                    log_status=log_status_str,
                    created_by=log.created_by,
                    created_on=log.created_on,
                )
            )
        
        total_pages = math.ceil(total_items / per_page) if per_page > 0 else 0
        
        return PaginatedEZPassImportLogResponse(
            items=response_items,
            total_items=total_items,
            page=page,
            per_page=per_page,
            total_pages=total_pages,
            # Include filter metadata for frontend
            available_log_types=AVAILABLE_LOG_TYPES,
            available_log_statuses=AVAILABLE_LOG_STATUSES,
        )
        
    except Exception as e:
        logger.error("Error fetching EZPass import logs: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail="An unexpected error occurred while fetching EZPass import logs."
        ) from e
