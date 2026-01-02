### app/ezpass/router.py

import math
from datetime import date , time
from io import BytesIO
from decimal import Decimal
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
    ReassignRequest,
    EZPassImportLogResponse,
    PaginatedEZPassImportLogResponse,
)
from app.ezpass.services import EZPassService, AVAILABLE_LOG_STATUSES, AVAILABLE_LOG_TYPES
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
        result = ezpass_service.import_csv(
            file_stream, file.filename, current_user.id
        )
        return JSONResponse(content=result, status_code=fast_status.HTTP_202_ACCEPTED)
    except EZPassError as e:
        logger.warning("Business logic error during EZPass CSV upload: %s", e)
        raise HTTPException(status_code=fast_status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    except Exception as e:
        logger.error("Error processing EZPass CSV: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred during file processing.") from e

@router.get("", response_model=PaginatedEZPassTransactionResponse, summary="List EZPass Transactions")
def list_ezpass_transactions(
    # Pagination
    page: int = Query(1, ge=1, description="Page number for pagination."),
    per_page: int = Query(50, ge=1, le=1000, description="Items per page (max 1000)."),
    
    # Sorting
    sort_by: Optional[str] = Query(
        "transaction_datetime",
        description="Field to sort by (transaction_datetime, transaction_id, plate_number, posting_date, amount, status, entry_plaza, exit_plaza, driver_name, lease_id, ledger_balance)."
    ),
    sort_order: str = Query("desc", enum=["asc", "desc"], description="Sort order."),
    
    # Date range filters
    from_posting_date: Optional[date] = Query(
        None,
        description="Filter from posting date (inclusive)."
    ),
    to_posting_date: Optional[date] = Query(
        None,
        description="Filter to posting date (inclusive)."
    ),
    from_transaction_date: Optional[date] = Query(
        None,
        description="Filter from transaction date (inclusive)."
    ),
    to_transaction_date: Optional[date] = Query(
        None,
        description="Filter to transaction date (inclusive)."
    ),
    from_transaction_time: Optional[time] = Query(
        None,
        description="Filter from transaction time (HH:MM:SS)."
    ),
    to_transaction_time: Optional[time] = Query(
        None,
        description="Filter to transaction time (HH:MM:SS)."
    ),
    
    # Amount range filters
    from_amount: Optional[Decimal] = Query(
        None,
        ge=0,
        description="Filter from amount (inclusive)."
    ),
    to_amount: Optional[Decimal] = Query(
        None,
        ge=0,
        description="Filter to amount (inclusive)."
    ),
    
    # Comma-separated multi-value filters
    plate_number: Optional[str] = Query(
        None,
        description="Filter by plate number (comma-separated for multiple, supports partial match)."
    ),
    transaction_id: Optional[str] = Query(
        None,
        description="Filter by transaction ID (comma-separated for multiple, supports partial match)."
    ),
    entry_lane: Optional[str] = Query(
        None,
        description="Filter by entry lane (comma-separated for multiple, supports partial match)."
    ),
    exit_lane: Optional[str] = Query(
        None,
        description="Filter by exit lane (comma-separated for multiple, supports partial match)."
    ),
    entry_plaza: Optional[str] = Query(
        None,
        description="Filter by entry plaza (comma-separated for multiple, supports partial match)."
    ),
    exit_plaza: Optional[str] = Query(
        None,
        description="Filter by exit plaza (comma-separated for multiple, supports partial match)."
    ),
    vin: Optional[str] = Query(
        None,
        description="Filter by VIN number (comma-separated for multiple, supports partial match)."
    ),
    medallion_no: Optional[str] = Query(
        None,
        description="Filter by medallion number (comma-separated for multiple, supports partial match)."
    ),
    driver_id: Optional[str] = Query(
        None,
        description="Filter by driver ID (comma-separated for multiple, supports partial match)."
    ),
    driver_name: Optional[str] = Query(
        None,
        description="Filter by driver name (comma-separated for multiple, supports partial match)."
    ),
    lease_id: Optional[str] = Query(
        None,
        description="Filter by lease ID (comma-separated for multiple, supports partial match)."
    ),
    from_ledger_balance: Optional[Decimal] = Query(
        None,
        ge=0,
        description="Filter from ledger balance (inclusive)."
    ),
    to_ledger_balance: Optional[Decimal] = Query(
        None,
        ge=0,
        description="Filter to ledger balance (inclusive)."
    ),
    status: Optional[str] = Query(
        None,
        description="Filter by transaction status (comma-separated for multiple: IMPORTED, ASSOCIATED, POSTED_TO_LEDGER, ASSOCIATION_FAILED, POSTING_FAILED)."
    ),
    
    # Other filters
    agency: Optional[str] = Query(
        None,
        description="Filter by agency (supports partial match)."
    ),
    ezpass_class: Optional[str] = Query(
        None,
        description="Filter by EZPass class (supports partial match)."
    ),
    
    # Dependencies
    ezpass_service: EZPassService = Depends(get_ezpass_service),
    current_user: User = Depends(get_current_user),
):
    """
    **List EZPass transactions with comprehensive filtering**
    
    Optimized for handling millions of records through:
    - Strategic use of database indexes (16 indexes covering common query patterns)
    - Efficient query construction with conditional joins
    - Pagination with accurate counts
    - Proper eager loading of relationships
    
    **Filter Types:**
    
    1. **Date Range Filters:**
       - Posting Date: from_posting_date / to_posting_date
       - Transaction Date: from_transaction_date / to_transaction_date
       - Transaction Time: from_transaction_time / to_transaction_time
    
    2. **Amount Range Filter:**
       - from_amount / to_amount
    
    3. **Comma-Separated Multi-Value Filters (supports partial matching):**
       - Plate Number
       - Transaction ID
       - Entry Lane
       - Exit Lane
       - Entry Plaza
       - Exit Plaza
       - VIN Number
       - Medallion Number
       - Driver ID
       - Status
    
    4. **Text Filters (supports partial matching):**
       - Agency
       - EZPass Class
    
    **Sorting:**
    - Supports sorting by any major field
    - Default: Most recent first (transaction_datetime desc)
    
    **Pagination:**
    - Default: 50 items per page
    - Maximum: 1000 items per page
    
    **Example Queries:**
    
    1. Find transactions for specific plates:
       `/trips/ezpass?plate_number=ABC123,XYZ789`
    
    2. Find transactions in date range with amount filter:
       `/trips/ezpass?from_transaction_date=2024-01-01&to_transaction_date=2024-01-31&from_amount=5.00&to_amount=20.00`
    
    3. Find posted transactions for specific drivers:
       `/trips/ezpass?driver_id=DRV001,DRV002&status=POSTED_TO_LEDGER`
    
    4. Find transactions at specific entry/exit plazas:
       `/trips/ezpass?entry_plaza=Lincoln Tunnel&exit_plaza=Holland Tunnel`
    """
    try:
        transactions, total_items = ezpass_service.repo.get_paginated_transactions(
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order,
            from_posting_date=from_posting_date,
            to_posting_date=to_posting_date,
            from_transaction_date=from_transaction_date,
            to_transaction_date=to_transaction_date,
            from_transaction_time=from_transaction_time,
            to_transaction_time=to_transaction_time,
            from_amount=from_amount,
            to_amount=to_amount,
            plate_number=plate_number,
            transaction_id=transaction_id,
            entry_lane=entry_lane,
            exit_lane=exit_lane,
            entry_plaza=entry_plaza,
            exit_plaza=exit_plaza,
            vin=vin,
            medallion_no=medallion_no,
            driver_id=driver_id,
            driver_name=driver_name,
            lease_id=lease_id,
            from_ledger_balance=from_ledger_balance,
            to_ledger_balance=to_ledger_balance,
            status=status,
            agency=agency,
            ezpass_class=ezpass_class,
        )

        # Transform to response schema
        response_items = [
            EZPassTransactionResponse(
                id=t.id,
                transaction_id=t.transaction_id,
                transaction_date=t.transaction_datetime,
                transaction_time=t.transaction_datetime.time() if t.transaction_datetime else None,
                entry_plaza=t.entry_plaza,
                exit_plaza=t.exit_plaza,
                ezpass_class=t.ezpass_class,
                medallion_no=(
                    t.medallion.medallion_number if t.medallion 
                    else (t.vehicle.medallions.medallion_number if t.vehicle and hasattr(t.vehicle, 'medallions') and t.vehicle.medallions else "")
                ),
                vin=t.vehicle.vin if t.vehicle else None,
                driver_id=t.driver.driver_id if t.driver else None,
                driver_name=t.driver.full_name if t.driver else None,
                lease_id=t.lease_id,
                ledger_balance=None,  # TODO: Calculate total outstanding balance for driver/lease
                tag_or_plate=t.tag_or_plate,
                posting_date=t.posting_date,
                status=t.status.value if t.status else None,
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
        logger.error(f"Error fetching EZPass transactions: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred while fetching EZPass data."
        ) from e


@router.get("/export", summary="Export EZPass Transaction Data")
def export_ezpass_transactions(
    # Export format
    export_format: str = Query(
        "excel",
        regex="^(excel|csv|pdf|json)$",
        alias="format",
        description="Export format: excel, csv, pdf, or json"
    ),
    
    # Sorting
    sort_by: Optional[str] = Query("transaction_datetime", description="Field to sort by."),
    sort_order: str = Query("desc", enum=["asc", "desc"], description="Sort order."),
    
    # All the same filters as list endpoint
    from_posting_date: Optional[date] = Query(None, description="Filter from posting date."),
    to_posting_date: Optional[date] = Query(None, description="Filter to posting date."),
    from_transaction_date: Optional[date] = Query(None, description="Filter from transaction date."),
    to_transaction_date: Optional[date] = Query(None, description="Filter to transaction date."),
    from_transaction_time: Optional[time] = Query(None, description="Filter from transaction time."),
    to_transaction_time: Optional[time] = Query(None, description="Filter to transaction time."),
    from_amount: Optional[Decimal] = Query(None, description="Filter from amount."),
    to_amount: Optional[Decimal] = Query(None, description="Filter to amount."),
    plate_number: Optional[str] = Query(None, description="Filter by plate number (comma-separated)."),
    transaction_id: Optional[str] = Query(None, description="Filter by transaction ID (comma-separated)."),
    entry_lane: Optional[str] = Query(None, description="Filter by entry lane (comma-separated)."),
    exit_lane: Optional[str] = Query(None, description="Filter by exit lane (comma-separated)."),
    entry_plaza: Optional[str] = Query(None, description="Filter by entry plaza (comma-separated)."),
    exit_plaza: Optional[str] = Query(None, description="Filter by exit plaza (comma-separated)."),
    vin: Optional[str] = Query(None, description="Filter by VIN (comma-separated)."),
    medallion_no: Optional[str] = Query(None, description="Filter by medallion number (comma-separated)."),
    driver_id: Optional[str] = Query(None, description="Filter by driver ID (comma-separated)."),
    status: Optional[str] = Query(None, description="Filter by status (comma-separated)."),
    agency: Optional[str] = Query(None, description="Filter by agency."),
    ezpass_class: Optional[str] = Query(None, description="Filter by EZPass class."),
    
    # Dependencies
    ezpass_service: EZPassService = Depends(get_ezpass_service),
    current_user: User = Depends(get_current_user),
):
    """
    **Export EZPass transaction data with the same filtering capabilities as the list endpoint**
    
    Supports formats: excel, csv, pdf, json
    
    **Performance Notes:**
    - Exports up to 100,000 records (configurable)
    - Uses same optimized query as list endpoint
    - Streams results to avoid memory issues
    
    **Export Limits:**
    - Maximum 100,000 records per export
    - For larger datasets, use date range filters to break into chunks
    
    **All filters from the list endpoint are supported**
    """
    try:
        # Fetch transactions with filters (limit to 100,000 for export)
        transactions, total_count = ezpass_service.repo.get_paginated_transactions(
            page=1,
            per_page=100000,  # Export limit
            sort_by=sort_by,
            sort_order=sort_order,
            from_posting_date=from_posting_date,
            to_posting_date=to_posting_date,
            from_transaction_date=from_transaction_date,
            to_transaction_date=to_transaction_date,
            from_transaction_time=from_transaction_time,
            to_transaction_time=to_transaction_time,
            from_amount=from_amount,
            to_amount=to_amount,
            plate_number=plate_number,
            transaction_id=transaction_id,
            entry_lane=entry_lane,
            exit_lane=exit_lane,
            entry_plaza=entry_plaza,
            exit_plaza=exit_plaza,
            vin=vin,
            medallion_no=medallion_no,
            driver_id=driver_id,
            status=status,
            agency=agency,
            ezpass_class=ezpass_class,
        )
        
        if not transactions:
            raise HTTPException(
                status_code=404,
                detail="No data available for export with the given filters."
            )
        
        # Prepare export data
        export_data = []
        for t in transactions:
            # Get medallion number with fallback logic
            medallion_number = ""
            if t.medallion:
                medallion_number = t.medallion.medallion_number
            elif t.vehicle and hasattr(t.vehicle, 'medallions') and t.vehicle.medallions:
                medallion_number = t.vehicle.medallions.medallion_number
            
            # Get active plate number
            plate_number_display = t.tag_or_plate
            
            export_data.append({
                "Transaction ID": t.transaction_id,
                "Transaction Date": t.transaction_datetime.strftime("%Y-%m-%d") if t.transaction_datetime else "",
                "Transaction Time": t.transaction_datetime.strftime("%H:%M:%S") if t.transaction_datetime else "",
                "Plate Number": plate_number_display,
                "Entry Plaza": t.entry_plaza or "",
                "Exit Plaza": t.exit_plaza or "",
                "Entry Lane": t.entry_plaza or "",  # Note: Model doesn't have separate lane field
                "Exit Lane": t.exit_plaza or "",    # Using plaza as proxy
                "Amount": float(t.amount) if t.amount else 0.0,
                "Agency": t.agency or "",
                "EZPass Class": t.ezpass_class or "",
                "Driver ID": t.driver.driver_id if t.driver else "",
                "Driver Name": t.driver.full_name if t.driver else "",
                "VIN": t.vehicle.vin if t.vehicle else "",
                "Medallion Number": medallion_number,
                "Lease ID": t.lease.lease_id if t.lease else "",
                "Status": t.status.value if t.status else "",
                "Posting Date": t.posting_date.strftime("%Y-%m-%d %H:%M:%S") if t.posting_date else "",
                "Failure Reason": t.failure_reason or "",
                "Created On": t.created_on.strftime("%Y-%m-%d %H:%M:%S") if t.created_on else "",
            })
        
        # Use ExporterFactory to generate file
        exporter = ExporterFactory.get_exporter(export_format, export_data)
        file_content = exporter.export()
        
        # Set file extension and media type
        ext_map = {"excel": "xlsx", "csv": "csv", "pdf": "pdf", "json": "json"}
        media_types = {
            "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "csv": "text/csv",
            "pdf": "application/pdf",
            "json": "application/json"
        }
        
        filename = f"ezpass_transactions_{date.today()}.{ext_map.get(export_format, 'xlsx')}"
        media_type = media_types.get(export_format, "application/octet-stream")
        
        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        return StreamingResponse(file_content, media_type=media_type, headers=headers)
        
    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Export validation error: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Error exporting EZPass transactions: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred during the export process."
        ) from e
   
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
    - New lease must be an active lease
    - New lease must belong to the specified new driver (valid primary driver)
    - Both new driver and new lease must exist in the system
    - All entries in bulk must originate from exactly one source lease
    - Source entries must have valid driver/lease associations
    
    **Process:**
    1. Validates source entries have valid associations
    2. Validates bulk source consistency (all from same lease)
    3. Validates new driver and new lease exist
    4. Verifies new lease belongs to new driver
    5. Updates transaction associations and performs ledger operations as needed
    6. Creates complete audit trail records
    
    **Supported Statuses:**
    - IMPORTED: Simple association update
    - ASSOCIATION_FAILED: Association update, status changed to IMPORTED
    - POSTED_TO_LEDGER: Full financial responsibility reconstruction with ledger reversal/reposting
    
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
            new_vehicle_id=request.new_vehicle_id,
            user_id=current_user.id,
            reason=request.reason
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
        import_logs, total_items = ezpass_service.repo.get_paginated_import_logs(
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
