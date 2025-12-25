# app/curb/router.py

"""
CURB API Router

Provides REST endpoints for CURB operations.
"""

from datetime import datetime, date
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.core.dependencies import get_current_user
from app.curb.exceptions import CurbAccountNotFoundError, CurbError
from app.curb.repository import CurbRepository
from app.curb.schemas import (
    CurbAccountCreate,
    CurbAccountResponse,
    CurbAccountUpdate,
    CurbImportRequest,
    CurbLedgerPostRequest,
    CurbLedgerPostResponse,
    CurbTripResponse,
    PaymentModeReference,
    CurbTripListResponse,
)
from app.curb.services import CurbService
from app.curb.models import CurbAccount
from app.curb.tasks import import_trips_task
from app.users.models import User
from app.utils.exporter_utils import ExporterFactory
from app.utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/curb", tags=["CURB Trips"])


def get_curb_service(db: Session = Depends(get_db)) -> CurbService:
    """Dependency to get CurbService instance"""
    return CurbService(db)


def get_curb_repository(db: Session = Depends(get_db)) -> CurbRepository:
    """Dependency to get CurbRepository instance"""
    return CurbRepository(db)


# --- Account management endpoints --- #

@router.get("/accounts", response_model=List[CurbAccountResponse])
def list_curb_accounts(
    repo: CurbRepository = Depends(get_curb_repository),
    current_user: User = Depends(get_current_user),
):
    """
    List all CURB accounts
    
    Returns all configured CURB accounts with their settings.
    """
    try:
        accounts = repo.db.query(CurbAccount).all()
        return accounts
    except Exception as e:
        logger.error(f"Failed to list CURB accounts: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/accounts", response_model=CurbAccountResponse, status_code=status.HTTP_201_CREATED)
def create_curb_account(
    account_data: CurbAccountCreate,
    repo: CurbRepository = Depends(get_curb_repository),
    current_user: User = Depends(get_current_user),
):
    """
    Create a new CURB account
    
    Adds a new CURB account configuration for data import.
    """
    try:
        # Check if account name already exists
        existing = repo.get_account_by_name(account_data.account_name)
        if existing:
            raise HTTPException(
                status_code=400,
                detail=f"Account with name '{account_data.account_name}' already exists"
            )
        
        account = repo.create_account(account_data.dict())
        repo.db.commit()
        
        logger.info(f"Created CURB account: {account.account_name} by user {current_user.id}")
        return account
        
    except HTTPException:
        raise
    except Exception as e:
        repo.db.rollback()
        logger.error(f"Failed to create CURB account: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/accounts/{account_id}", response_model=CurbAccountResponse)
def get_curb_account(
    account_id: int,
    repo: CurbRepository = Depends(get_curb_repository),
    current_user: User = Depends(get_current_user),
):
    """Get a specific CURB account by ID"""
    try:
        account = repo.get_account_by_id(account_id)
        return account
    except CurbAccountNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Failed to get CURB account: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.put("/accounts/{account_id}", response_model=CurbAccountResponse)
def update_curb_account(
    account_id: int,
    update_data: CurbAccountUpdate,
    repo: CurbRepository = Depends(get_curb_repository),
    current_user: User = Depends(get_current_user),
):
    """Update an existing CURB account"""
    try:
        # Filter out None values
        update_dict = {k: v for k, v in update_data.model_dump(exclude_unset=True).items() if v is not None}
        
        account = repo.update_account(account_id, update_dict)
        repo.db.commit()
        
        logger.info(f"Updated CURB account {account_id} by user {current_user.id}")
        return account
        
    except CurbAccountNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        repo.db.rollback()
        logger.error(f"Failed to update CURB account: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e
    
# --- Trip listing and filtering --- #

@router.get("/trips", response_model=CurbTripListResponse, summary="List CURB Trips with Enhanced Filters")
def list_curb_trips_enhanced(
    # Comma-separated ID/Text filters
    trip_ids: Optional[str] = Query(None, description="Comma-separated trip IDs"),
    driver_ids: Optional[str] = Query(None, description="Comma-separated driver IDs"),
    vehicle_plates: Optional[str] = Query(None, description="Comma-separated vehicle plates"),
    medallion_numbers: Optional[str] = Query(None, description="Comma-separated medallion numbers"),
    tlc_license_numbers: Optional[str] = Query(None, description="Comma-separated TLC license numbers"),
    
    # Date range filters
    trip_start_from: Optional[datetime] = Query(None, description="Trip start from datetime"),
    trip_start_to: Optional[datetime] = Query(None, description="Trip start to datetime"),
    trip_end_from: Optional[datetime] = Query(None, description="Trip end from datetime"),
    trip_end_to: Optional[datetime] = Query(None, description="Trip end to datetime"),
    transaction_date_from: Optional[datetime] = Query(None, description="Transaction date from"),
    transaction_date_to: Optional[datetime] = Query(None, description="Transaction date to"),
    
    # Amount range filters
    total_amount_from: Optional[Decimal] = Query(None, ge=0, description="Minimum total amount"),
    total_amount_to: Optional[Decimal] = Query(None, ge=0, description="Maximum total amount"),
    
    # Payment modes (comma-separated)
    payment_modes: Optional[str] = Query(
        None,
        description="Comma-separated payment modes (CASH, CREDIT_CARD, PRIVATE_CARD, etc.)"
    ),
    
    # Status filter
    status: Optional[str] = Query(None, description="Status filter (IMPORTED, POSTED_TO_LEDGER)"),
    
    # Account filter
    account_ids: Optional[str] = Query(None, description="Comma-separated account IDs"),
    
    # Pagination
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(50, ge=1, le=1000, description="Items per page"),
    
    # Sorting
    sort_by: str = Query("start_time", description="Sort field"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order"),
    
    # Dependencies
    repo: CurbRepository = Depends(lambda: CurbRepository(next(get_db()))),
    current_user: User = Depends(get_current_user),
):
    """
    **List CURB trips with comprehensive filtering**
    
    Optimized for handling millions of records through:
    - Strategic use of database indexes
    - Efficient query construction
    - Minimal joins
    - Pagination
    
    **Filters Available:**
    - Trip ID (comma-separated multiple)
    - Driver ID (comma-separated multiple)
    - Vehicle Plate (comma-separated multiple)
    - Medallion Number (comma-separated multiple)
    - TLC License Number (comma-separated multiple)
    - Trip Start/End Date Range (from/to)
    - Transaction Date Range (from/to)
    - Total Amount Range (from/to)
    - Payment Modes (comma-separated: CASH, CREDIT_CARD, etc.)
    - Status (IMPORTED, POSTED_TO_LEDGER)
    """
    try:
        # Parse account_ids if provided
        account_ids_list = None
        if account_ids:
            account_ids_list = [int(aid.strip()) for aid in account_ids.split(',') if aid.strip().isdigit()]
        
        # Call repository method with all filters
        trips, total_count = repo.list_trips_with_enhanced_filters(
            page=page,
            per_page=per_page,
            trip_ids=trip_ids,
            driver_ids=driver_ids,
            vehicle_plates=vehicle_plates,
            medallion_numbers=medallion_numbers,
            tlc_license_numbers=tlc_license_numbers,
            trip_start_from=trip_start_from,
            trip_start_to=trip_start_to,
            trip_end_from=trip_end_from,
            trip_end_to=trip_end_to,
            transaction_date_from=transaction_date_from,
            transaction_date_to=transaction_date_to,
            total_amount_from=total_amount_from,
            total_amount_to=total_amount_to,
            payment_modes=payment_modes,
            status=status,
            account_ids=account_ids_list,
            sort_by=sort_by,
            sort_order=sort_order,
        )
        
        # Calculate total pages
        total_pages = (total_count + per_page - 1) // per_page
        
        # Transform trips to include vehicle plate
        from app.curb.schemas import CurbTripResponseEnhanced
        enhanced_trips = []
        for trip in trips:
            # Get active plate number
            plate_number = None
            if trip.vehicle:
                # Try to get from eager-loaded relationship
                if hasattr(trip.vehicle, 'registrations'):
                    active_reg = next(
                        (reg for reg in trip.vehicle.registrations if reg.is_active),
                        None
                    )
                    if active_reg:
                        plate_number = active_reg.plate_number
                # Fallback to method if available
                elif hasattr(trip.vehicle, 'get_active_plate_number'):
                    try:
                        plate_number = trip.vehicle.get_active_plate_number()
                    except:
                        pass
            
            enhanced_trip = CurbTripResponseEnhanced(
                id=trip.id,
                curb_trip_id=trip.curb_trip_id,
                account_id=trip.account_id,
                status=trip.status.value if hasattr(trip.status, 'value') else str(trip.status),
                driver_id=trip.driver_id,
                driver_name=trip.driver.full_name if trip.driver else None,
                lease_id=trip.lease_id,
                vehicle_id=trip.vehicle_id,
                vehicle_plate=plate_number,  # Include plate number
                medallion_id=trip.medallion_id,
                medallion_number=trip.curb_cab_number,
                curb_driver_id=trip.curb_driver_id,
                curb_cab_number=trip.curb_cab_number,
                start_time=trip.start_time,
                end_time=trip.end_time,
                transaction_date=trip.transaction_date,
                fare=trip.fare,
                tips=trip.tips,
                tolls=trip.tolls,
                extras=trip.extras,
                total_amount=trip.total_amount,
                surcharge=trip.surcharge,
                improvement_surcharge=trip.improvement_surcharge,
                congestion_fee=trip.congestion_fee,
                airport_fee=trip.airport_fee,
                cbdt_fee=trip.cbdt_fee,
                payment_type=trip.payment_type.value if hasattr(trip.payment_type, 'value') else str(trip.payment_type),
                ledger_posting_ref=trip.ledger_posting_ref,
                posted_to_ledger_at=trip.posted_to_ledger_at,
                distance_miles=trip.distance_miles,
                num_passengers=trip.num_passengers,
            )
            enhanced_trips.append(enhanced_trip)
        
        # Get available payment modes for reference
        payment_mode_reference = [
            PaymentModeReference(value="CASH", label="Cash"),
            PaymentModeReference(value="CREDIT_CARD", label="Credit Card"),
            PaymentModeReference(value="PRIVATE_CARD", label="Private Card"),
            PaymentModeReference(value="OTHER", label="Other"),
        ]
        
        return CurbTripListResponse(
            items=enhanced_trips,
            total_items=total_count,
            page=page,
            per_page=per_page,
            total_pages=total_pages,
            payment_modes=payment_mode_reference,
        )
        
    except Exception as e:
        logger.error(f"Failed to list CURB trips: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while listing CURB trips: {str(e)}"
        ) from e


@router.get("/trips/{trip_id}", response_model=CurbTripResponse)
def get_curb_trip(
    trip_id: int,
    repo: CurbRepository = Depends(get_curb_repository),
    current_user: User = Depends(get_current_user),
):
    """Get a specific CURB trip by ID"""
    try:
        trip = repo.get_trip_by_id(trip_id)
        return trip
    except Exception as e:
        logger.error(f"Failed to get CURB trip: {e}", exc_info=True)
        raise HTTPException(status_code=404, detail="Trip not found") from e
    
# --- Data import --- #

@router.post("/import", status_code=202)
def import_curb_data(
    import_request: CurbImportRequest = None,
    current_user: User = Depends(get_current_user),
):
    """
    Trigger CURB data import via background task
    
    Imports CASH trips from configured CURB accounts for the specified
    datetime range. If no range specified, imports last 3 hours.
    
    Returns immediately with task ID. Import runs asynchronously.
    """
    try:
        if import_request is None:
            import_request = CurbImportRequest()
        
        # Prepare task arguments
        task_kwargs = {}
        
        if import_request.account_ids:
            task_kwargs['account_ids'] = import_request.account_ids
        
        if import_request.from_datetime:
            task_kwargs['from_datetime'] = import_request.from_datetime.isoformat()
        
        if import_request.to_datetime:
            task_kwargs['to_datetime'] = import_request.to_datetime.isoformat()
        
        # Trigger async task
        task = import_trips_task.apply_async(kwargs=task_kwargs)
        
        logger.info(
            f"CURB import task queued by user {current_user.id} - "
            f"task_id: {task.id}, params: {task_kwargs}"
        )
        
        return {
            "status": "accepted",
            "message": "CURB import task queued successfully",
            "task_id": task.id,
            "accounts": import_request.account_ids or "all active accounts",
            "datetime_range": {
                "from": import_request.from_datetime.isoformat() if import_request.from_datetime else "3 hours ago",
                "to": import_request.to_datetime.isoformat() if import_request.to_datetime else "now"
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to queue CURB import task: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e
    
# --- Ledger Posting --- #

@router.post("/post-to-ledger", response_model=CurbLedgerPostResponse)
def post_to_ledger(
    post_request: CurbLedgerPostRequest,
    service: CurbService = Depends(get_curb_service),
    current_user: User = Depends(get_current_user),
):
    """
    Manually post CURB trips to ledger
    
    Posts individual CASH trips as CREDIT postings to the ledger
    for the specified date range.
    """
    try:
        result = service.post_trips_to_ledger(
            start_date=datetime.combine(post_request.start_date, datetime.min.time()),
            end_date=datetime.combine(post_request.end_date, datetime.max.time()),
            driver_ids=post_request.driver_ids,
            lease_ids=post_request.lease_ids,
        )
        
        logger.info(f"Manual ledger posting by user {current_user.id}: {result['trips_posted_to_ledger']} trips posted")
        
        return CurbLedgerPostResponse(**result)
        
    except CurbError as e:
        logger.error(f"Ledger posting failed: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Unexpected error during ledger posting: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e
    
# --- Statistics --- #

@router.get("/stats")
def get_curb_statistics(
    repo: CurbRepository = Depends(get_curb_repository),
    current_user: User = Depends(get_current_user),
):
    """Get overall CURB system statistics"""
    try:
        system_stats = repo.get_system_statistics()
        
        # Get per-account stats
        accounts = repo.get_active_accounts()
        account_stats = []
        
        for account in accounts:
            stats = repo.get_account_statistics(account.id)
            account_stats.append({
                "account_id": account.id,
                "account_name": account.account_name,
                "is_active": account.is_active,
                **stats
            })
        
        return {
            **system_stats,
            "accounts": account_stats
        }
        
    except Exception as e:
        logger.error(f"Failed to get CURB statistics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e
    

@router.get("/trips/export", summary="Export CURB Trips Data")
def export_curb_trips(
    export_format: str = Query("excel", regex="^(excel|csv|pdf|json)$", alias="format"),
    
    # Same filters as list endpoint
    trip_ids: Optional[str] = Query(None, description="Comma-separated trip IDs"),
    driver_ids: Optional[str] = Query(None, description="Comma-separated driver IDs"),
    vehicle_plates: Optional[str] = Query(None, description="Comma-separated vehicle plates"),
    medallion_numbers: Optional[str] = Query(None, description="Comma-separated medallion numbers"),
    tlc_license_numbers: Optional[str] = Query(None, description="Comma-separated TLC license numbers"),
    trip_start_from: Optional[datetime] = Query(None, description="Trip start from datetime"),
    trip_start_to: Optional[datetime] = Query(None, description="Trip start to datetime"),
    trip_end_from: Optional[datetime] = Query(None, description="Trip end from datetime"),
    trip_end_to: Optional[datetime] = Query(None, description="Trip end to datetime"),
    transaction_date_from: Optional[datetime] = Query(None, description="Transaction date from"),
    transaction_date_to: Optional[datetime] = Query(None, description="Transaction date to"),
    total_amount_from: Optional[Decimal] = Query(None, ge=0, description="Minimum total amount"),
    total_amount_to: Optional[Decimal] = Query(None, ge=0, description="Maximum total amount"),
    payment_modes: Optional[str] = Query(None, description="Comma-separated payment modes"),
    status: Optional[str] = Query(None, description="Status filter"),
    account_ids: Optional[str] = Query(None, description="Comma-separated account IDs"),
    sort_by: str = Query("start_time", description="Sort field"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order"),
    
    # Dependencies
    repo: CurbRepository = Depends(lambda: CurbRepository(next(get_db()))),
    current_user: User = Depends(get_current_user),
):
    """
    **Export CURB trips data with the same filtering capabilities as the list endpoint**
    
    Supports formats: excel, csv, pdf, json
    
    **Performance Notes:**
    - Exports up to 100,000 records (configurable)
    - Uses same optimized query as list endpoint
    - Streams results to avoid memory issues
    """
    try:
        # Parse account_ids if provided
        account_ids_list = None
        if account_ids:
            account_ids_list = [int(aid.strip()) for aid in account_ids.split(',') if aid.strip().isdigit()]
        
        # Fetch trips with filters (limit to 100,000 for export)
        trips, total_count = repo.list_trips_with_enhanced_filters(
            page=1,
            per_page=100000,  # Export limit
            trip_ids=trip_ids,
            driver_ids=driver_ids,
            vehicle_plates=vehicle_plates,
            medallion_numbers=medallion_numbers,
            tlc_license_numbers=tlc_license_numbers,
            trip_start_from=trip_start_from,
            trip_start_to=trip_start_to,
            trip_end_from=trip_end_from,
            trip_end_to=trip_end_to,
            transaction_date_from=transaction_date_from,
            transaction_date_to=transaction_date_to,
            total_amount_from=total_amount_from,
            total_amount_to=total_amount_to,
            payment_modes=payment_modes,
            status=status,
            account_ids=account_ids_list,
            sort_by=sort_by,
            sort_order=sort_order,
        )
        
        if not trips:
            raise HTTPException(
                status_code=404,
                detail="No data available for export with the given filters."
            )
        
        # Prepare export data
        export_data = []
        for trip in trips:
            # Get active plate number
            plate_number = ""
            if trip.vehicle:
                # Try to get from eager-loaded relationship
                if hasattr(trip.vehicle, 'registrations'):
                    active_reg = next(
                        (reg for reg in trip.vehicle.registrations if reg.is_active),
                        None
                    )
                    if active_reg:
                        plate_number = active_reg.plate_number
                # Fallback to method if available
                elif hasattr(trip.vehicle, 'get_active_plate_number'):
                    try:
                        plate_number = trip.vehicle.get_active_plate_number()
                    except:
                        pass
            
            export_data.append({
                "Trip ID": trip.curb_trip_id,
                "Trip Start Date": trip.start_time.strftime("%Y-%m-%d %H:%M:%S") if trip.start_time else "",
                "Trip End Date": trip.end_time.strftime("%Y-%m-%d %H:%M:%S") if trip.end_time else "",
                "Transaction Date": trip.transaction_date.strftime("%Y-%m-%d %H:%M:%S") if trip.transaction_date else "",
                "Driver ID": trip.driver.driver_id if trip.driver else "",
                "Driver Name": trip.driver.full_name if trip.driver else "",
                "TLC License No": trip.driver.tlc_license.tlc_license_number if trip.driver and trip.driver.tlc_license else "",
                "Vehicle Plate": plate_number,  # Vehicle plate number
                "Medallion No": trip.curb_cab_number,
                "Total Amount": float(trip.total_amount),
                "Fare": float(trip.fare),
                "Tips": float(trip.tips),
                "Tolls": float(trip.tolls),
                "Extras": float(trip.extras),
                "Surcharge": float(trip.surcharge),
                "Improvement Surcharge": float(trip.improvement_surcharge) if trip.improvement_surcharge else 0.0,
                "Congestion Fee": float(trip.congestion_fee) if trip.congestion_fee else 0.0,
                "Airport Fee": float(trip.airport_fee) if trip.airport_fee else 0.0,
                "CBDT Fee": float(trip.cbdt_fee) if trip.cbdt_fee else 0.0,
                "Payment Mode": trip.payment_type.value if trip.payment_type else "",
                "Status": trip.status.value if trip.status else "",
                "Account": trip.account.account_name if trip.account else "",
                "Distance (Miles)": float(trip.distance_miles) if trip.distance_miles else "",
                "Passengers": trip.num_passengers if trip.num_passengers else "",
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
        
        filename = f"curb_trips_{date.today()}.{ext_map.get(export_format, 'xlsx')}"
        media_type = media_types.get(export_format, "application/octet-stream")
        
        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        return StreamingResponse(file_content, media_type=media_type, headers=headers)
        
    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Export validation error: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Error exporting CURB trips: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during the export process"
        ) from e