# app/curb/router.py

"""
CURB API Router

Provides REST endpoints for CURB operations.
"""

from datetime import datetime
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
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
    CurbImportResponse,
    CurbLedgerPostRequest,
    CurbLedgerPostResponse,
    CurbTripFilters,
    CurbTripResponse,
    PaginatedCurbTripResponse,
)
from app.curb.services import CurbService
from app.curb.models import CurbAccount
from app.users.models import User
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

@router.get("/trips", response_model=PaginatedCurbTripResponse)
def list_curb_trips(
    filters: CurbTripFilters = Depends(),
    repo: CurbRepository = Depends(get_curb_repository),
    current_user: User = Depends(get_current_user),
):
    """
    List CURB trips with pagination and filters
    
    Supports filtering by:
    - Account ID(s)
    - Driver ID(s)
    - Lease ID(s)
    - Status
    - Date range
    - Amount range
    """
    try:
        trips, total_count = repo.list_trips_paginated(
            page=filters.page,
            per_page=filters.per_page,
            account_ids=filters.account_ids,
            driver_ids=filters.driver_ids,
            lease_ids=filters.lease_ids,
            status=filters.status,
            start_date=filters.start_date,
            end_date=filters.end_date,
            sort_by=filters.sort_by,
            sort_order=filters.sort_order,
        )
        
        total_pages = (total_count + filters.per_page - 1) // filters.per_page
        
        return PaginatedCurbTripResponse(
            items=trips,
            total_items=total_count,
            page=filters.page,
            per_page=filters.per_page,
            total_pages=total_pages,
        )
        
    except Exception as e:
        logger.error(f"Failed to list CURB trips: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


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

@router.post("/import", response_model=CurbImportResponse)
def import_curb_data(
    import_request: CurbImportRequest = None,
    service: CurbService = Depends(get_curb_service),
    current_user: User = Depends(get_current_user),
):
    """
    Manually trigger CURB data import
    
    Imports CASH trips from configured CURB accounts for the specified
    datetime range. If no range specified, imports last 3 hours.
    """
    try:
        if import_request is None:
            import_request = CurbImportRequest()
        
        result = service.import_trips_from_accounts(
            account_ids=import_request.account_ids,
            from_datetime=import_request.from_datetime,
            to_datetime=import_request.to_datetime,
        )
        
        logger.info(f"Manual CURB import triggered by user {current_user.id}: {result['trips_imported']} trips imported")
        
        return CurbImportResponse(**result)
        
    except CurbError as e:
        logger.error(f"CURB import failed: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Unexpected error during CURB import: {e}", exc_info=True)
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