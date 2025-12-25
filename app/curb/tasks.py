# app/curb/tasks.py

"""
CURB Celery Tasks

Scheduled background tasks for automated CURB operations.
"""

from datetime import datetime, timedelta, timezone
from typing import List, Optional

from app.worker.app import app
from app.core.db import SessionLocal
from app.curb.services import CurbService
from app.utils.logger import get_logger

logger = get_logger(__name__)


@app.task(name="curb.import_trips_task", bind=True, max_retries=3)
def import_trips_task(
    self,
    account_ids: Optional[List[int]] = None,
    from_datetime: Optional[str] = None,  # ISO format string
    to_datetime: Optional[str] = None     # ISO format string
):
    """
    Import CURB trips from specified accounts and datetime range
    
    Scheduled to run every 3 hours with clean datetime format (no microseconds)
    """
    db = SessionLocal()
    
    try:
        # Convert ISO strings to datetime objects if provided
        from_dt = None
        to_dt = None
        
        if from_datetime:
            from_dt = datetime.fromisoformat(from_datetime.replace('Z', '+00:00'))
        
        if to_datetime:
            to_dt = datetime.fromisoformat(to_datetime.replace('Z', '+00:00'))
        
        # Set defaults if not provided (3-hour window)
        if not to_dt:
            # Get current time and strip microseconds
            to_dt = datetime.now(timezone.utc).replace(microsecond=0)
        
        if not from_dt:
            from_dt = to_dt - timedelta(hours=3)
        
        # ====== ENSURE CLEAN DATETIME FORMAT ======
        # Strip microseconds to ensure clean format: YYYY-MM-DD HH:MM:SS
        # This prevents issues with CURB API formatting
        from_dt = from_dt.replace(microsecond=0)
        to_dt = to_dt.replace(microsecond=0)
        # ===========================================
        
        logger.info(
            f"Starting CURB trip import - "
            f"accounts: {account_ids or 'all'}, "
            f"from: {from_dt.strftime('%m/%d/%Y %H:%M:%S')}, "
            f"to: {to_dt.strftime('%m/%d/%Y %H:%M:%S')}"
        )
        
        service = CurbService(db)
        
        result = service.import_trips_from_accounts(
            account_ids=account_ids,
            from_datetime=from_dt,  # Clean datetime object (no microseconds)
            to_datetime=to_dt,      # Clean datetime object (no microseconds)
        )
        
        logger.info(
            f"CURB import completed: {result['trips_imported']} new trips, "
            f"{result['trips_updated']} updated from {len(result['accounts_processed'])} account(s)"
        )
        
        # Chain: Post imported trips to ledger immediately if any were imported
        if result['trips_imported'] > 0:
            logger.info("Triggering ledger posting for newly imported trips")
            post_trips_to_ledger_task.apply_async(
                kwargs={
                    'start_date': from_dt.isoformat(),
                    'end_date': to_dt.isoformat()
                }
            )
        
        return {
            "status": "success",
            "trips_imported": result["trips_imported"],
            "trips_updated": result["trips_updated"],
            "accounts_processed": len(result["accounts_processed"]),
        }
        
    except Exception as e:
        db.rollback()
        logger.error(f"CURB import task failed: {e}", exc_info=True)
        
        # Retry up to 3 times with exponential backoff
        try:
            raise self.retry(exc=e, countdown=60 * (2 ** self.request.retries))
        except self.MaxRetriesExceededError:
            logger.error("Max retries exceeded for CURB import task")
            return {"status": "failed", "error": str(e)}
        
    finally:
        db.close()


@app.task(name="curb.post_trips_to_ledger_task", bind=True)
def post_trips_to_ledger_task(
    self,
    start_date: Optional[str] = None,  # ISO format string
    end_date: Optional[str] = None     # ISO format string
):
    """
    Post CURB trips to ledger
    
    Can be triggered:
    1. Automatically after trip import (chained task)
    2. Scheduled every Sunday at 3:30 AM as catchall
    3. Manually via API endpoint
    
    Posts all IMPORTED trips from the specified date range as individual
    CREDIT entries to the ledger.
    """
    db = SessionLocal()
    
    try:
        # Convert ISO strings to datetime objects if provided
        start_dt = None
        end_dt = None
        
        if start_date:
            start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        
        if end_date:
            end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        
        # Default to last week if not specified
        if not end_dt:
            end_dt = datetime.now(timezone.utc)
        if not start_dt:
            start_dt = end_dt - timedelta(days=7)
        
        logger.info(f"Starting CURB ledger posting from {start_dt} to {end_dt}")
        
        service = CurbService(db)
        
        result = service.post_trips_to_ledger(
            start_date=start_dt,
            end_date=end_dt,
            driver_ids=None,
            lease_ids=None,
        )
        
        logger.info(
            f"CURB ledger posting completed: {result.get('trips_processed', 0)} trips, "
            f"${result.get('total_amount', 0)} total"
        )
        
        return {
            "status": "success",
            "trips_processed": result.get("trips_processed", 0),
            "total_amount": float(result.get("total_amount", 0)),
        }
        
    except Exception as e:
        db.rollback()
        logger.error(f"CURB ledger posting task failed: {e}", exc_info=True)
        
        try:
            raise self.retry(exc=e, countdown=60 * (2 ** self.request.retries))
        except self.MaxRetriesExceededError:
            logger.error("Max retries exceeded for CURB ledger posting task")
            return {"status": "failed", "error": str(e)}
        
    finally:
        db.close()