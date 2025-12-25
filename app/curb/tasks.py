# app/curb/tasks.py

"""
CURB Celery Tasks

Scheduled background tasks for automated CURB operations.
"""

from datetime import datetime, timedelta, timezone

from app.worker.app import app
from app.core.db import SessionLocal
from app.curb.services import CurbService
from app.utils.logger import get_logger

logger = get_logger(__name__)


@app.task(name="curb.import_trips_task", bind=True, max_retries=3)
def import_trips_task(self):
    """
    Import CURB trips for all active accounts
    
    Scheduled to run every 3 hours.
    Imports CASH trips from the last 3-hour window.
    
    Schedule: crontab(minute=0, hour='*/3')
    """
    db = SessionLocal()
    
    try:
        logger.info("Starting scheduled CURB trip import (3-hour window)")
        
        service = CurbService(db)
        
        # Import last 3 hours
        to_datetime = datetime.now(timezone.utc)
        from_datetime = to_datetime - timedelta(hours=3)
        
        result = service.import_trips_from_accounts(
            account_ids=None,  # All active accounts
            from_datetime=from_datetime,
            to_datetime=to_datetime,
        )
        
        logger.info(
            f"CURB import completed: {result['trips_imported']} new trips, "
            f"{result['trips_updated']} updated from {len(result['accounts_processed'])} account(s)"
        )
        
        # Chain: Post imported trips to ledger immediately
        if result['trips_imported'] > 0:
            logger.info("Triggering ledger posting for newly imported trips")
            post_trips_to_ledger_task.apply_async(
                kwargs={
                    'start_date': from_datetime,
                    'end_date': to_datetime
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
def post_trips_to_ledger_task(self, start_date=None, end_date=None):
    """
    Post CURB trips to ledger
    
    Can be triggered:
    1. Automatically after trip import (chained task)
    2. Scheduled every Sunday at 3:30 AM as catchall (crontab(hour=3, minute=30, day_of_week=0))
    
    Posts all IMPORTED trips from the specified date range as individual
    CREDIT entries to the ledger.
    
    Args:
        start_date: Start of date range (defaults to 7 days ago)
        end_date: End of date range (defaults to now)
    """
    db = SessionLocal()
    
    try:
        logger.info("Starting CURB ledger posting")
        
        service = CurbService(db)
        
        # Use provided dates or default to past week
        if end_date is None:
            end_date = datetime.now(timezone.utc)
        if start_date is None:
            start_date = end_date - timedelta(days=7)
        
        result = service.post_trips_to_ledger(
            start_date=start_date,
            end_date=end_date,
            driver_ids=None,  # All drivers
            lease_ids=None,   # All leases
        )
        
        logger.info(
            f"CURB ledger posting completed: {result['trips_posted_to_ledger']} trips posted, "
            f"total amount: ${result['total_amount_posted']}"
        )
        
        return {
            "status": "success",
            "trips_posted": result["trips_posted_to_ledger"],
            "total_amount": float(result["total_amount_posted"]),
        }
        
    except Exception as e:
        db.rollback()
        logger.error(f"CURB ledger posting task failed: {e}", exc_info=True)
        return {"status": "failed", "error": str(e)}
        
    finally:
        db.close()