# app/curb/tasks.py - ADD this enhanced task

from datetime import datetime, timedelta, timezone
from typing import List, Optional

from app.worker.app import app
from app.core.db import SessionLocal
from app.curb.services import CurbService
from app.utils.logger import get_logger

logger = get_logger(__name__)


@app.task(name="curb.import_trips_aligned_task", bind=True, max_retries=3)
def import_trips_aligned_task(
    self,
    account_ids: Optional[List[int]] = None,
):
    """
    Import CURB trips using ALIGNED 3-hour windows
    
    This version calculates the exact 3-hour window based on the current time,
    ensuring imports align to: 00:00-02:59, 03:00-05:59, 06:00-08:59, etc.
    
    Scheduled to run 8 times per day at the END of each window:
    - 02:59 → imports 00:00-02:59:59
    - 05:59 → imports 03:00-05:59:59
    - 08:59 → imports 06:00-08:59:59
    - 11:59 → imports 09:00-11:59:59
    - 14:59 → imports 12:00-14:59:59
    - 17:59 → imports 15:00-17:59:59
    - 20:59 → imports 18:00-20:59:59
    - 23:59 → imports 21:00-23:59:59
    """
    db = SessionLocal()
    
    try:
        # Get current time in America/New_York timezone
        import pytz
        ny_tz = pytz.timezone('America/New_York')
        now = datetime.now(ny_tz)
        
        # Calculate which 3-hour window we're in
        # Windows: 0-2, 3-5, 6-8, 9-11, 12-14, 15-17, 18-20, 21-23
        current_hour = now.hour
        window_start_hour = (current_hour // 3) * 3
        
        # Calculate window boundaries
        from_dt = now.replace(
            hour=window_start_hour,
            minute=0,
            second=0,
            microsecond=0
        )
        to_dt = from_dt + timedelta(hours=3) - timedelta(seconds=1)
        
        # Format: HH:MM:SS (inclusive left, inclusive right with 59:59)
        logger.info(
            f"Starting CURB aligned import - "
            f"Window: {from_dt.strftime('%H:%M:%S')} - {to_dt.strftime('%H:%M:%S')}, "
            f"accounts: {account_ids or 'all'}"
        )
        
        service = CurbService(db)
        
        result = service.import_trips_from_accounts(
            account_ids=account_ids,
            from_datetime=from_dt,
            to_datetime=to_dt,
        )
        
        logger.info(
            f"CURB aligned import completed: {result['trips_imported']} new trips, "
            f"{result['trips_updated']} updated from {len(result['accounts_processed'])} account(s)"
        )
        
        # Chain: Post imported trips to ledger if any were imported
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
            "window_start": from_dt.strftime('%H:%M:%S'),
            "window_end": to_dt.strftime('%H:%M:%S'),
            "trips_imported": result["trips_imported"],
            "trips_updated": result["trips_updated"],
            "accounts_processed": len(result["accounts_processed"]),
        }
        
    except Exception as e:
        db.rollback()
        logger.error(f"CURB aligned import task failed: {e}", exc_info=True)
        
        # Retry up to 3 times with exponential backoff
        try:
            raise self.retry(exc=e, countdown=60 * (2 ** self.request.retries))
        except self.MaxRetriesExceededError:
            logger.error("Max retries exceeded for CURB aligned import task")
            return {"status": "failed", "error": str(e)}
        
    finally:
        db.close()


@app.task(name="curb.import_trips_task", bind=True, max_retries=3)
def import_trips_task(
    self,
    account_ids: Optional[List[int]] = None,
    from_datetime: Optional[str] = None,  # ISO format string
    to_datetime: Optional[str] = None     # ISO format string
):
    """
    Import CURB trips from specified accounts and datetime range
    
    ENHANCED: Now automatically aligns to 3-hour windows when no datetime specified
    
    Scheduled to run every 3 hours: 00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00
    
    Import Strategy:
    - When run at 00:00 → imports 21:00-23:59:59 (previous day)
    - When run at 03:00 → imports 00:00-02:59:59 (completed window)
    - When run at 06:00 → imports 03:00-05:59:59 (completed window)
    - When run at 09:00 → imports 06:00-08:59:59 (completed window)
    - When run at 12:00 → imports 09:00-11:59:59 (completed window)
    - When run at 15:00 → imports 12:00-14:59:59 (completed window)
    - When run at 18:00 → imports 15:00-17:59:59 (completed window)
    - When run at 21:00 → imports 18:00-20:59:59 (completed window)
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
        
        # ENHANCED: Calculate aligned 3-hour window if not specified
        if not to_dt or not from_dt:
            import pytz
            ny_tz = pytz.timezone('America/New_York')
            now = datetime.now(ny_tz)
            
            # Calculate the PREVIOUS completed 3-hour window
            current_hour = now.hour
            
            # Determine which window just completed
            # If it's 03:00, the window 00:00-02:59 just completed
            # If it's 06:00, the window 03:00-05:59 just completed
            if current_hour >= 3:
                window_end_hour = (current_hour // 3) * 3
                window_start_hour = window_end_hour - 3
            else:
                # It's 00:00-02:59, so import previous day's 21:00-23:59
                window_end_hour = 0
                window_start_hour = 21
                now = now - timedelta(days=1)
            
            from_dt = now.replace(
                hour=window_start_hour,
                minute=0,
                second=0,
                microsecond=0
            )
            
            # End at 59:59 of the window
            to_dt = now.replace(
                hour=window_end_hour if window_end_hour > 0 else 23,
                minute=59,
                second=59,
                microsecond=0
            )
            
            if window_end_hour == 0:
                # Adjust to correct day
                to_dt = to_dt.replace(hour=23)
        
        # Strip microseconds for clean API format
        from_dt = from_dt.replace(microsecond=0)
        to_dt = to_dt.replace(microsecond=0)
        
        logger.info(
            f"Starting CURB trip import - "
            f"Window: {from_dt.strftime('%Y-%m-%d %H:%M:%S')} to {to_dt.strftime('%Y-%m-%d %H:%M:%S')}, "
            f"accounts: {account_ids or 'all'}"
        )
        
        service = CurbService(db)
        
        result = service.import_trips_from_accounts(
            account_ids=account_ids,
            from_datetime=from_dt,
            to_datetime=to_dt,
        )
        
        logger.info(
            f"CURB import completed: {result['trips_imported']} new trips, "
            f"{result['trips_updated']} updated from {len(result['accounts_processed'])} account(s)"
        )
        
        # Chain: Post imported trips to ledger if any were imported
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
            "window": {
                "from": from_dt.strftime('%Y-%m-%d %H:%M:%S'),
                "to": to_dt.strftime('%Y-%m-%d %H:%M:%S')
            }
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