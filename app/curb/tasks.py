# app/curb/tasks.py - ADD this enhanced task

from datetime import datetime, timedelta, timezone
from typing import List, Optional
import redis

from app.worker.app import app
from app.core.db import SessionLocal
from app.curb.services import CurbService
from app.utils.logger import get_logger
from app.core.config import settings

logger = get_logger(__name__)

# Initialize Redis client for distributed locking
redis_client = redis.Redis(
    host=settings.redis_host,
    port=settings.redis_port,
    db=0,
    decode_responses=True
)


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


@app.task(
    name="curb.import_past_trips_task",
    bind=True,
    max_retries=3,
    soft_time_limit=86400,  # 24 hours soft limit
    time_limit=90000,       # 25 hours hard limit
    acks_late=True,         # Acknowledge after completion, not before
    reject_on_worker_lost=True  # Requeue if worker crashes
)
def import_past_trips_task(
    self,
    account_ids: Optional[List[int]] = None,
    start_date_str: Optional[str] = None,  # ISO format string 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'
    end_date_str: Optional[str] = None,  # ISO format string 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'
):
    """
    Import CURB trips from specified accounts and datetime range
    
    This is a long-running task that processes historical data from Nov 1, 2025 (or custom start date).
    Designed to run safely as a background Celery task with proper timeouts
    and connection management.
    
    Args:
        account_ids: Optional list of account IDs to import from
        start_date_str: Optional start date in ISO format (e.g., '2025-11-01' or '2025-11-01 00:00:00')
                       If not provided, defaults to November 1, 2025
        end_date_str: Optional end date in ISO format (e.g., '2025-12-31' or '2025-12-31 23:59:59')
                     If not provided, defaults to current time minus 5 minutes
    """
    # Distributed lock to prevent multiple workers from running the same task
    lock_key = f"curb:import_past_trips:lock:{start_date_str or 'default'}:{end_date_str or 'default'}"
    lock_timeout = 90000  # 25 hours in seconds (longer than task time limit)
    
    # Try to acquire lock
    acquired = redis_client.set(lock_key, "locked", nx=True, ex=lock_timeout)
    if not acquired:
        logger.warning(
            f"Another instance of import_past_trips_task is already running for "
            f"date range {start_date_str or 'default'} to {end_date_str or 'default'}. Skipping."
        )
        return {
            "status": "skipped",
            "reason": "Another instance is already running for this date range"
        }
    
    logger.info(f"Acquired lock: {lock_key}")
    
    db = SessionLocal()
    
    try:
        # Loop through all days starting from November 1, 2025
        # and process 1.5-hour intervals for each day
        import pytz
        import time
        ny_tz = pytz.timezone('America/New_York')
        
        # Parse start date if provided, otherwise default to November 1, 2025
        if start_date_str:
            try:
                # Try parsing with time first
                start_date = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
                # Ensure timezone is set to NY
                if start_date.tzinfo is None:
                    start_date = ny_tz.localize(start_date)
                else:
                    start_date = start_date.astimezone(ny_tz)
            except ValueError:
                # If that fails, try parsing just the date
                try:
                    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                    start_date = ny_tz.localize(start_date.replace(hour=0, minute=0, second=0))
                except ValueError:
                    logger.error(f"Invalid start_date format: {start_date_str}. Using default.")
                    start_date = datetime(2025, 11, 1, 0, 0, 0, tzinfo=ny_tz)
        else:
            # Default: November 1, 2025
            start_date = datetime(2025, 11, 1, 0, 0, 0, tzinfo=ny_tz)
        
        # Parse end date if provided, otherwise use current time minus buffer
        if end_date_str:
            try:
                # Try parsing with time first
                end_date = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
                # Ensure timezone is set to NY
                if end_date.tzinfo is None:
                    end_date = ny_tz.localize(end_date)
                else:
                    end_date = end_date.astimezone(ny_tz)
            except ValueError:
                # If that fails, try parsing just the date
                try:
                    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
                    end_date = ny_tz.localize(end_date.replace(hour=23, minute=59, second=59))
                except ValueError:
                    logger.error(f"Invalid end_date format: {end_date_str}. Using default.")
                    end_date = datetime.now(ny_tz) - timedelta(minutes=5)
        else:
            # Use current time minus a buffer to avoid incomplete intervals
            # This ensures we only process fully completed time windows
            end_date = datetime.now(ny_tz) - timedelta(minutes=5)
        
        logger.info(
            f"Starting CURB historical trip import - "
            f"Period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}, "
            f"Intervals: 1.5 hours (90 minutes), "
            f"accounts: {account_ids or 'all'}"
        )
        
        total_trips_imported = 0
        total_trips_updated = 0
        processed_intervals = 0
        failed_intervals = []  # Track failed intervals for reporting
        
        # Loop through each day
        current_date = start_date
        while current_date.date() <= end_date.date():
            # Define 1.5-hour intervals for the day (16 intervals total)
            # 00:00-01:29:59, 01:30-02:59:59, 03:00-04:29:59, etc.
            intervals = []
            for hour in range(0, 24):
                for minute_offset in [0, 30]:
                    if hour == 23 and minute_offset == 30:
                        # Last interval: 22:30-23:59:59
                        continue
                    intervals.append(minute_offset)
            
            # Generate all 16 intervals for the day
            time_slots = []
            for i in range(16):
                hour = (i * 90) // 60  # Calculate hour from 90-minute blocks
                minute = (i * 90) % 60  # Calculate minute
                
                from_time = current_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
                to_time = from_time + timedelta(minutes=90) - timedelta(seconds=1)
                
                # Don't process future intervals
                if from_time > end_date:
                    break
                
                # Adjust to_time if it exceeds our safe end date
                # This prevents querying for trips that haven't occurred yet
                if to_time > end_date:
                    to_time = end_date
                    
                time_slots.append((from_time, to_time))
            
            # Process each interval for this day
            for from_dt, to_dt in time_slots:
                # Retry mechanism: attempt up to 3 times per interval
                max_retries = 3
                retry_count = 0
                interval_success = False
                
                while retry_count < max_retries and not interval_success:
                    try:
                        attempt_msg = f" (attempt {retry_count + 1}/{max_retries})" if retry_count > 0 else ""
                        logger.info(
                            f"Processing interval{attempt_msg}: {from_dt.strftime('%Y-%m-%d %H:%M:%S')} to "
                            f"{to_dt.strftime('%Y-%m-%d %H:%M:%S')}"
                        )
                        
                        # Refresh database session to avoid stale connections
                        service = CurbService(db)
                        
                        # Keep Celery broker connection alive for long-running tasks
                        self.update_state(
                            state='PROGRESS',
                            meta={
                                'current_interval': f"{from_dt.strftime('%Y-%m-%d %H:%M')}",
                                'processed': processed_intervals,
                                'imported': total_trips_imported,
                                'updated': total_trips_updated
                            }
                        )
                        
                        result = service.import_trips_from_accounts(
                            account_ids=account_ids,
                            from_datetime=from_dt,
                            to_datetime=to_dt,
                        )
                        
                        # Handle different response formats from the service
                        if result.get('status') == 'no_accounts':
                            logger.warning(
                                f"No active CURB accounts found for interval "
                                f"{from_dt.strftime('%Y-%m-%d %H:%M:%S')} to {to_dt.strftime('%Y-%m-%d %H:%M:%S')}"
                            )
                            # Mark as success but skip counting
                            interval_success = True
                            continue
                        
                        # Normal processing
                        total_trips_imported += result.get('trips_imported', 0)
                        total_trips_updated += result.get('trips_updated', 0)
                        processed_intervals += 1
                        interval_success = True
                        
                        logger.info(
                            f"Interval completed: {result.get('trips_imported', 0)} new trips, "
                            f"{result.get('trips_updated', 0)} updated from {len(result.get('accounts_processed', []))} account(s)"
                        )
                        
                        # Chain: Post imported trips to ledger if any were imported
                        if result.get('trips_imported', 0) > 0:
                            logger.info("Triggering ledger posting for newly imported trips")
                            post_trips_to_ledger_task.apply_async(
                                kwargs={
                                    'start_date': from_dt.isoformat(),
                                    'end_date': to_dt.isoformat()
                                }
                            )
                        
                        # Small delay between intervals to avoid API rate limiting
                        time.sleep(1)
                    
                    except Exception as interval_error:
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = 30 * retry_count  # Exponential backoff: 30s, 60s, 90s
                            logger.warning(
                                f"Failed to process interval {from_dt.strftime('%Y-%m-%d %H:%M:%S')} to "
                                f"{to_dt.strftime('%Y-%m-%d %H:%M:%S')} (attempt {retry_count}/{max_retries}): {interval_error}. "
                                f"Retrying in {wait_time} seconds...",
                                exc_info=True
                            )
                            time.sleep(wait_time)
                        else:
                            logger.error(
                                f"Failed to process interval {from_dt.strftime('%Y-%m-%d %H:%M:%S')} to "
                                f"{to_dt.strftime('%Y-%m-%d %H:%M:%S')} after {max_retries} attempts: {interval_error}. "
                                f"Skipping to next interval.",
                                exc_info=True
                            )
                            # Track failed interval for final report
                            failed_intervals.append({
                                'from': from_dt.strftime('%Y-%m-%d %H:%M:%S'),
                                'to': to_dt.strftime('%Y-%m-%d %H:%M:%S'),
                                'error': str(interval_error)
                            })
            
            # Move to next day
            current_date += timedelta(days=1)
            
            # Commit DB changes periodically (after each day) to avoid long transactions
            try:
                db.commit()
            except Exception as commit_error:
                logger.warning(f"Failed to commit after day {current_date.date()}: {commit_error}")
                db.rollback()
        
        logger.info(
            f"CURB historical import completed: {total_trips_imported} total new trips, "
            f"{total_trips_updated} total updated from {processed_intervals} intervals. "
            f"Failed intervals: {len(failed_intervals)}"
        )
        
        # RETRY PHASE: Loop until all failed intervals are resolved or max cycles reached
        retry_cycle = 0
        max_retry_cycles = 5  # Prevent infinite loops
        total_retried_intervals = 0
        intervals_to_retry = failed_intervals.copy()
        
        while intervals_to_retry and retry_cycle < max_retry_cycles:
            retry_cycle += 1
            logger.info(
                f"Starting retry cycle {retry_cycle}/{max_retry_cycles} "
                f"for {len(intervals_to_retry)} failed intervals..."
            )
            
            current_cycle_failed = []
            current_cycle_recovered = 0
            
            for failed_interval in intervals_to_retry:
                # Parse the stored datetime strings back to datetime objects
                from_dt = datetime.strptime(failed_interval['from'], '%Y-%m-%d %H:%M:%S')
                from_dt = ny_tz.localize(from_dt)
                to_dt = datetime.strptime(failed_interval['to'], '%Y-%m-%d %H:%M:%S')
                to_dt = ny_tz.localize(to_dt)
                
                # Retry this interval (up to 3 attempts per cycle)
                max_retries = 3
                retry_count = 0
                retry_success = False
                
                while retry_count < max_retries and not retry_success:
                    try:
                        attempt_msg = f" (cycle {retry_cycle}, attempt {retry_count + 1}/{max_retries})"
                        logger.info(
                            f"Retrying failed interval{attempt_msg}: {from_dt.strftime('%Y-%m-%d %H:%M:%S')} to "
                            f"{to_dt.strftime('%Y-%m-%d %H:%M:%S')}"
                        )
                        
                        service = CurbService(db)
                        
                        self.update_state(
                            state='PROGRESS',
                            meta={
                                'phase': f'retry_cycle_{retry_cycle}',
                                'current_interval': f"{from_dt.strftime('%Y-%m-%d %H:%M')}",
                                'processed': processed_intervals,
                                'imported': total_trips_imported,
                                'updated': total_trips_updated,
                                'retry_cycle': retry_cycle,
                                'intervals_to_retry': len(intervals_to_retry),
                                'recovered_this_cycle': current_cycle_recovered
                            }
                        )
                        
                        result = service.import_trips_from_accounts(
                            account_ids=account_ids,
                            from_datetime=from_dt,
                            to_datetime=to_dt,
                        )
                        
                        if result.get('status') == 'no_accounts':
                            logger.warning(f"No accounts for retry interval, marking as success")
                            retry_success = True
                            continue
                        
                        # Success! Add to counters
                        total_trips_imported += result.get('trips_imported', 0)
                        total_trips_updated += result.get('trips_updated', 0)
                        processed_intervals += 1
                        retry_success = True
                        current_cycle_recovered += 1
                        total_retried_intervals += 1
                        
                        logger.info(
                            f"Retry successful: {result.get('trips_imported', 0)} new trips, "
                            f"{result.get('trips_updated', 0)} updated"
                        )
                        
                        if result.get('trips_imported', 0) > 0:
                            post_trips_to_ledger_task.apply_async(
                                kwargs={
                                    'start_date': from_dt.isoformat(),
                                    'end_date': to_dt.isoformat()
                                }
                            )
                        
                        time.sleep(1)
                    
                    except Exception as retry_error:
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = 60 * retry_count  # Longer backoff: 60s, 120s, 180s
                            logger.warning(
                                f"Retry failed for interval (attempt {retry_count}/{max_retries}): {retry_error}. "
                                f"Waiting {wait_time} seconds...",
                                exc_info=True
                            )
                            time.sleep(wait_time)
                        else:
                            logger.error(
                                f"Retry failed after {max_retries} attempts in cycle {retry_cycle}: {retry_error}",
                                exc_info=True
                            )
                            # Still failed, add to list for next cycle
                            current_cycle_failed.append({
                                'from': from_dt.strftime('%Y-%m-%d %H:%M:%S'),
                                'to': to_dt.strftime('%Y-%m-%d %H:%M:%S'),
                                'error': str(retry_error),
                                'cycle': retry_cycle
                            })
            
            logger.info(
                f"Retry cycle {retry_cycle} completed: {current_cycle_recovered} intervals recovered, "
                f"{len(current_cycle_failed)} still failed"
            )
            
            # Update intervals_to_retry with failures from this cycle
            intervals_to_retry = current_cycle_failed
            
            # Commit after each cycle
            try:
                db.commit()
            except Exception as commit_error:
                logger.warning(f"Failed to commit after retry cycle {retry_cycle}: {commit_error}")
                db.rollback()
        
        # Final status
        if intervals_to_retry:
            logger.warning(
                f"Task completed with {len(intervals_to_retry)} intervals still failed after "
                f"{retry_cycle} retry cycles. These require manual intervention."
            )
        else:
            logger.info(
                f"All intervals successfully processed! Total retry cycles: {retry_cycle}, "
                f"Total intervals recovered through retries: {total_retried_intervals}"
            )
        
        
        return {
            "status": "success" if not intervals_to_retry else "partial_success",
            "trips_imported": total_trips_imported,
            "trips_updated": total_trips_updated,
            "intervals_processed": processed_intervals,
            "initial_failed_intervals": len(failed_intervals),
            "retry_cycles_completed": retry_cycle,
            "intervals_recovered_through_retries": total_retried_intervals,
            "still_failed_intervals": intervals_to_retry,
            "period": {
                "from": start_date.strftime('%Y-%m-%d'),
                "to": end_date.strftime('%Y-%m-%d')
            }
        }
        
    except Exception as e:
        db.rollback()
        logger.error(f"CURB import task failed catastrophically: {e}", exc_info=True)
        
        # Release the lock on error
        try:
            redis_client.delete(lock_key)
            logger.info(f"Released lock after error: {lock_key}")
        except Exception as lock_error:
            logger.error(f"Failed to release lock {lock_key}: {lock_error}")
        
        # DO NOT retry the entire task - interval retries already handled failures
        # Retrying here would cause the ENTIRE loop to restart and duplicate data
        return {
            "status": "failed", 
            "error": str(e),
            "trips_imported": total_trips_imported,
            "trips_updated": total_trips_updated,
            "intervals_processed": processed_intervals,
            "failed_intervals": failed_intervals
        }
        
    finally:
        db.close()
        # Release the lock when task completes successfully
        try:
            redis_client.delete(lock_key)
            logger.info(f"Released lock after completion: {lock_key}")
        except Exception as lock_error:
            logger.error(f"Failed to release lock {lock_key}: {lock_error}")


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