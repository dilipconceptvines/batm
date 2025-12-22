# app/leases/tasks.py

"""
Celery tasks for lease schedule automation

Scheduled to run every Sunday at 05:00 AM to post weekly lease fees
"""

from datetime import date
from celery import shared_task

from app.core.db import SessionLocal
from app.leases.lease_schedule_service import LeaseScheduleService
from app.utils.logger import get_logger

logger = get_logger(__name__)


@shared_task(name="leases.post_weekly_lease_fees")
def post_weekly_lease_fees_task():
    """
    Post lease fees for upcoming week to ledger
    
    Scheduled to run: Every Sunday at 05:00 AM
    
    Process:
    1. Find all unposted lease schedule entries due this week
    2. For each entry:
        - Validate lease is active
        - Create DEBIT posting in ledger (category: LEASE)
        - Mark schedule entry as posted
    3. Log results and handle errors
    
    Returns:
        Dictionary with posting results:
        {
            'success_count': int,
            'failure_count': int,
            'posted_schedules': List[int],
            'failed_schedules': List[dict],
            'total_amount_posted': float
        }
    """
    logger.info("Starting weekly lease fee posting task")
    
    db = SessionLocal()
    
    try:
        service = LeaseScheduleService(db)
        
        current_date = date.today()
        result = service.post_weekly_lease_fees(current_date)
        
        logger.info(
            f"Weekly lease fee posting completed: "
            f"{result['success_count']} posted, {result['failure_count']} failed, "
            f"Total amount: ${result['total_amount_posted']}"
        )
        
        if result['failure_count'] > 0:
            logger.warning(f"Failed schedules: {result['failed_schedules']}")
        
        return result
        
    except Exception as e:
        db.rollback()
        logger.error(f"Weekly lease fee posting task failed: {str(e)}", exc_info=True)
        raise
        
    finally:
        db.close()


# Configuration for Celery Beat schedule
# This should be added to your celery beat schedule configuration in app/worker/config.py:
"""
from celery.schedules import crontab

beat_schedule = {
    ...
    'post-weekly-lease-fees': {
        'task': 'leases.post_weekly_lease_fees',
        'schedule': crontab(hour=5, minute=0, day_of_week=0),  # Sunday 05:00 AM
        'options': {
            'expires': 3600,  # Task expires after 1 hour if not executed
            'timezone': 'America/New_York'
        }
    },
    ...
}
"""