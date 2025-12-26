# app/ezpass/tasks.py

from celery import shared_task

from app.core.db import SessionLocal
from app.ezpass.services import EZPassService
from app.utils.logger import get_logger

logger = get_logger(__name__)


@shared_task(name="ezpass.associate_and_post_transactions")
def associate_and_post_transactions_task():
    """
    Celery task to associate IMPORTED transactions and immediately post to ledger.
    
    This task runs on a schedule to process all IMPORTED transactions.
    Replaces the old two-task workflow (associate → post).
    
    Schedule: Every 3 hours
    """
    db = SessionLocal()
    try:
        ezpass_service = EZPassService(db)
        result = ezpass_service.associate_and_post_transactions()
        
        logger.info(
            "EZPass associate and post task completed",
            processed=result["processed"],
            posted=result["posted"],
            failed=result["failed"]
        )
        
        return result
    except Exception as e:
        logger.error(f"Error in associate_and_post_transactions_task: {e}", exc_info=True)
        raise
    finally:
        db.close()