### app/ezpass/tasks.py

"""
Celery Task Definitions for the EZPass Module.

This file ensures that tasks defined within the ezpass module are discoverable
by the Celery worker. By importing them here, we provide a single entry point
for Celery's autodiscovery mechanism.
"""

from celery import shared_task
from sqlalchemy.orm import Session

from app.worker.app import app as celery_app
from app.core.db import SessionLocal
from app.utils.logger import get_logger

logger = get_logger(__name__)

@celery_app.task(name="ezpass.associate_and_post_transactions")
def associate_and_post_ezpass_transactions_task():
    """
    Background task to associate EZPass transactions and immediately post to ledger.
    
    This replaces the previous two-step process:
    - OLD: associate_ezpass_transactions_task + post_ezpass_tolls_to_ledger_task
    - NEW: Single combined task that does both atomically
    
    Process:
    1. Find all IMPORTED transactions
    2. Match plate → vehicle → CURB trip → driver/lease
    3. If match found, immediately post to ledger
    4. Update status to POSTED_TO_LEDGER
    
    Returns:
        Dict with processing statistics
    """
    logger.info("Executing Celery task: associate_and_post_ezpass_transactions")
    db: Session = SessionLocal()
    
    try:
        from app.ezpass.services import EZPassService
        
        service = EZPassService(db)
        result = service.associate_and_post_transactions()
        
        logger.info(
            f"EZPass association and posting task completed successfully",
            **result
        )
        
        return result
        
    except Exception as e:
        logger.error(
            f"Celery task associate_and_post_ezpass_transactions failed: {e}",
            exc_info=True
        )
        db.rollback()
        raise
        
    finally:
        db.close()