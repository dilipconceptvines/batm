### app/repairs/tasks.py

"""
Celery Task Definitions for the Vehicle Repairs Module.

This file ensures that tasks defined in other parts of the repairs module are
discoverable by the Celery worker. The main task handles the automated weekly
posting of due repair installments to the Centralized Ledger.
"""
from celery import shared_task
from app.core.db import SessionLocal
from app.repairs.services import RepairService
from app.utils.logger import get_logger

logger = get_logger(__name__)

@shared_task(name="repairs.post_due_installments")
def post_due_repair_installments_task():
    """
    Celery task to find all due repair installments and post them as obligations
    to the Centralized Ledger.

    This task is scheduled to run weekly on Sunday morning before DTR generation.
    """
    logger.info("Executing Celery task: post_due_repair_installments_task")
    db = SessionLocal()
    try:
        service = RepairService(db)
        result = service.post_due_installments_to_ledger(post_all_due=True)
        return result
    except Exception as e:
        logger.error(
            f"Celery task post_due_repair_installments_task failed: {e}", exc_info=True
        )
        # The service handles its own rollback, so we just re-raise
        raise
    finally:
        db.close()