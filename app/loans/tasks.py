### app/loans/tasks.py

"""
Celery Task Definitions for the Driver Loans Module.

This file ensures that tasks defined in the loans module are discoverable by
the Celery worker. The main task handles the automated weekly posting of due
loan installments to the Centralized Ledger.
"""
from celery import shared_task
from app.core.db import SessionLocal
from app.loans.services import LoanService
from app.utils.logger import get_logger

logger = get_logger(__name__)


@shared_task(name="loans.post_due_installments")
def post_due_loan_installments_task():
    """
    Celery task to find all due loan installments and post them as obligations
    to the Centralized Ledger.

    This task is scheduled to run weekly on Sunday morning, alongside other
    financial tasks, before the DTR generation.
    """
    logger.info("Executing Celery task: post_due_loan_installments_task")
    db = SessionLocal()
    try:
        service = LoanService(db)
        result = service.post_due_installments_to_ledger()
        return result
    except Exception as e:
        logger.error(
            f"Celery task post_due_loan_installments_task failed: {e}", exc_info=True
        )
        # The service handles its own rollback, so we just re-raise
        raise
    finally:
        db.close()