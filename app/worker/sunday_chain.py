# app/worker/sunday_chain.py

"""
Sunday Morning Financial Processing Chain (UPDATED)

This module orchestrates all Sunday morning financial tasks to run sequentially.
The chain ensures each task completes before the next one starts, maintaining
data integrity and proper execution order.

NEW: Added Step 8 - Send DTR emails to drivers with violation reports
"""

from datetime import datetime

from celery import chain
from celery.result import AsyncResult

from app.driver_payments.tasks import generate_weekly_dtrs_task
from app.dtr.tasks import send_weekly_dtr_emails_task  # NEW IMPORT

# Import all the individual task functions
from app.leases.tasks import post_weekly_lease_fees_task
from app.loans.tasks import post_due_loan_installments_task
from app.repairs.tasks import post_due_repair_installments_task
from app.driver_payments.tasks import generate_weekly_dtrs_task
from app.pvb.tasks import post_pvb_violations_to_ledger_task
from app.repairs.tasks import post_due_repair_installments_task
from app.utils.logger import get_logger
from app.worker.app import app

logger = get_logger(__name__)


@app.task(name="worker.sunday_financial_chain")
def sunday_financial_processing_chain():
    """
    Master orchestrator for Sunday morning financial processing.

    Executes tasks in the following order:
    1. Post CURB earnings to ledger (CREDIT postings)
    2. Post EZPass tolls to ledger (DEBIT postings)
    3. Post PVB violations to ledger (DEBIT postings)
    4. Post lease fees to ledger (DEBIT postings)
    5. Post loan installments to ledger (DEBIT postings)
    6. Post repair installments to ledger (DEBIT postings)
    7. Generate DTRs for all active leases (reads finalized ledger state)
    8. Send DTR emails to drivers with violation reports (NEW)

    Each task waits for the previous task to complete before starting.

    Returns:
        Chain result ID for monitoring
    """
    logger.info("=" * 80)
    logger.info("SUNDAY FINANCIAL PROCESSING CHAIN STARTED")
    logger.info("Triggered on", datetime=datetime.now().isoformat())
    logger.info("=" * 80)

    # Create the sequential chain
    # .s() creates a signature (immutable) - each task runs independently
    # .si() creates signature immutable - ignores previous task result
    workflow = chain(
        post_pvb_violations_to_ledger_task.si(),    # Step 3: PVB violations
        post_weekly_lease_fees_task.si(),           # Step 4: Lease fees
        post_due_loan_installments_task.si(),       # Step 5: Loan installments
        post_due_repair_installments_task.si(),     # Step 6: Repair installments
        generate_weekly_dtrs_task.si(),             # Step 7: DTR Generation
        send_weekly_dtr_emails_task.si(),           # Step 8: Send DTR Emails (NEW)
        log_chain_completion.si(),                  # Step 9: Log completion 
    )

    # Execute the chain
    result = workflow.apply_async()

    logger.info("Chain dispatched with root task ID", result_id=result.id)
    logger.info("Monitor chain progress using this ID")
    logger.info("=" * 80)

    return {
        "chain_id": result.id,
        "start_time": datetime.now().isoformat(),
        "status": "dispatched",
        "tasks": [
            "post_pvb_violations_to_ledger_task",
            "post_weekly_lease_fees_task",
            "post_due_loan_installments_task",
            "post_due_repair_installments_task",
            "generate_weekly_dtrs_task",
            "send_weekly_dtr_emails_task",  # NEW TASK
        ],
    }


@app.task(name="worker.log_chain_completion")
def log_chain_completion():
    """
    Final task in the chain to log successful completion.
    """
    logger.info("=" * 80)
    logger.info("SUNDAY FINANCIAL PROCESSING CHAIN COMPLETED SUCCESSFULLY")
    logger.info(f"Completed on: {datetime.now().isoformat()}")
    logger.info("=" * 80)

    return {"status": "completed", "end_time": datetime.now().isoformat()}


@app.task(name="worker.check_chain_status")
def check_chain_status(chain_id: str):
    """
    Utility task to check the status of the chain.

    Args:
        chain_id: The chain result ID returned by sunday_financial_processing_chain

    Returns:
        Dictionary with chain status information
    """
    result = AsyncResult(chain_id, app=app)

    status_info = {
        "chain_id": chain_id,
        "status": result.status,
        "ready": result.ready(),
        "successful": result.successful() if result.ready() else None,
        "failed": result.failed() if result.ready() else None,
    }

    if result.ready():
        try:
            status_info["result"] = result.result
        except Exception as e:
            status_info["error"] = str(e)

    return status_info
