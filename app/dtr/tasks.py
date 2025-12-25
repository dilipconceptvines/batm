# app/dtr/tasks.py

"""
Celery Task Definitions for the DTR Module.

Handles automated weekly DTR email delivery and on-demand sending.
"""

import asyncio
from datetime import date, timedelta

from celery import shared_task

from app.core.db import SessionLocal
from app.worker.app import app as celery_app
from app.dtr.models import DTR
from app.dtr.email_service import get_dtr_email_service
from app.utils.logger import get_logger

logger = get_logger(__name__)


@celery_app.task(name="dtr.send_weekly_dtr_emails")
def send_weekly_dtr_emails_task():
    """
    Weekly task to send DTR emails to all drivers whose DTRs were generated.
    
    This task runs after DTR generation completes in the Sunday financial chain.
    It sends emails with DTR PDFs and violation reports to all primary drivers.
    
    Schedule: Sunday morning, immediately after DTR generation (Step 8 in chain)
    
    Process:
    1. Find all DTRs generated for the previous week
    2. For each DTR, send email to primary driver with:
       - DTR PDF attachment
       - PVB Violations report (if any)
       - TLC Violations report (if any)
    3. Log results and errors
    
    Returns:
        Dictionary with email sending results:
        {
            "week_start": str (ISO format),
            "week_end": str (ISO format),
            "total_dtrs": int,
            "emails_sent": int,
            "emails_failed": int,
            "failures": List[Dict],
        }
    """
    logger.info("Starting Weekly DTR Email Delivery task")
    db = SessionLocal()

    try:
        # Calculate previous week's date range (same logic as DTR generation)
        today = date.today()
        week_end = today - timedelta(days=1)  # Yesterday (Saturday)
        week_start = week_end - timedelta(days=6)  # Previous Sunday

        logger.info(
            "Sending DTR emails for week",
            week_start=week_start,
            week_end=week_end
        )

        # Get all DTRs generated for this week
        dtrs = db.query(DTR).filter(
            DTR.week_start_date == week_start,
            DTR.week_end_date == week_end
        ).all()

        total_dtrs = len(dtrs)
        logger.info("Found DTRs to send", count=total_dtrs)

        if total_dtrs == 0:
            logger.warning("No DTRs found for the week - no emails to send")
            return {
                "week_start": week_start.isoformat(),
                "week_end": week_end.isoformat(),
                "total_dtrs": 0,
                "emails_sent": 0,
                "emails_failed": 0,
                "failures": []
            }

        # Send emails for each DTR
        email_service = get_dtr_email_service(db)
        emails_sent = 0
        emails_failed = 0
        failures = []

        for dtr in dtrs:
            try:
                logger.info(
                    f"Sending DTR email",
                    dtr_id=dtr.id,
                    receipt_number=dtr.receipt_number,
                    driver_id=dtr.primary_driver_id
                )
                
                # Send email asynchronously
                result = asyncio.run(
                    email_service.send_weekly_dtr_email(
                        dtr_id=dtr.id,
                        include_violations=True
                    )
                )
                
                if result.get("success"):
                    emails_sent += 1
                    logger.info(
                        f"DTR email sent successfully",
                        dtr_id=dtr.id,
                        email=result.get("driver_email")
                    )
                else:
                    emails_failed += 1
                    error_detail = {
                        "dtr_id": dtr.id,
                        "receipt_number": dtr.receipt_number,
                        "error": result.get("error", "Unknown error")
                    }
                    failures.append(error_detail)
                    logger.error(
                        f"Failed to send DTR email",
                        dtr_id=dtr.id,
                        error=result.get("error")
                    )
                    
            except Exception as e:
                emails_failed += 1
                error_detail = {
                    "dtr_id": dtr.id,
                    "receipt_number": dtr.receipt_number,
                    "error": str(e)
                }
                failures.append(error_detail)
                logger.error(
                    f"Exception sending DTR email for DTR {dtr.id}: {str(e)}",
                    exc_info=True
                )

        # Summary
        logger.info("="*80)
        logger.info("WEEKLY DTR EMAIL DELIVERY COMPLETED")
        logger.info(f"Week: {week_start} to {week_end}")
        logger.info(f"Total DTRs: {total_dtrs}")
        logger.info(f"Emails Sent: {emails_sent}")
        logger.info(f"Emails Failed: {emails_failed}")
        logger.info("="*80)

        return {
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
            "total_dtrs": total_dtrs,
            "emails_sent": emails_sent,
            "emails_failed": emails_failed,
            "failures": failures
        }

    except Exception as e:
        logger.error(
            f"Weekly DTR email delivery task failed: {str(e)}",
            exc_info=True
        )
        raise
    finally:
        db.close()


@celery_app.task(name="dtr.send_dtr_email_on_demand")
def send_dtr_email_on_demand_task(
    dtr_id: int,
    recipient_email: str = None,
    include_violations: bool = True
):
    """
    On-demand task to send a specific DTR via email.
    
    This task is triggered via API endpoint for manual DTR delivery.
    
    Args:
        dtr_id: The DTR ID to send
        recipient_email: Optional override email (defaults to driver's email)
        include_violations: Whether to include violation reports
    
    Returns:
        Dictionary with send status
    """
    logger.info(
        f"Starting on-demand DTR email delivery",
        dtr_id=dtr_id,
        recipient_email=recipient_email
    )
    db = SessionLocal()

    try:
        email_service = get_dtr_email_service(db)
        
        # Send email asynchronously
        result = asyncio.run(
            email_service.send_on_demand_dtr_email(
                dtr_id=dtr_id,
                recipient_email=recipient_email,
                include_violations=include_violations
            )
        )
        
        if result.get("success"):
            logger.info(
                f"On-demand DTR email sent successfully",
                dtr_id=dtr_id,
                email=result.get("recipient_email")
            )
        else:
            logger.error(
                f"Failed to send on-demand DTR email",
                dtr_id=dtr_id,
                error=result.get("error")
            )
        
        return result

    except Exception as e:
        logger.error(
            f"On-demand DTR email task failed for DTR {dtr_id}: {str(e)}",
            exc_info=True
        )
        return {
            "success": False,
            "dtr_id": dtr_id,
            "error": str(e)
        }
    finally:
        db.close()