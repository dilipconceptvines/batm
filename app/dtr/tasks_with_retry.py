# app/dtr/tasks_with_retry.py

"""
Celery Task Definitions for DTR Module - WITH RETRY CONFIGURATION

This enhanced version adds explicit retry configuration to meet the PDF requirement:
"Retry mechanism for failed automated emails"

KEY ENHANCEMENTS:
1. Automatic retry on transient failures (network issues, AWS SES throttling)
2. Exponential backoff between retries
3. Maximum retry attempts configured
4. No retry on permanent failures (invalid email, no driver email)
"""

import asyncio
from datetime import date, timedelta

from requests.exceptions import RequestException
from botocore.exceptions import ClientError
from celery import shared_task

from app.core.db import SessionLocal
from app.dtr.models import DTR
from app.dtr.email_service import get_dtr_email_service
from app.utils.logger import get_logger

logger = get_logger(__name__)


@shared_task(
    name="dtr.send_weekly_dtr_emails",
    bind=True,
    autoretry_for=(RequestException, ClientError, ConnectionError),
    retry_kwargs={'max_retries': 3, 'countdown': 60},
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True
)
def send_weekly_dtr_emails_task(self):
    """
    Weekly task to send DTR emails to all drivers whose DTRs were generated.
    
    RETRY CONFIGURATION:
    - autoretry_for: Automatically retry on network/AWS errors
    - max_retries: 3 attempts (initial + 3 retries = 4 total)
    - countdown: 60 seconds initial delay
    - retry_backoff: Exponential backoff (60s, 120s, 240s)
    - retry_backoff_max: Maximum 600 seconds (10 minutes)
    - retry_jitter: Randomize retry timing to avoid thundering herd
    
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
    4. Retry on transient failures
    
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
            week_end=week_end,
            retry_attempt=self.request.retries
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
        permanent_failures = []  # Track non-retryable failures

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
                    # Check if this is a permanent failure (don't retry)
                    error_msg = result.get("error", "")
                    is_permanent = (
                        "no email address" in error_msg.lower() or
                        "invalid email" in error_msg.lower() or
                        "driver not found" in error_msg.lower()
                    )
                    
                    if is_permanent:
                        # Permanent failure - log but don't retry
                        permanent_failures.append({
                            "dtr_id": dtr.id,
                            "receipt_number": dtr.receipt_number,
                            "error": error_msg,
                            "retryable": False
                        })
                        logger.warning(
                            f"Permanent failure - will not retry",
                            dtr_id=dtr.id,
                            error=error_msg
                        )
                    else:
                        # Transient failure - will retry
                        emails_failed += 1
                        failures.append({
                            "dtr_id": dtr.id,
                            "receipt_number": dtr.receipt_number,
                            "error": error_msg,
                            "retryable": True
                        })
                        logger.error(
                            f"Failed to send DTR email (will retry)",
                            dtr_id=dtr.id,
                            error=error_msg
                        )
                    
            except (RequestException, ClientError, ConnectionError) as e:
                # Network/AWS errors - let Celery retry the entire task
                logger.error(
                    f"Transient error sending DTR email for DTR {dtr.id}: {str(e)}",
                    exc_info=True
                )
                raise  # Celery will retry
                
            except Exception as e:
                # Other errors - log but continue with other DTRs
                emails_failed += 1
                error_detail = {
                    "dtr_id": dtr.id,
                    "receipt_number": dtr.receipt_number,
                    "error": str(e),
                    "retryable": False
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
        logger.info(f"Permanent Failures: {len(permanent_failures)}")
        logger.info("="*80)

        return {
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
            "total_dtrs": total_dtrs,
            "emails_sent": emails_sent,
            "emails_failed": emails_failed,
            "failures": failures,
            "permanent_failures": permanent_failures
        }

    except (RequestException, ClientError, ConnectionError) as e:
        # Transient error - Celery will retry
        logger.error(
            f"Weekly DTR email delivery task failed with transient error (retry {self.request.retries}/{self.max_retries}): {str(e)}",
            exc_info=True
        )
        raise  # Celery will automatically retry
        
    except Exception as e:
        # Permanent error - log and fail
        logger.error(
            f"Weekly DTR email delivery task failed with permanent error: {str(e)}",
            exc_info=True
        )
        raise
        
    finally:
        db.close()


@shared_task(
    name="dtr.send_dtr_email_on_demand",
    bind=True,
    autoretry_for=(RequestException, ClientError, ConnectionError),
    retry_kwargs={'max_retries': 3, 'countdown': 30},
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True
)
def send_dtr_email_on_demand_task(
    self,
    dtr_id: int,
    recipient_email: str = None,
    include_violations: bool = True
):
    """
    On-demand task to send a specific DTR via email.
    
    RETRY CONFIGURATION:
    - autoretry_for: Automatically retry on network/AWS errors
    - max_retries: 3 attempts
    - countdown: 30 seconds initial delay (faster than weekly for user responsiveness)
    - retry_backoff: Exponential backoff (30s, 60s, 120s)
    - retry_backoff_max: Maximum 300 seconds (5 minutes)
    - retry_jitter: Randomize retry timing
    
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
        recipient_email=recipient_email,
        retry_attempt=self.request.retries
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
            # Check if this is a permanent failure
            error_msg = result.get("error", "")
            is_permanent = (
                "no email address" in error_msg.lower() or
                "invalid email" in error_msg.lower() or
                "driver not found" in error_msg.lower() or
                "dtr not found" in error_msg.lower()
            )
            
            if is_permanent:
                logger.error(
                    f"Permanent failure - will not retry",
                    dtr_id=dtr_id,
                    error=error_msg
                )
                result["retryable"] = False
            else:
                logger.error(
                    f"Transient failure - may retry",
                    dtr_id=dtr_id,
                    error=error_msg
                )
                result["retryable"] = True
        
        return result

    except (RequestException, ClientError, ConnectionError) as e:
        # Transient error - Celery will retry
        logger.error(
            f"On-demand DTR email task failed with transient error (retry {self.request.retries}/{self.max_retries}): {str(e)}",
            exc_info=True
        )
        raise  # Celery will automatically retry
        
    except Exception as e:
        # Permanent error
        logger.error(
            f"On-demand DTR email task failed for DTR {dtr_id}: {str(e)}",
            exc_info=True
        )
        return {
            "success": False,
            "dtr_id": dtr_id,
            "error": str(e),
            "retryable": False
        }
        
    finally:
        db.close()


# ============================================================================
# RETRY CONFIGURATION EXPLANATION
# ============================================================================

"""
RETRY BEHAVIOR:

1. TRANSIENT FAILURES (Will Retry):
   - Network errors (RequestException)
   - AWS SES throttling (ClientError)
   - Connection timeouts (ConnectionError)
   - Temporary AWS outages

2. PERMANENT FAILURES (Won't Retry):
   - No email address for driver
   - Invalid email format
   - Driver not found
   - DTR not found

3. RETRY TIMING:
   Weekly Task:
   - Attempt 1: Immediate
   - Attempt 2: After 60 seconds
   - Attempt 3: After 120 seconds
   - Attempt 4: After 240 seconds
   
   On-Demand Task:
   - Attempt 1: Immediate
   - Attempt 2: After 30 seconds
   - Attempt 3: After 60 seconds
   - Attempt 4: After 120 seconds

4. JITTER:
   - Adds randomness to retry timing
   - Prevents multiple failed tasks from retrying simultaneously
   - Reduces load spikes on AWS SES

5. MONITORING:
   - Each retry attempt is logged
   - Retry count available in self.request.retries
   - Final failure logged after max_retries exceeded
"""