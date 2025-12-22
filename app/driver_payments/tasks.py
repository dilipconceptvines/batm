# app/driver_payments/tasks.py

"""
Celery Task Definitions for the Driver Payments Module.

This file contains weekly automation tasks for DTR generation.
"""

from celery import shared_task
from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy.orm import Session

from app.core.db import SessionLocal
from app.dtr.services import DTRService
from app.leases.models import Lease
from app.leases.schemas import LeaseStatus
from app.utils.logger import get_logger

logger = get_logger(__name__)


@shared_task(name="driver_payments.generate_weekly_dtrs")
def generate_weekly_dtrs_task():
    """
    Weekly task to generate DTRs for all active leases.

    Schedule: Sunday 6:00 AM (after all financial postings complete)

    Process:
    1. Calculate previous week's date range (Sunday to Saturday)
    2. Query for all ACTIVE leases
    3. Generate DTR for each lease
    4. Log results and errors

    Returns:
        Dictionary with generation results:
        {
            "week_start": str (ISO format),
            "week_end": str (ISO format),
            "total_leases": int,
            "success_count": int,
            "failed_count": int,
            "errors": List[str],
        }
    """
    logger.info("Staring Weekly DTR generation task")
    db = SessionLocal()

    try:
        # Calculate previous week's date range
        today = date.today()
        week_end = today - timedelta(days=1)
        week_start = week_end - timedelta(days=6)

        logger.info("Generating DTR for week", week_start=week_start, week_end=week_end)

        # Get all active leases
        active_leases = db.query(Lease).filter(
            Lease.lease_status == LeaseStatus.ACTIVE
        ).all()

        total_leases = len(active_leases)
        logger.info("Found active leases", count=total_leases)

        if total_leases == 0:
            logger.warning("No active leases found - no DTRs to generate")
            return {
                "week_start": week_start.isoformat(),
                "week_end": week_end.isoformat(),
                "total_leases": 0,
                "success_count": 0,
                "failed_count": 0,
                "errors": []
            }
        
        service = DTRService(db)
        success_count = 0
        failed_count = 0
        errors = []

        # Generate DTR for each lease
        for lease in active_leases:
            try:
                dtr = service.generate_dtr_for_lease(
                    lease_id=lease.id,
                    week_start=week_start,
                    week_end=week_end,
                    force_final=False
                )

                success_count += 1
                # Get primary driver for logging
                primary_driver = None
                for ld in lease.lease_driver:
                    if not ld.is_additional_driver:
                        primary_driver = ld.driver
                        break
                
                logger.info(
                    "Generated DTR for Lease",
                    dtr_number=dtr.dtr_number, lease=lease.id,
                    driver=f"{primary_driver.full_name if primary_driver else 'N/A'}"
                )

            except ValueError as e:
                # DTR already exists or validation error
                failed_count += 1
                error_msg = f"Lease {lease.id}: {str(e)}"
                errors.append(error_msg)
                logger.warning(error_msg)

            except Exception as e:
                failed_count += 1
                error_msg = f"Lease {lease.id}: Unexpected error - {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg, exc_info=True)

        result = {
            'week_start': week_start.isoformat(),
            'week_end': week_end.isoformat(),
            'total_leases': total_leases,
            'success_count': success_count,
            'failed_count': failed_count,
            'errors': errors
        }

        logger.info(
            "DTR generation completed",
            total_leases=total_leases,
            success_count=success_count, failed_count=failed_count,
            success_rate=f"{(success_count/total_leases*100) if total_leases > 0 else 0:.1f}%"
        )

        if errors:
            logger.warning("Errors encountered", errors=errors)

        return result
    
    except Exception as e:
        logger.error("DTR generation task failed", exc_info=True)
        logger.error("Error", error=e)
        db.rollback()
        raise
    finally:
        db.close()


__all__ = [
    'generate_weekly_dtrs_task'
]