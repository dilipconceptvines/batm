# app/deposits/tasks.py

"""
Celery Task Definitions for the Security Deposits Module.

This file contains automated tasks for deposit lifecycle management including
payment reminders, hold period processing, refund alerts, and ledger reconciliation.
"""

from datetime import date, datetime, timedelta
from decimal import Decimal

from celery import shared_task
from sqlalchemy.orm import Session

from app.core.db import SessionLocal
from app.deposits.services import DepositService
from app.deposits.models import DepositStatus
from app.deposits.repository import DepositRepository
from app.ledger.services import LedgerService
from app.audit_trail.services import audit_trail_service
from app.audit_trail.schemas import AuditTrailType
from app.utils.logger import get_logger

logger = get_logger(__name__)


@shared_task(name="deposits.send_payment_reminders")
def send_deposit_payment_reminders():
    """
    Daily task to send payment reminders for outstanding deposits.

    Checks for deposits that are:
    - Week 1: lease_start_date = today - 7 days
    - Week 2: lease_start_date = today - 14 days

    Updates reminder_flags to prevent duplicate notifications.
    """
    logger.info("Executing Celery task: send_deposit_payment_reminders")

    db = SessionLocal()
    try:
        service = DepositService(db)
        repo = DepositRepository(db)
        today = date.today()

        # Week 1 reminders: deposits where lease started 7 days ago
        week1_date = today - timedelta(days=7)
        week1_deposits = repo.get_unpaid_deposits_by_start_date(week1_date)

        # Week 2 reminders: deposits where lease started 14 days ago
        week2_date = today - timedelta(days=14)
        week2_deposits = repo.get_unpaid_deposits_by_start_date(week2_date)

        reminders_sent = 0

        # Process Week 1 reminders
        for deposit in week1_deposits:
            if deposit.reminder_flags and deposit.reminder_flags.get('week1', False):
                continue  # Already sent

            # Send reminder (log for now - can integrate email/SMS later)
            logger.info(
                "Sending Week 1 deposit payment reminder",
                deposit_id=deposit.deposit_id,
                lease_id=deposit.lease_id,
                driver_tlc=deposit.driver_tlc_license,
                outstanding_amount=float(deposit.outstanding_amount)
            )

            # Update reminder flags
            flags = deposit.reminder_flags or {}
            flags['week1'] = True
            deposit.reminder_flags = flags
            repo.update(deposit)
            reminders_sent += 1

        # Process Week 2 reminders
        for deposit in week2_deposits:
            if deposit.reminder_flags and deposit.reminder_flags.get('week2', False):
                continue  # Already sent

            # Send reminder (log for now - can integrate email/SMS later)
            logger.info(
                "Sending Week 2 deposit payment reminder",
                deposit_id=deposit.deposit_id,
                lease_id=deposit.lease_id,
                driver_tlc=deposit.driver_tlc_license,
                outstanding_amount=float(deposit.outstanding_amount)
            )

            # Update reminder flags
            flags = deposit.reminder_flags or {}
            flags['week2'] = True
            deposit.reminder_flags = flags
            repo.update(deposit)
            reminders_sent += 1

        logger.info(
            "Deposit payment reminders completed",
            reminders_sent=reminders_sent,
            week1_deposits=len(week1_deposits),
            week2_deposits=len(week2_deposits)
        )

        return {
            "reminders_sent": reminders_sent,
            "week1_deposits": len(week1_deposits),
            "week2_deposits": len(week2_deposits)
        }

    except Exception as e:
        logger.error(
            "Celery task send_deposit_payment_reminders failed",
            error=str(e),
            exc_info=True
        )
        raise
    finally:
        db.close()


@shared_task(name="deposits.process_expired_holds")
def process_expired_deposit_holds():
    """
    Daily task to process deposits where the 30-day hold period has expired.

    Automatically applies held deposits to outstanding obligations and processes
    refunds for remaining balances.
    """
    logger.info("Executing Celery task: process_expired_deposit_holds")

    db = SessionLocal()
    try:
        service = DepositService(db)
        ledger_service = LedgerService(db)
        today = date.today()

        # Find deposits where hold_expiry_date = today
        expired_holds = db.query(service.repo.model).filter(
            service.repo.model.hold_expiry_date == today,
            service.repo.model.deposit_status == DepositStatus.HELD
        ).all()

        processed_count = 0
        applied_count = 0
        refunded_count = 0

        for deposit in expired_holds:
            try:
                logger.info(
                    "Processing expired deposit hold",
                    deposit_id=deposit.deposit_id,
                    lease_id=deposit.lease_id,
                    hold_expiry_date=deposit.hold_expiry_date.isoformat()
                )

                # Auto-apply deposit to obligations
                application_result = service.auto_apply_deposit(
                    db=db,
                    deposit_id=deposit.deposit_id,
                    ledger_service=ledger_service
                )

                processed_count += 1

                # Check if refund was processed
                if application_result.get('remaining_refund', Decimal('0.00')) > 0:
                    refunded_count += 1
                    logger.info(
                        "Deposit refund processed",
                        deposit_id=deposit.deposit_id,
                        refund_amount=float(application_result['remaining_refund'])
                    )
                else:
                    applied_count += 1
                    logger.info(
                        "Deposit fully applied to obligations",
                        deposit_id=deposit.deposit_id,
                        total_applied=float(application_result['total_applied'])
                    )

                # Create audit trail
                audit_trail_service.create_audit_trail(
                    db=db,
                    description=f"Auto-processed expired deposit hold: ${application_result['total_applied']:.2f} applied, ${application_result.get('remaining_refund', 0):.2f} refunded",
                    case=None,  # No BPM case for automated tasks
                    meta_data={
                        "deposit_id": deposit.deposit_id,
                        "lease_id": deposit.lease_id,
                        "total_applied": float(application_result['total_applied']),
                        "remaining_refund": float(application_result.get('remaining_refund', 0)),
                        "processed_by": "automated_task"
                    },
                    audit_type=AuditTrailType.AUTOMATED,
                )

            except Exception as deposit_error:
                logger.error(
                    "Failed to process expired deposit hold",
                    deposit_id=deposit.deposit_id,
                    error=str(deposit_error),
                    exc_info=True
                )
                # Continue processing other deposits

        logger.info(
            "Expired deposit holds processing completed",
            total_expired=len(expired_holds),
            processed=processed_count,
            fully_applied=applied_count,
            refunded=refunded_count
        )

        return {
            "total_expired": len(expired_holds),
            "processed": processed_count,
            "fully_applied": applied_count,
            "refunded": refunded_count
        }

    except Exception as e:
        logger.error(
            "Celery task process_expired_deposit_holds failed",
            error=str(e),
            exc_info=True
        )
        raise
    finally:
        db.close()


@shared_task(name="deposits.send_refund_overdue_alerts")
def send_refund_overdue_alerts():
    """
    Daily task to send alerts for deposits that are overdue for refund processing.

    Identifies deposits where hold_expiry_date < today - 5 days AND status = HELD,
    indicating refunds should have been processed but haven't been.
    """
    logger.info("Executing Celery task: send_refund_overdue_alerts")

    db = SessionLocal()
    try:
        service = DepositService(db)
        today = date.today()
        overdue_threshold = today - timedelta(days=5)

        # Find overdue held deposits
        overdue_deposits = db.query(service.repo.model).filter(
            service.repo.model.hold_expiry_date < overdue_threshold,
            service.repo.model.deposit_status == DepositStatus.HELD
        ).all()

        alerts_sent = 0

        for deposit in overdue_deposits:
            # Check if alert already sent
            if deposit.reminder_flags and deposit.reminder_flags.get('refund_overdue_alert', False):
                continue  # Already alerted

            # Send alert to finance team (log for now - can integrate email/notifications later)
            logger.warning(
                "REFUND OVERDUE ALERT: Deposit hold expired but not processed",
                deposit_id=deposit.deposit_id,
                lease_id=deposit.lease_id,
                driver_tlc=deposit.driver_tlc_license,
                hold_expiry_date=deposit.hold_expiry_date.isoformat(),
                days_overdue=(today - deposit.hold_expiry_date).days,
                collected_amount=float(deposit.collected_amount)
            )

            # Update reminder flags
            flags = deposit.reminder_flags or {}
            flags['refund_overdue_alert'] = True
            deposit.reminder_flags = flags
            service.repo.update(deposit)
            alerts_sent += 1

        logger.info(
            "Refund overdue alerts completed",
            overdue_deposits=len(overdue_deposits),
            alerts_sent=alerts_sent
        )

        return {
            "overdue_deposits": len(overdue_deposits),
            "alerts_sent": alerts_sent
        }

    except Exception as e:
        logger.error(
            "Celery task send_refund_overdue_alerts failed",
            error=str(e),
            exc_info=True
        )
        raise
    finally:
        db.close()


@shared_task(name="deposits.reconcile_ledger_balances")
def reconcile_deposit_ledger_balances():
    """
    Weekly task to reconcile deposit balances with ledger balances.

    Verifies that deposits.collected_amount matches corresponding ledger balances
    and flags any discrepancies for manual review.
    """
    logger.info("Executing Celery task: reconcile_deposit_ledger_balances")

    db = SessionLocal()
    try:
        service = DepositService(db)
        ledger_service = LedgerService(db)

        # Get all deposits that should have ledger balances
        deposits = db.query(service.repo.model).filter(
            service.repo.model.collected_amount > 0
        ).all()

        total_deposits = len(deposits)
        matched_count = 0
        discrepancies = []

        for deposit in deposits:
            try:
                # Get ledger balance for this deposit
                # Note: This assumes deposits are tracked with reference_id pattern "DEP-{deposit_id}-*"
                ledger_balance = ledger_service.get_balance_by_reference_pattern(
                    f"DEP-{deposit.deposit_id}"
                )

                # Compare amounts
                if abs(ledger_balance - deposit.collected_amount) < Decimal('0.01'):  # Allow for rounding
                    matched_count += 1
                else:
                    discrepancy = {
                        "deposit_id": deposit.deposit_id,
                        "lease_id": deposit.lease_id,
                        "expected_amount": float(deposit.collected_amount),
                        "ledger_balance": float(ledger_balance),
                        "difference": float(ledger_balance - deposit.collected_amount),
                        "status": deposit.deposit_status.value
                    }
                    discrepancies.append(discrepancy)

                    logger.warning(
                        "Deposit ledger balance discrepancy detected",
                        **discrepancy
                    )

            except Exception as balance_error:
                logger.error(
                    "Failed to get ledger balance for deposit",
                    deposit_id=deposit.deposit_id,
                    error=str(balance_error)
                )
                discrepancies.append({
                    "deposit_id": deposit.deposit_id,
                    "lease_id": deposit.lease_id,
                    "error": str(balance_error)
                })

        # Generate reconciliation report
        report = {
            "total_deposits_checked": total_deposits,
            "matched_count": matched_count,
            "discrepancy_count": len(discrepancies),
            "discrepancies": discrepancies,
            "reconciliation_date": datetime.now().isoformat()
        }

        # Log summary
        if discrepancies:
            logger.warning(
                "Deposit ledger reconciliation found discrepancies",
                total_deposits=total_deposits,
                matched=matched_count,
                discrepancies=len(discrepancies)
            )
        else:
            logger.info(
                "Deposit ledger reconciliation completed successfully",
                total_deposits=total_deposits,
                all_matched=True
            )

        # TODO: Could save this report to database or send notification

        return report

    except Exception as e:
        logger.error(
            "Celery task reconcile_deposit_ledger_balances failed",
            error=str(e),
            exc_info=True
        )
        raise
    finally:
        db.close()