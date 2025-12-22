from sqlalchemy.orm import Session

from app.audit_trail.schemas import AuditTrailType
from app.audit_trail.services import audit_trail_service
from app.bpm.models import Case, CaseEntity
from app.esign import utils as esign_utils
from app.leases.services import lease_service
from app.utils.logger import get_logger

logger = get_logger(__name__)


async def additionaldriver_recipient_completed(db: Session, ctx: dict, payload: dict):
    """
    Handle recipient-completed event for additional driver signatures.
    Updates LeaseDriverDocument signoff status when driver completes signature.

    For additional driver agreements with two signers:
    - recipient_id '1' = Additional Driver → has_driver_signed
    - recipient_id '2' = Primary Driver (Lessee) → has_frontend_signed
    """
    logger.info("[additionaldriver] recipient-completed: %s", ctx)
    try:
        # Reuse the same function as driverlease since we use the same LeaseDriverDocument model
        summary = lease_service.update_lease_driver_document_signoff_latest(db, ctx=ctx)
        if not summary.get("ok"):
            logger.warning(
                "[additionaldriver] recipient-completed: no update performed "
                "(envelope_id=%s, recipient_id=%s, reason=%s)",
                ctx.get("envelope_id"),
                ctx.get("recipient_id"),
                summary.get("reason"),
            )
        else:
            logger.info(
                "[additionaldriver] recipient-completed: updated LDD id=%s for envelope %s (recipient_id=%s)",
                summary.get("lease_driver_document_id"),
                summary.get("envelope_id"),
                summary.get("recipient_id"),
            )

        # Create audit trail entry if case_no is available
        case_no = ctx.get("case_no")
        if case_no:
            try:
                case = db.query(Case).filter(Case.case_no == case_no).first()
                if case:
                    # Get lease_drivers_id from case_entities table
                    lease_drivers_entity = (
                        db.query(CaseEntity)
                        .filter(
                            CaseEntity.case_no == case_no,
                            CaseEntity.entity_name == "lease_drivers",
                        )
                        .first()
                    )

                    lease_drivers_id = (
                        lease_drivers_entity.identifier_value
                        if lease_drivers_entity
                        else None
                    )

                    # Get lease_id from lease_drivers
                    lease_id = None
                    if lease_drivers_id:
                        lease_driver = lease_service.get_lease_drivers(
                            db=db, driver_id=lease_drivers_id
                        )
                        if lease_driver and lease_driver.lease:
                            lease_id = lease_driver.lease.id

                    audit_trail_service.create_audit_trail(
                        db=db,
                        description=f"Additional driver recipient completed signing (envelope_id: {ctx.get('envelope_id')}, recipient_id: {ctx.get('recipient_id')})",
                        case=case,
                        user=None,
                        meta_data={
                            "envelope_id": ctx.get("envelope_id"),
                            "recipient_id": ctx.get("recipient_id"),
                            "event": "recipient-completed",
                            "lease_drivers_id": lease_drivers_id,
                            "lease_id": lease_id,
                        },
                        audit_type=AuditTrailType.AUTOMATED,
                    )
                    db.flush()
                    logger.info(
                        "[additionaldriver] recipient-completed: audit trail created for case %s",
                        case_no,
                    )
                else:
                    logger.warning(
                        "[additionaldriver] recipient-completed: case not found for case_no=%s",
                        case_no,
                    )
            except Exception as audit_error:
                logger.error(
                    "[additionaldriver] recipient-completed: failed to create audit trail: %s",
                    audit_error,
                    exc_info=True,
                )

        # No commit/rollback here; upstream handles transaction boundaries.
        return summary
    except Exception as e:
        logger.exception("[additionaldriver] recipient-completed: error: %s", e)
        return {"ok": False, "error": str(e), "envelope_id": ctx.get("envelope_id")}


async def additionaldriver_recipient_delivered(db: Session, ctx: dict, payload: dict):
    """
    Handle recipient-delivered event for additional driver signatures.
    Logs when signature request is delivered to the driver.
    """
    logger.info("[additionaldriver] recipient-delivered: %s", ctx)


async def additionaldriver_recipient_declined(db: Session, ctx: dict, payload: dict):
    """
    Handle recipient-declined event for additional driver signatures.
    Logs when driver declines to sign the document.
    """
    logger.info("[additionaldriver] recipient-declined: %s", ctx)


async def additionaldriver_envelope_sent(db: Session, ctx: dict, payload: dict):
    """
    Handle envelope-sent event for additional driver signatures.
    Updates envelope status in the database when envelope is sent.
    """
    logger.info("[additionaldriver] envelope-sent: %s", ctx)
    rows = esign_utils.update_envelope_status(db, ctx=ctx, status="envelope-sent")
    # Ensure pending UPDATE is pushed to the DB connection without committing
    db.flush()
    if rows == 0:
        logger.warning(
            "[additionaldriver] envelope-sent: no envelope updated (id=%s, object_id=%s)",
            ctx.get("envelope_id"),
            ctx.get("object_id"),
        )

    # Create audit trail entry if case_no is available
    case_no = ctx.get("case_no")
    if case_no:
        try:
            case = db.query(Case).filter(Case.case_no == case_no).first()
            if case:
                # Get lease_drivers_id from case_entities table
                lease_drivers_entity = (
                    db.query(CaseEntity)
                    .filter(
                        CaseEntity.case_no == case_no,
                        CaseEntity.entity_name == "lease_drivers",
                    )
                    .first()
                )

                lease_drivers_id = (
                    lease_drivers_entity.identifier_value
                    if lease_drivers_entity
                    else None
                )

                # Get lease_id from lease_drivers
                lease_id = None
                if lease_drivers_id:
                    lease_driver = lease_service.get_lease_drivers(
                        db=db, driver_id=lease_drivers_id
                    )
                    if lease_driver and lease_driver.lease:
                        lease_id = lease_driver.lease.id

                audit_trail_service.create_audit_trail(
                    db=db,
                    description=f"Additional driver e-signature envelope sent (envelope_id: {ctx.get('envelope_id')})",
                    case=case,
                    user=None,
                    meta_data={
                        "envelope_id": ctx.get("envelope_id"),
                        "event": "envelope-sent",
                        "lease_drivers_id": lease_drivers_id,
                        "lease_id": lease_id,
                    },
                    audit_type=AuditTrailType.AUTOMATED,
                )
                db.flush()
                logger.info(
                    "[additionaldriver] envelope-sent: audit trail created for case %s",
                    case_no,
                )
            else:
                logger.warning(
                    "[additionaldriver] envelope-sent: case not found for case_no=%s",
                    case_no,
                )
        except Exception as e:
            logger.error(
                "[additionaldriver] envelope-sent: failed to create audit trail: %s",
                e,
                exc_info=True,
            )

    return {
        "ok": rows > 0,
        "updated_rows": rows,
        "envelope_id": ctx.get("envelope_id"),
        "status": "sent",
    }


async def additionaldriver_envelope_completed(db: Session, ctx: dict, payload: dict):
    """
    Handle envelope-completed event for additional driver signatures.
    Updates envelope status when all signatures are completed.
    """
    logger.info("[additionaldriver] envelope-completed: %s", ctx)
    rows = esign_utils.update_envelope_status(db, ctx=ctx, status="envelope-completed")
    db.flush()
    if rows == 0:
        logger.warning(
            "[additionaldriver] envelope-completed: no envelope updated (id=%s, object_id=%s)",
            ctx.get("envelope_id"),
            ctx.get("object_id"),
        )

    # Create audit trail entry if case_no is available
    case_no = ctx.get("case_no")
    if case_no:
        try:
            case = db.query(Case).filter(Case.case_no == case_no).first()
            if case:
                # Get lease_drivers_id from case_entities table
                lease_drivers_entity = (
                    db.query(CaseEntity)
                    .filter(
                        CaseEntity.case_no == case_no,
                        CaseEntity.entity_name == "lease_drivers",
                    )
                    .first()
                )

                lease_drivers_id = (
                    lease_drivers_entity.identifier_value
                    if lease_drivers_entity
                    else None
                )

                # Get lease_id from lease_drivers
                lease_id = None
                if lease_drivers_id:
                    lease_driver = lease_service.get_lease_drivers(
                        db=db, driver_id=lease_drivers_id
                    )
                    if lease_driver and lease_driver.lease:
                        lease_id = lease_driver.lease.id

                audit_trail_service.create_audit_trail(
                    db=db,
                    description=f"Additional driver e-signature envelope completed (envelope_id: {ctx.get('envelope_id')})",
                    case=case,
                    user=None,
                    meta_data={
                        "envelope_id": ctx.get("envelope_id"),
                        "event": "envelope-completed",
                        "lease_drivers_id": lease_drivers_id,
                        "lease_id": lease_id,
                    },
                    audit_type=AuditTrailType.AUTOMATED,
                )
                db.flush()
                logger.info(
                    "[additionaldriver] envelope-completed: audit trail created for case %s",
                    case_no,
                )
            else:
                logger.warning(
                    "[additionaldriver] envelope-completed: case not found for case_no=%s",
                    case_no,
                )
        except Exception as e:
            logger.error(
                "[additionaldriver] envelope-completed: failed to create audit trail: %s",
                e,
                exc_info=True,
            )

    return {
        "ok": rows > 0,
        "updated_rows": rows,
        "envelope_id": ctx.get("envelope_id"),
        "status": "completed",
    }
