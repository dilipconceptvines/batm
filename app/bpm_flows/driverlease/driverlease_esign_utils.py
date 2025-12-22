from sqlalchemy.orm import Session

from app.audit_trail.schemas import AuditTrailType
from app.audit_trail.services import audit_trail_service
from app.bpm.models import Case, CaseEntity
from app.bpm.services import bpm_service
from app.esign import utils as esign_utils
from app.esign.models import ESignEnvelope
from app.leases.services import lease_service
from app.utils.logger import get_logger

logger = get_logger(__name__)


async def driverlease_recipient_completed(db: Session, ctx: dict, payload: dict):
    logger.info("[driverlease] recipient-completed: %s", ctx)
    try:
        summary = lease_service.update_lease_driver_document_signoff_latest(db, ctx=ctx)
        if not summary.get("ok"):
            logger.warning(
                "[driverlease] recipient-completed: no update performed "
                "(envelope_id=%s, recipient_id=%s, reason=%s)",
                ctx.get("envelope_id"),
                ctx.get("recipient_id"),
                summary.get("reason"),
            )
        else:
            logger.info(
                "[driverlease] recipient-completed: updated LDD id=%s for envelope %s (recipient_id=%s)",
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
                    # Get lease_id from case_entities table
                    lease_entity = db.query(CaseEntity).filter(
                        CaseEntity.case_no == case_no,
                        CaseEntity.entity_name == "lease"
                    ).first()

                    lease_id = lease_entity.identifier_value if lease_entity else None

                    audit_trail_service.create_audit_trail(
                        db=db,
                        description=f"Recipient completed signing (envelope_id: {ctx.get('envelope_id')}, recipient_id: {ctx.get('recipient_id')})",
                        case=case,
                        user=None,
                        meta_data={
                            "envelope_id": ctx.get("envelope_id"),
                            "recipient_id": ctx.get("recipient_id"),
                            "event": "recipient-completed",
                            "lease_id": lease_id,
                        },
                        audit_type=AuditTrailType.AUTOMATED,
                    )
                    db.flush()
                    logger.info(
                        "[driverlease] recipient-completed: audit trail created for case %s",
                        case_no,
                    )
                else:
                    logger.warning(
                        "[driverlease] recipient-completed: case not found for case_no=%s",
                        case_no,
                    )
            except Exception as audit_error:
                logger.error(
                    "[driverlease] recipient-completed: failed to create audit trail: %s",
                    audit_error,
                    exc_info=True,
                )

        # No commit/rollback here; upstream handles transaction boundaries.
        return summary
    except Exception as e:
        logger.exception("[driverlease] recipient-completed: error: %s", e)
        return {"ok": False, "error": str(e), "envelope_id": ctx.get("envelope_id")}


async def driverlease_recipient_delivered(db: Session, ctx: dict, payload: dict):
    logger.info("[driverlease] recipient-delivered: %s", ctx)


async def driverlease_recipient_declined(db: Session, ctx: dict, payload: dict):
    logger.info("[driverlease] recipient-declined: %s", ctx)


async def driverlease_envelope_sent(db: Session, ctx: dict, payload: dict):
    logger.info("[driverlease] envelope-sent: %s", ctx)
    rows = esign_utils.update_envelope_status(db, ctx=ctx, status="envelope-sent")
    # Ensure pending UPDATE is pushed to the DB connection without committing
    db.flush()
    if rows == 0:
        logger.warning(
            "[driverlease] envelope-sent: no envelope updated (id=%s, lease_id=%s)",
            ctx.get("envelope_id"),
            ctx.get("lease_id") or ctx.get("object_id"),
        )

    # Create audit trail entry if case_no is available
    case_no = ctx.get("case_no")
    if case_no:
        try:
            case = db.query(Case).filter(Case.case_no == case_no).first()
            if case:
                # Get lease_id from case_entities table
                lease_entity = db.query(CaseEntity).filter(
                    CaseEntity.case_no == case_no,
                    CaseEntity.entity_name == "lease"
                ).first()

                lease_id = lease_entity.identifier_value if lease_entity else None

                audit_trail_service.create_audit_trail(
                    db=db,
                    description=f"E-signature envelope sent (envelope_id: {ctx.get('envelope_id')})",
                    case=case,
                    user=None,
                    meta_data={
                        "envelope_id": ctx.get("envelope_id"),
                        "event": "envelope-sent",
                        "lease_id": lease_id,
                    },
                    audit_type=AuditTrailType.AUTOMATED,
                )
                db.flush()
                logger.info(
                    "[driverlease] envelope-sent: audit trail created for case %s",
                    case_no,
                )
            else:
                logger.warning(
                    "[driverlease] envelope-sent: case not found for case_no=%s",
                    case_no,
                )
        except Exception as e:
            logger.error(
                "[driverlease] envelope-sent: failed to create audit trail: %s",
                e,
                exc_info=True,
            )

    return {
        "ok": rows > 0,
        "updated_rows": rows,
        "envelope_id": ctx.get("envelope_id"),
        "status": "sent",
    }


async def driverlease_envelope_completed(db: Session, ctx: dict, payload: dict):
    logger.info("[driverlease] envelope-completed: %s", ctx)
    rows = esign_utils.update_envelope_status(db, ctx=ctx, status="envelope-completed")
    db.flush()
    if rows == 0:
        logger.warning(
            "[driverlease] envelope-completed: no envelope updated (id=%s, lease_id=%s)",
            ctx.get("envelope_id"),
            ctx.get("lease_id") or ctx.get("object_id"),
        )

    # Create audit trail entry if case_no is available
    case_no = ctx.get("case_no")
    if case_no:
        try:
            case = db.query(Case).filter(Case.case_no == case_no).first()
            if case:
                # Get lease_id from case_entities table
                lease_entity = db.query(CaseEntity).filter(
                    CaseEntity.case_no == case_no,
                    CaseEntity.entity_name == "lease"
                ).first()

                lease_id = lease_entity.identifier_value if lease_entity else None

                audit_trail_service.create_audit_trail(
                    db=db,
                    description=f"E-signature envelope completed (envelope_id: {ctx.get('envelope_id')})",
                    case=case,
                    user=None,
                    meta_data={
                        "envelope_id": ctx.get("envelope_id"),
                        "event": "envelope-completed",
                        "lease_id": lease_id,
                    },
                    audit_type=AuditTrailType.AUTOMATED,
                )
                db.flush()
                logger.info(
                    "[driverlease] envelope-completed: audit trail created for case %s",
                    case_no,
                )
            else:
                logger.warning(
                    "[driverlease] envelope-completed: case not found for case_no=%s",
                    case_no,
                )
        except Exception as e:
            logger.error(
                "[driverlease] envelope-completed: failed to create audit trail: %s",
                e,
                exc_info=True,
            )

    return {
        "ok": rows > 0,
        "updated_rows": rows,
        "envelope_id": ctx.get("envelope_id"),
        "status": "completed",
    }
