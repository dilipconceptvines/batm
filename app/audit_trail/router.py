## app/audit_trail/router.py

# Standard library imports
from typing import Optional

# Third party imports
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.audit_trail.models import AuditTrail
from app.audit_trail.schemas import AuditTrailCreate, AuditTrailType
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.core.config import settings
from app.core.db import get_db
from app.users.models import User
from app.users.services import UserService
from app.users.utils import get_current_user
from app.core.dependencies import get_db_with_current_user

# Local application imports
from app.utils.logger import get_logger

router = APIRouter(prefix="/audit-trail", tags=["Audit Trail"])
logger = get_logger(__name__)


@router.post("/manual", status_code=status.HTTP_201_CREATED)
async def create_manual_audit_trail(
    entry: AuditTrailCreate,
    db: Session = Depends(get_db_with_current_user),
    get_current_user: User = Depends(get_current_user),
):
    """
    Create a manual audit trail entry.
    """
    try:
        case = None
        if entry.step_id:
            logger.info("entry.step_id -*-*-*-: %s", entry.step_id)
            case = bpm_service.get_cases(
                db, case_no=entry.case_no, step_id=entry.step_id
            )
        else:
            case = bpm_service.get_cases(db, case_no=entry.case_no, sort_order="desc")

        # Prepare meta_data with optional fields
        meta_data = {}
        if entry.driver_id:
            meta_data["driver_id"] = entry.driver_id
        if entry.medallion_id:
            meta_data["medallion_id"] = entry.medallion_id
        if entry.vehicle_id:
            meta_data["vehicle_id"] = entry.vehicle_id
        if entry.lease_id:
            meta_data["lease_id"] = entry.lease_id
        if entry.medallion_owner_id:
            meta_data["medallion_owner_id"] = entry.medallion_owner_id
        if entry.vehicle_owner_id:
            meta_data["vehicle_owner_id"] = entry.vehicle_owner_id
        if entry.ledger_id:
            meta_data["ledger_id"] = entry.ledger_id
        if entry.pvb_id:
            meta_data["pvb_id"] = entry.pvb_id
        if entry.correspondence_id:
            meta_data["correspondence_id"] = entry.correspondence_id

        audit_entry = audit_trail_service.create_audit_trail(
            db,
            case=case if case else None,
            description=entry.description,
            user=get_current_user,
            meta_data=meta_data,
            audit_type=AuditTrailType.MANUAL,
        )
        return audit_entry
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error creating manual audit trail: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        ) from e


@router.get("/case/{case_no}", status_code=status.HTTP_200_OK)
async def get_case_audit(
    case_no: str,
    db: Session = Depends(get_db),
    user_service: UserService = Depends(),
    _: User = Depends(get_current_user),
):
    """
    Get the audit trail for a given case.
    """
    try:
        case_info = bpm_service.get_cases(db, case_no=case_no, multiple=True)
        if not case_info:
            return {}
        case_ids = [case.id for case in case_info]
        audits = audit_trail_service.get_audit_trail_by_case_ids(db, case_ids)

        results = []

        for audit in audits:
            if audit.created_by:
                user_data = await user_service.repo.get_user_by_id(
                    user_id=audit.created_by
                )
            elif audit.done_by:
                user_data = await user_service.repo.get_user_by_id(
                    user_id=audit.done_by
                )
            else:
                user_data = await user_service.repo.get_user_by_email(
                    settings.super_admin_email_id
                )
            results.append(
                {
                    "id": audit.id,
                    "timestamp": audit.timestamp,
                    "done_by": audit.done_by,
                    "user_role": user_data.roles[0].name if user_data.roles else None,
                    "case_id": audit.case_id,
                    "case_type": audit.case_type,
                    "step_name": audit.step_name,
                    "description": audit.description,
                    "audit_type": audit.audit_trail_type,
                    "meta_data": audit.meta_data,
                    "user": {
                        "id": user_data.id,
                        "first_name": user_data.first_name,
                        "last_name": user_data.last_name,
                        "email": user_data.email_address,
                    },
                    "is_archived": audit.is_archived,
                    "is_active": audit.is_active,
                    "created_by": audit.created_by,
                    "modified_by": audit.modified_by,
                    "created_on": audit.created_on,
                    "updated_on": audit.updated_on,
                }
            )

        return {"results": results, "total": len(results)}
    except Exception as e:
        logger.error("Error getting case audit trail: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        ) from e


@router.get("/related-view", status_code=status.HTTP_200_OK)
async def get_related_view(
    medallion_id: Optional[int] = None,
    driver_id: Optional[int] = None,
    vehicle_id: Optional[int] = None,
    lease_id: Optional[int] = None,
    medallion_owner_id: Optional[int] = None,
    vehicle_owner_id: Optional[int] = None,
    ledger_id: Optional[int] = None,
    pvb_id: Optional[int] = None,
    correspondence_id: Optional[int] = None,
    page: Optional[int] = Query(1, ge=1),
    per_page: Optional[int] = Query(10, ge=1),
    db: Session = Depends(get_db),
):
    """
    Get the related view for the given medallion, driver, and vehicle.
    Returns all matching audit trail entries without pagination.
    Results are ordered from oldest to latest.
    """
    try:
        # Create base query
        total, audit_trails = audit_trail_service.get_related_audit_trail(
            db=db,
            medallion_id=medallion_id,
            driver_id=driver_id,
            vehicle_id=vehicle_id,
            lease_id=lease_id,
            vehicle_owner_id=vehicle_owner_id,
            pvb_id=pvb_id,
            correspondence_id=correspondence_id,
            medallion_owner_id=medallion_owner_id,
            ledger_id=ledger_id,
            page=page,
            per_page=per_page,
        )
        return {
            "items": audit_trails,
            "page": page,
            "per_page": per_page,
            "total_items": total,
            "total_pages": (total // per_page) + (1 if total % per_page > 0 else 0),
        }
    except ValueError as e:
        logger.error("Validation error: %s", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)
        ) from e
    except SQLAlchemyError as e:
        logger.error("Database error in related view: %s", e)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database operation failed",
        ) from e
    except Exception as e:
        logger.error("Unexpected error in related view: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred",
        ) from e
