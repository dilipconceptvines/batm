## app/audit_trail/services.py

# Standard library imports
from typing import Dict, List, Optional

from fastapi import HTTPException

# Third party imports
from sqlalchemy import asc, desc
from sqlalchemy.orm import Session

from app.audit_trail.models import AuditTrail
from app.audit_trail.schemas import AuditTrailType
from app.bpm.models import Case
from app.users.models import User

# Local imports
from app.utils.logger import get_logger

logger = get_logger(__name__)


class AuditTrailService:
    """Service for audit trail operations"""

    def get_audit_trail_by_case_ids(
        self, db: Session, case_ids: List[int]
    ) -> List[AuditTrail]:
        """Get audit trail by case id"""
        try:
            query = (
                db.query(AuditTrail)
                .filter(AuditTrail.case_id.in_(case_ids))
                .order_by(asc(AuditTrail.timestamp))
            )
            result = query.all()
            return result
        except Exception as e:
            logger.error("Error getting audit trail by case id: %s", e, exc_info=True)
            raise e

    def get_audit_trail_by_user(self, db: Session, user_id: int) -> List[AuditTrail]:
        """Get audit trail by user id"""
        try:
            query = (
                db.query(AuditTrail)
                .filter(AuditTrail.done_by == user_id)
                .order_by(asc(AuditTrail.timestamp))
            )
            result = query.all()
            return result
        except Exception as e:
            logger.error("Error getting audit trail by user id: %s", e)
            raise e

    def get_audit_trail_by_role(self, db: Session, role: str) -> List[AuditTrail]:
        """Get audit trail by role"""
        try:
            query = (
                db.query(AuditTrail)
                .filter(AuditTrail.user_role == role)
                .order_by(asc(AuditTrail.timestamp))
            )
            result = query.all()
            return result
        except Exception as e:
            logger.error("Error getting audit trail by role: %s", e)
            raise e

    def get_related_audit_trail(
        self,
        db: Session,
        medallion_id: Optional[int] = None,
        driver_id: Optional[int] = None,
        vehicle_id: Optional[int] = None,
        lease_id: Optional[int] = None,
        medallion_owner_id: Optional[int] = None,
        vehicle_owner_id: Optional[int] = None,
        ledger_id: Optional[int] = None,
        pvb_id: Optional[int] = None,
        correspondence_id: Optional[int] = None,
        page: Optional[int] = None,
        per_page: Optional[int] = None,
    ) -> List[AuditTrail]:
        """Get related audit trail"""
        try:
            query = db.query(AuditTrail)

            if medallion_id:
                query = query.filter(
                    AuditTrail.meta_data["medallion_id"].as_integer() == medallion_id
                )
            if driver_id:
                query = query.filter(
                    AuditTrail.meta_data["driver_id"].as_integer() == driver_id
                )
            if vehicle_id:
                query = query.filter(
                    AuditTrail.meta_data["vehicle_id"].as_integer() == vehicle_id
                )
            if lease_id:
                query = query.filter(
                    AuditTrail.meta_data["lease_id"].as_integer() == lease_id
                )
            if vehicle_owner_id:
                query = query.filter(
                    AuditTrail.meta_data["vehicle_owner_id"].as_integer()
                    == vehicle_owner_id
                )
            if medallion_owner_id:
                query = query.filter(
                    AuditTrail.meta_data["medallion_owner_id"].as_integer()
                    == medallion_owner_id
                )
            if ledger_id:
                query = query.filter(
                    AuditTrail.meta_data["ledger_id"].as_integer() == ledger_id
                )
            if pvb_id:
                query = query.filter(
                    AuditTrail.meta_data["pvb_id"].as_integer() == pvb_id
                )
            if correspondence_id:
                query = query.filter(
                    AuditTrail.meta_data["correspondence_id"].as_integer()
                    == correspondence_id
                )

            query = query.order_by(desc(AuditTrail.created_on))

            if page and per_page:
                total = query.count()
                query = query.offset((page - 1) * per_page).limit(per_page)
                result = [
                    {
                        "id": audit.id,
                        "timestamp": audit.timestamp,
                        "done_by": audit.done_by,
                        "created_by": audit.user.name if audit.user else "",
                        "case_id": audit.case_id,
                        "case_no": audit.case_no,
                        "case_type": audit.case_type,
                        "case_type_prefix": audit.case.case_type.prefix
                        if audit.case and audit.case.case_type
                        else "",
                        "step_name": audit.step_name,
                        "description": audit.description,
                        "audit_type": audit.audit_trail_type,
                        "meta_data": audit.meta_data,
                        "is_archived": audit.is_archived,
                        "is_active": audit.is_active,
                        "created_on": audit.created_on,
                    }
                    for audit in query.all()
                ]

                return total, result

            result = [
                {
                    "id": audit.id,
                    "timestamp": audit.timestamp,
                    "done_by": audit.done_by,
                    "created_by": audit.user.name if audit.user else "",
                    "case_id": audit.case_id,
                    "case_no": audit.case_no,
                    "case_type": audit.case_type,
                    "case_type_prefix": audit.case.case_type.prefix
                    if audit.case and audit.case.case_type
                    else "",
                    "step_name": audit.step_name,
                    "description": audit.description,
                    "audit_type": audit.audit_trail_type,
                    "meta_data": audit.meta_data,
                    "is_archived": audit.is_archived,
                    "is_active": audit.is_active,
                    "created_on": audit.created_on,
                }
                for audit in query.all()
            ]
            return result
        except Exception as e:
            logger.error("Error getting related audit trail: %s", e)
            raise e

    def create_audit_trail(
        self,
        db: Session,
        description: str,
        case: Optional[Case] = None,
        user: Optional[User] = None,
        meta_data: Dict = None,
        audit_type: AuditTrailType = AuditTrailType.AUTOMATED,
    ) -> AuditTrail:
        """Create an audit trail entry"""
        try:
            new_audit_log = AuditTrail(
                case_id=case.id if case else None,
                case_type=case.case_type.name if case else None,
                case_no=case.case_no if case else None,
                step_name=case.case_step_config.step_name
                if case and case.case_step_config
                else None,
                done_by=user.id if user else None,
                user_role=user.roles[0].name if user and user.roles else None,
                description=description,
                audit_trail_type=audit_type,
                meta_data=meta_data,
            )
            db.add(new_audit_log)
            db.flush()
            return new_audit_log
        except Exception as e:
            logger.error("Error creating audit trail: %s", e)
            raise e

    def update_audit_trail(
        self, db: Session, audit_id: int, update_data: Dict
    ) -> AuditTrail:
        """Update an existing audit trail entry"""
        try:
            audit = db.query(AuditTrail).filter(AuditTrail.id == audit_id).first()
            if not audit:
                raise HTTPException(status_code=404, detail="Audit trail not found")

            for key, value in update_data.items():
                setattr(audit, key, value)

            db.commit()
            db.refresh(audit)
            return audit
        except Exception as e:
            logger.error("Error updating audit trail: %s", e)
            raise e


audit_trail_service = AuditTrailService()
