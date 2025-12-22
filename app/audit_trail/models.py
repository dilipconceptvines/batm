## app/audit_trail/models.py

# Standard library imports
from datetime import datetime, timezone

# Third party imports
from sqlalchemy import Column, String, DateTime, Integer, Enum, ForeignKey, JSON
from sqlalchemy.orm import relationship

# Local imports
from app.core.db import Base
from app.users.models import AuditMixin
from app.audit_trail.schemas import AuditTrailType


class AuditTrail(Base, AuditMixin):
    """Audit trail model"""
    __tablename__ = "audit_trail"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.now(timezone.utc), nullable=False)
    done_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    user_role = Column(String(255), nullable=True)
    case_id = Column(Integer, ForeignKey("cases.id"), nullable=True)
    case_type = Column(String(255), nullable=True)
    case_no = Column(String(255), nullable=True)
    step_name = Column(String(255), nullable=True)
    description = Column(String(255), nullable=True)
    audit_trail_type = Column(Enum(AuditTrailType), nullable=False, default=AuditTrailType.AUTOMATED)

    # Optional fields for related view
    meta_data = Column(JSON, nullable=True, default={})

    # Relationships
    case = relationship("Case", back_populates="audit_trail")
    
    # User who created the record (from AuditMixin)
    creator = relationship(
        "User",
        back_populates="created_audit_trails",
        foreign_keys="[AuditTrail.created_by]",
        overlaps="user"
    )
    
    # User who performed the action
    user = relationship(
        "User",
        foreign_keys="[AuditTrail.done_by]",
        overlaps="creator"
    )