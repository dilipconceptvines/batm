## app/audit_trail/schemas.py

# Standard library imports
from enum import Enum as PyEnum
from datetime import datetime
from typing import Optional
# Third party imports
from pydantic import BaseModel


class AuditTrailType(str, PyEnum):
    """Audit trail types"""
    AUTOMATED = "automated"
    MANUAL = "manual"


class AuditTrailCreate(BaseModel):
    """Manual audit trail"""
    case_no: Optional[str] = None
    step_id: Optional[int] = None
    description: str
    driver_id: Optional[int] = None
    medallion_id: Optional[int] = None
    vehicle_id: Optional[int] = None
    lease_id : Optional[int] = None
    medallion_owner_id: Optional[int] = None
    vehicle_owner_id: Optional[int] = None
    ledger_id: Optional[int] = None
    pvb_id: Optional[int] = None
    correspondence_id: Optional[int] = None


    class Config:
        """Pydantic config"""
        from_attributes = True


class AuditTrailResponse(BaseModel):
    """Audit trail response"""
    id: int
    case_id: int
    done_by: int
    user_role: str
    case_type: str
    step_name: str
    description: str
    audit_trail_type: AuditTrailType
    timestamp: datetime
    meta_data: dict

    class Config:
        """Pydantic config"""
        from_attributes = True