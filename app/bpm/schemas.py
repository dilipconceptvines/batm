## app/bpm/schemas.py

# Standard library imports
from datetime import datetime
from typing import Optional, List, Any

# Third party imports
from pydantic import BaseModel

# Local imports
from app.users.schemas import UserResponse, RoleResponse


class CaseStatusBase(BaseModel):
    """Case status base schema"""
    name: str


class CaseStatusCreate(CaseStatusBase):
    """Case status create schema"""
    pass


class CaseStatusResponse(CaseStatusBase):
    """Case status response schema"""
    id: int
    is_active: bool
    created_on: datetime
    updated_on: datetime

    class Config:
        """Pyndantic configuration"""
        from_attributes = True


class CaseTypeBase(BaseModel):
    """Case type base schema"""
    name: str
    prefix: str


class CaseTypeCreate(CaseTypeBase):
    """Case type create schema"""
    pass


class CaseTypeResponse(CaseTypeBase):
    """Case type response schema"""
    id: int
    is_active: bool
    created_on: datetime
    updated_on: datetime

    class Config:
        """Pyndantic configuration"""
        from_attributes = True


class CaseStepResponse(BaseModel):
    """Case step response schema"""
    id: int
    name: str

    class Config:
        """Pyndantic configuration"""
        from_attributes = True


class CaseStepConfigBase(BaseModel):
    """Case step config base schema"""
    step_id: str
    case_step_id: int
    next_assignee_id: Optional[int] = None
    next_step_id: Optional[str] = None
    roles: List[int] = []  # List of Role IDs to assign


class CaseStepConfigCreate(CaseStepConfigBase):
    """Case step config create schema"""
    pass


class CaseStepConfigResponse(CaseStepConfigBase):
    """Case step config response schema"""
    id: int
    is_active: bool
    created_on: datetime
    updated_on: Optional[datetime]
    case_step: CaseStepResponse
    next_assignee: Optional[UserResponse]
    roles: List[RoleResponse] = []  # Nested list of Role objects

    class Config:
        """Pyndantic configuration"""
        from_attributes = True


class SLABase(BaseModel):
    """SLA base schema"""
    object_name: str
    sla_in_minutes: int


class SLACreate(SLABase):
    """SLA create schema"""
    pass


class SLAResponse(SLABase):
    """SLA response schema"""
    id: int
    is_active: bool
    created_on: datetime
    updated_on: datetime

    class Config:
        """Pyndantic configuration"""
        from_attributes = True


class CreateCaseRequest(BaseModel):
    """Create case request schema"""
    case_type: str


class SubStepResponse(BaseModel):
    """Sub step response schema"""
    step_name: str
    step_id: str
    step_due_date: datetime
    time_to_step_completion: str


class StepResponse(BaseModel):
    """Step response schema"""
    step_name: str
    sub_steps: List[SubStepResponse]


class CreateCaseResponse(BaseModel):
    """Create case response schema"""
    case_no: str
    case_created_on: datetime
    case_created_by: str
    case_status: str
    steps: List[StepResponse]


class StepDataRequest(BaseModel):
    """Step data request schema"""
    step_id: str
    data: Any