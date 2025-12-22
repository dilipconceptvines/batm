### app/reports/schemas.py

# Standard library imports
from datetime import datetime
from typing import List, Optional, Dict

# Third party imports
from pydantic import BaseModel


class QueryRecordCreate(BaseModel):
    """Schema for creating a query record"""
    prompt: str


class QueryRecordExecute(BaseModel):
    """Schema for executing a query record"""
    query_id: int
    filters: Optional[dict] = {}


class QueryRecordResponse(BaseModel):
    """Schema for the response of a query record"""
    id: int
    prompt: str
    validated_sql: str
    execution_status: str
    executed_at: Optional[datetime] = None
    rows_returned: int
    columns_returned: List[str]
    favorite: bool
    is_shared: bool
    exported_formats: List[Dict[str, str]]
    created_on: datetime

    class Config:
        """Config for the QueryRecordResponse"""
        from_attributes = True


class SharedQueryRequest(BaseModel):
    """Schema for sharing a query record"""
    query_id: int
    user_ids: List[int]


class SharedQueryResponse(BaseModel):
    """Schema for the response of a shared query"""
    shared_with_user_id: int
    shared_by_user_id: int
    shared_at: datetime

    class Config:
        """Config for the SharedQueryResponse"""
        from_attributes = True