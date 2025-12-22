### app/reports/models.py

# Third party imports
from sqlalchemy import (
    Column, Integer, String, JSON, DateTime, Boolean, Text, ForeignKey
)
from sqlalchemy.orm import relationship

# Local imports
from app.core.db import Base
from app.users.models import AuditMixin


class QueryRecord(Base, AuditMixin):
    """Model for storing query records"""
    __tablename__ = "query_records"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    prompt = Column(Text, nullable=False)
    validated_sql = Column(Text, nullable=False)
    execution_status = Column(String(64), default="PENDING")
    executed_at = Column(DateTime, nullable=True)

    filters_applied = Column(JSON, default={})   
    columns_returned = Column(JSON, default=[])
    rows_returned = Column(Integer, default=0)

    favorite = Column(Boolean, default=False)
    is_shared = Column(Boolean, default=False)
    exported_formats = Column(JSON, default=[])

    shared_queries = relationship("SharedQuery", back_populates="query_record")


class SharedQuery(Base, AuditMixin):
    """Model for storing shared queries"""
    __tablename__ = "shared_queries"

    id = Column(Integer, primary_key=True, index=True)
    query_id = Column(Integer, ForeignKey("query_records.id"))
    shared_with_user_id = Column(Integer, ForeignKey("users.id"))
    shared_by_user_id = Column(Integer, ForeignKey("users.id"))
    shared_with_user = relationship("User", foreign_keys=[shared_with_user_id])
    shared_by_user = relationship("User", foreign_keys=[shared_by_user_id])

    query_record = relationship("QueryRecord", back_populates="shared_queries")
    # shared_with_user = relationship("User", back_populates="shared_queries")
    # shared_by_user = relationship("User", back_populates="shared_queries")