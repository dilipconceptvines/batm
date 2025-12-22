### app/reports/services.py

# Standard library imports
import math
from datetime import datetime
from typing import Optional, List, Union

# Third party imports
import pandas as pd
from sqlalchemy import text, or_, desc
from sqlalchemy.orm import Session

# Local imports
from app.utils.logger import get_logger
from app.reports.utils import apply_filters
from app.reports.models import QueryRecord, SharedQuery
from app.users.models import User

logger = get_logger(__name__)


class ReportService:
    """Service for managing report operations"""

    def get_query_record(
        self, db: Session,
        user_id: Optional[int] = None,
        query_id: Optional[int] = None,
        multiple: Optional[bool] = False
    ) -> Union[QueryRecord, List[QueryRecord]]:
        """Get a query record"""
        try:
            query = db.query(QueryRecord).join(User, QueryRecord.user_id == User.id)
            if user_id:
                query = query.filter(QueryRecord.user_id == user_id)
            if query_id:
                query = query.filter(QueryRecord.id == query_id)
            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting query record: %s", e)
            raise e


    def execute_paginated_query(
        self, db: Session, stmt: str, filters: dict,
        sort_by: str, sort_order: str, page: int, per_page: int
    ) -> dict:
        """Execute a paginated query"""
        try:
            logger.info("Executing paginated query: %s", stmt)
            # Strip out markdown code block syntax if present and clean up the query
            stmt = stmt.replace("```sql", "").replace("```", "").strip()
            # Remove trailing semicolon if present
            stmt = stmt.rstrip(';')
            
            final_query = apply_filters(stmt, filters, sort_by, sort_order)
            paginated_query = f"{final_query}"

            if page and per_page:
                paginated_query += f" LIMIT {per_page} OFFSET {(page - 1) * per_page}"

            # Execute the query
            result = db.execute(text(paginated_query)).fetchall()
            
            # Clean up the count query by removing any trailing semicolon
            count_query = f"SELECT COUNT(*) FROM ({final_query}) AS count_sub"
            total_count = db.execute(text(count_query)).scalar()

            # Get column names from the query result
            if result:
                # Get column names from the first row's keys
                columns = result[0]._mapping.keys()
                # Convert result to list of dictionaries
                rows = [dict(row._mapping) for row in result]
            else:
                columns = []
                rows = []

            return {
                "rows": rows,
                "columns": list(columns),
                "total_count": total_count,
                "total_pages": math.ceil(total_count / per_page),
                "page": page,
                "per_page": per_page,
                "filters_applied": filters,
                "sort_by": sort_by,
                "sort_order": sort_order
            }
        except Exception as e:
            logger.error("Error executing paginated query: %s", e)
            raise e
        
    def upsert_query_record(
        self, db: Session, query_data: dict
    ) -> QueryRecord:
        """Upsert a query record"""
        try:
            if query_data.get("id"):
                # Update existing record
                query_record = db.query(QueryRecord).filter(QueryRecord.id == query_data["id"]).first()
                if not query_record:
                    raise ValueError(f"Query record with id {query_data['id']} not found")
                
                # Update fields 
                for key, value in query_data.items():
                    setattr(query_record, key, value)
            else:
                # Create new record
                query_record = QueryRecord(**query_data)

            # Save changes
            db.add(query_record)
            db.commit()
            db.refresh(query_record)
            return query_record
        except Exception as e:
            logger.error("Error upserting query record: %s", e)
            raise e
        
    def get_query_history(
        self, db: Session, user_id: int,
        favorite: Optional[bool] = None,
        shared: Optional[bool] = None,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        page: int = 1,
        per_page: int = 20,
    ) -> List[QueryRecord]:
        """Get user's query history"""
        try:
            query = db.query(QueryRecord).filter(
                or_(
                    QueryRecord.user_id == user_id,
                    QueryRecord.id.in_(
                        db.query(SharedQuery.query_id)
                        .filter(SharedQuery.shared_with_user_id == user_id)
                    )
                )
            )
            if favorite is not None:
                query = query.filter(QueryRecord.favorite == favorite)
            if shared is not None:
                query = query.filter(QueryRecord.is_shared == shared)
            if from_date:
                query = query.filter(QueryRecord.executed_at >= from_date.strftime("%Y-%m-%d"))
            if to_date:
                query = query.filter(QueryRecord.executed_at <= to_date.strftime("%Y-%m-%d"))

            total_count = query.count()
            # Pagination
            offset = (page - 1) * per_page
            query = query.order_by(desc(QueryRecord.created_on)).offset(offset).limit(per_page)

            return query.all(), total_count
        except Exception as e:
            logger.error("Error getting query history: %s", e)
            raise e
        
    def upsert_shared_query(
        self, db: Session, shared_data: dict
    ) -> SharedQuery:
        """Upsert a shared query"""
        try:
            if shared_data.get("id"):
                # Update existing record
                shared_query = db.query(SharedQuery).filter(SharedQuery.id == shared_data["id"]).first()
                if not shared_query:
                    raise ValueError(f"Shared query with id {shared_data['id']} not found")
                
                # Update fields
                for key, value in shared_data.items():
                    setattr(shared_query, key, value)
            else:
                # Create new record
                shared_query = SharedQuery(**shared_data)

            # Save changes
            db.add(shared_query)
            db.commit()
            db.refresh(shared_query)
            return shared_query
        except Exception as e:
            logger.error("Error upserting shared query: %s", e)
            raise e
            
    def get_shared_queries(
        self, db: Session,
        shared_id: Optional[int] = None,
        query_id: Optional[int] = None,
        multiple: Optional[bool] = False
    ) -> Union[SharedQuery, List[SharedQuery]]:
        """Get shared queries"""
        try:
            query = db.query(SharedQuery).join(User, SharedQuery.shared_with_user_id == User.id)
            if shared_id:
                query = query.filter(SharedQuery.id == shared_id)
            if query_id:
                query = query.filter(SharedQuery.query_id == query_id)
            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting shared queries: %s", e)
            raise e
        

report_service = ReportService()
