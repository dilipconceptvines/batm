### app/reports/router.py

# Standard library imports
import math
from typing import Optional, Dict, List
from datetime import datetime

# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, HTTPException, Query

# Local imports
from app.core.db import get_db, generate_schema_description
from app.utils.logger import get_logger
from app.reports.services import report_service
from app.reports.utils import export_to_xls, export_to_pdf
from app.reports.schemas import QueryRecordCreate
from app.utils.bedrock import bedrock_client
from app.users.models import User
from app.users.utils import get_current_user

logger = get_logger(__name__)
router = APIRouter(tags=["reports"], prefix="/reports")


@router.post("/generate")
def generate_report(
    payload: QueryRecordCreate,
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    per_page: int = Query(10, le=100),
    sort_by: Optional[str] = None,
    sort_order: Optional[str] = "asc",
    filters: Optional[Dict[str, str]] = None,
    query_id: Optional[int] = None,
    logged_in_user: User = Depends(get_current_user),
):
    """Generate a report"""
    try:
        if not query_id:
            # Step 1: Generate SQL from GenAI
            schema_desc = generate_schema_description()
            generated_sql = bedrock_client.generate_sql(payload.prompt, schema_desc)

            # Step 2: Validate SQL
            if not bedrock_client.validate_sql(generated_sql):
                raise HTTPException(status_code=400, detail="Invalid SQL query")
            
            # Step 3: Execute SQL
            result = report_service.execute_paginated_query(
                db, generated_sql, filters, sort_by, sort_order, page, per_page
            )

            df = pd.DataFrame(result["rows"])
            columns = result["columns"]
            row_count = result["total_count"]

            # Step 4: Export to PDF/XLS
            record = report_service.upsert_query_record(db, {
                "prompt": payload.prompt,
                "validated_sql": generated_sql,
                "execution_status": "SUCCESS",
                "executed_at": datetime.now(),
                "filters_applied": filters,
                "columns_returned": columns,
                "rows_returned": row_count,
                "exported_formats": [],
                "user_id": logged_in_user.id,
            })

            # pdf_url = export_to_pdf(df, filters, record.id)
            # xls_url = export_to_xls(df, filters, record.id)

            record = report_service.upsert_query_record(db, {
                "id": record.id,
                "exported_formats": [
                    {"format": "pdf", "url": ""},
                    {"format": "xls", "url": ""},
                ],
            })
        else:
            record = report_service.get_query_record(db, query_id=query_id)
            if not record:
                raise HTTPException(status_code=404, detail="Report not found")

            result = report_service.execute_paginated_query(
                db, record.validated_sql, filters,
                sort_by, sort_order, page, per_page
            )

            columns = result["columns"]
            row_count = result["total_count"]
        return {
            "query_id": record.id,
            "favorite": record.favorite,
            "rows": result["rows"],
            "columns": columns,
            "filters_applied": filters,
            "total_count": row_count,
            "total_pages": result["total_pages"],
            "page": page,
            "per_page": per_page,
            "pdf_url": "",
            "xls_url": ""
        }
    except Exception as e:
        logger.error("Error generating report: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e
    
@router.get("/history")
def get_history(
    db: Session = Depends(get_db),
    favorite: Optional[bool] = None,
    shared: Optional[bool] = None,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(10, le=100),
    logged_in_user: User = Depends(get_current_user),
):
    """Get user's report history"""
    try:
        history, total_count = report_service.get_query_history(
            db, logged_in_user.id, favorite, shared, from_date, to_date,
            page, per_page
        )
        return {
            "page": page,
            "per_page": per_page,
            "items": [{
                "id": item.id,
                "prompt": item.prompt,
                "validated_sql": item.validated_sql,
                "execution_status": item.execution_status,
                "executed_at": item.executed_at,
                "rows_returned": item.rows_returned,
                "columns_returned": item.columns_returned,
                "favorite": item.favorite,
                "is_shared": item.is_shared,
                "exported_formats": item.exported_formats,
                "created_on": item.created_on
            } for item in history],
            "total_count": total_count,
            "total_pages": math.ceil(total_count / per_page)
        }
    except Exception as e:
        logger.error("Error getting report history: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e
    
@router.post("/favorite")
def toggle_favorite(
    query_id: int,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user)
):
    """Toggle favorite status for a report"""
    try:
        record = report_service.get_query_record(db, query_id=query_id)
        if not record:
            raise HTTPException(status_code=404, detail="Report not found")
        
        report = report_service.upsert_query_record(db, {
            "id": record.id,
            "favorite": not record.favorite,
        })
        return {"favorite": report.favorite}
    except Exception as e:
        logger.error("Error toggling favorite status: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e
    
@router.post("/share")
def share_query(
    query_id: int,
    user_ids: List[int],
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user)
):
    """Share a report with other users"""
    try:
        report = report_service.get_query_record(db, query_id=query_id)
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")
        
        if report.user_id != logged_in_user.id:
            raise HTTPException(status_code=403, detail="You are not allowed to share this report")
        
        for user_id in user_ids:
            report_service.upsert_shared_query(db, {
                "query_id": report.id,
                "shared_by_user_id": logged_in_user.id,
                "shared_with_user_id": user_id,
            })

            report_service.upsert_query_record(db, {
                "id": report.id,
                "is_shared": True
            })

            report_service.upsert_query_record(db, {
                "prompt": report.prompt,
                "validated_sql": report.validated_sql,
                "execution_status": report.execution_status,
                "executed_at": report.executed_at,
                "filters_applied": report.filters_applied,
                "columns_returned": report.columns_returned,
                "rows_returned": report.rows_returned,
                "exported_formats": report.exported_formats,
                "is_shared": True,
                "user_id": user_id,
            })
        
        return {"shared_with": user_ids}
    except Exception as e:
        logger.error("Error sharing report: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e
        
@router.get("/{query_id}/shares")
def list_shares(
    query_id: int,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user)
):
    """List all users who have shared a report"""
    try:
        shared_queries = report_service.get_shared_queries(db, query_id=query_id, multiple=True)
        return {
            "items": [{
                "id": item.id,
                "shared_with_user_id": item.shared_with_user_id,
                "shared_by_user_id": item.shared_by_user_id,
                "shared_with_user": {
                    "id": item.shared_with_user.id,
                    "first_name": item.shared_with_user.first_name,
                    "last_name": item.shared_with_user.last_name,
                    "email_address": item.shared_with_user.email_address,
                }
            } for item in shared_queries]
        }
    except Exception as e:
        logger.error("Error listing shared users: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e        