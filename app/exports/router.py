### app/exports/router.py

"""
Export API Endpoints

Provides REST API for creating, monitoring, and downloading exports.
"""

import math
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.exports.models import ExportJob, ExportStatus
from app.exports.schemas import (
    ExportRequest,
    ExportResponse,
    ExportStatusResponse,
    ExportListItem,
    PaginatedExportListResponse,
)
from app.exports.tasks import export_data_async
from app.users.models import User
from app.users.utils import get_current_user
from app.utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/exports", tags=["Exports"])


@router.post("/request", response_model=ExportResponse, status_code=status.HTTP_202_ACCEPTED)
def request_export(
    export_request: ExportRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Create a new export job and trigger background processing.
    
    Returns immediately with export ID and status URL.
    User can poll the status URL to check progress.
    
    **Supported Export Types:**
    - EZPASS
    - PVB
    - CURB
    - LEDGER_POSTINGS
    - LEDGER_BALANCES
    
    **Supported Formats:**
    - excel (recommended for large datasets)
    - csv
    - json
    - pdf (limited to 10,000 records)
    """
    try:
        # Create export job record
        export_job = ExportJob(
            export_type=export_request.export_type,
            format=export_request.format,
            status=ExportStatus.PENDING,
            filters=export_request.filters,
            created_by=current_user.id,
        )
        
        db.add(export_job)
        db.commit()
        db.refresh(export_job)
        
        logger.info(
            f"Created export job {export_job.id} for user {current_user.id}: "
            f"type={export_request.export_type}, format={export_request.format}"
        )
        
        # Trigger Celery task
        task = export_data_async.delay(export_job.id)
        
        # Update with Celery task ID
        export_job.celery_task_id = task.id
        db.commit()
        
        logger.info(f"Triggered Celery task {task.id} for export job {export_job.id}")
        
        # Build response
        return ExportResponse(
            export_id=export_job.id,
            status=ExportStatus.PENDING,
            message=(
                f"Export job created successfully. "
                f"Check status at /api/exports/{export_job.id}/status"
            ),
            status_url=f"/api/exports/{export_job.id}/status",
            file_url=None,
        )
        
    except Exception as e:
        logger.error(f"Error creating export job: {e}", exc_info=True)
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create export job: {str(e)}"
        )


@router.get("/{export_id}/status", response_model=ExportStatusResponse)
def get_export_status(
    export_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Check the status of an export job.
    
    Returns current status, progress (if available), and download URL when completed.
    
    **Status Values:**
    - PENDING: Export job created, waiting to start
    - PROCESSING: Export is being generated
    - COMPLETED: Export ready for download
    - FAILED: Export failed (see error_message)
    """
    export_job = db.query(ExportJob).filter(
        ExportJob.id == export_id,
        ExportJob.created_by == current_user.id
    ).first()
    
    if not export_job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Export job {export_id} not found or access denied"
        )
    
    # Build response
    response = ExportStatusResponse(
        export_id=export_job.id,
        export_type=export_job.export_type,
        format=export_job.format,
        status=export_job.status,
        progress=100 if export_job.status == ExportStatus.COMPLETED else None,
        total_records=export_job.total_records,
        file_url=f"/api/exports/{export_job.id}/download" if export_job.status == ExportStatus.COMPLETED else None,
        file_name=export_job.file_name,
        error_message=export_job.error_message,
        created_at=export_job.created_at,
        completed_at=export_job.completed_at,
        created_by=export_job.created_by,
    )
    
    return response


@router.get("/{export_id}/download")
def download_export(
    export_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Download the exported file.
    
    Returns 404 if export not found or not yet completed.
    Returns file with appropriate Content-Disposition header.
    """
    export_job = db.query(ExportJob).filter(
        ExportJob.id == export_id,
        ExportJob.created_by == current_user.id
    ).first()
    
    if not export_job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Export job {export_id} not found or access denied"
        )
    
    if export_job.status != ExportStatus.COMPLETED:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Export not ready yet. Current status: {export_job.status}"
        )
    
    if not export_job.file_url:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Export file not found"
        )
    
    # Get file path
    file_path = Path(export_job.file_url)
    
    if not file_path.exists():
        logger.error(f"Export file missing: {file_path}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Export file not found on disk"
        )
    
    # Determine media type
    media_types = {
        "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "csv": "text/csv",
        "json": "application/json",
        "pdf": "application/pdf",
    }
    
    media_type = media_types.get(export_job.format.value, "application/octet-stream")
    
    # Return file
    return FileResponse(
        path=str(file_path),
        media_type=media_type,
        filename=export_job.file_name,
        headers={"Content-Disposition": f"attachment; filename={export_job.file_name}"}
    )


@router.get("/my-exports", response_model=PaginatedExportListResponse)
def list_my_exports(
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(10, ge=1, le=100, description="Items per page"),
    status_filter: Optional[ExportStatus] = Query(None, description="Filter by status"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    List all export jobs for the current user.
    
    Returns paginated list of exports with their status.
    Useful for showing export history in UI.
    """
    # Build query
    query = db.query(ExportJob).filter(ExportJob.created_by == current_user.id)
    
    # Apply status filter
    if status_filter:
        query = query.filter(ExportJob.status == status_filter)
    
    # Get total count
    total_items = query.count()
    
    # Apply pagination and sorting
    offset = (page - 1) * per_page
    exports = query.order_by(desc(ExportJob.created_at)).offset(offset).limit(per_page).all()
    
    # Build response items
    items = [
        ExportListItem(
            export_id=exp.id,
            export_type=exp.export_type,
            format=exp.format,
            status=exp.status,
            total_records=exp.total_records,
            file_name=exp.file_name,
            created_at=exp.created_at,
            completed_at=exp.completed_at,
        )
        for exp in exports
    ]
    
    # Calculate total pages
    total_pages = math.ceil(total_items / per_page) if per_page > 0 else 0
    
    return PaginatedExportListResponse(
        items=items,
        total_items=total_items,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
    )


@router.delete("/{export_id}")
def delete_export(
    export_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Delete an export job and its associated file.
    
    Useful for cleaning up old exports.
    """
    export_job = db.query(ExportJob).filter(
        ExportJob.id == export_id,
        ExportJob.created_by == current_user.id
    ).first()
    
    if not export_job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Export job {export_id} not found or access denied"
        )
    
    # Delete file if exists
    if export_job.file_url:
        file_path = Path(export_job.file_url)
        if file_path.exists():
            try:
                file_path.unlink()
                logger.info(f"Deleted export file: {file_path}")
            except Exception as e:
                logger.warning(f"Failed to delete export file {file_path}: {e}")
    
    # Delete database record
    db.delete(export_job)
    db.commit()
    
    logger.info(f"Deleted export job {export_id}")
    
    return {"message": f"Export job {export_id} deleted successfully"}
