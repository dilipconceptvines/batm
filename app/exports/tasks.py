### app/exports/tasks.py

"""
Celery Tasks for Async Export Processing

These tasks handle the actual export generation in the background,
freeing up the API to return immediately to the user.
"""

from datetime import datetime

from celery import shared_task

from app.core.db import SessionLocal
from app.exports.models import ExportJob, ExportStatus, ExportType, ExportFormat
from app.exports.streaming_service import StreamingExportService
from app.exports.builders.ezpass_builder import build_ezpass_export_query, transform_ezpass_row
from app.exports.builders.curb_builder import build_curb_export_query, transform_curb_row
from app.exports.builders.pvb_builder import build_pvb_export_query, transform_pvb_row
from app.exports.builders.ledger_builder import (
    build_ledger_postings_export_query,
    build_ledger_balances_export_query,
    transform_ledger_posting_row,
    transform_ledger_balance_row,
)
from app.exports.builders.current_balances_builder import (
    build_current_balances_export_query,
    transform_current_balance_row,
)
from app.exports.builders.driver_payments_builder import (
    build_driver_payments_export_query,
    transform_driver_payment_row,
)
from app.utils.logger import get_logger

logger = get_logger(__name__)


@shared_task(name="exports.export_data_async", bind=True)
def export_data_async(self, export_id: int):
    """
    Celery task to generate export file asynchronously.
    
    This task:
    1. Loads export job from database
    2. Builds appropriate query based on export_type
    3. Uses streaming service to generate file
    4. Updates export job with results
    5. Handles errors gracefully
    
    Args:
        export_id: ID of ExportJob to process
        
    Returns:
        dict: Result summary
    """
    db = SessionLocal()
    export_job = None
    
    try:
        # Load export job
        export_job = db.query(ExportJob).filter(ExportJob.id == export_id).first()
        
        if not export_job:
            logger.error(f"Export job {export_id} not found")
            return {"status": "error", "message": "Export job not found"}
        
        logger.info(
            f"Starting export job {export_id}: "
            f"type={export_job.export_type}, format={export_job.format}"
        )
        
        # Update status to PROCESSING
        export_job.status = ExportStatus.PROCESSING
        export_job.celery_task_id = self.request.id
        db.commit()
        
        # Initialize streaming service
        streaming_service = StreamingExportService(output_dir="/tmp/exports")
        
        # Build query and get transformer based on export type
        query, transformer, headers = _get_query_and_transformer(
            db, export_job.export_type, export_job.filters or {}
        )
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{export_job.export_type.value.lower()}_export_{timestamp}"
        
        # Stream query to file
        logger.info(f"Streaming export to file: {filename}.{export_job.format}")
        
        file_path, record_count = streaming_service.stream_query_to_file(
            query=query,
            filename=filename,
            export_format=export_job.format.value,
            row_transformer=transformer,
            headers=headers,
            batch_size=1000
        )
        
        logger.info(
            f"Export job {export_id} completed: "
            f"{record_count} records exported to {file_path}"
        )
        
        # Update export job with results
        export_job.status = ExportStatus.COMPLETED
        export_job.file_url = file_path  # TODO: Upload to S3 and store URL
        export_job.file_name = f"{filename}.{export_job.format.value}"
        export_job.total_records = record_count
        export_job.completed_at = datetime.utcnow()
        export_job.error_message = None
        db.commit()
        
        return {
            "status": "success",
            "export_id": export_id,
            "record_count": record_count,
            "file_path": file_path
        }
        
    except Exception as e:
        logger.error(f"Error in export_data_async for job {export_id}: {e}", exc_info=True)
        
        # Update export job with error
        if export_job:
            try:
                export_job.status = ExportStatus.FAILED
                export_job.error_message = str(e)
                export_job.completed_at = datetime.utcnow()
                db.commit()
            except Exception as commit_error:
                logger.error(f"Failed to update export job status: {commit_error}")
                db.rollback()
        
        # Re-raise to mark task as failed in Celery
        raise
        
    finally:
        db.close()


def _get_query_and_transformer(db, export_type: ExportType, filters: dict):
    """
    Get appropriate query builder and transformer for export type.
    
    Args:
        db: Database session
        export_type: Type of export
        filters: Filter parameters
        
    Returns:
        Tuple of (query, transformer_function, headers)
    """
    if export_type == ExportType.EZPASS:
        query = build_ezpass_export_query(db, filters)
        transformer = transform_ezpass_row
        headers = None  # Will be inferred from first row
        
    elif export_type == ExportType.PVB:
        query = build_pvb_export_query(db, filters)
        transformer = transform_pvb_row
        headers = None
        
    elif export_type == ExportType.CURB:
        query = build_curb_export_query(db, filters)
        transformer = transform_curb_row
        headers = None
        
    elif export_type == ExportType.LEDGER_POSTINGS:
        query = build_ledger_postings_export_query(db, filters)
        transformer = transform_ledger_posting_row
        headers = None
        
    elif export_type == ExportType.LEDGER_BALANCES:
        query = build_ledger_balances_export_query(db, filters)
        transformer = transform_ledger_balance_row
        headers = None
        
    elif export_type == ExportType.CURRENT_BALANCES:
        from datetime import date, timedelta
        # Get week dates from filters or use current week
        week_start = filters.get('week_start')
        if isinstance(week_start, str):
            week_start = date.fromisoformat(week_start)
        elif not week_start:
            today = date.today()
            days_since_sunday = (today.weekday() + 1) % 7
            week_start = today - timedelta(days=days_since_sunday)
        
        week_end = week_start + timedelta(days=6)
        
        query = build_current_balances_export_query(db, filters)
        # Create partial function with week dates
        from functools import partial
        transformer = partial(transform_current_balance_row, week_start=week_start, week_end=week_end)
        headers = None
        
    elif export_type == ExportType.DRIVER_PAYMENTS:
        query = build_driver_payments_export_query(db, filters)
        transformer = transform_driver_payment_row
        headers = None
        
    else:
        raise ValueError(f"Unsupported export type: {export_type}")
    
    return query, transformer, headers
