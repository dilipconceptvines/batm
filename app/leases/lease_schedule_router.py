# app/leases/lease_schedule_router.py

"""
API endpoints for lease schedule management and posting
"""

from datetime import date
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.leases.lease_schedule_service import LeaseScheduleService
from app.utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/leases/schedules", tags=["Lease Schedules"])


@router.post("/post-weekly-fees")
def post_weekly_lease_fees(
    target_date: Optional[date] = Query(None, description="Target date for posting (defaults to today)"),
    lease_id: Optional[int] = Query(None, description="Specific lease to post (defaults to all)"),
    db: Session = Depends(get_db)
):
    """
    Post weekly lease fees to ledger
    
    This endpoint manually triggers the lease fee posting process.
    It's normally run automatically every Sunday at 05:00 AM via Celery.
    
    Query Parameters:
        - target_date: Date to post fees for (defaults to today)
        - lease_id: Specific lease ID to post (defaults to all active leases)
    
    Returns:
        {
            "success_count": 15,
            "failure_count": 0,
            "posted_schedules": [1, 2, 3, ...],
            "failed_schedules": [],
            "total_amount_posted": 7500.00,
            "message": "Posted 15 lease fees successfully"
        }
    """
    try:
        service = LeaseScheduleService(db)
        
        if target_date is None:
            target_date = date.today()
        
        result = service.post_weekly_lease_fees(
            target_date=target_date,
            lease_id=lease_id
        )
        
        message = f"Posted {result['success_count']} lease fees successfully"
        if result['failure_count'] > 0:
            message += f", {result['failure_count']} failed"
        
        return {
            **result,
            'message': message
        }
        
    except Exception as e:
        logger.error(f"Failed to post lease fees: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/unposted")
def get_unposted_schedules(
    lease_id: Optional[int] = Query(None, description="Filter by lease ID"),
    driver_id: Optional[str] = Query(None, description="Filter by driver ID"),
    start_date: Optional[date] = Query(None, description="Filter by due date >= start_date"),
    end_date: Optional[date] = Query(None, description="Filter by due date <= end_date"),
    db: Session = Depends(get_db)
):
    """
    Get all unposted lease schedule entries
    
    Query Parameters:
        - lease_id: Filter by specific lease
        - driver_id: Filter by specific driver
        - start_date: Filter by due date >= start_date
        - end_date: Filter by due date <= end_date
    
    Returns:
        List of unposted lease schedule entries with details
    """
    try:
        service = LeaseScheduleService(db)
        
        schedules = service.get_unposted_schedule_entries(
            lease_id=lease_id,
            driver_id=driver_id,
            start_date=start_date,
            end_date=end_date
        )
        
        result = []
        for schedule in schedules:
            result.append({
                'id': schedule.id,
                'lease_id': schedule.lease_id,
                'installment_number': schedule.installment_number,
                'due_date': schedule.installment_due_date.isoformat() if schedule.installment_due_date else None,
                'amount': float(schedule.installment_amount) if schedule.installment_amount else 0.0,
                'period_start': schedule.period_start_date.isoformat() if schedule.period_start_date else None,
                'period_end': schedule.period_end_date.isoformat() if schedule.period_end_date else None,
                'posted_to_ledger': schedule.posted_to_ledger,
                'is_active': schedule.is_active
            })
        
        return {
            'count': len(result),
            'schedules': result
        }
        
    except Exception as e:
        logger.error(f"Failed to get unposted schedules: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/post-single/{schedule_id}")
def post_single_schedule(
    schedule_id: int,
    db: Session = Depends(get_db)
):
    """
    Post a single lease schedule entry to ledger
    
    Path Parameters:
        - schedule_id: ID of the lease schedule entry to post
    
    Returns:
        {
            "success": true,
            "schedule_id": 123,
            "posting_id": "POST-2025-123456",
            "balance_id": "BAL-2025-123456",
            "message": "Schedule entry posted successfully"
        }
    """
    try:
        service = LeaseScheduleService(db)
        
        result = service.post_single_schedule_entry(schedule_id)
        
        return {
            **result,
            'message': 'Schedule entry posted successfully'
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to post schedule entry: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/statistics")
def get_posting_statistics(
    start_date: Optional[date] = Query(None, description="Start date for statistics"),
    end_date: Optional[date] = Query(None, description="End date for statistics"),
    db: Session = Depends(get_db)
):
    """
    Get statistics about lease fee postings
    
    Query Parameters:
        - start_date: Start date for stats period
        - end_date: End date for stats period
    
    Returns:
        {
            "total_schedules": 100,
            "posted_schedules": 85,
            "unposted_schedules": 15,
            "total_amount": 50000.00,
            "posted_amount": 42500.00,
            "unposted_amount": 7500.00,
            "posting_rate": 85.00
        }
    """
    try:
        service = LeaseScheduleService(db)
        
        stats = service.get_posting_statistics(
            start_date=start_date,
            end_date=end_date
        )
        
        return stats
        
    except Exception as e:
        logger.error(f"Failed to get statistics: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e