# app/dtr/router.py

import math
import json
import decimal
from io import BytesIO
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.users.models import User
from app.users.utils import get_current_user
from app.dtr.services import DTRService
from app.dtr.repository import DTRRepository
from app.dtr.schemas import (
    DTRResponse, DTRListResponse, DTRListItemResponse,
    DTRGenerationRequest, BatchDTRGenerationRequest,
    CheckNumberUpdateRequest, FinalizeDTRRequest,
    DTRSummaryResponse, SendDTREmailRequest, SendDTREmailResponse
)
from app.dtr.models import DTRStatus, PaymentMethod
from app.dtr.pdf_service import DTRPdfService
from app.dtr.tasks import send_dtr_email_on_demand_task
from app.dtr.email_service import DTREmailService
from app.dtr.models import DTR
from app.utils.logger import get_logger
from kombu.exceptions import OperationalError
from app.utils.exporter_utils import ExporterFactory

logger = get_logger(__name__)
router = APIRouter(prefix="/dtrs", tags=["DTRs"])


@router.post("/generate", response_model=DTRResponse, status_code=status.HTTP_201_CREATED)
def generate_dtr(
    request: DTRGenerationRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Generate DTR for a LEASE (not individual driver).
    
    IMPORTANT: One DTR per lease. Additional drivers are consolidated.
    """
    try:
        service = DTRService(db)
        
        # Calculate week_end if not provided
        week_end = request.week_end
        if not week_end:
            week_end = request.week_start + timedelta(days=6)
        
        dtr = service.generate_dtr_for_lease(
            lease_id=request.lease_id,
            week_start=request.week_start,
            week_end=week_end,
            force_final=request.force_final
        )
        
        return dtr
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Error generating DTR: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to generate DTR") from e


@router.post("/generate-batch", status_code=status.HTTP_201_CREATED)
def generate_batch_dtrs(
    request: BatchDTRGenerationRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Generate DTRs for multiple leases for a specific week.
    
    If lease_ids not provided, generates for ALL active leases.
    """
    try:
        service = DTRService(db)
        week_end = request.week_start + timedelta(days=6)
        
        # Get leases to process
        from app.leases.models import Lease
        from app.leases.schemas import LeaseStatus
        
        query = db.query(Lease).filter(
            Lease.status.in_([LeaseStatus.ACTIVE, LeaseStatus.TERMINATED])
        )
        
        if request.lease_ids:
            query = query.filter(Lease.id.in_(request.lease_ids))
        
        leases = query.all()
        
        results = {
            'generated': [],
            'skipped': [],
            'errors': []
        }
        
        for lease in leases:
            try:
                dtr = service.generate_dtr_for_lease(
                    lease_id=lease.id,
                    week_start=request.week_start,
                    week_end=week_end
                )
                results['generated'].append({
                    'lease_id': lease.id,
                    'dtr_number': dtr.dtr_number
                })
            except ValueError as e:
                if "already exists" in str(e):
                    results['skipped'].append({
                        'lease_id': lease.id,
                        'reason': 'DTR already exists'
                    })
                else:
                    results['errors'].append({
                        'lease_id': lease.id,
                        'error': str(e)
                    })
            except Exception as e:
                results['errors'].append({
                    'lease_id': lease.id,
                    'error': str(e)
                })
        
        return {
            'summary': {
                'total_leases': len(leases),
                'generated_count': len(results['generated']),
                'skipped_count': len(results['skipped']),
                'error_count': len(results['errors'])
            },
            'details': results
        }
        
    except Exception as e:
        logger.error(f"Error generating batch DTRs: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to generate batch DTRs") from e


@router.get("/list", response_model=DTRListResponse)
def list_dtrs(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    receipt_number: Optional[str] = Query(None),
    status: Optional[DTRStatus] = Query(None),
    payment_method: Optional[PaymentMethod] = Query(None),
    week_start: Optional[date] = Query(None),
    week_end: Optional[date] = Query(None),
    medallion_number: Optional[str] = Query(None),
    tlc_license: Optional[str] = Query(None),
    driver_name: Optional[str] = Query(None),
    plate_number: Optional[str] = Query(None),
    ach_batch_number: Optional[str] = Query(None),
    check_number: Optional[str] = Query(None),
    sort_by: str = Query('generation_date'),
    sort_order: str = Query('desc', regex='^(asc|desc)$'),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    List DTRs with comprehensive filtering and sorting.
    
    Supports filtering by:
    - Receipt number, status, payment method
    - Date range, medallion, TLC license
    - Driver name, plate number
    - ACH batch number, check number
    """
    try:
        repo = DTRRepository(db)
        
        dtrs, total = repo.list_with_filters(
            page=page,
            per_page=per_page,
            receipt_number=receipt_number,
            status=status,
            payment_method=payment_method,
            week_start_date_from=week_start,
            week_end_date_to=week_end,
            medallion_number=medallion_number,
            tlc_license=tlc_license,
            driver_name=driver_name,
            plate_number=plate_number,
            ach_batch_number=ach_batch_number,
            check_number=check_number,
            sort_by=sort_by,
            sort_order=sort_order
        )
        
        # Map to response schema
        items = []
        for dtr in dtrs:
            items.append(DTRListItemResponse(
                id=dtr.id,
                receipt_number=dtr.receipt_number,
                dtr_number=dtr.dtr_number,
                week_start_date=dtr.week_start_date,
                week_end_date=dtr.week_end_date,
                medallion_number=dtr.medallion.medallion_number if dtr.medallion else None,
                tlc_license=(dtr.primary_driver.tlc_license.tlc_license_number if dtr.primary_driver and getattr(dtr.primary_driver, 'tlc_license', None) else None),
                driver_name=f"{dtr.primary_driver.first_name} {dtr.primary_driver.last_name}" if dtr.primary_driver else None,
                plate_number=(dtr.vehicle.get_active_plate_number() if dtr.vehicle and hasattr(dtr.vehicle, 'get_active_plate_number')
                              else (dtr.vehicle.plate_number if dtr.vehicle and getattr(dtr.vehicle, 'plate_number', None) else None)),
                total_due_to_driver=dtr.total_due_to_driver,
                status=dtr.status,
                payment_method=dtr.payment_method,
                ach_batch_number=dtr.ach_batch_number,
                check_number=dtr.check_number
            ))
        
        total_pages = math.ceil(total / per_page) if total > 0 else 0
        
        return DTRListResponse(
            items=items,
            total=total,
            page=page,
            per_page=per_page,
            total_pages=total_pages
        )
        
    except Exception as e:
        logger.error(f"Error listing DTRs: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to list DTRs") from e


@router.get("/{dtr_id}", response_model=DTRResponse)
def get_dtr(
    dtr_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get DTR by ID with complete details"""
    try:
        repo = DTRRepository(db)
        dtr = repo.get_by_id(dtr_id)
        
        if not dtr:
            raise HTTPException(status_code=404, detail="DTR not found")
        
        # Enhance response with related data
        response = DTRResponse.model_validate(dtr)
        
        # Add related entity names
        if dtr.lease:
            response.lease_number = dtr.lease.lease_number
        
        if dtr.primary_driver:
            response.driver_name = f"{dtr.primary_driver.first_name} {dtr.primary_driver.last_name}"
            response.tlc_license = (dtr.primary_driver.tlc_license.tlc_license_number if getattr(dtr.primary_driver, 'tlc_license', None) else None)
        
        if dtr.vehicle:
            response.plate_number = (dtr.vehicle.get_active_plate_number() if hasattr(dtr.vehicle, 'get_active_plate_number')
                                     else getattr(dtr.vehicle, 'plate_number', None))
            response.vin = getattr(dtr.vehicle, 'vin', None)
        
        if dtr.medallion:
            response.medallion_number = dtr.medallion.medallion_number
        
        if dtr.additional_driver_ids:
            response.additional_driver_count = len(dtr.additional_driver_ids)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting DTR: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get DTR") from e


@router.put("/{dtr_id}/check-number", response_model=DTRResponse)
def update_check_number(
    dtr_id: int,
    request: CheckNumberUpdateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update check number for a DTR (marks as PAID)"""
    try:
        repo = DTRRepository(db)
        
        dtr = repo.update_check_number(
            dtr_id=dtr_id,
            check_number=request.check_number,
            payment_date=request.payment_date
        )
        
        return dtr
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Error updating check number: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to update check number") from e


@router.put("/{dtr_id}/finalize", response_model=DTRResponse)
def finalize_dtr(
    dtr_id: int,
    request: FinalizeDTRRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Manually finalize a DRAFT DTR.
    
    Use this when all pending charges have been posted and confirmed.
    """
    try:
        repo = DTRRepository(db)
        
        dtr = repo.finalize_dtr(dtr_id=dtr_id, user_id=current_user.id)
        
        return dtr
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Error finalizing DTR: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to finalize DTR") from e


@router.get("/summary/stats", response_model=DTRSummaryResponse)
def get_summary_stats(
    week_start: Optional[date] = Query(None),
    week_end: Optional[date] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get summary statistics for DTRs"""
    try:
        repo = DTRRepository(db)
        
        stats = repo.get_summary_stats(
            week_start=week_start,
            week_end=week_end
        )
        
        return DTRSummaryResponse(**stats)
        
    except Exception as e:
        logger.error(f"Error getting summary stats: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get summary stats") from e


@router.get("/export/all")
def export_dtrs(
    export_format: str = Query("excel", regex='^(excel|csv|pdf|json)$'),
    receipt_number: Optional[str] = Query(None),
    status: Optional[DTRStatus] = Query(None),
    payment_method: Optional[PaymentMethod] = Query(None),
    week_start_date_from: Optional[date] = Query(None),
    week_start_date_to: Optional[date] = Query(None),
    week_end_date_from: Optional[date] = Query(None),
    week_end_date_to: Optional[date] = Query(None),
    ach_batch_number: Optional[str] = Query(None),
    total_due_min: Optional[float] = Query(None, ge=0),
    total_due_max: Optional[float] = Query(None, ge=0),
    receipt_type: Optional[str] = Query(None),
    medallion_number: Optional[str] = Query(None),
    tlc_license: Optional[str] = Query(None),
    driver_name: Optional[str] = Query(None),
    plate_number: Optional[str] = Query(None),
    check_number: Optional[str] = Query(None),
    sort_by: str = Query('generation_date'),
    sort_order: str = Query('desc', regex='^(asc|desc)$'),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Export DTRs in the requested format with comprehensive filtering. Supported formats: excel, csv, pdf, json"""
    try:
        repo = DTRRepository(db)

        dtrs, _ = repo.list_with_filters(
            page=1,
            per_page=10000,  # Large limit for export
            receipt_number=receipt_number,
            status=status,
            payment_method=payment_method,
            week_start_date_from=week_start_date_from,
            week_start_date_to=week_start_date_to,
            week_end_date_from=week_end_date_from,
            week_end_date_to=week_end_date_to,
            ach_batch_number=ach_batch_number,
            total_due_min=total_due_min,
            total_due_max=total_due_max,
            receipt_type=receipt_type,
            medallion_number=medallion_number,
            tlc_license=tlc_license,
            driver_name=driver_name,
            plate_number=plate_number,
            check_number=check_number,
            sort_by=sort_by,
            sort_order=sort_order
        )

        # Prepare data for export: export ALL columns from the DTR table
        from app.dtr.models import DTR as DTRModel

        # Collect column names from the model table in defined order
        columns = [c.name for c in DTRModel.__table__.columns]

        def _format_value(v):
            if v is None:
                return ""
            # Decimal -> float for CSV friendliness
            if isinstance(v, decimal.Decimal):
                return float(v)
            # date/datetime -> ISO string
            if isinstance(v, (date, datetime)):
                return v.isoformat()
            # Enums (SQLAlchemy Enum) often expose .value
            if hasattr(v, 'value'):
                return v.value
            # JSON-like Python structures -> JSON string
            if isinstance(v, (dict, list)):
                try:
                    return json.dumps(v, default=str)
                except Exception:
                    return str(v)
            # Fallback to string
            return str(v)

        data = []
        for dtr in dtrs:
            # Preserve column order using dict comprehension
            row = {col: _format_value(getattr(dtr, col, None)) for col in columns}

            # Add a few helpful related fields (optional): driver name, medallion number, plate
            # These are not table columns but are very useful in exports.
            try:
                row['driver_name'] = (f"{dtr.primary_driver.first_name} {dtr.primary_driver.last_name}" if getattr(dtr, 'primary_driver', None) else "")
            except Exception:
                row['driver_name'] = ""

            try:
                row['medallion_number'] = (dtr.medallion.medallion_number if getattr(dtr, 'medallion', None) else "")
            except Exception:
                row['medallion_number'] = ""

            try:
                if getattr(dtr, 'vehicle', None):
                    row['plate_number'] = (dtr.vehicle.get_active_plate_number() if hasattr(dtr.vehicle, 'get_active_plate_number') else getattr(dtr.vehicle, 'plate_number', ""))
                else:
                    row['plate_number'] = ""
            except Exception:
                row['plate_number'] = ""

            data.append(row)

        # Use ExporterFactory
        exporter = ExporterFactory.get_exporter(export_format, data)
        buffer = exporter.export()

        # Determine media type and filename
        now_str = datetime.now().strftime('%Y%m%d')
        if export_format == 'excel':
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            filename = f"dtrs_{now_str}.xlsx"
        elif export_format == 'csv':
            media_type = "text/csv"
            filename = f"dtrs_{now_str}.csv"
        elif export_format == 'pdf':
            media_type = "application/pdf"
            filename = f"dtrs_{now_str}.pdf"
        else:  # json
            media_type = "application/json"
            filename = f"dtrs_{now_str}.json"

        return StreamingResponse(
            buffer,
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except ValueError as e:
        logger.error(f"Export error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Error exporting DTRs: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to export DTRs") from e
        
@router.get("/{dtr_id}/pdf", summary="Download DTR PDF")
def download_dtr_pdf(
    dtr_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Generates and downloads the Driver Transaction Receipt (DTR) PDF.
    """
    try:
        pdf_service = DTRPdfService(db)
        pdf_content = pdf_service.generate_dtr_pdf(dtr_id=dtr_id)

        # Determine content type based on Whether we generated PDF or fallback HTML
        is_pdf = pdf_content.startswith(b'%PDF')
        media_type = "application/pdf" if is_pdf else "text/html"
        ext = "pdf" if is_pdf else "html"

        filename = f"DTR_{dtr_id}_{date.today()}.{ext}"

        return StreamingResponse(
            BytesIO(pdf_content),
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Error generating DTR PDF: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to generate DTR PDF") from e
    

@router.post("/send-dtr-emails", response_model=SendDTREmailResponse, summary="Send DTR Emails On-Demand")
async def send_dtr_emails_on_demand(
    request: SendDTREmailRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Send DTR emails to selected drivers with attachments at any point in time.
    
    This endpoint allows staff to manually trigger DTR email delivery for specific DTRs.
    Each email includes:
    - DTR PDF
    - PVB Violations report (if any violations exist for the period)
    - TLC Violations report (if any violations exist for the period)
    
    **Use Cases:**
    - Resend DTRs that failed to deliver
    - Send DTRs to alternate email addresses
    - Manually trigger delivery for specific drivers
    - Send DTRs without violation reports (set include_violations=false)
    
    **Process:**
    1. Validates all DTR IDs exist
    2. Queues Celery tasks for each DTR
    3. Returns immediate response (emails sent asynchronously)
    
    **Email Templates Used:**
    - On-demand DTR email template with full receipt details
    
    **Note:** Emails are sent asynchronously via Celery. This endpoint returns
    immediately after queuing the tasks. Check logs for delivery status.
    """
    try:
        # Validate all DTR IDs exist
        dtrs = db.query(DTR).filter(DTR.id.in_(request.dtr_ids)).all()
        found_ids = {dtr.id for dtr in dtrs}
        missing_ids = set(request.dtr_ids) - found_ids
        
        if missing_ids:
            raise HTTPException(
                status_code=404,
                detail=f"DTRs not found: {sorted(missing_ids)}"
            )
        
        # Queue Celery tasks for each DTR
        emails_queued = 0
        emails_failed = 0
        results = []
        
        for dtr_id in request.dtr_ids:
            try:
                # Try to queue the Celery task first
                task = send_dtr_email_on_demand_task.delay(
                    dtr_id=dtr_id,
                    recipient_email=request.recipient_email,
                    include_violations=request.include_violations
                )
                
                emails_queued += 1
                results.append({
                    "dtr_id": dtr_id,
                    "status": "queued",
                    "task_id": task.id
                })
                
                logger.info(
                    "DTR email queued", dtr_id=dtr_id,
                    task_id=task.id, user_id=current_user.id
                )
                
            except (OperationalError, ConnectionRefusedError, Exception) as e:
                # Fallback: If Celery is down, send email directly
                if isinstance(e, (OperationalError, ConnectionRefusedError)) or "Connection refused" in str(e):
                    logger.warning(
                        "Celery broker unavailable, falling back to direct email send", 
                        dtr_id=dtr_id, error=str(e)
                    )
                    
                    try:
                        # Send email directly using email service
                        email_service = DTREmailService(db)
                        result = await email_service.send_on_demand_dtr_email(
                            dtr_id=dtr_id,
                            recipient_email=request.recipient_email,
                            include_violations=request.include_violations
                        )
                        
                        if result.get("success", False):
                            emails_queued += 1
                            results.append({
                                "dtr_id": dtr_id,
                                "status": "sent_directly",
                                "task_id": None
                            })
                            
                            logger.info(
                                "DTR email sent directly (Celery fallback)", 
                                dtr_id=dtr_id, user_id=current_user.id
                            )
                        else:
                            raise Exception(result.get("error", "Unknown error in direct email send"))
                            
                    except Exception as direct_error:
                        emails_failed += 1
                        results.append({
                            "dtr_id": dtr_id,
                            "status": "failed",
                            "error": f"Celery down and direct send failed: {str(direct_error)}"
                        })
                        
                        logger.error(
                            "Failed to send DTR email (both Celery and direct)", 
                            dtr_id=dtr_id, celery_error=str(e), 
                            direct_error=str(direct_error), exc_info=True
                        )
                else:
                    # Non-connection related error, handle normally
                    emails_failed += 1
                    results.append({
                        "dtr_id": dtr_id,
                        "status": "failed",
                        "error": str(e)
                    })
                    
                    logger.error(
                        "Failed to queue DTR email", dtr_id=dtr_id,
                        error=str(e), exc_info=True
                    )
        
        return SendDTREmailResponse(
            total_requested=len(request.dtr_ids),
            emails_queued=emails_queued,
            emails_failed=emails_failed,
            results=results
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in send_dtr_emails_on_demand: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to queue DTR emails"
        ) from e


@router.post("/send-dtr-email/{dtr_id}", summary="Send Single DTR Email")
async def send_single_dtr_email(
    dtr_id: int,
    recipient_email: Optional[EmailStr] = Query(
        None,
        description="Optional override email. If not provided, uses driver's email."
    ),
    include_violations: bool = Query(
        True,
        description="Whether to include violation reports"
    ),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Send a single DTR email (convenience endpoint).
    
    This is a simplified version of the batch endpoint for sending a single DTR.
    
    **Parameters:**
    - `dtr_id`: The DTR ID to send
    - `recipient_email`: Optional override email (defaults to driver's email)
    - `include_violations`: Include PVB/TLC violation reports (default: true)
    
    **Returns:**
    - Task ID for tracking the email delivery
    """
    try:
        # Validate DTR exists
        dtr = db.query(DTR).filter(DTR.id == dtr_id).first()
        if not dtr:
            raise HTTPException(
                status_code=404, detail=f"DTR not found: {dtr_id}"
            )
        
        # Try to queue the Celery task first
        try:
            task = send_dtr_email_on_demand_task.delay(
                dtr_id=dtr_id,
                recipient_email=recipient_email,
                include_violations=include_violations
            )
            
            logger.info(
                "Single DTR email queued", dtr_id=dtr_id,
                task_id=task.id, user_id=current_user.id
            )
            
            return {
                "dtr_id": dtr_id,
                "status": "queued",
                "task_id": task.id,
                "message": "DTR email has been queued for delivery"
            }
            
        except (OperationalError, ConnectionRefusedError, Exception) as e:
            # Fallback: If Celery is down, send email directly
            if isinstance(e, (OperationalError, ConnectionRefusedError)) or "Connection refused" in str(e):
                logger.warning(
                    "Celery broker unavailable, falling back to direct email send", 
                    dtr_id=dtr_id, error=str(e)
                )
                
                # Send email directly using email service
                email_service = DTREmailService(db)
                result = await email_service.send_on_demand_dtr_email(
                    dtr_id=dtr_id,
                    recipient_email=recipient_email,
                    include_violations=include_violations
                )
                
                if result.get("success", False):
                    logger.info(
                        "Single DTR email sent directly (Celery fallback)", 
                        dtr_id=dtr_id, user_id=current_user.id
                    )
                    
                    return {
                        "dtr_id": dtr_id,
                        "status": "sent_directly",
                        "task_id": None,
                        "message": "DTR email sent directly (Celery unavailable)"
                    }
                else:
                    raise HTTPException(
                        status_code=500,
                        detail=f"Failed to send DTR email: {result.get('error', 'Unknown error')}"
                    )
            else:
                # Non-connection related error, re-raise
                raise e
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error sending single DTR email: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to queue DTR email"
        ) from e