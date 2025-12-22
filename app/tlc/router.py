### app/tlc/router.py

import math
from datetime import date , time
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.bpm.services import bpm_service
from app.core.db import get_db
from app.core.dependencies import get_db_with_current_user
from app.tlc.exceptions import TLCError, TLCViolationNotFoundError
from app.tlc.schemas import (
    PaginatedTLCViolationResponse,
    TLCViolationListResponse,
)
from app.tlc.services import TLCService
from app.tlc.stubs import create_stub_tlc_response
from app.users.models import User
from app.users.utils import get_current_user
from app.utils.exporter_utils import ExporterFactory
from app.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/violations/tlc", tags=["TLC Violations"])

# Dependency to inject the TLCService
def get_tlc_service(db: Session = Depends(get_db)) -> TLCService:
    """Provides an instance of TLCService with the current DB session."""
    return TLCService(db)


@router.get("", response_model=PaginatedTLCViolationResponse, summary="List TLC Violations")
def list_tlc_violations(
    use_stubs: bool = Query(False, description="Return stubbed data for testing."),
    page: int = Query(1, ge=1, description="Page number for pagination."),
    per_page: int = Query(10, ge=1, le=100, description="Items per page."),
    sort_by: Optional[str] = Query(None, description="Field to sort by."),
    sort_order: str = Query(None , description="Sort order (asc or desc)."),
    plate: Optional[str] = Query(None, description="Filter by plate number."),
    state: Optional[str] = Query(None, description="Filter by state."),
    type: Optional[str] = Query(None, description="Filter by violation type (FI, FN, RF)."),
    summons: Optional[str] = Query(None, description="Filter by summons number."),
    from_issue_date: Optional[date] = Query(None, description="Filter by issue date."),
    to_issue_date: Optional[date] = Query(None, description="Filter by to issue date."),
    from_issue_time: Optional[time] = Query(None, description="Filter by issue time."),
    to_issue_time: Optional[time] = Query(None, description="Filter by to issue time."),
    from_due_date: Optional[date] = Query(None, description="Filter by from due date."),
    to_due_date: Optional[date] = Query(None, description="Filter by to due date."),
    from_penalty_amount: Optional[float] = Query(None, description="Filter by from penalty amount."),
    to_penalty_amount: Optional[float] = Query(None, description="Filter by to penalty amount."),
    from_service_fee: Optional[float] = Query(None, description="Filter by from service fee."),
    to_service_fee: Optional[float] = Query(None, description="Filter by to service fee."),
    from_total_payable: Optional[float] = Query(None, description="Filter by from total payable."),
    to_total_payable: Optional[float] = Query(None, description="Filter by to total payable."),
    from_driver_payable: Optional[float] = Query(None, description="Filter by from driver payable."),
    to_driver_payable: Optional[float] = Query(None, description="Filter by to driver payable."),
    disposition: Optional[str] = Query(None , description="Filter by disposition."),
    status: Optional[str] = Query(None, description="Filter by status."),
    description: Optional[str] = Query(None, description="Filter by description."),
    driver_id: Optional[str] = Query(None, description="Filter by Driver ID."),
    medallion_no: Optional[str] = Query(None, description="Filter by Medallion Number."),
    driver_name: Optional[str] = Query(None, description="Filter by Driver Name."),
    driver_email: Optional[str] = Query(None, description="Filter by Driver Email."),
    lease_id: Optional[str] = Query(None, description="Filter by Lease ID."),
    lease_type: Optional[str] = Query(None, description="Filter by Lease Type."),
    vin: Optional[str] = Query(None, description="Filter by VIN."),
    note: Optional[str] = Query(None, description="Filter by Note."),
    tlc_service: TLCService = Depends(get_tlc_service),
    current_user: User = Depends(get_current_user),
):
    """
    Retrieves a paginated and filterable list of all TLC violations.
    Supports filtering by plate, state, type, summons, issue date, driver, and medallion.
    """
    if use_stubs:
        return create_stub_tlc_response(page, per_page)
    
    try:
        violations, total_items = tlc_service.repo.list_violations(
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order,
            plate=plate,
            state=state,
            type=type,
            summons=summons,
            from_issue_date=from_issue_date,
            to_issue_date=to_issue_date,
            from_issue_time=from_issue_time,
            to_issue_time=to_issue_time,
            from_due_date=from_due_date,
            to_due_date=to_due_date,
            from_penalty_amount=from_penalty_amount,
            to_penalty_amount=to_penalty_amount,
            from_service_fee=from_service_fee,
            to_service_fee=to_service_fee,
            from_total_payable=from_total_payable,
            to_total_payable=to_total_payable,
            from_driver_payable=from_driver_payable,
            to_driver_payable=to_driver_payable,
            disposition=disposition,
            status=status,
            description=description,
            driver_id=driver_id,
            medallion_no=medallion_no,
            driver_name=driver_name,
            driver_email=driver_email,
            lease_id=lease_id,
            lease_type=lease_type,
            vin=vin,
            note=note
        )

        response_items = []
        for violation in violations:
            response_items.append(
                TLCViolationListResponse(
                    id=violation.id,
                    plate=violation.plate if hasattr(violation, 'plate') else "N/A",
                    state=violation.state if hasattr(violation, 'state') else "NY",
                    type=violation.violation_type,
                    summons_no=violation.summons_no,
                    issue_date=violation.issue_date,
                    issue_time=violation.issue_time,
                    due_date = violation.due_date,
                    description = violation.description if violation.description else "",
                    penalty_amount=violation.amount or 0.0,
                    service_fee=violation.service_fee or 0.0,
                    total_payable=violation.total_payable or 0.0,
                    driver_payable=violation.driver_payable or 0.0,
                    disposition=violation.disposition,
                    status=violation.status,
                    driver_id=violation.driver.driver_id if violation.driver and violation.driver.driver_id else "N/A",
                    medallion_no=violation.medallion.medallion_number if violation.medallion and violation.medallion.medallion_number else "N/A",
                    driver_name = violation.driver.full_name if violation.driver and violation.driver.full_name else "N/A",
                    driver_email = violation.driver.email_address if violation.driver and violation.driver.email_address else "N/A",
                    driver_phone = violation.driver.phone_number_1 if violation.driver and violation.driver.phone_number_1 else "N/A",
                    lease_id = violation.lease.lease_id if violation.lease and violation.lease.lease_id else "N/A",
                    lease_type = violation.lease.lease_type if violation.lease and violation.lease.lease_type else "N/A",
                    vin = violation.vehicle.vin if violation.vehicle and violation.vehicle.vin else "N/A",
                    note = violation.note if violation.note else "",
                    document = violation.attachment.to_dict() if violation.attachment else {},
                ).model_dump()
            )
        
        total_pages = math.ceil(total_items / per_page) if per_page > 0 else 0

        return PaginatedTLCViolationResponse(
            items=response_items,
            total_items=total_items,
            page=page,
            per_page=per_page,
            total_pages=total_pages
        )
    except Exception as e:
        logger.error("Error fetching TLC violations: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail="An error occurred while fetching TLC violations."
        ) from e


@router.post("/create-case", summary="Create a New TLC Violation Case", status_code=status.HTTP_201_CREATED)
def create_tlc_violation_case(
    db: Session = Depends(get_db_with_current_user),
    current_user: User = Depends(get_current_user),
):
    """
    Initiates a new BPM workflow for manually creating a TLC Violation.
    """
    try:
        new_case = bpm_service.create_case(db, prefix="CRTLC", user=current_user)
        return {
            "message": "New Create TLC Violation case started successfully.",
            "case_no": new_case.case_no,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error("Error creating TLC violation case: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail="Could not start a new TLC violation case."
        ) from e


@router.get("/{summons_no}", summary="View TLC Violation Details")
def get_tlc_violation_details(
    summons_no: str,
    tlc_service: TLCService = Depends(get_tlc_service),
    current_user: User = Depends(get_current_user),
):
    """
    Retrieves the detailed view of a single TLC violation by summons number.
    """
    try:
        violation = tlc_service.repo.get_violation_by_summons(summons_no)
        if not violation:
            raise TLCViolationNotFoundError(summons_no)

        # Build detailed response
        driver_details = {
            "driver_id": violation.driver.driver_id if violation.driver else None,
            "full_name": violation.driver.full_name if violation.driver else None,
            "tlc_license": violation.driver.tlc_license.tlc_license_number if violation.driver and violation.driver.tlc_license else None,
            "driver_email" : violation.driver.email_address if violation.driver and violation.driver.email_address else "N/A",
            "driver_phone" : violation.driver.phone_number_1 if violation.driver and violation.driver.phone_number_1 else "N/A"
        }
        
        lease_details = {
            "lease_id": violation.lease.lease_id if violation.lease else None,
            "lease_type": violation.lease.lease_type if violation.lease else None
        }
        
        medallion_details = {
            "medallion_no": violation.medallion.medallion_number if violation.medallion else None
        }
        
        vehicle_details = {
            "vin": violation.vehicle.vin if violation.vehicle else None
        }


        return {
            "id" : violation.id,
            "summons_no": violation.summons_no,
            "violation_type": violation.violation_type,
            "issue_date": violation.issue_date,
            "issue_time": violation.issue_time,
            "description": violation.description,
            "amount": violation.amount,
            "service_fee": violation.service_fee,
            "total_payable": violation.total_payable,
            "driver_payable": violation.driver_payable,
            "disposition": violation.disposition,
            "status": violation.status,
            "driver_details": driver_details,
            "lease_details": lease_details,
            "medallion_details": medallion_details,
            "vehicle_details": vehicle_details,
            "original_posting_id": violation.original_posting_id,
            "reversal_posting_id": violation.reversal_posting_id,
            "due_date": violation.due_date,
            "note": violation.note,
            "document": violation.attachment.to_dict() if violation.attachment else {},
            "created_on": violation.created_on,
        }
    except TLCViolationNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Error fetching details for TLC violation {summons_no}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail="An error occurred while fetching TLC violation details."
        ) from e


@router.get("/list/export", summary="Export TLC Violations Data")
def export_tlc_violations(
    export_format: str = Query("excel", enum=["excel", "pdf" , "csv"], alias="format"),
    sort_by: Optional[str] = Query("issue_date"),
    sort_order: str = Query("desc"),
    plate: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    type: Optional[str] = Query(None),
    summons: Optional[str] = Query(None),
    from_issue_date: Optional[date] = Query(None),
    to_issue_date: Optional[date] = Query(None),
    from_issue_time: Optional[time] = Query(None),
    to_issue_time: Optional[time] = Query(None),
    from_due_date: Optional[date] = Query(None),
    to_due_date: Optional[date] = Query(None),
    from_penalty_amount: Optional[float] = Query(None),
    to_penalty_amount: Optional[float] = Query(None),
    from_service_fee: Optional[float] = Query(None),
    to_service_fee: Optional[float] = Query(None),
    from_total_payable: Optional[float] = Query(None),
    to_total_payable: Optional[float] = Query(None),
    from_driver_payable: Optional[float] = Query(None),
    to_driver_payable: Optional[float] = Query(None),
    disposition: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    description: Optional[str] = Query(None),
    driver_id: Optional[str] = Query(None),
    medallion_no: Optional[str] = Query(None),
    driver_name: Optional[str] = Query(None),
    driver_email: Optional[str] = Query(None),
    lease_id: Optional[str] = Query(None),
    lease_type: Optional[str] = Query(None),
    vin: Optional[str] = Query(None),
    note: Optional[str] = Query(None),
    tlc_service: TLCService = Depends(get_tlc_service),
    current_user: User = Depends(get_current_user),
):
    """
    Exports filtered TLC violation data to the specified format (Excel or PDF or CSV).
    """
    try:
        violations, _ = tlc_service.repo.list_violations(
            page=1, per_page=10000, sort_by=sort_by, sort_order=sort_order,
            plate=plate, state=state, type=type, summons=summons,
            from_issue_date=from_issue_date, to_issue_date=to_issue_date,
            from_issue_time=from_issue_time, to_issue_time=to_issue_time,
            from_due_date=from_due_date, to_due_date=to_due_date,
            from_penalty_amount=from_penalty_amount, to_penalty_amount=to_penalty_amount,
            from_service_fee=from_service_fee, to_service_fee=to_service_fee,
            from_total_payable=from_total_payable, to_total_payable=to_total_payable,
            from_driver_payable=from_driver_payable, to_driver_payable=to_driver_payable,
            disposition=disposition, status=status, description=description,
            driver_id=driver_id, medallion_no=medallion_no,
            driver_name=driver_name, driver_email=driver_email,
            lease_id=lease_id, lease_type=lease_type,
            vin=vin, note=note,
        )

        if not violations:
            raise ValueError("No TLC violation data available for export with the given filters.")

        export_data = []
        for violation in violations:
            export_data.append({
                "Summons No": violation.summons_no,
                "Plate": getattr(violation, 'plate', 'N/A'),
                "State": getattr(violation, 'state', 'NY'),
                "Type": violation.violation_type.value,
                "Issue Date": violation.issue_date.strftime("%Y-%m-%d"),
                "Issue Time": violation.issue_time.strftime("%H:%M") if violation.issue_time else "",
                "Driver ID": violation.driver.driver_id if violation.driver and violation.driver.driver_id else "N/A",
                "Medallion": violation.medallion.medallion_number if violation.medallion and violation.medallion.medallion_number else "N/A",
                "Amount": float(violation.amount) if violation.amount else 0.0,
                "Service Fee": float(violation.service_fee) if violation.service_fee else 0.0,
                "Total": float(violation.total_payable) if violation.total_payable else 0.0,
                "Driver Payable": float(violation.driver_payable) if violation.driver_payable else 0.0,
                "Disposition": violation.disposition.value if violation.disposition else "N/A",
                "Status": violation.status.value if violation.status else "N/A",
                "Description": violation.description if violation.description else "N/A",
                "Driver Name": violation.driver.full_name if violation.driver and violation.driver.full_name else "N/A",
                "Driver Email": violation.driver.email_address if violation.driver and violation.driver.email_address else "N/A",
                "Lease ID": violation.lease.lease_id if violation.lease and violation.lease.lease_id else "N/A",
                "Lease Type": violation.lease.lease_type if violation.lease and violation.lease.lease_type else "N/A",
                "Vin": violation.vehicle.vin if violation.vehicle and violation.vehicle.vin else "N/A",
                "Note": violation.note if violation.note else "",
            })
        
        filename = f"tlc_violations_{date.today()}.{'xlsx' if export_format == 'excel' else export_format}"

        exporter = ExporterFactory.get_exporter(export_format, export_data)
        file_content = exporter.export()

        media_types = {
            "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "pdf": "application/pdf"
        }
        media_type = media_types.get(export_format, "application/octet-stream")

        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        return StreamingResponse(file_content, media_type=media_type, headers=headers)

    except TLCError as e:
        logger.warning("Business logic error during TLC export: %s", e)
        raise HTTPException(status_code=400, detail=str(e)) from e

    except Exception as e:
        logger.error("Error exporting TLC violation data: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred during the export process.",
        ) from e