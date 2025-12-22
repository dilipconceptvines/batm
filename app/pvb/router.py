### app/pvb/router.py

import math
from datetime import date , datetime , time
from io import BytesIO
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi import status as fast_status
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.core.dependencies import get_db_with_current_user
from app.pvb.exceptions import PVBError
from app.pvb.schemas import (
    PaginatedPVBViolationResponse,
    PVBManualAssociateRequest,
    PVBManualPostRequest,
    PVBReassignRequest,
    PVBViolationResponse,
    EditPVBViolationRequest,
)
from app.pvb.services import PVBService
from app.pvb.models import PVBViolation , PVBImportStatus
from app.pvb.stubs import create_stub_pvb_response
from app.users.models import User
from app.users.utils import get_current_user
from app.utils.exporter_utils import ExporterFactory
from app.utils.logger import get_logger
from app.bpm.services import bpm_service
from app.uploads.services import upload_service

logger = get_logger(__name__)
router = APIRouter(prefix="/trips/pvb", tags=["PVB"])

# Dependency to inject the PVBService
def get_pvb_service(db: Session = Depends(get_db)) -> PVBService:
    """Provides an instance of PVBService with the given database session."""
    return PVBService(db)


@router.post("/upload-csv", summary="Upload and Process PVB CSV", status_code=fast_status.HTTP_202_ACCEPTED)
async def upload_pvb_csv(
    file: UploadFile = File(...),
    pvb_service: PVBService = Depends(get_pvb_service),
    current_user: User = Depends(get_current_user),
):
    """
    Accepts a CSV file of PVB violations, validates it, and triggers a background
    task for processing and association.
    """
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a CSV file.")

    try:
        file_stream = BytesIO(await file.read())
        result = pvb_service.process_uploaded_csv(
            file_stream, file.filename, current_user.id
        )
        return JSONResponse(content=result, status_code=fast_status.HTTP_202_ACCEPTED)
    except PVBError as e:
        logger.warning("Business logic error during PVB CSV upload: %s", e, exc_info=True)
        raise HTTPException(status_code=fast_status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    except Exception as e:
        logger.error("Error processing PVB CSV: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred during file processing.") from e


@router.get("/get_logs" , summary="Get PVB Processing Logs")
def get_pvb_processing_logs(
    page: int = Query(1, ge=1, description="Page number for pagination."),
    per_page: int = Query(10, ge=1, le=100, description="Items per page."),
    import_id: Optional[int]= Query(None , description="Filter by Import ID."),
    file_name: Optional[str]= Query(None , description="Filter by File Name."),
    from_date: Optional[date]= Query(None , description="Filter from Import Date."),
    to_date: Optional[date]= Query(None , description="Filter to Import Date."),
    status: Optional[PVBImportStatus]= Query(None , description="Filter by Import Status."),
    sort_by: Optional[str] = Query(None, description="Field to sort by."),
    sort_order: Optional[str] = Query(None , description="Sort order (asc or desc)."),
    pvb_service: PVBService = Depends(get_pvb_service),
    current_user: User = Depends(get_current_user),
):
    """
    Retrieves logs related to PVB processing activities.
    """
    try:
        logs = pvb_service.get_pvb_logs(
            page=page,
            per_page=per_page,
            import_id=import_id,
            file_name=file_name,
            from_date=from_date,
            to_date=to_date,
            status=status,
            sort_by=sort_by,
            sort_order=sort_order,
            multiple=True
        )
        return {
            "items": logs,
            "total_items": len(logs),
            "page": page,
            "per_page": per_page,
            "total_pages": math.ceil(len(logs) / per_page) if per_page > 0 else 0
        }
    except Exception as e:
        logger.error("Error fetching PVB processing logs: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while fetching logs.") from e

@router.get("", response_model=PaginatedPVBViolationResponse, summary="List PVB Violations")
def list_pvb_violations(
    use_stubs: bool = Query(False, description="Return stubbed data for testing."),
    page: int = Query(1, ge=1, description="Page number for pagination."),
    per_page: int = Query(10, ge=1, le=100, description="Items per page."),
    sort_by: Optional[str] = Query(None, description="Field to sort by."),
    sort_order: Optional[str] = Query(None , description="Sort order (asc or desc)."),
    plate: Optional[str] = Query(None, description="Filter by Plate Number."),
    state: Optional[str] = Query(None, description="Filter by State."),
    type: Optional[str] = Query(None, description="Filter by violation Type."),
    summons: Optional[str] = Query(None, description="Filter by Summons Number."),
    from_issue_date: Optional[date] = Query(None, description="Filter by Issue Date (from)."),
    to_issue_date: Optional[date] = Query(None, description="Filter by Issue Date (to)."),
    from_issue_time: Optional[time] = Query(None, description="Filter by Issue Time (from)."),
    to_issue_time: Optional[time] = Query(None, description="Filter by Issue Time (to)."),
    from_posting_date: Optional[date] = Query(None, description="Filter by Posting Date (from)."),
    to_posting_date: Optional[date] = Query(None, description="Filter by Posting Date (to)."),
    from_amount: Optional[float] = Query(None, description="Filter by Amount Due (from)."),
    to_amount: Optional[float] = Query(None, description="Filter by Amount Due (to)."),
    from_fine: Optional[float] = Query(None, description="Filter by Fine (from)."),
    to_fine: Optional[float] = Query(None, description="Filter by Fine (to)."),
    from_penalty: Optional[float] = Query(None, description="Filter by Penalty (from)."),
    to_penalty: Optional[float] = Query(None, description="Filter by Penalty (to)."),
    from_interest: Optional[float] = Query(None, description="Filter by Interest (from)."),
    to_interest: Optional[float] = Query(None, description="Filter by Interest (to)."),
    from_reduction: Optional[float] = Query(None, description="Filter by Reduction (from)."),
    to_reduction: Optional[float] = Query(None, description="Filter by Reduction (to)."),
    from_processing_fee: Optional[float] = Query(None, description="Filter by Processing Fee (from)."),
    to_processing_fee: Optional[float] = Query(None, description="Filter by Processing Fee (to)."),
    failure_reason: Optional[str] = Query(None, description="Filter by Failure Reason."),
    lease_id: Optional[str] = Query(None, description="Filter by associated Lease ID."),
    vin: Optional[str] = Query(None, description="Filter by associated Vehicle VIN."),
    driver_id: Optional[str] = Query(None, description="Filter by associated Driver ID."),
    medallion_no: Optional[str] = Query(None, description="Filter by associated Medallion No."),
    status: Optional[str] = Query(None, description="Filter by Status."),
    source: Optional[str] = Query(None, description="Filter by Source."),
    violation_code = Query(None, description="Filter by Violation Code."),
    violation_country = Query(None, description="Filter by Violation Country."),
    street_name = Query(None, description="Filter by Street Name."),
    pvb_service: PVBService = Depends(get_pvb_service),
    current_user: User = Depends(get_current_user),
):
    """
    Provides a paginated and filterable view of all imported and manually created
    PVB violations, matching the UI requirements.
    """
    if use_stubs:
        return create_stub_pvb_response(page, per_page)
    
    try:
        violations, total_items , states , types = pvb_service.repo.list_violations(
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
            from_posting_date=from_posting_date,
            to_posting_date=to_posting_date,
            from_amount=from_amount,
            to_amount=to_amount,
            from_fine=from_fine,
            to_fine=to_fine,
            from_penalty=from_penalty,
            to_penalty=to_penalty,
            from_interest=from_interest,
            to_interest=to_interest,
            from_reduction=from_reduction,
            to_reduction=to_reduction,
            from_processing_fee=from_processing_fee,
            to_processing_fee=to_processing_fee,
            failure_reason=failure_reason,
            lease_id=lease_id,
            vin=vin,
            driver_id=driver_id,
            medallion_no=medallion_no,
            status=status,
            source=source,
            violation_code = violation_code,
            violation_country = violation_country,
            street_name = street_name
        )

        response_items = [
            PVBViolationResponse(
                id=v.id,
                plate=v.plate,
                state=v.state,
                type=v.type,
                summons=v.summons,
                source=v.source,    
                issue_datetime = datetime.combine(v.issue_date, v.issue_time) if v.issue_time and v.issue_date else None,
                vin= v.vehicle.vin if v.vehicle else None,
                lease_id=v.lease.lease_id if v.lease else None,
                medallion_no=v.medallion.medallion_number if v.medallion else None,
                driver_id=v.driver.driver_id if v.driver else None,
                posting_date=v.posting_date,
                status=v.status,
                amount=v.amount_due,
                fine=v.fine,
                penalty=v.penalty,
                interest=v.interest,
                reduction=v.reduction,
                processing_fee=v.processing_fee,
                failure_reason=v.failure_reason,
                violation_code= v.violation_code,
                violation_country= v.violation_country,
                street_name = v.street_name
            ) for v in violations
        ]
        
        total_pages = math.ceil(total_items / per_page) if per_page > 0 else 0

        return PaginatedPVBViolationResponse(
            items=response_items,
            total_items=total_items,
            page=page,
            per_page=per_page,
            total_pages=total_pages,
            states=states,
            types=types
        )

    except Exception as e:
        logger.error("Error fetching PVB violations: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while fetching PVB data.")
    
@router.get("/export", summary="Export PVB Violation Data")
def export_pvb_violations(
    format: str = Query("excel", enum=["excel", "pdf" , "csv" , "json"], alias="format"),
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
    from_posting_date: Optional[date] = Query(None),
    to_posting_date: Optional[date] = Query(None),
    from_amount: Optional[float] = Query(None),
    to_amount: Optional[float] = Query(None),
    from_fine: Optional[float] = Query(None),
    to_fine: Optional[float] = Query(None),
    from_penalty: Optional[float] = Query(None),
    to_penalty: Optional[float] = Query(None),
    from_interest: Optional[float] = Query(None),
    to_interest: Optional[float] = Query(None),
    from_reduction: Optional[float] = Query(None),
    to_reduction: Optional[float] = Query(None),
    from_processing_fee: Optional[float] = Query(None),
    to_processing_fee: Optional[float] = Query(None),
    failure_reason: Optional[str] = Query(None),
    lease_id: Optional[str] = Query(None),
    vin: Optional[str] = Query(None),
    driver_id: Optional[str] = Query(None),
    medallion_no: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    source: Optional[str] = Query(None),
    pvb_service: PVBService = Depends(get_pvb_service),
    _current_user: User = Depends(get_current_user),
):
    """
    Exports filtered PVB violation data to the specified format (Excel or PDF).
    """
    try:
        violations, _, _, _ = pvb_service.repo.list_violations(
            page=1, per_page=10000, sort_by=sort_by, sort_order=sort_order,
            plate=plate, state=state, type=type, summons=summons,
            from_issue_date=from_issue_date, to_issue_date=to_issue_date,
            from_issue_time=from_issue_time, to_issue_time=to_issue_time,
            from_posting_date=from_posting_date, to_posting_date=to_posting_date,
            from_amount=from_amount, to_amount=to_amount,
            from_fine=from_fine, to_fine=to_fine,
            from_penalty=from_penalty, to_penalty=to_penalty,
            from_interest=from_interest, to_interest=to_interest,
            from_reduction=from_reduction, to_reduction=to_reduction,
            from_processing_fee=from_processing_fee, to_processing_fee=to_processing_fee,
            failure_reason=failure_reason, lease_id=lease_id,
            vin=vin, driver_id=driver_id, medallion_no=medallion_no,
            status=status, source=source
        )

        if not violations:
            raise ValueError("No PVB data available for export with the given filters.")

        export_data = [
            PVBViolationResponse(
                plate=v.plate,
                state=v.state,
                type=v.type,
                summons=v.summons,
                source=v.source,
                issue_datetime=datetime.combine(v.issue_date, v.issue_time) if v.issue_time and v.issue_date else None,
                vin=v.vehicle.vin if v.vehicle else None,
                lease_id=v.lease.lease_id if v.lease else None,
                medallion_no=v.medallion.medallion_number if v.medallion else None,
                driver_id=v.driver.driver_id if v.driver else None,
                posting_date=v.posting_date,
                status=v.status,
                amount=v.amount_due,
                fine=v.fine,
                penalty=v.penalty,
                interest=v.interest,
                reduction=v.reduction,
                processing_fee=v.processing_fee,
                failure_reason=v.failure_reason,
                violation_code= v.violation_code,
                violation_country= v.violation_country,
                street_name = v.street_name
            ).model_dump(exclude={"id"}) for v in violations
        ]
        
        filename = f"pvb_violations_{date.today()}.{'xlsx' if format == 'excel' else format}"

        exporter = ExporterFactory.get_exporter(format, export_data)
        file_content = exporter.export()

        media_types = {
            "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "pdf": "application/pdf"
        }
        media_type = media_types.get(format, "application/octet-stream")

        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        return StreamingResponse(file_content, media_type=media_type, headers=headers)

    except PVBError as e:
        logger.warning("Business logic error during PVB export: %s", e)
        raise HTTPException(status_code=400, detail=str(e)) from e

    except Exception as e:
        logger.error("Error exporting PVB data: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred during the export process.",
        ) from e    


@router.post("/post-to-batm", summary="Manually post violations to ledger", status_code=fast_status.HTTP_200_OK)
def manual_post_to_batm(
    request: PVBManualPostRequest,
    pvb_service: PVBService = Depends(get_pvb_service),
    current_user: User = Depends(get_current_user),
):
    """
    Manually post PVB violations to the centralized ledger.
    Used to force posting of ASSOCIATED violations without waiting for the automatic Celery task.

    This endpoint allows staff to:
    - Immediately post ASSOCIATED violations to ledger
    - Retry failed postings (POSTING_FAILED status)
    - Process specific violations urgently

    **Restrictions:**
    - Only ASSOCIATED status violations can be posted
    - Cannot re-post violations already POSTED_TO_LEDGER
    - Requires valid driver_id, lease_id, and positive amount

    **Process:**
    1. Validates violation is ASSOCIATED
    2. Creates DEBIT obligation in centralized ledger (category: PVB)
    3. Updates violation status to POSTED_TO_LEDGER
    4. Sets posting_date to current timestamp
    """
    try:
        if request.transaction_ids and request.all_transactions:
            raise ValueError("Cannot specify both transaction_ids and all_transactions")
        
        if not request.transaction_ids and not request.all_transactions:
            raise ValueError("Must specify either transaction_ids or all_transactions")

        result = pvb_service.manual_post_to_ledger(
            transaction_ids=request.transaction_ids , all_transactions=request.all_transactions
        )
        return JSONResponse(content=result, status_code=fast_status.HTTP_200_OK)
    except PVBError as e:
        logger.warning("Business logic error during PVB manual posting: %s", e)
        raise HTTPException(status_code=fast_status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    except Exception as e:
        logger.error("Error during PVB manual posting: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred during manual posting.") from e


@router.post("/reassign", summary="Reassign PVB violations to different driver", status_code=fast_status.HTTP_200_OK)
def reassign_pvb_transactions(
    request: PVBReassignRequest,
    pvb_service: PVBService = Depends(get_pvb_service),
    current_user: User = Depends(get_current_user),
):
    """
    Reassign PVB violations from one driver/lease to another.
    Used to correct incorrect associations or handle driver changes.

    This endpoint allows staff to:
    - Move violations between valid lease primary drivers
    - Correct misattributed summons
    - Handle mid-lease driver changes

    **Restrictions:**
    - Cannot reassign violations already POSTED_TO_LEDGER
    - New lease must be an active lease
    - New lease must belong to the specified new driver (valid primary driver)
    - Both new driver and new lease must exist in the system

    **Process:**
    1. Validates new driver and new lease exist
    2. Verifies new lease belongs to new driver
    3. Updates violation with new associations
    4. Sets status to ASSOCIATED
    5. Clears any previous failure_reason
    """
    try:
        result = pvb_service.reassign_transactions(
            transaction_ids=request.transaction_ids,
            new_driver_id=request.new_driver_id,
            new_lease_id=request.new_lease_id,
            new_medallion_id=request.new_medallion_id,
            new_vehicle_id=request.new_vehicle_id,
        )
        return JSONResponse(content=result, status_code=fast_status.HTTP_200_OK)
    except PVBError as e:
        logger.warning("Business logic error during PVB reassignment: %s", e)
        raise HTTPException(status_code=fast_status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    except Exception as e:
        logger.error("Error during PVB reassignment: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred during reassignment.") from e


@router.post("/associate-with-batm", summary="Retry failed associations", status_code=fast_status.HTTP_200_OK)
def retry_association_with_batm(
    request: PVBManualAssociateRequest,
    pvb_service: PVBService = Depends(get_pvb_service),
    current_user: User = Depends(get_current_user),
):
    """
    Retry automatic association logic for failed or specific PVB violations.

    This endpoint does NOT manually assign to a driver - it retries the same
    automatic association logic (plate → vehicle → CURB trip → driver/lease).

    **Use Cases:**
    - Retry all ASSOCIATION_FAILED violations (send empty request)
    - Retry specific violations that failed (provide transaction_ids)
    - Re-run association after CURB data updates

    **Request Body:**
    - transaction_ids: Optional list of violation IDs to retry
    - If null/empty: Retries ALL violations with ASSOCIATION_FAILED status

    **Association Logic:**
    1. Extract plate number from violation data
    2. Find Vehicle via plate registration
    3. Find CURB trip on that vehicle ±2 hours of violation time
    4. If found: Associate with driver/lease/medallion from CURB trip
    5. Update status to ASSOCIATED or ASSOCIATION_FAILED
    """
    try:
        result = pvb_service.retry_failed_associations(
            transaction_ids=request.transaction_ids
        )
        return JSONResponse(content=result, status_code=fast_status.HTTP_200_OK)
    except PVBError as e:
        logger.warning("Business logic error during PVB association retry: %s", e)
        raise HTTPException(status_code=fast_status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    except Exception as e:
        logger.error("Error during PVB association retry: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred during association retry.") from e

@router.get("/{pvb_id}" , summary="Get PVB Violation Details")
def get_pvb_violation(
    pvb_id: int,
    db: Session = Depends(get_db),
    pvb_service: PVBService = Depends(get_pvb_service),
    current_user: User = Depends(get_current_user),
):
    """
    Provides detailed information about a specific PVB violation.
    """

    try:
        pvb = db.query(PVBViolation).filter(PVBViolation.id == pvb_id).first()
        if not pvb:
            raise HTTPException(status_code=404, detail="PVB violation not found.")
        
        pvb_response = {
                "id":pvb.id,
                "plate":pvb.plate,
                "state":pvb.state,
                "type":pvb.type,
                "summons":pvb.summons,
                "source":pvb.source,    
                "issue_datetime": datetime.combine(pvb.issue_date, pvb.issue_time) if pvb.issue_time and pvb.issue_date else None,
                "vin": pvb.vehicle.vin if pvb.vehicle else None,
                "lease_id":pvb.lease.lease_id if pvb.lease else None,
                "medallion_no":pvb.medallion.medallion_number if pvb.medallion else None,
                "driver_id":pvb.driver.driver_id if pvb.driver else None,
                "posting_date":pvb.posting_date,
                "status":pvb.status,
                "amount":pvb.amount_due,
                "fine":pvb.fine,
                "penalty":pvb.penalty,
                "interest":pvb.interest,
                "reduction":pvb.reduction,
                "processing_fee":pvb.processing_fee,
                "failure_reason":pvb.failure_reason,
                "violation_code": pvb.violation_code,
                "violation_country": pvb.violation_country,
                "street_name": pvb.street_name,
                "is_terminated":pvb.is_terminated,
                "non_program":pvb.non_program,
                "system_entry_date":pvb.system_entry_date,
                "new_issue":pvb.new_issue,
                "hearing_ind":pvb.hearing_ind,
                "penalty_warning":pvb.penalty_warning,
                "front_or_opp":pvb.front_or_opp,
                "house_number":pvb.house_number,
                "intersect_street":pvb.intersect_street,
                "geo_location":pvb.geo_location,
                "street_code_1":pvb.street_code_1,
                "street_code_2":pvb.street_code_2,
                "street_code_3":pvb.street_code_3,
                "driver_payment_amount":pvb.driver_payment_amount,
                "judgement":pvb.judgement,
                "ng_pmt":pvb.ng_pmt,
                "payment":pvb.payment,
                "document":upload_service.get_documents(
                    db=db , object_id=pvb.id , object_type="pvb" , document_type="pvb_invoice"
                )
        }
        
        return pvb_response
    except Exception as e:
        logger.error("Error fetching PVB violation: %s", e, exc_info=True)
        raise e

@router.put("/{pvb_id}", summary="Edit PVB Violation", response_model=PVBViolationResponse)
def edit_pvb_violation(
    pvb_id: int,
    request: EditPVBViolationRequest,
    pvb_service: PVBService = Depends(get_pvb_service),
    current_user: User = Depends(get_current_user),
):
    """
    Edits an existing PVB violation.
    """
    try:
        # Exclude unset fields to only update what's passed
        pvb = pvb_service.repo.get_violation_by_id(pvb_id)
        if not pvb:
            raise HTTPException(status_code=404, detail="PVB violation not found.")
        if request.summons != pvb.summons:
            summons_exists = pvb_service.repo.get_violation_by_summons(request.summons)
            if summons_exists:
                raise HTTPException(status_code=400, detail="Summons number already exists.")

        updates = request.model_dump(exclude_unset=True)
        updated_violation = pvb_service.repo.update_violation(
            violation_id=pvb_id, updates=updates
        )
        
        v = pvb
        return PVBViolationResponse(
            id=v.id,
            plate=v.plate,
            state=v.state,
            type=v.type,
            summons=v.summons,
            source=v.source,    
            issue_datetime=datetime.combine(v.issue_date, v.issue_time) if v.issue_time and v.issue_date else None,
            vin=v.vehicle.vin if v.vehicle else None,
            lease_id=v.lease.lease_id if v.lease else None,
            medallion_no=v.medallion.medallion_number if v.medallion else None,
            driver_id=v.driver.driver_id if v.driver else None,
            posting_date=v.posting_date,
            status=v.status,
            amount=v.amount_due,
            fine=v.fine,
            penalty=v.penalty,
            interest=v.interest,
            reduction=v.reduction,
            processing_fee=v.processing_fee,
            failure_reason=v.failure_reason,
            violation_code=v.violation_code,
            violation_country=v.violation_country,
            street_name=v.street_name
        )
    except PVBError as e:
        logger.warning("Business logic error during PVB update: %s", e)
        raise HTTPException(status_code=fast_status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    except Exception as e:
         logger.error("Error updating PVB violation: %s", e, exc_info=True)
         raise HTTPException(status_code=500, detail="An unexpected error occurred during update.") from e

@router.post("/create-case", summary="Create a New PVB Case", status_code=fast_status.HTTP_201_CREATED)
def create_pvb_case(
    db: Session = Depends(get_db_with_current_user),
    current_user: User = Depends(get_current_user),
):
    """
    Initiates a new BPM workflow for manually creating a PVB violation.
    """
    try:
        new_case = bpm_service.create_case(db, prefix="CRPVB", user=current_user)
        return {
            "message": "New Create PVB case started successfully.",
            "case_no": new_case.case_no
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Error creating PVB case: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Could not start a new PVB case.")