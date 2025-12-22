### app/leases/router.py

import base64
from datetime import date
from io import BytesIO
from typing import Any, Dict, Optional

import aiohttp
from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.audit_trail.services import audit_trail_service
from app.core.config import settings
from app.core.db import get_db
from app.leases.schemas import (
    LeasePresetCreate,
    LeasePresetResponse,
    LeasePresetUpdate,
    LeaseStatus,
    LeaseType,
)
from app.leases.search_service import format_lease_export, format_lease_response
from app.leases.services import lease_service
from app.leases.utils import (
    calculate_short_term_lease_schedule,
    calculate_weekly_lease_schedule,
    get_driver_documents_with_envelope,
)
from app.uploads.services import upload_service
from app.users.models import User
from app.users.utils import get_current_user
from app.utils.exporter_utils import ExporterFactory
from app.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter(tags=["Leases"])


@router.get("/can_lease", summary="List Of vehicle can Create Lease")
def can_lease(
    db: Session = Depends(get_db),
    vin: Optional[str] = Query(None, description="Filter by VIN number"),
    medallion_number: Optional[str] = Query(
        None, description="Filter by medallion number"
    ),
    plate_number: Optional[str] = Query(None, description="Filter by plate number"),
    shift_availability: Optional[str] = Query(
        None,
        description="Filter by shift availability: 'full' (both shifts available), 'day' (day shift available), 'night' (night shift available)",
    ),
    page: int = Query(1, description="Page number"),
    per_page: int = Query(10, description="Number of leases per page"),
    sort_by: Optional[str] = Query("created_on", description="Sort by field"),
    sort_order: Optional[str] = Query("desc", description="Sort order"),
):
    """List Of vehicle can Create Lease"""

    try:
        results, total_count = lease_service.get_can_lease(
            db=db,
            vin=vin,
            medallion_number=medallion_number,
            plate_number=plate_number,
            shift_availability=shift_availability,
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order,
            multiple=True,
        )

        return {
            "items": results,
            "total_count": total_count,
            "page": page,
            "per_page": per_page,
            "total_pages": total_count // per_page + 1
            if total_count % per_page
            else total_count // per_page,
        }
    except Exception as e:
        logger.error("Error retrieving can lease list: %s", str(e), exc_info=True)
        raise HTTPException(
            status_code=500, detail="Error retrieving can lease list"
        ) from e


@router.get("/lease/{lease_id}/documents/preview", tags=["Leases"])
async def get_lease_documents_preview(
    lease_id: str, db: Session = Depends(get_db), _: User = Depends(get_current_user)
):
    """Get preview of all documents associated with a lease from DocuSign"""
    try:
        # Get the lease
        lease = lease_service.get_lease(db, lease_id=lease_id)

        if not lease:
            raise HTTPException(status_code=404, detail="Lease not found")

        # Get all active lease driver documents
        lease_driver_docs = []
        for lease_driver in lease.lease_driver:
            if not lease_driver.is_active:
                continue

            doc = lease_service.get_lease_driver_documents(
                db, lease_driver_id=lease_driver.id, status=True
            )

            if doc and doc.document_envelope_id:
                lease_driver_docs.append(doc)

        if not lease_driver_docs:
            return []
        # Initialize DocuSign client
        # access_token = client.get_access_token()
        access_token = "1234567890"

        # Get document previews
        previews = []
        for doc in lease_driver_docs:
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/pdf",
            }

            # Using DocuSign's combined PDF endpoint
            url = (
                f"{settings.docusign_base_url}/restapi/v2.1/accounts/{settings.docusign_account_id}"
                f"/envelopes/{doc.document_envelope_id}/documents/combined"
            )

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        content = await response.read()
                        base64_pdf = base64.b64encode(content).decode("utf-8")
                        previews.append(
                            {
                                "lease_driver_id": doc.lease_driver_id,
                                "envelope_id": doc.document_envelope_id,
                                "has_frontend_signed": doc.has_frontend_signed,
                                "has_driver_signed": doc.has_driver_signed,
                                "preview_base64": base64_pdf,
                                "content_type": "application/pdf",
                                "filename": f"lease_doc_{doc.lease_driver_id}.pdf",
                            }
                        )
                    else:
                        error_text = await response.text()
                        logger.error(
                            "Failed to get document preview for envelope %s: %s",
                            doc.document_envelope_id,
                            error_text,
                        )
                        continue

        return previews

    except Exception as e:
        logger.error("Error getting lease document previews: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/leases", summary="List all the leases", tags=["Leases"])
def list_leases(
    db: Session = Depends(get_db),
    page: int = Query(1, description="Page number"),
    per_page: int = Query(10, description="Number of leases per page"),
    lease_id: Optional[str] = Query(None, description="Filter by lease ID"),
    medallion_no: Optional[str] = Query(None, description="Filter by medallion number"),
    tlc_number: Optional[str] = Query(None, description="Filter by TLC license number"),
    driver_id: Optional[str] = Query(None, description="Filter by driver ID"),
    driver_name: Optional[str] = Query(None, description="Filter by driver name"),
    vin_no: Optional[str] = Query(None, description="Filter by VIN number"),
    lease_type: Optional[str] = Query(None, description="Filter by lease type"),
    plate_no: Optional[str] = Query(None, description="Filter by plate number"),
    lease_start_date: Optional[date] = Query(
        None, description="Filter by lease start date"
    ),
    lease_end_date: Optional[date] = Query(
        None, description="Filter by lease end date"
    ),
    status: Optional[str] = Query(None, description="Filter by lease status"),
    lease_amount: Optional[str] = Query(None, description="Filter by lease amount (comma-separated for multiple)"),
    sort_by: Optional[str] = Query(None, description="Sort by field"),
    sort_order: Optional[str] = Query(None, description="Sort order"),
    exclude_additional_drivers: Optional[bool] = Query(
        None , 
        description="Exclude leases where driver is additional driver (default: true)"
    ),  # ✅ NEW PARAMETER
    logged_in_user: User = Depends(get_current_user),
):
    """List all the leases"""
    try:
        leases, total_count = lease_service.get_lease(
            db=db,
            page=page,
            per_page=per_page,
            lease_id=lease_id,
            is_lease_list=True,
            medallion_number=medallion_no,
            tlc_number=tlc_number,
            lease_type=lease_type,
            driver_id=driver_id,
            driver_name=driver_name,
            vin_number=vin_no,
            plate_number=plate_no,
            lease_start_date=lease_start_date,
            lease_end_date=lease_end_date,
            status=status,
            lease_amount=lease_amount,
            sort_by=sort_by,
            sort_order=sort_order,
            exclude_additional_drivers=exclude_additional_drivers,
            multiple=True,
        )
        lease_info = [format_lease_response(db, lease) for lease in leases]
        lease_types = [lease_type.value for lease_type in LeaseType]
        lease_statuses = [status.value for status in LeaseStatus]

        return {
            "items": lease_info,
            "total_count": total_count,
            "lease_types": lease_types,
            "lease_statuses": lease_statuses,
            "page": page,
            "per_page": per_page,
            "sort_by": sort_by,
            "sort_order": sort_order,
        }
    except Exception as e:
        logger.exception("Error retrieving lease list")
        raise HTTPException(status_code=500, detail="Error retrieving leases") from e


@router.get("/view/lease/{leaseId}", summary="View a lease details", tags=["Leases"])
def view_lease(
    leaseId: str,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    """View a lease details with driver documents and envelope information"""
    try:
        if not leaseId:
            raise HTTPException(status_code=400, detail="Lease ID is required")
        lease = lease_service.get_lease(db=db, lease_id=leaseId)
        if not lease:
            raise HTTPException(status_code=404, detail="Lease not found")

        lease_drivers = lease.lease_driver

        documents = upload_service.get_documents(
            db=db, object_type="lease", object_id=lease.id, multiple=True
        )
        lease_documents = []
        lease_documents.extend(documents or [])
        history = audit_trail_service.get_related_audit_trail(db=db, lease_id=lease.id)

        # Get main drivers with their documents (exclude additional drivers)
        main_drivers = []
        additional_drivers = []
        removed_drivers = []
        if lease_drivers:
            for lease_driver in lease_drivers:
                driver_dict = lease_driver.driver.to_dict()
                driver_documents = get_driver_documents_with_envelope(db, lease_driver)
                driver_dict["documents"] = driver_documents
                lease_documents.extend(driver_documents)
                driver_dict["lease_driver_id"] = lease_driver.id
                driver_dict["is_day_night_shift"] = lease_driver.is_day_night_shift
                driver_dict["driver_role"] = lease_driver.driver_role

                # Add joined_date and removed_date
                driver_dict["joined_date"] = (
                    lease_driver.date_added.strftime(settings.common_date_format)
                    if lease_driver.date_added and settings.common_date_format
                    else None
                )
                driver_dict["removed_date"] = (
                    lease_driver.date_terminated.strftime(settings.common_date_format)
                    if lease_driver.date_terminated and settings.common_date_format
                    else None
                )

                # Get case details for additional driver
                case_detail = None
                if lease_driver.is_additional_driver:
                    from app.bpm.models import Case, CaseEntity

                    case_entity = (
                        db.query(CaseEntity)
                        .filter(
                            CaseEntity.entity_name == "lease_drivers",
                            CaseEntity.identifier == "driver_id",
                            CaseEntity.identifier_value
                            == lease_driver.driver.driver_id,
                        )
                        .first()
                    )
                    if case_entity:
                        case = (
                            db.query(Case)
                            .filter(Case.case_no == case_entity.case_no)
                            .first()
                        )
                        if case:
                            case_detail = {
                                "case_no": case.case_no,
                                "case_status": case.case_status.name
                                if case.case_status
                                else None,
                            }
                driver_dict["case_detail"] = case_detail

                if lease_driver.is_additional_driver:
                    if lease_driver.is_active:
                        additional_drivers.append(driver_dict)
                    else:
                        removed_drivers.append(driver_dict)
                else:
                    main_drivers.append(driver_dict)

        lease_details = lease.to_dict()
        lease_details["drivers"] = main_drivers
        lease_details["documents"] = lease_documents
        lease_details["additional_drivers"] = additional_drivers
        lease_details["removed_drivers"] = removed_drivers
        lease_details["history"] = history or []

        return lease_details
    except Exception as e:
        logger.error("Error retrieving lease details: %s", str(e), exc_info=True)
        raise HTTPException(
            status_code=500, detail="Error retrieving lease details"
        ) from e


@router.get("/lease/export", summary="Export Leases Data")
def export_leases(
    export_format: str = Query("excel", enum=["excel", "pdf" , "csv"], alias="format"),
    sort_by: Optional[str] = Query(None),
    sort_order: str = Query("desc"),
    lease_id: Optional[str] = Query(None),
    medallion_no: Optional[str] = Query(None),
    tlc_number: Optional[str] = Query(None),
    driver_id: Optional[str] = Query(None),
    driver_name: Optional[str] = Query(None),
    vin_no: Optional[str] = Query(None),
    lease_type: Optional[str] = Query(None),
    plate_no: Optional[str] = Query(None),
    lease_start_date: Optional[date] = Query(None),
    lease_end_date: Optional[date] = Query(None),
    status: Optional[str] = Query(None),
    exclude_additional_drivers: Optional[bool] = Query(True),
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
):
    """
    Exports filtered leases data to the specified format (Excel or PDF).
    """
    try:
        leases, _ = lease_service.get_lease(
            db=db,
            page=1,
            per_page=10000,
            lease_id=lease_id,
            medallion_number=medallion_no,
            tlc_number=tlc_number,
            lease_type=lease_type,
            driver_id=driver_id,
            driver_name=driver_name,
            vin_number=vin_no,
            plate_number=plate_no,
            lease_start_date=lease_start_date,
            lease_end_date=lease_end_date,
            exclude_additional_drivers=exclude_additional_drivers,
            status=status,
            sort_by=sort_by,
            sort_order=sort_order,
            multiple=True,
        )

        if not leases:
            raise ValueError("No leases data available for export with the given filters.")

        export_data = [format_lease_export(db, lease) for lease in leases]

        filename = f"leases_{date.today()}.{'xlsx' if export_format == 'excel' else export_format}"

        exporter = ExporterFactory.get_exporter(export_format, export_data)
        file_content = exporter.export()

        media_types = {
            "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "pdf": "application/pdf"
        }
        media_type = media_types.get(export_format, "application/octet-stream")

        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        return StreamingResponse(file_content, media_type=media_type, headers=headers)

    except ValueError as e:
        logger.warning("Business logic error during leases export: %s", e)
        raise HTTPException(status_code=400, detail=str(e)) from e

    except Exception as e:
        logger.error("Error exporting leases: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred during the export process.",
        ) from e


@router.get("/lease/{lease_id}/documents", summary="Get lease with documents")
def get_lease_with_documents(
    lease_id: str,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    """
    Get lease with documents
    """
    try:
        lease = lease_service.get_lease(db, lease_id=lease_id)
        lease_details = format_lease_response(db, lease)

        if not lease:
            raise HTTPException(
                status_code=404, detail=f"Lease with lease_id {lease_id} not found"
            )

        documents = {
            "documents": upload_service.get_documents(
                db, object_type="lease", object_id=lease.id, multiple=True
            ),
            "lease_details": lease_details,
        }
        return documents

    except Exception as e:
        logger.error("Error in get_medallions_with_documents: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/lease/config", summary="post lease configuration")
def post_lease_configuration(
    db: Session = Depends(get_db),
    config_data: Dict[str, Any] = Body(...),
    logged_in_user: User = Depends(get_current_user),
):
    """Post lease configuration"""
    try:
        for config_type, values in config_data.items():
            lease_service.upsert_lease_payment_configuration(
                db=db, lease_payment_config_data={"config_type": config_type, **values}
            )

        return {"message": "Lease configuration saved successfully"}

    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500, detail=f"Failed to save configuration: {str(e)}"
        )


@router.get("/lease/config", summary="get lease configuration")
def get_lease_configuration(
    db: Session = Depends(get_db), logged_in_user: User = Depends(get_current_user)
):
    """Get lease configuration"""
    try:
        config = lease_service.get_lease_payment_configuration(db)
        return config
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get configuration: {str(e)}"
        )


@router.get("/lease/schedule", summary="get lease schedule")
def get_lease_schedule(
    db: Session = Depends(get_db),
    lease_id: str = None,
    logged_in_user: User = Depends(get_current_user),
):
    """Get lease schedule from database"""

    try:
        lease = lease_service.get_lease(db, lease_id=lease_id)
        if not lease:
            raise HTTPException(status_code=404, detail="Lease not found")

        # Get lease schedule from database
        lease_schedules = lease_service.get_lease_schedule(
            db=db, lease_id=lease.id, multiple=True
        )

        # If no schedules found in database, fallback to calculation (for backward compatibility)
        if not lease_schedules:
            logger.warning(
                f"No lease schedule found in database for lease {lease.id}, calculating on-the-fly"
            )

            if lease.lease_type == LeaseType.SHORT_TERM.value:
                lease_config = lease_service.get_lease_configurations(
                    db=db, lease_id=lease.id, multiple=True
                )
                lease_amount = sum(config.lease_limit for config in lease_config)

                return calculate_short_term_lease_schedule(
                    lease.lease_start_date, 6, lease_amount
                )
            else:
                lease_config = lease_service.get_lease_configurations(
                    db=db,
                    lease_id=lease.id,
                    lease_breakup_type="lease_amount",
                    sort_order="desc",
                )
                lease_amount = lease_config.lease_limit if lease_config else 0

                if not lease_config:
                    return []  # This could be from migration.

                return calculate_weekly_lease_schedule(
                    lease.lease_start_date,
                    lease.duration_in_weeks,
                    lease.lease_pay_day
                    if lease.lease_pay_day
                    else settings.payment_date,
                    float(lease_amount),
                )

        # Format the response from database records
        schedule_response = []
        for schedule in lease_schedules:
            # Calculate active days from period dates
            active_days = 7  # Default
            if schedule.period_start_date and schedule.period_end_date:
                active_days = (
                    schedule.period_end_date - schedule.period_start_date
                ).days + 1

            schedule_response.append(
                {
                    "installment_no": schedule.installment_number,
                    "due_date": schedule.installment_due_date.strftime("%a, %B %d, %Y")
                    if schedule.installment_due_date
                    else "",
                    "period_start": schedule.period_start_date.strftime("%a, %B %d, %Y")
                    if schedule.period_start_date
                    else "",
                    "period_end": schedule.period_end_date.strftime("%a, %B %d, %Y")
                    if schedule.period_end_date
                    else "",
                    "active_days": active_days,
                    "is_prorated": active_days != 7,  # Prorated if not a full week
                    "amount_due": f"$ {schedule.installment_amount:,.2f}"
                    if schedule.installment_amount
                    else "$ 0.00",
                    "medallion_amount": f"$ {schedule.medallion_installment_amount:,.2f}"
                    if schedule.medallion_installment_amount
                    else "$ 0.00",
                    "vehicle_amount": f"$ {schedule.vehicle_installment_amount:,.2f}"
                    if schedule.vehicle_installment_amount
                    else "$ 0.00",
                    "status": schedule.installment_status,
                }
            )

        return schedule_response

    except Exception as e:
        logger.error(f"Failed to get schedule: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to get schedule: {str(e)}"
        ) from e


# --- LEASE PRESET CRUD ENDPOINTS ---


@router.post(
    "/presets",
    response_model=LeasePresetResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new lease price preset",
)
def create_lease_preset(
    preset_data: LeasePresetCreate,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    """Creates a new default pricing rule for a combination of lease type and vehicle."""
    try:
        return lease_service.create_lease_preset(db, preset_data)
    except Exception as e:
        logger.error(f"Error creating lease preset: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="Could not create lease preset."
        ) from e


@router.get(
    "/presets/{preset_id}",
    response_model=LeasePresetResponse,
    summary="Get a specific lease price preset",
)
def get_lease_preset(
    preset_id: int,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    """Retrieves a single lease preset by its ID."""
    preset = lease_service.get_lease_preset(db, preset_id)
    if not preset:
        raise HTTPException(status_code=404, detail="Lease preset not found.")
    return preset


@router.get("/presets", summary="List all lease price presets")
def list_lease_presets(
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    sort_by: str = Query("created_on", description="Field to sort by"),
    sort_order: str = Query("desc", enum=["asc", "desc"]),
    lease_type: Optional[str] = Query(None),
    vehicle_make: Optional[str] = Query(None),
    vehicle_model: Optional[str] = Query(None),
    vehicle_year: Optional[int] = Query(None),
    logged_in_user: User = Depends(get_current_user),
):
    """Lists all lease presets with filtering, sorting, and pagination."""
    presets, total_items = lease_service.list_lease_presets(
        db,
        page,
        per_page,
        sort_by,
        sort_order,
        lease_type,
        vehicle_make,
        vehicle_model,
        vehicle_year,
    )
    return {
        "items": [p.to_dict() for p in presets],
        "total_items": total_items,
        "page": page,
        "per_page": per_page,
        "total_pages": (total_items + per_page - 1) // per_page,
    }


@router.put(
    "/presets/{preset_id}",
    response_model=LeasePresetResponse,
    summary="Update a lease price preset",
)
def update_lease_preset(
    preset_id: int,
    preset_data: LeasePresetUpdate,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    """Updates an existing lease preset."""
    try:
        return lease_service.update_lease_preset(db, preset_id, preset_data)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Error updating lease preset {preset_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="Could not update lease preset."
        ) from e


@router.delete(
    "/presets/{preset_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a lease price preset",
)
def delete_lease_preset(
    preset_id: int,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    """Deletes a lease preset."""
    try:
        lease_service.delete_lease_preset(db, preset_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Error deleting lease preset {preset_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="Could not delete lease preset."
        ) from e


# --- LEASE RENEWAL ENDPOINTS ---


@router.post("/lease/auto-renew", summary="Process lease auto-renewals")
def process_lease_auto_renewals(
    db: Session = Depends(get_db),
    renewal_date: Optional[str] = Query(
        None,
        description="Date to process renewals for (YYYY-MM-DD). Defaults to today.",
    ),
    logged_in_user: User = Depends(get_current_user),
):
    """
    Process auto-renewals for leases expiring on the given date.

    This endpoint:
    1. Finds all active leases with is_auto_renewed = True that are expiring
    2. Renews them based on their lease type:
       - DOV: Auto-renews for configured period for up to eight segments (four years).
              After the final segment, auto_renew = False
       - Long Term, Medallion, Shift: Renew for configured period until terminated
    3. For DOV, increments the segment and updates term dates
    4. For all types, the term start date is the renewal date and end date is calculated
       from the configured renewal period per lease type
    5. Sends admin notification email with summary of all renewals
    """
    try:
        from datetime import datetime

        from app.leases.lease_renewal_service import process_auto_renewals

        # Parse renewal date or use today
        if renewal_date:
            try:
                parsed_date = datetime.strptime(renewal_date, "%Y-%m-%d").date()
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid date format. Use YYYY-MM-DD",
                )
        else:
            parsed_date = datetime.now().date()

        # Process auto-renewals
        result = process_auto_renewals(db, parsed_date)

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing auto-renewals: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Error processing auto-renewals: {str(e)}"
        ) from e


@router.post("/lease/renewal-reminders", summary="Send lease renewal reminders")
def send_lease_renewal_reminders(
    db: Session = Depends(get_db),
    check_date: Optional[str] = Query(
        None, description="Date to check from (YYYY-MM-DD). Defaults to today."
    ),
    reminder_days: int = Query(
        30, description="Number of days before expiry to send reminders"
    ),
    logged_in_user: User = Depends(get_current_user),
):
    """
    Send renewal reminders to drivers for leases expiring in the given number of days.

    This endpoint:
    1. Finds all active leases expiring within the configured reminder period
    2. Fetches email and SMS templates from S3
    3. Sends email (via SES) and SMS (via SNS) reminders to all drivers on the lease
    4. Returns a summary of reminders sent

    Parameters:
        - check_date: Date to check from (default: current date, format: YYYY-MM-DD)
        - reminder_days: Number of days before expiry to send reminders (default: 30)
    """
    try:
        from datetime import datetime

        from app.leases.lease_renewal_service import process_renewal_reminders

        # Parse check date or use today
        if check_date:
            try:
                parsed_date = datetime.strptime(check_date, "%Y-%m-%d").date()
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid date format. Use YYYY-MM-DD",
                )
        else:
            parsed_date = datetime.now().date()

        # Process renewal reminders
        result = process_renewal_reminders(db, parsed_date, reminder_days)

        return result

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Error sending renewal reminders: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Error sending renewal reminders: {str(e)}"
        ) from e


@router.post("/lease/process-expiries", summary="Process lease expiries")
def process_lease_expiries_endpoint(
    db: Session = Depends(get_db),
    check_date: Optional[str] = Query(
        None,
        description="Date to check expiries for (YYYY-MM-DD). Defaults to today.",
    ),
    logged_in_user: User = Depends(get_current_user),
):
    """
    Process lease expiries for leases past their end date with auto-renewal disabled.

    This endpoint:
    1. Finds all active leases with is_auto_renewed = False that have passed their lease_end_date
    2. Marks them as EXPIRED
    3. Sends admin notification email with summary of all expired leases

    This is separate from auto-renewal:
    - Auto-renewal handles active renewal of leases
    - This endpoint handles marking leases as expired when they won't renew
    """
    try:
        from datetime import datetime

        from app.leases.lease_expiry_service import process_lease_expiries

        # Parse check date or use today
        if check_date:
            try:
                parsed_date = datetime.strptime(check_date, "%Y-%m-%d").date()
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid date format. Use YYYY-MM-DD",
                )
        else:
            parsed_date = datetime.now().date()

        # Process lease expiries
        result = process_lease_expiries(db, parsed_date)

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing lease expiries: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Error processing lease expiries: {str(e)}"
        ) from e
