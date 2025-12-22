### app/medallions/router.py

# Standard library imports
import json
import math
from datetime import date
from io import BytesIO
from typing import Optional

# Third party imports
from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

# Local imports
from app.utils.logger import get_logger
from app.core.db import get_db
from app.users.models import User
from app.users.utils import get_current_user
from app.audit_trail.services import audit_trail_service
from app.medallions.services import medallion_service
from app.uploads.services import upload_service
from app.bpm_flows.newmed.utils import format_medallion_basic_details
from app.medallions.utils import format_medallion_owner_response , get_medallions_list_owner
from app.medallions.search_service import medallion_search_service
from app.utils.exporter_utils import ExporterFactory

logger = get_logger(__name__)
router = APIRouter(tags=["Medallions"])

@router.get("/api/owner-listing/v2")
def owner_listing_v2(
    medallion_owner_name: Optional[str] = Query(None, description="Filter by medallion owner name"),
    ein: Optional[str] = Query(None, description="Filter by EIN"),
    ssn: Optional[str] = Query(None, description="Filter by SSN"),
    holding_entity : Optional[str] = Query(None, description="Filter by Holding Entity"),
    contact_number: Optional[str] = Query(None, description="Filter by contact number"),
    email: Optional[str] = Query(None, description="Filter by email"),
    owner_type : Optional[str] = Query(None , description ="Filter For Owner Type C for Corporation and I for Individual"),
    is_holding_entity : Optional[bool] = Query(None, description="Filter by Holding Entity True/False"),
    page: int = Query(1, description="Page number for pagination", ge=1),
    per_page: int = Query(10, description="Number of items per page", ge=1, le=100),
    sort_by: Optional[str] = Query(None, description="Field to sort by"),
    sort_order: Optional[str] = Query(None, description="Sort order", enum=["asc", "desc"]),
    db: Session = Depends(get_db)
):
    """
    Get a list of medallion owners with pagination
    """
    try:
        return medallion_service.search_medallion_owners(
            db=db, medallion_owner_name=medallion_owner_name, ssn=ssn, ein=ein,
            holding_entity=holding_entity, contact_number=contact_number ,email=email,
            is_holding_entity=is_holding_entity,
            owner_type=owner_type, 
            page=page, per_page=per_page,
            sort_by=sort_by, sort_order=sort_order
        )
    except Exception as e:
        logger.error("Error in owner_listing_v2: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e
    
@router.get("/owners/export", summary="Export Medallion Owners Data")
def export_medallion_owners(
    export_format: str = Query("excel", enum=["excel", "pdf" , "csv"], alias="format"),
    sort_by: Optional[str] = Query("medallion_owner_name"),
    sort_order: str = Query("asc"),
    medallion_owner_name: Optional[str] = Query(None),
    ein: Optional[str] = Query(None),
    ssn: Optional[str] = Query(None),
    contact_number: Optional[str] = Query(None),
    email: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
):
    """
    Exports filtered medallion owners data to the specified format (Excel or PDF).
    """
    try:
        results = medallion_service.search_medallion_owners(
            db=db, medallion_owner_name=medallion_owner_name, ssn=ssn, ein=ein,
            contact_number=contact_number, email=email,
            page=1, per_page=1000, sort_by=sort_by, sort_order=sort_order
        )

        if not results.get("items"):
            raise ValueError("No medallion owners data available for export with the given filters.")

        export_data = medallion_service.flatten_medallion_owner_records(results["items"])

        filename = f"medallion_owners_{date.today()}.{'xlsx' if export_format == 'excel' else export_format}"

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
        logger.warning("Business logic error during medallion owners export: %s", e)
        raise HTTPException(status_code=400, detail=str(e)) from e

    except Exception as e:
        logger.error("Error exporting medallion owners: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred during the export process.",
        ) from e

@router.get("/medallions")
def search_medallions(
    page: int = Query(1, ge=1),
    per_page: int = Query(5, ge=1, le=100),
    medallion_list_days: Optional[int] = None,
    medallion_created_from: Optional[date] = None,
    medallion_created_to: Optional[date] = None,
    medallion_number: Optional[str] = Query(None),
    medallion_status: Optional[str] = Query(None),
    medallion_type: Optional[str] = Query(None),
    medallion_owner: Optional[str] = Query(None),
    renewal_date_from: Optional[date] = Query(None),
    renewal_date_to: Optional[date] = Query(None),
    validity_end_date_from: Optional[date] = Query(None),
    validity_end_date_to: Optional[date] = Query(None),
    lease_expiry_from: Optional[date] = Query(None),
    lease_expiry_to: Optional[date] = Query(None),
    has_vehicle: Optional[bool] = Query(None),
    in_storage: Optional[bool] = Query(None),
    sort_by: Optional[str] = Query(None),
    sort_order: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user)
):
    """
    Search for medallions with pagination and filtering options
    """
    try:
        results = medallion_search_service.search_medallions(
            db, page, per_page,medallion_list_days, medallion_created_from, medallion_created_to,
            medallion_number, medallion_status, medallion_type, medallion_owner,
            renewal_date_from, renewal_date_to, validity_end_date_from, validity_end_date_to,
            lease_expiry_from, lease_expiry_to, has_vehicle,
            in_storage, sort_by,
            sort_order
        )

        total_pages = math.ceil(
            results["total_count"] / (per_page if per_page else 1000)
        )

        return {
            "items": results["medallions"],
            "total_items": results["total_count"],
            "filters": results["filters"],
            "medallion_status_list": results["statuses"],
            "medallion_type_list": results["medallion_type_list"],
            "page": page if page else 1,
            "per_page": per_page if per_page else 1000,
            "total_pages": total_pages,
            "sort_fields": ["created_on", "medallion_number", "medallion_owner", "lease_expiry_date", "renewal_date"],
            "visibility": {
                "medallion_id": True,
                "medallion_number": True,
                "renewal_date": True,
                "contract_start_data": True,
                "contract_end_date": True,
                "hack_indicator": True,
                "medallion_owner": True,
                "medallion_status": True,
                "medallion_type": True,
                "validity_end_date": True,
                "lease_expiry_date": True,
                "in_storage": True,
                "does_medallion_have_documents": True,
                "vehicle": True,
            },
        }
    except Exception as e:
        logger.error("Error in search_medallions: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e
    
@router.get("/view/medallion_owner/{id}" , summary= "Get medallion owner details")
def view_medallion_owner(
    id: int,
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    per_page: int = Query(5, ge=1, le=100),
    logged_in_user: User = Depends(get_current_user)
):
    """
    Get medallion owner details
    """
    try:
        if not id:
            raise HTTPException(status_code=404, detail="Owner Id is required")
        
        medallion_owner = medallion_service.get_medallion_owner(db, medallion_owner_id=id)
        if not medallion_owner:
            raise HTTPException(status_code=404, detail="Medallion owner not found")

        owner_details = format_medallion_owner_response(db=db, medallion_owner=medallion_owner)
        medallions = get_medallions_list_owner(medallion_owner=medallion_owner, page=page, per_page=per_page)

        history = audit_trail_service.get_related_audit_trail(db=db , medallion_owner_id=id)

        medallion_owner_documents = upload_service.get_documents(db=db , object_type="medallion_owner", object_id=id, multiple=True) or []
        documents = None

        if medallion_owner.medallion_owner_type == "I":
            documents = upload_service.get_documents(db=db , object_type="individual_owner", object_id=medallion_owner.individual.id, multiple=True) or []
        else:
            documents = upload_service.get_documents(db=db , object_type="corporation", object_id=medallion_owner.corporation.id, multiple=True) or []

        medallion_owner_documents.extend(documents)

        owner_details["medallions"] = medallions
        owner_details["history"] = history
        owner_details["documents"] = medallion_owner_documents

        return owner_details
    except Exception as e:
        logger.error("Error in view_medallion_owner: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e
    
@router.get("/medallion/lease_expiry" , summary="Get medallions with expiry dates")
def get_medallions_with_expiry_dates(
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    per_page: int = Query(5, ge=1, le=100),
    day_in_advance: int = Query(30, ge=1),
    medallion_number: Optional[str] = Query(None),
    medallion_status: Optional[str] = Query(None),
    medallion_type: Optional[str] = Query(None),
    medallion_owner: Optional[str] = Query(None),
    lease_expiry_from: Optional[date] = Query(None),
    lease_expiry_to: Optional[date] = Query(None),
    sort_by: Optional[str] = Query("expiry_date"),
    sort_order: Optional[str] = Query("dese", enum=["asc", "desc"]),
    logged_in_user: User = Depends(get_current_user)
):
    """
    Get medallions with expiry dates
    """

    try:
        results= medallion_search_service.medallion_lease_report(db , page, per_page, day_in_advance,
                            medallion_number, medallion_status, medallion_type, medallion_owner,
                            lease_expiry_from, lease_expiry_to,sort_by, sort_order)
        
        total_pages = math.ceil(
            results["total_count"] / (per_page if per_page else 1000)
        )

        return {
            "days_in_advance": results["days_in_advance"],
            "date_before": results["check_date"],
            "items": results["medallions"],
            "total_items": results["total_count"],
            "filters": results["filters"],
            "medallion_status_list": results["statuses"],
            "medallion_type_list": results["medallion_type_list"],
            "page": page if page else 1,
            "per_page": per_page if per_page else 1000,
            "total_pages": total_pages,
            "sort_fields": ["created_on", "medallion_number", "medallion_owner", "contract_end_date"],
            "visibility": {
                "medallion_id": True,
                "medallion_number": True,
                "renewal_date": True,
                "contract_start_data": True,
                "contract_end_date": True,
                "medallion_owner": True,
                "medallion_status": True,
                "medallion_type": True
            }
        }
    except Exception as e:
        logger.error("Error in get_medallions_with_expiry_dates: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e
    
@router.get("/medallions/{medallion_number}/documents" , summary="Get medallions with documents")
def get_medallions_with_documents(
    medallion_number: str,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user)
):
    """
    Get medallions with documents
    """
    try:
        medallion = medallion_service.get_medallion(db, medallion_number=medallion_number)
        medallion_owner = medallion_service.get_medallion_owner(db, medallion_owner_id=medallion.owner_id)
        medallion_details = format_medallion_basic_details(medallion, medallion_owner)

        if not medallion:
            raise HTTPException(
                status_code=404, detail=f"Medallion with medallion_number {medallion_number} not found")
        
        documents={
        "documents":upload_service.get_documents(
            db,object_type="medallion", object_id=medallion.id,multiple=True
        ),
        "medallion_details":medallion_details
        }
        return documents

    except Exception as e:
        logger.error("Error in get_medallions_with_documents: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e
    

@router.get("/medallions/export", summary="Export Medallions Data")
def export_medallions(
    export_format: str = Query("excel", enum=["excel", "pdf" , "csv"], alias="format"),
    sort_by: Optional[str] = Query("created_on"),
    sort_order: str = Query("asc"),
    medallion_number: Optional[str] = Query(None),
    medallion_status: Optional[str] = Query(None),
    medallion_type: Optional[str] = Query(None),
    medallion_owner: Optional[str] = Query(None),
    renewal_date_from: Optional[date] = Query(None),
    renewal_date_to: Optional[date] = Query(None),
    lease_expiry_from: Optional[date] = Query(None),
    lease_expiry_to: Optional[date] = Query(None),
    has_vehicle: Optional[bool] = Query(None),
    in_storage: Optional[bool] = Query(None),
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
):
    """
    Exports filtered medallions data to the specified format (Excel or PDF).
    """
    try:
        results = medallion_search_service.search_medallions(
            db=db, page=1, per_page=1000,
            medallion_number=medallion_number, medallion_status=medallion_status,
            medallion_type=medallion_type, medallion_owner=medallion_owner,
            renewal_date_from=renewal_date_from, renewal_date_to=renewal_date_to,
            lease_expiry_from=lease_expiry_from, lease_expiry_to=lease_expiry_to,
            has_vehicle=has_vehicle, in_storage=in_storage,
            sort_by=sort_by, sort_order=sort_order
        )

        if not results.get("medallions"):
            raise ValueError("No medallions data available for export with the given filters.")

        export_data = results["medallions"]

        filename = f"medallions_{date.today()}.{'xlsx' if export_format == 'excel' else export_format}"

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
        logger.warning("Business logic error during medallions export: %s", e)
        raise HTTPException(status_code=400, detail=str(e)) from e

    except Exception as e:
        logger.error("Error exporting medallions: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred during the export process.",
        ) from e
    
@router.get("/medallions/view", summary="Get detailed view of related entities")
def detailed_object_view(
    db: Session = Depends(get_db),
    medallion_number: Optional[str] = Query(None, description="Medallion number"),
    logged_in_user: User = Depends(get_current_user)
):
    """
    Fetch comprehensive information about related medallion, driver, and vehicle entities.
    Any combination of parameters can be provided to find related entities.
    """
    try:
        return medallion_search_service.get_medallion_details(db, medallion_number)
    except Exception as e:
        logger.error("Error fetching detailed view: %s", str(e))
        raise HTTPException(
            status_code=500,
            detail="Error retrieving detailed information"
        ) from e
    
@router.put("/medallions/deactivate", summary="Deactivate all the medallion numbers passed")
def deactivate_medallions(
    medallion_numbers: list[str],
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user)
):
    """
    Fetches documents associated with the specified medallion number.
    """
    try:
        deactivated_medallions = []
        for medallion_number in medallion_numbers:
            medallion = medallion_service.get_medallion(db, medallion_number=medallion_number)
            if not medallion:
                raise HTTPException(status_code=404, detail="Medallion not found")
            
            medallion_service.upsert_medallion(db, {
                "id": medallion.id,
                "is_active": False
            })
            deactivated_medallions.append(medallion.medallion_number)

        return {
            'no_medallions_deactivated': len(deactivated_medallions),
            "deactivated_medallions": deactivated_medallions
        }
    except Exception as e:
        logger.error("Error deactivating medallions: %s", str(e))
        raise HTTPException(status_code=500, detail="Error deactivating medallions") from e
