### app/vehicles/router.py

# Standard library imports
import csv
import math
from datetime import date, datetime
from io import BytesIO, StringIO
from typing import Optional

# Third party imports
import requests
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy.orm import Session

from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service

# Local imports
from app.core.config import settings
from app.core.db import get_db
from app.drivers.services import driver_service
from app.leases.schemas import LeaseStatus
from app.leases.services import lease_service
from app.medallions.schemas import MedallionStatus
from app.medallions.services import medallion_service
# from app.notifications.fcm.fcm import send_fcm_notification_to_topic
from app.uploads.services import upload_service
from app.users.models import User
from app.users.utils import get_current_user
from app.core.dependencies import get_db_with_current_user
from app.utils.exporter_utils import ExporterFactory
from app.utils.logger import get_logger
from app.vehicles.schemas import (
    DeliveryData,
    HackUpData,
    HackupProcessStatus,
    HackupStatus,
    NewDealer,
    VehicleStatus,
    UpdateVehicleLocation
)
from app.vehicles.search_service import (
    build_inspection_query,
    build_plate_number_query,
    calculate_macrs_schedule,
    format_plate_result,
    get_filtered_values,
    get_inspection_paginated_results,
    get_inspection_total_items,
    get_paginated_results,
    get_plate_paginated_results,
    get_plate_total_items,
    get_total_items,
    get_vehicle_deprecation,
    get_vehicles_list,
    total_depreciation_till_now,
    get_vehicle_expenses
)
from app.vehicles.services import vehicle_service
from app.vehicles.utils import (
    format_vehicle_entity,
    format_vehicle_response,
    formate_vehicle_hackup,
    get_vehicles_from_owner,
)

logger = get_logger(__name__)
router = APIRouter(tags=["Vehicles"])

@router.get("/vin/{vin}", summary="Get vehicle by VIN")
def get_vehicle_by_vin(vin: str, db: Session = Depends(get_db)):
    """Get a vehicle by its VIN."""
    try:
        base = (
            f"https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVinValues/{vin}?format=json"
        )

        response = requests.get(base, timeout=20)
        if response.status_code != 200:
            raise HTTPException(status_code=404, detail="Vehicle not found")

        return response.json().get("Results", [])[0]
    except Exception as e:
        logger.error("Error getting vehicle by VIN: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/vehicle_owners", summary="list vehicle owners")
def list_vehicle_owners(
    db: Session = Depends(get_db),
    entity_name: Optional[str] = Query(None),
    owner_id: Optional[str] = Query(None),
    entity_status: Optional[str] = Query(None),
    ein: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    sort_by: str = Query(None),
    sort_order: str = Query(None),
    logged_in_user: User = Depends(get_current_user),
):
    """
    Lists all vehicle owners.
    """
    try:
        owners, total_count = vehicle_service.get_vehicle_entity(
            db=db,
            entity_name=entity_name,
            ein=ein,
            entity_status=entity_status,
            owner_id=owner_id,
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order,
            multiple=True,
        )
        results = [
            {
                "id": owner.id,
                "entity_name": owner.entity_name,
                "ein": owner.ein if owner.ein else "",
                "entity_status": owner.entity_status,
                "owner_id": owner.owner_id,
                "vehicles": [
                    {"id": vehicle.id, "vin": vehicle.vin}
                    for vehicle in owner.vehicles
                    if vehicle.vehicle_status != VehicleStatus.IN_PROGRESS
                ],
                "created_on": owner.created_on,
                "updated_on": owner.updated_on,
            }
            for owner in owners
        ]

        return {
            "items": results,
            "total_count": total_count,
            "page": page,
            "per_page": per_page,
            "total_pages": math.ceil(total_count / per_page),
        }
    except Exception as e:
        logger.error("Error listing vehicle owners: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.put("/change/delivery_status", summary="change deliver status of vehicle")
def change_delivery_status(
    db: Session = Depends(get_db),
    vin: str = None,
    delivery_data: DeliveryData = None,
    logged_in_user: User = Depends(get_current_user),
):
    """Change the delivery status of a vehicle."""
    try:
        vehicle = vehicle_service.get_vehicles(db=db, vin=vin)
        if not vehicle:
            raise HTTPException(status_code=404, detail="Vehicle not found")

        if vehicle.vehicle_status != VehicleStatus.NOT_DELIVERED:
            raise HTTPException(
                status_code=400, detail="Vehicle is not in 'Not Delivered' status"
            )

        vehicle = vehicle_service.upsert_vehicle(
            db=db, vehicle_data={"id": vehicle.id, **delivery_data.dict()}
        )

        return {
            "detail": f"Delivery status updated successfully for vin : {vehicle.vin}"
        }
    except Exception as e:
        logger.error("Error changing delivery status: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/export/vehicle_owner", summary="Export Vehicle Owner Data")
def export_vehicle_owner(
    export_format: str = Query("excel", enum=["excel", "pdf"], alias="format"),
    sort_by: Optional[str] = Query("created_on"),
    sort_order: str = Query("desc"),
    entity_name: Optional[str] = Query(None),
    owner_id: Optional[int] = Query(None),
    entity_status: Optional[str] = Query(None),
    ein: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
):
    """
    Exports filtered vehicle owner data to the specified format (Excel or PDF).
    """
    try:
        owners, _ = vehicle_service.get_vehicle_entity(
            db=db,
            entity_name=entity_name,
            ein=ein,
            entity_status=entity_status,
            owner_id=owner_id,
            page=1,
            per_page=10000,
            sort_by=sort_by,
            sort_order=sort_order,
            multiple=True,
        )

        if not owners:
            raise ValueError("No vehicle owner data available for export with the given filters.")

        export_data = [
            {
                "Entity Name": owner.entity_name if owner.entity_name else "",
                "EIN": owner.ein if owner.ein else "",
                "Entity Status": owner.entity_status if owner.entity_status else "",
                "Owner ID": owner.owner_id if owner.owner_id else "",
                "Vehicles": ", ".join([f"({vehicle.vin})" for vehicle in owner.vehicles]),
                "Created On": owner.created_on or "",
                "Updated On": owner.updated_on or "",
            }
            for owner in owners
        ]

        filename = f"vehicle_owners_{date.today()}.{'xlsx' if export_format == 'excel' else export_format}"

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
        logger.warning("Business logic error during vehicle owner export: %s", e)
        raise HTTPException(status_code=400, detail=str(e)) from e

    except Exception as e:
        logger.error("Error exporting vehicle owner data: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred during the export process.",
        ) from e


@router.put("/change_ownership/{vin}", summary="change ownership of vehicle")
def change_ownership(
    vin: str,
    owner_id: int,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    """
    Changes the ownership of a vehicle.
    """
    try:
        vehicle = None
        owner = None
        if vin and owner_id:
            vehicle = vehicle_service.get_vehicles(db=db, vin=vin)
            if not vehicle:
                raise HTTPException("Vehicle Not Found")

            owner = vehicle_service.get_vehicle_entity(db=db, entity_id=owner_id)
            if not owner:
                raise HTTPException("owner Not Found")

            vehicle = vehicle_service.upsert_vehicle(
                db=db, vehicle_data={"id": vehicle.id, "entity_id": owner.id}
            )
        else:
            raise HTTPException("vin And owner_id are required")
        return f"vehicle {vehicle.vin} is now owned by {owner.entity_name}"
    except Exception as e:
        logger.error("Error changing ownership of vehicle: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.put("/remove_owner_ship/{vin}", summary="remove ownership of vehicle")
def remove_ownership(
    vin: str,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    """Remove ownership of a vehicle."""
    try:
        if not vin:
            raise HTTPException("vin is required")
        vehicle = vehicle_service.get_vehicles(db=db, vin=vin)

        owner = vehicle_service.get_vehicle_entity(db=db, entity_id=vehicle.entity_id)
        if not owner:
            raise HTTPException("This Vehicle Don't Have owner")

        if not vehicle:
            raise HTTPException("Vehicle Not Found")

        if vehicle.vehicle_status != VehicleStatus.AVAILABLE:
            raise HTTPException(f"Vehicle is not {VehicleStatus.AVAILABLE.value}")

        if vehicle.is_medallion_assigned:
            medallion = medallion_service.get_medallion(
                db=db, medallion_id=vehicle.medallion_id
            )
            if medallion:
                medallion_service.upsert_medallion(
                    db=db,
                    medallion_data={
                        "id": medallion.id,
                        "medallion_status": MedallionStatus.AVAILABLE,
                    },
                )

        vehicle = vehicle_service.upsert_vehicle(
            db=db,
            vehicle_data={
                "id": vehicle.id,
                "entity_id": None,
                "vehicle_status": VehicleStatus.ARCHIVED,
                "is_medallion_assigned": False,
                "medallion_id": None,
            },
        )

        if not vehicle:
            raise HTTPException("Not able to Remove Owner")

        return f"{vin} is removed from ownership of {owner.entity_name}"
    except Exception as e:
        logger.error("Error removing ownership of vehicle: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/view/vehicle_owners/{id}", summary="view vehicle owners details")
def view_vehicle_owners(
    id: int,
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    logged_in_user: User = Depends(get_current_user),
):
    """
    view vehicle owners details
    """
    try:
        if not id:
            raise HTTPException("id is required")

        vehicle_owner = vehicle_service.get_vehicle_entity(db=db, entity_id=id)

        owner_datails = format_vehicle_entity(vehicle_owner)
        owner_datails["ein"] = (
            f"XX-XXX{owner_datails.get('ein')[-4:]}" if owner_datails.get("ein") else ""
        )
        vehicles = get_vehicles_from_owner(
            owner=vehicle_owner, page=page, per_page=per_page
        )
        history = audit_trail_service.get_related_audit_trail(
            db=db, vehicle_owner_id=vehicle_owner.id
        )
        documents = upload_service.get_documents(
            db=db,
            object_type="vehicle_owner",
            object_id=vehicle_owner.id,
            multiple=True,
        )

        owner_datails["vehicles"] = vehicles
        owner_datails["owner_history"] = history
        owner_datails["documents"] = documents

        return owner_datails
    except Exception as e:
        logger.error("Error viewing vehicle owners details: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/view/vehicle/{vin}", summary="view vehicle details")
def view_vehicle(
    vin: str,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    """
    view vehicle details
    """
    try:
        if not vin:
            raise HTTPException("vin is required")

        vehicle = vehicle_service.get_vehicles(db=db, vin=vin)
        if not vehicle:
            raise HTTPException("Vehicle Not Found")

        vehicle_details = format_vehicle_response(vehicle)
        medallion = vehicle.medallions if vehicle.medallions else {}
        lease = vehicle.lease[0] if vehicle.lease else {}
        lease_driver = (
            lease_service.get_lease_drivers(db=db, lease_id=lease.id, multiple=True)
            if lease
            else {}
        )
        driver = (
            [
                driver_service.get_drivers(db=db, driver_id=driver.driver_id).to_dict()
                for driver in lease_driver
            ]
            if lease_driver
            else {}
        )

        documents = upload_service.get_documents(
            db=db, object_type="vehicle", object_id=vehicle.id, multiple=True
        )
        history = audit_trail_service.get_related_audit_trail(
            db=db, vehicle_id=vehicle.id
        )

        vehicle_details["medallions"] = medallion
        vehicle_details["lease"] = lease
        vehicle_details["drivers"] = driver
        vehicle_details["documents"] = documents or {}
        vehicle_details["vehicle_history"] = history or {}

        return vehicle_details
    except Exception as e:
        logger.error("Error viewing vehicle details: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/new_dealer", summary="create new dealer")
def create_new_dealer(
    dealer_data: NewDealer,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    try:
        dealer = vehicle_service.get_dealer(db=db, dealer_name=dealer_data.dealer_name)

        if dealer:
            dealer = vehicle_service.upsert_dealer(
                db=db, dealer_data={"id": dealer.id, **dealer_data.dict()}
            )
        else:
            dealer = vehicle_service.upsert_dealer(
                db=db, dealer_data=dealer_data.dict()
            )

        if not dealer:
            raise HTTPException("Error Creating Dealer")
        return dealer
    except Exception as e:
        logger.error("Error Creating Dealer: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get(
    "/vehicle/{vehicle_vin_number}/documents",
    summary="List all documents associated with the vehicle vin number",
)
def get_vehicle_documents(
    vehicle_vin_number: str,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    """
    Fetches documents associated with the specified medallion number.
    """
    try:
        # Find the vehicle by vehicle vin
        vehicle = vehicle_service.get_vehicles(db, vin=vehicle_vin_number)

        if not vehicle:
            raise HTTPException(
                status_code=404,
                detail=f"Vehicle with vin number {vehicle_vin_number} not found",
            )

        return {
            "vehicle_details": vehicle.to_dict(),
            "documents": upload_service.get_documents(
                db, object_type="vehicle", object_id=vehicle.id, multiple=True
            ),
        }
    except Exception as e:
        logger.error("Error fetching vehicle documents: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/vehicle/dehack", summary="List Vehicles with Details")
def de_hack_vehicle(
    db: Session = Depends(get_db),
    vin: str = Query(None, description="Vehicle Identification Number"),
    logged_in_user: User = Depends(get_current_user),
):
    """
    De-hacks a vehicle by updating the vehicle status to available and the hackup status to inactive.
    """
    try:
        vehicle = vehicle_service.get_vehicles(db, vin=vin)
        if not vehicle:
            raise HTTPException(
                status_code=404, detail=f"Vehicle with vin number {vin} not found"
            )
        medallion = None
        if vehicle.is_medallion_assigned:
            medallion = medallion_service.get_medallion(
                db, medallion_id=int(vehicle.medallion_id)
            )
            if not medallion or medallion.medallion_status != MedallionStatus.ACTIVE:
                raise HTTPException(
                    status_code=404,
                    detail=f"Medallion with number {medallion.medallion_number} not found",
                )

        if vehicle.vehicle_status != VehicleStatus.HACKED_UP:
            raise HTTPException(
                status_code=400,
                detail=f"Vehicle with vin number {vin} is not {VehicleStatus.HACKED_UP.value}",
            )

        # Update medallion status to available
        medallion_service.upsert_medallion(
            db, {"id": medallion.id, "medallion_status": MedallionStatus.AVAILABLE}
        )

        # Update vehicle status to available
        vehicle_service.upsert_vehicle(
            db,
            {
                "id": vehicle.id,
                "is_medallion_assigned": False,
                "medallion_id": None,
                "vehicle_status": VehicleStatus.AVAILABLE,
            },
        )

        # Update hackup status to inactive
        hackup = vehicle_service.get_vehicle_hackup(db, vehicle_id=int(vehicle.id))
        if not hackup:
            raise HTTPException(
                status_code=404, detail=f"Hackup with vehicle id {vehicle.id} not found"
            )

        vehicle_service.upsert_vehicle_hackup(
            db, {"id": hackup.id, "is_active": False, "status": HackupStatus.INACTIVE}
        )

        return {"message": f"Vehicle with {vin} is de hacked"}
    except Exception as e:
        logger.error("Error de-hacking vehicle: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/vehicle/hackup/{id}", summary="details of vehicle hackup")
def vehicle_hackup_details(
    id: int,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    try:
        hack_up = vehicle_service.get_vehicle_hackup(db=db, vehicle_id=id)
        vehicle = vehicle_service.get_vehicles(db=db, vehicle_id=id)
        medallion = medallion_service.get_medallion(
            db=db, medallion_id=vehicle.medallion_id
        )

        return formate_vehicle_hackup(hack_up, medallion, vehicle)
    except Exception as e:
        logger.error("Error fetching vehicle documents: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/vehicle/hackup/{id}", summary="hackup the vehicle")
def hackup_vehicle(
    id: int,
    data: HackUpData,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    try:
        vehicle = vehicle_service.get_vehicle_hackup(db=db, vehicle_id=id)
        vehicle_data = vehicle_service.get_vehicles(db=db, vehicle_id=id)

        if vehicle:
            raise HTTPException(
                status_code=400, detail=f"Vehicle with id {id} is already hacked up"
            )

        if (
            vehicle_data.vehicle_status != VehicleStatus.AVAILABLE
            or vehicle_data.is_medallion_assigned == False
        ):
            raise HTTPException(
                status_code=400,
                detail=f"Vehicle with id {id} is not {VehicleStatus.AVAILABLE.value} for hackup",
            )

        data_hackup = {"vehicle_id": id, **data.dict()}

        vehicle_hackup = vehicle_service.upsert_vehicle_hackup(
            db, vehicle_hackup_data=data_hackup
        )
        vehicle_status = vehicle_service.upsert_vehicle(
            db=db,
            vehicle_data={
                "id": id,
                "vehicle_status": VehicleStatus.HACK_UP_IN_PROGRESS,
            },
        )

        return {
            "message": f"Vehicle with id {id} is {VehicleStatus.HACK_UP_IN_PROGRESS}"
        }
    except Exception as e:
        logger.error("Error fetching vehicle documents: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.put("/vehicle/hackup_update/{id}", summary="update the vehicle hackup")
def update_vehicle_hackup(
    id: int,
    data: HackUpData,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    try:
        vehicle = vehicle_service.get_vehicle_hackup(db=db, vehicle_id=id)

        if not vehicle:
            raise HTTPException(
                status_code=400, detail=f"Vehicle with id {id} is not in hackup Process"
            )
        data_dict = data.dict(exclude_unset=True)

        vehicle_hackup = vehicle_service.upsert_vehicle_hackup(
            db, vehicle_hackup_data={"id": vehicle.id, **data_dict}
        )

        return {"message": f"Vehicle with id {id} is updated"}
    except Exception as e:
        logger.error("Error fetching vehicle documents: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/vehicle/terminate", summary="terminate the vehicle")
def terminate_lease(
    db: Session = Depends(get_db),
    vin: str = Query(None, description="vehicle Identification Number"),
    logged_in_user: User = Depends(get_current_user),
):
    """
    Terminates a vehicle lease by updating the vehicle status to terminated.
    """
    try:
        vehicle = vehicle_service.get_vehicles(db, vin=vin)

        if not vehicle:
            raise HTTPException(
                status_code=404, detail=f"Vehicle with vin number {vin} not found"
            )

        medallion = None
        if vehicle.is_medallion_assigned:
            medallion = medallion_service.get_medallion(
                db, medallion_id=int(vehicle.medallion_id)
            )
            if (
                not medallion
                or medallion.medallion_status != MedallionStatus.ASSIGNED_TO_VEHICLE
            ):
                raise HTTPException(
                    status_code=404,
                    detail=f"Medallion with id {vehicle.medallion_id} not found or not active",
                )

        hackup = vehicle_service.get_vehicle_hackup(
            db, vehicle_id=int(vehicle.id), hackup_status=HackupStatus.ACTIVE
        )
        if hackup:
            raise HTTPException(
                status_code=400, detail=f"Vehicle with vin number {vin} is hacked up"
            )

        active_lease = lease_service.get_lease(
            db, vehicle_id=int(vehicle.id), status=LeaseStatus.ACTIVE
        )
        if active_lease:
            raise HTTPException(
                status_code=400,
                detail=f"Vehicle with vin number {vin} has an active lease",
            )

        if (
            medallion
            and medallion.medallion_status == MedallionStatus.ASSIGNED_TO_VEHICLE
        ):
            medallion_service.upsert_medallion(
                db, {"id": medallion.id, "medallion_status": MedallionStatus.AVAILABLE}
            )

        vehicle_service.upsert_vehicle(
            db,
            {
                "id": vehicle.id,
                "vehicle_status": VehicleStatus.ARCHIVED,
                "is_medallion_assigned": False,
                "medallion_id": None,
            },
        )

        # lease_service.upsert_lease(db, {
        #     "id": active_lease.id,
        #     "lease_status": LeaseStatus.TERMINATED
        # })

        return {"message": f"Vehicle with {vin} is terminated"}
    except Exception as e:
        logger.error("Error terminating vehicle: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get(
    "/vehicle/{vin_id}/ext",
    summary="List vehicle information associated with vin number",
)
def vehicle_vin_details(vin_id: str, logged_in_user: User = Depends(get_current_user)):
    """
    Fetches vehicle information associated with the specified VIN number.
    """
    try:
        url = f"https://api.vehicledatabases.com/vin-decode/{vin_id}"
        payload = {}
        headers = {"x-AuthKey": settings.vin_x_auth_key}

        try:
            response = requests.request(
                "GET",
                url,
                headers=headers,
                data=payload,
                timeout=int(settings.docusign_envelope_timeout),
            )
        except requests.exceptions.Timeout:
            logger.error("The request for fetching vin number has timed out")

        if response.status_code == 200:
            vehicle_info = response.json()
            return JSONResponse(
                {
                    "make": vehicle_info["data"]["basic"]["make"],
                    "model": vehicle_info["data"]["basic"]["model"],
                    "year": vehicle_info["data"]["basic"]["year"],
                }
            )
        elif response.status_code == 400:
            raise HTTPException(
                status_code=400, detail=f"VIN is not available in the database"
            )
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Error while fetching vehicle from the database",
            )
    except Exception as e:
        logger.error("Error fetching vehicle information: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/vehicles", summary="List Vehicles with Details")
def list_vehicles(
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    vin: str = None,
    make: str = None,
    model: str = None,
    medallion_number: str = None,
    plate_number: str = None,
    from_make_year: int = None,
    to_make_year: int = None,
    vehicle_type: str = None,
    current_location: str = None,
    color: str = None,
    entity_name: str = None,
    medallion_owner :str = None,
    vehicle_status: str = None,
    registration_expiry_from: date = None,
    registration_expiry_to: date = None,
    has_documents: bool = None,
    has_medallion: bool = None,
    has_driver: bool = None,
    include_archived: bool = None,
    sort_by: str = None,
    sort_order: str = None
):
    """
    List vehicles based on the provided filters
    """
    try:
        return get_vehicles_list(
            db,
            page,
            per_page,
            sort_by,
            sort_order,
            vin,
            make,
            model,
            medallion_number,
            current_location,
            plate_number,
            vehicle_type,
            entity_name,
            medallion_owner,
            from_make_year,
            to_make_year,
            vehicle_status,
            color,
            registration_expiry_from,
            registration_expiry_to,
            has_documents,
            has_medallion,
            has_driver,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error listing vehicles")
        raise HTTPException(
            status_code=500, detail="Error retrieving vehicle list"
        ) from e


@router.get("/vehicles/deprecation", summary="List Vehicles deprecation with Details")
def list_vehicles_deprecation(
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    vin: str = None,
    make: str = None,
    model: str = None,
    from_make_year: int = None,
    to_make_year: int = None,
    vehicle_type: str = None,
    entity_name: str = None,
    sort_by: str = None,
    sort_order: str = "asc",
    logged_in_user: User = Depends(get_current_user),
):
    """
    List vehicles based on the provided filters
    """
    try:
        filters = {
            "vin": vin,
            "make": make,
            "model": model,
            "from_make_year": from_make_year,
            "to_make_year": to_make_year,
            "vehicle_type": vehicle_type,
            "entity_name": entity_name,
        }

        query = get_vehicle_deprecation(db, filters, sort_by, sort_order)
        total_items = get_plate_total_items(db, query)
        vehicles = get_plate_paginated_results(query, page, per_page)
        filtered_status, filtered_make, filtered_model, filtered_vehicle_type = (
            get_filtered_values(db)
        )

        results = []

        for vehicle in vehicles:
            schedule = calculate_macrs_schedule(
                cost=vehicle.base_price, purchase_date=vehicle.expected_delivery_date
            )
            total_depreciation = total_depreciation_till_now(schedule)

            results.append(
                {
                    "vehicle_id": vehicle.id,
                    "vin": vehicle.vin,
                    "make": vehicle.make,
                    "model": vehicle.model,
                    "make_year": vehicle.year,
                    "price": vehicle.base_price,
                    "purchase_date": vehicle.expected_delivery_date,
                    "depreciation_schedule": schedule,
                    "total_depreciated_till_now": total_depreciation,
                }
            )

        return {
            "page": page,
            "per_page": per_page,
            "total_items": total_items,
            "items": results,
            "filtered_status": filtered_status,
            "filtered_make": filtered_make,
            "filtered_model": filtered_model,
            "filtered_vehicle_type": filtered_vehicle_type,
        }
    except Exception as e:
        logger.exception("Error listing vehicles")
        raise HTTPException(
            status_code=500, detail="Error retrieving vehicle list"
        ) from e


@router.get("/vehicles/export", summary="Export Vehicles Search Results")
def export_vehicles(
    export_format: str = Query("excel", enum=["excel", "pdf" , "csv"], alias="format"),
    sort_by: Optional[str] = Query(None),
    sort_order: str = Query("asc"),
    vin: Optional[str] = Query(None),
    make: Optional[str] = Query(None),
    model: Optional[str] = Query(None),
    from_make_year: Optional[int] = Query(None, ge=1900, le=2100),
    to_make_year: Optional[int] = Query(None, ge=1900, le=2100),
    vehicle_type: Optional[str] = Query(None),
    color: Optional[str] = Query(None),
    entity_name: Optional[str] = Query(None),
    vehicle_status: Optional[str] = Query(None),
    registration_expiry_from: Optional[date] = Query(None),
    registration_expiry_to: Optional[date] = Query(None),
    has_documents: Optional[bool] = Query(None),
    has_medallion: Optional[bool] = Query(None),
    has_driver: Optional[bool] = Query(None),
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
):
    """
    Exports filtered vehicles data to the specified format (Excel or PDF).
    """
    try:
        query = list_vehicles(
            db, 1, 10000, vin, make, model, from_make_year, to_make_year,
            vehicle_type, color, entity_name, vehicle_status,
            registration_expiry_from, registration_expiry_to,
            has_documents, has_medallion, has_driver, sort_by, sort_order,
        )
        vehicles = query["items"]

        if not vehicles:
            raise ValueError("No vehicles data available for export with the given filters.")

        export_data = [
            {
                "VIN": vehicle["vin"],
                "Make": vehicle["make"],
                "Model": vehicle["model"],
                "Year": vehicle["year"],
                "Vehicle Type": vehicle["vehicle_type"],
                "Color": vehicle["color"],
                "Entity Name": vehicle["vehicle_owner"],
                "Medallion Number": vehicle["medallion_number"],
                "Plate Number": vehicle["plate_number"],
                "Base Price": vehicle["base_price"],
                "Sales Tax": vehicle["sales_tax"],
                "Vehicle Hackup Cost": vehicle["vehicle_hack_up_cost"],
                "Vehicle True Cost": vehicle["vehicle_true_cost"],
                "Vehicle Lifetime Cap": vehicle["vehicle_lifetime_cap"],
                "Recoverable Base": vehicle["recoverable_base"],
                "Deliver Date": vehicle["expected_delivery_date"],
                "Medallion Owner": vehicle["medallion_owner"],
                "Vehicle Status": vehicle["vehicle_status"],
                "Registration Expiry Date": vehicle["registration_expiry_date"],
                "Is Documents": vehicle["has_documents"],
                "Is Medallion": vehicle["has_medallion"],
                "Is Driver Associated": vehicle["is_driver_associated"],
            }
            for vehicle in vehicles
        ]

        filename = f"vehicles_{date.today()}.{'xlsx' if export_format == 'excel' else export_format}"

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
        logger.warning("Business logic error during vehicles export: %s", e)
        raise HTTPException(status_code=400, detail=str(e)) from e

    except Exception as e:
        logger.error("Error exporting vehicles: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred during the export process.",
        ) from e


@router.get(
    "/vehicle-inspections",
    summary="List Vehicle Inspection records based on search criteria",
)
def list_vehicle_inspections(
    inspection_id: Optional[int] = Query(None),
    vin_numbers: Optional[str] = Query(None),
    inspection_type: Optional[str] = Query(None),
    mile_run: Optional[int] = Query(None),
    odometer_reading: Optional[int] = Query(None),
    result: Optional[str] = Query(None),
    next_inspection_due_from: Optional[date] = Query(None),
    next_inspection_due_to: Optional[date] = Query(None),
    sort_by: Optional[str] = Query(None),
    sort_order: Optional[str] = Query("asc"),
    page: int = Query(1, gt=0),
    per_page: int = Query(10, gt=0),
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    """
    List vehicle inspections based on the provided filters
    """
    try:
        filters = {
            "inspection_id": inspection_id,
            "vin_numbers": vin_numbers,
            "inspection_type": inspection_type,
            "mile_run": mile_run,
            "odometer_reading": odometer_reading,
            "result": result,
            "next_inspection_due_from": next_inspection_due_from,
            "next_inspection_due_to": next_inspection_due_to,
        }

        query = build_inspection_query(db, filters, sort_by, sort_order)
        total_items = get_inspection_total_items(db, query)
        inspections = get_inspection_paginated_results(query, page, per_page)

        # items = [i.to_dict() for i in inspections]

        items = [
            {
                "id": inspection.id,
                "inspection_type": inspection.vehicle.vehicle_type,
                "medallion_number": inspection.vehicle.medallions.medallion_number,
                "mile_run": inspection.mile_run,
                "inspection_date": inspection.inspection_date,
                "inspection_time": inspection.inspection_time,
                "odometer_reading_date": inspection.odometer_reading_date,
                "odometer_reading": inspection.odometer_reading,
                "logged_date": inspection.logged_date,
                "logged_time": inspection.logged_time,
                "result": inspection.result,
                "inspection_fee": inspection.inspection_fee,
                "next_inspection_due_date": inspection.next_inspection_due_date,
                "documents": vehicle_service.get_documents_for_vehicle_inspection(
                    db, inspection.vehicle_id
                ),
            }
            for inspection in inspections
        ]

        return {
            "page": page,
            "per_page": per_page,
            "items": items,
            "total_count": total_items,
            "total_pages": math.ceil(total_items / per_page),
        }

    except Exception as e:
        logger.exception("Error listing inspections")
        raise HTTPException(
            status_code=500, detail="Internal error while retrieving inspections"
        ) from e


@router.get("/vehicle-inspections/export", summary="Export Vehicle Inspection Records")
def export_vehicle_inspections(
    export_format: str = Query("excel", enum=["excel", "pdf" , "csv"], alias="format"),
    sort_by: Optional[str] = Query(None),
    sort_order: str = Query("asc"),
    inspection_id: Optional[int] = Query(None),
    vin_numbers: Optional[str] = Query(None),
    inspection_type: Optional[str] = Query(None),
    mile_run: Optional[int] = Query(None),
    odometer_reading: Optional[int] = Query(None),
    result: Optional[str] = Query(None),
    next_inspection_due_from: Optional[date] = Query(None),
    next_inspection_due_to: Optional[date] = Query(None),
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
):
    """
    Exports filtered vehicle inspection records to the specified format (Excel or PDF).
    """
    try:
        filters = {
            "inspection_id": inspection_id,
            "vin_numbers": vin_numbers,
            "inspection_type": inspection_type,
            "mile_run": mile_run,
            "odometer_reading": odometer_reading,
            "result": result,
            "next_inspection_due_from": next_inspection_due_from,
            "next_inspection_due_to": next_inspection_due_to,
        }

        query = build_inspection_query(db, filters, sort_by, sort_order)
        inspections = get_inspection_paginated_results(query, 1, 10000)

        if not inspections:
            raise ValueError("No vehicle inspection data available for export with the given filters.")

        export_data = [
            {
                "ID": inspection.id,
                "Inspection Type": inspection.vehicle.vehicle_type if inspection.vehicle and inspection.vehicle.vehicle_type else "",
                "Mile Run": inspection.mile_run if inspection.mile_run else "",
                "medallion_number": inspection.vehicle.medallion.medallion_number if inspection.vehicle and inspection.vehicle.medallion else "",
                "Inspection Date": inspection.inspection_date if inspection.inspection_date else "",
                "Inspection Time": inspection.inspection_time if inspection.inspection_time else "",
                "Odometer Reading Date": inspection.odometer_reading_date if inspection.odometer_reading_date else "",
                "Odometer Reading": inspection.odometer_reading if inspection.odometer_reading else "",
                "Logged Date": inspection.logged_date if inspection.logged_date else "",
                "Logged Time": inspection.logged_time if inspection.logged_time else "",
                "Result": inspection.result if inspection.result else "",
                "Inspection Fee": inspection.inspection_fee if inspection.inspection_fee else "",
                "Next Inspection Due Date": inspection.next_inspection_due_date if inspection.next_inspection_due_date else "",
            }
            for inspection in inspections
        ]

        filename = f"vehicle_inspections_{date.today()}.{'xlsx' if export_format == 'excel' else export_format}"

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
        logger.warning("Business logic error during vehicle inspections export: %s", e)
        raise HTTPException(status_code=400, detail=str(e)) from e

    except Exception as e:
        logger.error("Error exporting vehicle inspections: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred during the export process.",
        ) from e


@router.get("/dealers", summary="List all dealers with detailed information")
def list_dealers(
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1, description="Page number, starts from 1"),
    per_page: int = Query(
        10, ge=1, le=100, description="Number of items per page, maximum 100"
    ),
    # Filters
    dealer_name: str = Query(None, description="Filter by dealer name"),
    dealer_bank_name: str = Query(None, description="Filter by dealer type"),
    dealer_bank_account_number: str = Query(
        None, description="Filter by dealer status"
    ),
    # Sorting
    sort_by: str = Query(
        None,
        description="Sort by field",
        enum=["dealer_name", "dealer_bank_name", "dealer_bank_account_number"],
    ),
    sort_order: str = Query("asc", enum=["asc", "desc"], description="Sort order"),
    logged_in_user: User = Depends(get_current_user),
):
    """
    Lists dealers with multiple filtering and sorting capabilities.
    """

    try:
        matched_filters = []

        if dealer_name:
            matched_filters.append("dealer_name")
        if dealer_bank_name:
            matched_filters.append("dealer_bank_name")
        if dealer_bank_account_number:
            matched_filters.append("dealer_bank_account_number")

        count, dealers = vehicle_service.get_dealer(
            db,
            dealer_name=dealer_name,
            dealer_bank_name=dealer_bank_name,
            dealer_bank_account_number=dealer_bank_account_number,
            sort_by=sort_by,
            sort_order=sort_order,
            page=page,
            per_page=per_page,
            multiple=True,
        )

        # **Format Response**
        result = []

        for dealer in dealers:
            result.append(
                {
                    "dealer_id": dealer.id,
                    "dealer_name": dealer.dealer_name,
                    "dealer_bank_name": dealer.dealer_bank_name,
                    "dealer_bank_account_number": dealer.dealer_bank_account_number,
                    "created_on": dealer.created_on,
                    "updated_on": dealer.updated_on,
                }
            )

        return {
            "page": page,
            "per_page": per_page,
            "total_items": count,
            "items": result,
        }
    except Exception as e:
        logger.error("Error listing dealers: %s", str(e))
        raise HTTPException(
            status_code=500, detail="Error retrieving dealer list"
        ) from e


@router.get("/plate_number", summary="List vehicle plate numbers", tags=["Vehicles"])
def list_plate_number(
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    plate_number: str = None,
    vehicle_type: str = None,
    make: str = None,
    model: str = None,
    from_make_year: int = None,
    to_make_year: int = None,
    vin: str = None,
    sort_by: str = None,
    sort_order: str = "asc",
    logged_in_user: User = Depends(get_current_user),
):
    """
    List vehicle plate numbers based on the provided filters
    """
    try:
        filters = {
            "plate_number": plate_number,
            "vehicle_type": vehicle_type,
            "make": make,
            "model": model,
            "from_make_year": from_make_year,
            "to_make_year": to_make_year,
            "vin": vin,
        }

        query = build_plate_number_query(db, filters, sort_by, sort_order)
        total_items = get_total_items(db, query)
        paginated = get_paginated_results(query, page, per_page)

        results = [format_plate_result(p) for p in paginated]

        return {
            "page": page,
            "per_page": per_page,
            "total_items": total_items,
            "items": results,
        }

    except Exception as e:
        logger.exception("Error listing plate numbers")
        raise HTTPException(
            status_code=500, detail="Error retrieving plate number list"
        ) from e


from typing import Any


def is_valid_hackup_case(hackup: Any, my_tasks_only: bool) -> bool:
    """
    Determine if a vehicle_hackup is eligible based on status and task filtering.
    """
    if not hackup:
        return False

    status_fields = [
        "paint_status",
        "meter_status",
        "rooftop_status",
        "camera_status",
        "partition_status",
    ]

    status_values = [getattr(hackup, field, None) for field in status_fields]

    if my_tasks_only:
        has_active_task = any(
            s in {"initial", "in_progress", "verify"} for s in status_values
        )
        has_open = any(s == "open" for s in status_values)
        return has_active_task and not has_open

    return True


@router.get("/hackup/process", tags=["Vehicles"])
def get_hackup_process(
    from_date: date = None,
    to_date: date = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    my_tasks_only: bool = Query(
        False, description="Only tasks in initial/in_progress/verify (no open)"
    ),
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    """
    Retrieve hackup cases.
    If `my_tasks_only=True`, show only:
    - Cases with any process in 'initial', 'in_progress', or 'verify'
    - AND none of the processes in 'open'
    """
    try:
        logged_in_user_role_ids = [role.id for role in logged_in_user.roles]

        closed_cases = bpm_service.get_cases(
            db, case_status="Closed", multiple=True, unique=True
        )
        closed_case_nos = [case.case_no for case in closed_cases]
        all_cases = bpm_service.get_cases_info(db, closed_case_nos, from_date, to_date)

        filtered_cases = [
            case
            for case in all_cases
            if (
                (case.current_user_id and case.current_user_id == logged_in_user.id)
                or (case.role_id and case.role_id in logged_in_user_role_ids)
            )
        ]

        total_count = 0
        results = []

        for case in filtered_cases:
            try:
                case_entity = bpm_service.get_case_entity(db, case_no=case.case_no)
                if not case_entity or not case_entity.identifier_value:
                    continue

                vehicle = vehicle_service.get_vehicles(
                    db, vehicle_id=int(case_entity.identifier_value)
                )
                if not vehicle:
                    continue

                hackup = vehicle_service.get_vehicle_hackup(db, vehicle_id=vehicle.id)
                if not is_valid_hackup_case(hackup, my_tasks_only):
                    continue

                case_details = bpm_service.get_case_details(db, case)

                results.append(
                    {
                        "case": case_details,
                        "vehicle_info": vehicle.to_dict(),
                        "hackup_info": hackup.to_dict(),
                    }
                )
                total_count += 1

            except Exception as e:
                logger.warning("Error processing case %s: %s", case.case_no, str(e))

        paginated_results = results[(page - 1) * per_page : page * per_page]

        return {
            "total_cases": total_count,
            "page": page,
            "per_page": per_page,
            "total_pages": (total_count // per_page)
            + (1 if total_count % per_page > 0 else 0),
            "cases": paginated_results,
        }

    except Exception as e:
        logger.error("Unhandled error in hackup process fetch: %s", e)
        raise HTTPException(
            status_code=500, detail="Error while fetching details"
        ) from e


@router.post(
    "/vehicle/hackup_process_status/{id}",
    summary="Update vehicle hackup status and logistics",
)
def update_vehicle_hackup(
    id: int,
    data: HackupProcessStatus,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    try:
        vehicle_hackup = vehicle_service.get_vehicle_hackup(db=db, vehicle_id=id)
        if not vehicle_hackup:
            raise HTTPException(
                status_code=400, detail=f"Vehicle with id {id} is not in hackup process"
            )

        logger.info("Vehicle Hackup: %s", vehicle_hackup.to_dict())

        # Mapping dictionaries
        process_field_map = {
            "Paint": "paint_status",
            "Meter": "meter_status",
            "Rooftop": "rooftop_status",
            "Camera": "camera_status",
            "Partition": "partition_status",
        }

        location_field_map = {
            "Paint": ("paint_from_location", "paint_to_location"),
            "Meter": ("meter_from_location", "meter_to_location"),
            "Rooftop": ("rooftop_from_location", "rooftop_to_location"),
            "Camera": ("camera_from_location", "camera_to_location"),
            "Partition": ("partition_from_location", "partition_to_location"),
        }

        installed_date_field_map = {
            "Paint": "paint_completed_date",
            "Meter": "meter_installed_date",
            "Rooftop": "rooftop_installed_date",
            "Camera": "camera_installed_date",
            "Partition": "partition_installed_date",
        }

        is_installed_field_map = {
            "Paint": "is_paint_completed",
            "Meter": "is_meter_installed",
            "Rooftop": "is_rooftop_installed",
            "Camera": "is_camera_installed",
            "Partition": "is_partition_installed",
        }

        incoming_type = data.process_type.title()
        incoming_status = data.status

        update_data = {"id": vehicle_hackup.id}
        also_verified = []
        from_location_set = None
        to_location_set = None
        from_location_value = None

        # Is there a state that is already in progress - api always sends to date

        # Change this status to verify
        # Keep the from location for updation later on.

        for other_type, status_field in process_field_map.items():
            if getattr(vehicle_hackup, status_field, None) == "In_Progress":
                update_data[status_field] = "verify"
                also_verified.append(other_type)

                _, other_to_field = location_field_map[other_type]
                from_location_value = getattr(vehicle_hackup, other_to_field)
                break

        # If the incoming status is open, the location value will be picked from the in progress
        # location or update the location from the vehicle status

        if incoming_status == "open":
            from_field, to_field = location_field_map[incoming_type]

            if from_location_value:
                update_data[from_field] = from_location_value
                from_location_set = from_location_value
            else:
                vehicle = vehicle_service.get_vehicles(db=db, vehicle_id=id)
                if vehicle and vehicle.delivery_location:
                    from_location_set = vehicle.delivery_location
                    update_data[from_field] = from_location_set

            # Set to_location only if incoming location is present
            if data.location:
                update_data[to_field] = data.location.title()
                to_location_set = data.location.title()

            try:
                topic = "Runner"
                title = f"{incoming_type} task started"
                body = f"A new {incoming_type} process is now OPEN for vehicle ID {id}"
                send_fcm_notification_to_topic(topic, title, body)
            except Exception as notify_error:
                logger.warning(f"FCM notification failed: {notify_error}")

        # Always set the process status
        update_data[process_field_map[incoming_type]] = incoming_status.title()

        # If completed, set installed/completed date
        if incoming_status == "completed":
            installed_date_field = installed_date_field_map.get(incoming_type)
            is_installed_field = is_installed_field_map.get(incoming_type)

            if installed_date_field:
                update_data[installed_date_field] = datetime.utcnow()
            if is_installed_field:
                update_data[is_installed_field] = True

        vehicle_service.upsert_vehicle_hackup(db, vehicle_hackup_data=update_data)

        return {
            "message": f"{incoming_type} status updated for vehicle_id {vehicle_hackup.vehicle.vin}",
            "final_status": incoming_status,
            "also_verified": also_verified,
            "from_location_set": from_location_set,
            "to_location_set": to_location_set,
            "installed_date_set": update_data.get(
                installed_date_field_map.get(incoming_type)
            ),
        }

    except Exception as e:
        logger.error("Error updating vehicle hackup info: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error") from e


@router.post(
    "/vehicle/{vehicle_id}/finalize-hackup",
    summary="Finalize the vehicle hack-up process",
)
def finalize_vehicle_hackup(
    vehicle_id: int,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    """
    Marks the entire vehicle hack-up process as complete.

    This action transitions the vehicle's status to 'Hacked up' and the
    associated medallion's status to 'Active', making the vehicle
    available for leasing. It also closes the corresponding BPMN case.
    """
    try:
        updated_vehicle = vehicle_service.finalize_hackup(db, vehicle_id=vehicle_id)

        return {
            "status": "success",
            "message": f"Vehicle VIN {updated_vehicle.vin} has been successfully hacked up and is now active.",
            "vehicle_status": updated_vehicle.vehicle_status,
            "medallion_status": updated_vehicle.medallions.medallion_status
            if updated_vehicle.medallions
            else None,
        }

    except ValueError as e:
        logger.warning(
            f"Failed to finalize hack-up for vehicle ID {vehicle_id}: {str(e)}"
        )
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(
            f"Error finalizing hack-up for vehicle ID {vehicle_id}: {e}", exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail="An internal server error occurred while finalizing the hack-up.",
        )
    
@router.get("/vehicle_expeneses/{vehicel_id}" , summary = "List Vehicle Expenses")
def list_vehicle_expenses(
    vehicel_id: int,
    category: str = None,
    sub_type: str = None,
    invoice_number : str = None,
    specific_info : str = None,
    from_amount : str = None,
    to_amount : str = None,
    vendor_name : str = None,
    sort_by: str = None,
    sort_order: str =  None,
    db: Session = Depends(get_db_with_current_user),
    logged_in_user: User = Depends(get_current_user),
):
    """
    List vehicle expenses based on the provided filters
    """
    try:

        return get_vehicle_expenses(
            vehicle_id=vehicel_id,
            db=db,
            category=category,
            sub_type=sub_type,
            invoice_number=invoice_number,
            specific_info=specific_info,
            amount_from=from_amount,
            amount_to=to_amount,
            vendor_name=vendor_name,
            sort_by=sort_by,
            sort_order=sort_order,
        )
    except Exception as e:
        logger.error("Error listing vehicle expenses: %s", str(e))
        raise e
    
@router.delete("/vehicle_expense/{id}", summary="Delete Vehicle Expense")
def delete_vehicle_expense(
    id: int,
    db: Session = Depends(get_db_with_current_user),
    logged_in_user: User = Depends(get_current_user),
):
    """
    Delete a vehicle expense by its ID.
    """
    try:
        if not id:
            raise ValueError("Id is required")
        
        expense = vehicle_service.get_vehicle_expenses(
            db=db , lookup_id=id
        )

        if not expense:
            raise ValueError("Expense not found")
        
        if not expense.is_active:
            raise ValueError("Expense Already Deleted")
        
        vehicle_service.upsert_vehicle_expenses(
            db=db , vehicle_expenses={
                "id":expense.id,
                "is_active": False,
                "deleted_at": datetime.utcnow(),
                "deleted_by": logged_in_user.id
            }
        )

        return {
            "status": "success",
            "massage": "Expense Deleted Successfully"
            }
    except Exception as e:
        logger.error("Error deleting vehicle expense: %s", str(e))
        raise e

@router.put("/update_location" , summary = "Update Vehicle Location")
def update_vehicle_location(
    input: UpdateVehicleLocation,
    db: Session = Depends(get_db_with_current_user),
    logged_in_user: User = Depends(get_current_user),
):
    """
    Docstring for update_vehicle_location
    
    :param input: Description
    :type input: UpdateVehicleLocation
    :param db: Description
    :type db: Session
    :param logged_in_user: Description
    :type logged_in_user: User
    """

    try:
        vehicle_ids = input.vehicle_ids or []
        new_location = input.new_location
        specific_location = input.specific_location
        comment_for_location_change = input.comment_for_location_change

        if not vehicle_ids:
            raise HTTPException(status_code=400, detail="vehicle_ids is required")
        if not new_location:
            raise HTTPException(status_code=400, detail="new_location is required")

        success_updates = []
        failed_updates = []

        for vehicle_id in vehicle_ids:
            try:
                vehicle = vehicle_service.get_vehicles(db=db, vehicle_id=vehicle_id)

                if not vehicle:
                    failed_updates.append(
                        {
                            "vehicle_id": vehicle_id,
                            "vin": vehicle.vin,
                            "reason": "Vehicle not found",
                        }
                    )
                    continue
                if vehicle.vehicle_status not in [VehicleStatus.AVAILABLE_FOR_HACK_UP , VehicleStatus.HACKED_UP , VehicleStatus.OUT_OF_SERVICE]:
                    failed_updates.append(
                        {
                            "vehicle_id": vehicle.id,
                            "vin": vehicle.vin,
                            "reason": f"Update Vehicle Location is not allowed for {vehicle.vehicle_status} vehicle"
                        }
                    )
                    continue


                past_location = vehicle.current_location if vehicle else None

                updated_vehicle = vehicle_service.upsert_vehicle(
                    db=db,
                    vehicle_data={
                        "id": vehicle.id,
                        "current_location": new_location.value
                        if hasattr(new_location, "value")
                        else str(new_location),
                        "specific_location": specific_location,
                        "comment_for_location_change": comment_for_location_change,
                        "location_changed_date": date.today()
                    },
                )

                success_updates.append(
                    {
                        "vehicle_id": updated_vehicle.id,
                        "vin": updated_vehicle.vin,
                        "current_location": updated_vehicle.current_location,
                        "specific_location": updated_vehicle.specific_location,
                    }
                )

                audit_comment = f"Vehicle vin {vehicle.vin} location changed from ({past_location}) to ({new_location.value}). Comment: {comment_for_location_change}"

                audit_trail_service.create_audit_trail(
                    db=db,
                    description=audit_comment,
                    meta_data={"vehicle_id": vehicle.id}
                )

            except Exception as exc:  # noqa: BLE001
                failed_updates.append(
                    {
                        "vehicle_id": vehicle_id,
                        "reason": str(exc),
                    }
                )

        return {
            "status": "success",
            "message": "Vehicle locations updated",
            "data": {
                "success": success_updates,
                "failed": failed_updates,
            },
        }
    except Exception as e:
        logger.error("Error updating vehicle locations: %s", str(e))
        raise e