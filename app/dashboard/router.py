## app/dashboard/router.py

# Third party imports
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

# Local imports 
from app.core.db import get_db
from app.utils.logger import get_logger
from app.vehicles.services import vehicle_service
from app.medallions.services import medallion_service
from app.drivers.services import driver_service
from app.users.models import User
from app.users.utils import get_current_user

logger = get_logger(__name__)
router = APIRouter(tags=["Dashboard"])

@router.get("/dashboard", summary="List all the dashboard elements", tags=["Dashboard"])
def bat_dashboard(db: Session = Depends(get_db), logged_in_user: User = Depends(get_current_user)):
    """
    List all the dashboard elements
    """
    medallion_details = medallion_service.get_medallions_by_status(db)
    vehicle_details = vehicle_service.get_vehicles_by_status(db)
    driver_details = driver_service.get_drivers_by_status(db)
    return JSONResponse(
        {
            "medallion_details": medallion_details,
            "vehicle_details": vehicle_details,
            "driver_details": driver_details
        }
    )