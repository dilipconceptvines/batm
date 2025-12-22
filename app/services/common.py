### app/services/common.py

# Standard library imports
from datetime import datetime
from typing import Optional, Tuple
from uuid import UUID

# Third party imports
from sqlalchemy.orm import Session

# Local imports
from app.utils.logger import get_logger
from app.curb.models import CURBTrip

logger = get_logger(__name__)


class CommonService:
    """Common service for the application operations"""
    def resolve_driver_from_curb(self, db: Session, plate_number: str, timestamp: datetime) -> Optional[Tuple[str, str]]:
        """
        Try to find the driver from a CURB trip using plate number and timestamp.
        Returns: (driver_id, trip_id)
        """
        try:
            trip = (
                db.query(CURBTrip).filter(
                    CURBTrip.cab_number == plate_number,
                    CURBTrip.start_date <= timestamp,
                    CURBTrip.end_date >= timestamp
                ).order_by(CURBTrip.start_date.desc()).first()
            )

            if trip:
                return trip.driver_id, trip.id
            return None, None
        except Exception as e:
            logger.error("Error resolving driver from CURB trip: %s", str(e), exc_info=True)
            raise e


common_service = CommonService()
