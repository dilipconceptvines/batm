# Standard library imports
from datetime import datetime

# Third party imports
import pandas as pd
from sqlalchemy.orm import Session

# Local imports
from app.core.db import SessionLocal
from app.core.config import settings
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.vehicles.models import Vehicle, VehicleHackUp , HackUpTasks
from app.medallions.models import Medallion
from app.medallions.schemas import MedallionStatus
from app.vehicles.schemas import VehicleStatus , ProcessStatusEnum
from app.utils.general import get_safe_value

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def parse_vehicle_hackup_information(db: Session, df: pd.DataFrame):
    """
    Parses the vehicle hackup information from the excel file and upserts the data into the database.
    """
    try:
        for _, row in df.iterrows():
            vehicle_vin = get_safe_value(row , "vin")
            status = get_safe_value(row , "status")


            # Convert dates to datetime objects
            def convert_date(date_str):
                if pd.notnull(date_str):
                    try:
                        if hasattr(date_str, 'to_pydatetime'):
                            return date_str.to_pydatetime()
                        elif isinstance(date_str, datetime):
                            return date_str
                        else:
                            logger.warning("Unexpected date format: %s. Skipping.", date_str)
                            return None
                    except ValueError:
                        logger.warning("Invalid date format: %s. Skipping.", date_str)
                        return None
                return None


            # Get Vehicle ID from VIN
            vehicle = db.query(Vehicle).filter_by(vin=vehicle_vin).first()
            medallion = db.query(Medallion).filter_by(id=vehicle.medallion_id).first() if vehicle else None


            if not vehicle:
                logger.warning("No vehicle found for VIN: %s. Skipping.", vehicle_vin)
                continue

            vehicle_id = vehicle.id

            # Check if vehicle hackup already exists
            vehicle_hackup = db.query(VehicleHackUp).filter_by(
                vehicle_id=vehicle_id).first()

            if vehicle_hackup is not None:
                # Update existing hackup details
                logger.info("Updating existing vehicle installation for VIN: %s", vehicle_vin)
                vehicle.vehicle_status = VehicleStatus.HACKED_UP
                medallion.medallion_status = MedallionStatus.ACTIVE
            else:
                # Insert new vehicle hackup
                logger.info("Inserting new vehicle hackup for VIN: %s", vehicle_vin)
                vehicle_hackup = VehicleHackUp(
                    vehicle_id=vehicle_id,
                    status=status
                )
                vehicle.vehicle_status = VehicleStatus.HACKED_UP
                medallion.medallion_status = MedallionStatus.ACTIVE

                db.add(vehicle_hackup)
                db.add(vehicle)
                db.add(medallion)

            db.flush()


        db.commit()
    except Exception as e:
        db.rollback()
        logger.error("Error parsing vehicle hackup data: %s", e)
        raise


if __name__ == "__main__":
    logger.info("Loading Vehicle Hackup Information")
    session = SessionLocal()
    xls = pd.ExcelFile(s3_utils.download_file(settings.bat_file_key))
    installation_df = pd.read_excel(xls, 'vehicle_hackups')
    parse_vehicle_hackup_information(session, installation_df)
    logger.info("Vehicle Hackup Information Seeded Successfully ✅")
