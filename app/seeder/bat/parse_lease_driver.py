import pandas as pd
from datetime import datetime
from sqlalchemy.orm import Session

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.leases.models import Lease , LeaseDriver
from app.drivers.models import Driver
from app.drivers.schemas import DriverStatus
from app.drivers.services import driver_service
from app.utils.general import get_safe_value

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def parse_lease_driver(db:Session , df: pd.DataFrame) :
    """parse Lease Driver"""

    try:
        for _, row in df.iterrows():
            tlc_license = get_safe_value(row , 'tlc_license')
            lease_id = get_safe_value(row , 'lease_id')
            driver_role = get_safe_value(row , 'driver_role')
            is_day_night_shift = get_safe_value(row , 'is_day_night_shift')
            is_additional_driver = get_safe_value(row , 'is_additional_driver')
            date_added = get_safe_value(row , 'date_added')

            # convert data to datetime form
            def convert_date(date_str):
                if pd.notnull(date_str):
                    try:
                        if hasattr(date_str , "to_pydatetime"):
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
            
            date_added = convert_date(date_added)

            driver = driver_service.get_drivers(db=db ,tlc_license_number=tlc_license)

            lease = db.query(Lease).filter_by(lease_id=lease_id).first()

            if not lease or not driver:
                logger.warning("No lease or driver found for lease ID: %s and driver ID: %s. Skipping.", lease_id, tlc_license)
                continue


            driver.driver_status = DriverStatus.ACTIVE
            driver.is_active = True
            db.add(driver)

            lease_driver = db.query(LeaseDriver).filter_by(driver_id=driver.driver_id, lease_id=lease_id).first()

            if lease_driver is not None :
                # update the exiting Lease Driver
                lease_driver.driver_role = driver_role
                lease_driver.is_day_night_shift = is_day_night_shift
                lease_driver.is_additional_driver = is_additional_driver
                lease_driver.date_added = date_added
                lease_driver.is_active = True
            
            else:
                # create a new lease
                logger.info("Creating new lease driver for driver ID: %s and lease ID: %s")

                driver_lease = LeaseDriver(
                    driver_id=driver.driver_id if driver else None,
                    lease_id=lease.id if lease else None,
                    driver_role=driver_role,
                    is_day_night_shift=is_day_night_shift,
                    is_additional_driver=is_additional_driver,
                    date_added=date_added,
                    is_active = True
                )

                db.add(driver_lease)

            db.flush()
            logger.info("Lease Driver '%s' added to Lease '%s'the database.", tlc_license , lease_id)

        db.commit()
    except Exception as e :
        db.rollback()
        logger.error("Error parsing Lease Driver data: %s", e)
        raise

if __name__ == "__main__":
    logger.info("Loading Lease Driver Information")
    session = SessionLocal()
    xls = pd.ExcelFile(s3_utils.download_file(settings.bat_file_key))
    installation_df = pd.read_excel(xls, 'lease_driver')
    parse_lease_driver(session, installation_df)
    logger.info("Lease Driver Information loaded successfully ✅")

