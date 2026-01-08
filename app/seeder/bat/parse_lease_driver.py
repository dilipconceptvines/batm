import pandas as pd
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.leases.models import Lease , LeaseDriver
from app.drivers.models import Driver
from app.drivers.schemas import DriverStatus
from app.drivers.services import driver_service
from app.utils.general import get_safe_value , parse_date
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df
from app.seeder_loader.parser_registry import parser
import tempfile
import os

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="lease_driver",
    sheet_names=[data_loader_settings.parser_lease_driver_sheet],
    version="1.0",
    deprecated=False,
    description="Process lease driver from Excel sheet"
)
def parse_lease_driver(db:Session , df: pd.DataFrame) -> ParseResult:
    """parse Lease Driver"""

    result = ParseResult(sheet_name="lease_driver")
    created_lease_drivers = 0
    updated_lease_drivers = 0

    try:
        for idx, row in df.iterrows():
            try:
                tlc_license = get_safe_value(row , 'tlc_license')
                lease_id = get_safe_value(row , 'lease_id')
                driver_role = get_safe_value(row , 'driver_role')
                is_day_night_shift = get_safe_value(row , 'is_day_night_shift')
                is_additional_driver = get_safe_value(row , 'is_additional_driver')
                date_added = get_safe_value(row , 'date_added')

                if not tlc_license or not lease_id:
                    result.record_failed(idx, "Missing tlc_license or lease_id")
                    continue
                
                date_added = parse_date(date_added)

                driver = driver_service.get_drivers(db=db ,tlc_license_number=tlc_license)

                lease = db.query(Lease).filter_by(lease_id=lease_id).first()

                if not lease or not driver:
                    logger.warning("No lease or driver found for lease ID: %s and driver ID: %s. Skipping.", lease_id, tlc_license)
                    result.record_failed(idx, "Lease or driver not found")
                    continue

                driver.driver_status = DriverStatus.ACTIVE
                driver.is_active = True
                db.add(driver)

                lease_driver = db.query(LeaseDriver).filter_by(driver_id=driver.driver_id, lease_id=lease.id).first()

                if lease_driver is not None :
                    # update the exiting Lease Driver
                    lease_driver.driver_role = driver_role
                    lease_driver.is_day_night_shift = is_day_night_shift
                    lease_driver.is_additional_driver = is_additional_driver
                    lease_driver.date_added = date_added
                    lease_driver.is_active = True
                    updated_lease_drivers += 1
                    result.record_updated(idx)
                    logger.info(f"Updating existing lease driver for driver ID: {tlc_license} and lease ID: {lease_id}")
                
                else:
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
                    created_lease_drivers += 1
                    result.record_inserted(idx)
                    logger.info("Creating new lease driver for driver ID: %s and lease ID: %s", tlc_license, lease_id)

                db.flush()
                logger.info("Lease Driver '%s' added to Lease '%s'the database.", tlc_license , lease_id)
            except Exception as row_error:
                logger.exception("Error parsing Lease Driver row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        logger.info("✅ Data successfully processed.")
        return result
    except Exception as e :
        logger.exception("Critical failure in parser lease_driver: %s", e)
        raise RuntimeError(f"Parser lease_driver failed: {e}") from e

if __name__ == "__main__":
    logger.info("Loading lease driver configuration")
    db_session = SessionLocal()

    tmp_file_path = None
    try:
        # Download file to temp
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
            tmp_file_path = tmp_file.name
            
        file_bytes = s3_utils.download_file(settings.bat_file_key)
        if not file_bytes:
             raise Exception("Failed to download file from S3")
        
        with open(tmp_file_path, 'wb') as f:
            f.write(file_bytes)
            
        excel_file = pd.ExcelFile(tmp_file_path)
        data_df = pd.read_excel(excel_file, "lease_driver")

        result = parse_lease_driver(db_session, data_df)
        
        # Apply results
        updated_df = apply_parse_result_to_df(data_df, result)

        # Write back to temp file
        with pd.ExcelWriter(
            tmp_file_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            updated_df.to_excel(writer, sheet_name="lease_driver", index=False)
            
        # Upload back to S3
        with open(tmp_file_path, 'rb') as f:
            s3_utils.upload_file(f, settings.bat_file_key)

        db_session.commit()
        logger.info("Lease driver committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing lease driver: %s", e)
        raise
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        db_session.close()
    logger.info("Lease Driver Information loaded successfully ✅")

