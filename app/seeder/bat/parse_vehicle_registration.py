# Standard library imports
from datetime import datetime , date , timedelta

# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.core.config import settings
from app.core.data_loader_config import data_loader_settings
from app.vehicles.models import Vehicle, VehicleRegistration  # Import models
from app.utils.s3_utils import s3_utils
from app.utils.general import get_safe_value , parse_date
from app.seeder.parsing_result import ParseResult
from app.seeder_loader.parser_registry import parser

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="vehicle_registration",
    sheet_names=[data_loader_settings.parser_vehicle_registration_sheet],
    version="1.0",
    deprecated=False,
    description="Process vehicle registration from Excel sheet"
)
def parse_vehicle_registration_information(db: Session, df: pd.DataFrame) -> ParseResult:
    """
    Parses the vehicle registration information from the excel file and upserts the data into the database.
    """
    result = ParseResult(sheet_name="vehicle_registration")
    created_registrations = 0
    updated_registrations = 0
    
    try:
        for idx, row in df.iterrows():
            try:
                vehicle_vin = get_safe_value(row, 'vin')
                registration_date = get_safe_value(row , 'registration_date') or date.today()
                registration_expiry_date = get_safe_value(row , 'registration_expiry_date') or date.today() + timedelta(days=365)
                registration_fee = get_safe_value(row , 'registration_fee')
                plate_number = get_safe_value(row , 'plate_number')
                status = get_safe_value(row , 'status')
                registration_state = get_safe_value(row , 'registration_state')
                registration_class = get_safe_value(row , 'registration_class')

                registration_date = parse_date(registration_date)
                registration_expiry_date = parse_date(registration_expiry_date)
                
                if not vehicle_vin:
                     logger.warning("Skipping row with missing VIN")
                     result.record_failed(idx, "Missing VIN")
                     continue

                # Get vehicle_id using VIN
                vehicle = db.query(Vehicle).filter_by(vin=vehicle_vin).first()
                if not vehicle:
                    logger.warning("No vehicle found for VIN: %s. Skipping.", vehicle_vin)
                    result.record_failed(idx, "Vehicle not found")
                    continue

                vehicle_id = vehicle.id

                if not registration_date:
                    result.record_failed(idx, "registration_date is mandatory")
                    continue

                registration_date = parse_date(registration_date)
                if not registration_date:
                    result.record_failed(idx, "Invalid registration_date format")
                    continue

                if not registration_expiry_date:
                    result.record_failed(idx, "registration_expiry_date is mandatory")
                    continue

                if not plate_number:
                    result.record_failed(idx, "plate_number is mandatory")
                    continue

                # Check if vehicle registration already exists
                vehicle_registration = db.query(VehicleRegistration).filter_by(
                    vehicle_id=vehicle_id).first()

                if vehicle_registration:
                    # Update Existing Record
                    logger.info("Updating existing vehicle registration for VIN: %s", vehicle_vin)
                    vehicle_registration.registration_date = registration_date
                    vehicle_registration.registration_expiry_date = registration_expiry_date
                    vehicle_registration.registration_fee = registration_fee
                    vehicle_registration.plate_number = plate_number
                    vehicle_registration.status = status
                    vehicle_registration.registration_class = registration_class
                    vehicle_registration.registration_state = registration_state
                    updated_registrations += 1
                    result.record_updated(idx)
                else:
                    # Insert New Record
                    logger.info("Inserting new vehicle registration for VIN: %s", vehicle_vin)
                    vehicle_registration = VehicleRegistration(
                        vehicle_id=vehicle_id,
                        registration_date=registration_date,
                        registration_expiry_date=registration_expiry_date,
                        registration_fee=registration_fee,
                        plate_number=plate_number,
                        registration_state=registration_state,
                        registration_class=registration_class,
                        created_by=SUPERADMIN_USER_ID,
                        status=status
                    )
                    db.add(vehicle_registration)
                    created_registrations += 1
                    result.record_inserted(idx)

                db.flush()
            except Exception as row_error:
                logger.exception("Error parsing vehicle registration row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        
        return result
    except Exception as e:
        logger.exception("Critical failure in parser vehicle_registration: %s", e)
        raise RuntimeError(f"Parser vehicle_registration failed: {e}") from e


if __name__ == "__main__":
    logger.info("Loading vehicle registration configuration")
    db_session = SessionLocal()

    try:
        excel_file = pd.ExcelFile(
            s3_utils.download_file(settings.bat_file_key)
        )
        data_df = pd.read_excel(excel_file, "vehicle_registration")

        parse_vehicle_registration_information(db_session, data_df)

        db_session.commit()
        logger.info("Vehicle registration committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing vehicle registration: %s", e)
        raise
    finally:
        db_session.close()
    logger.info("Vehicle Registration Information Seeded Successfully ✅")