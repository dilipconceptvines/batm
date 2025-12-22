
# Standard library imports
from datetime import datetime

# Third party imports
import pandas as pd
from sqlalchemy.orm import Session

# Local imports
from app.utils.s3_utils import s3_utils
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.core.config import settings
from app.vehicles.models import Vehicle, VehicleInspection
from app.utils.general import get_safe_value

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1


def parse_vehicle_inspection_information(db: Session, df: pd.DataFrame):
    """
    Parses the vehicle inspection information from the excel file and upserts the data into the database.
    """
    try:
        for _, row in df.iterrows():
            vehicle_vin = get_safe_value(row, 'vin')
            mile_run = get_safe_value(row, 'mile_run')
            inspection_date = get_safe_value(row, 'inspection_date')
            inspection_time = get_safe_value(row, 'inspection_time')
            odometer_reading_date = get_safe_value(row, 'odometer_reading_date')
            odometer_reading_time = get_safe_value(row, 'odometer_reading_time')
            odometer_reading = get_safe_value(row ,'odometer_reading')
            logged_date = get_safe_value(row, 'logged_date')
            logged_time = get_safe_value(row, 'logged_time')
            inspection_fee = get_safe_value(row, 'inspection_fee')
            result = get_safe_value(row, 'result')
            next_inspection_due_date = get_safe_value(row, 'next_inspection_due_date')
            status = get_safe_value(row , 'status')

            # Convert dates to datetime
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

            def convert_time(time_str):
                if pd.notnull(time_str):
                    try:
                        return time_str if isinstance(time_str, str) else time_str.strftime("%H:%M:%S")
                    except ValueError:
                        logger.warning("Invalid time format: %s. Skipping.", time_str)
                        return None
                return None


            inspection_date = convert_date(inspection_date)
            odometer_reading_date = convert_date(odometer_reading_date)
            logged_date = convert_date(logged_date)
            next_inspection_due_date = convert_date(next_inspection_due_date)

            inspection_time = convert_time(inspection_time)
            odometer_reading_time = convert_time(odometer_reading_time)
            logged_time = convert_time(logged_time)

            # Get vehicle_id using VIN
            vehicle = db.query(Vehicle).filter_by(vin=vehicle_vin).first()
            if not vehicle:
                logger.warning("No vehicle found for VIN: %s. Skipping.", vehicle_vin)
                continue

            vehicle_id = vehicle.id

            # Check if vehicle inspection already exists
            vehicle_inspection = db.query(VehicleInspection).filter_by(
                vehicle_id=vehicle_id, inspection_date=inspection_date).first()

            if vehicle_inspection:
                # Update Existing Record
                logger.info("Updating existing vehicle inspection for VIN: %s", vehicle_vin)
                vehicle_inspection.mile_run = mile_run
                vehicle_inspection.inspection_time = inspection_time
                vehicle_inspection.odometer_reading_date = odometer_reading_date
                vehicle_inspection.odometer_reading_time = odometer_reading_time
                vehicle_inspection.odometer_reading = odometer_reading
                vehicle_inspection.logged_date = logged_date
                vehicle_inspection.logged_time = logged_time
                vehicle_inspection.inspection_fee = inspection_fee
                vehicle_inspection.result = result
                vehicle_inspection.next_inspection_due_date = next_inspection_due_date
                vehicle_inspection.status = status
            else:
                # Insert New Record
                logger.info("Inserting new vehicle inspection for VIN: %s on %s", vehicle_vin, inspection_date)
                vehicle_inspection = VehicleInspection(
                    vehicle_id=vehicle_id,
                    mile_run=mile_run,
                    inspection_date=inspection_date,
                    inspection_time=inspection_time,
                    odometer_reading_date=odometer_reading_date,
                    odometer_reading_time=odometer_reading_time,
                    odometer_reading=odometer_reading,
                    logged_date=logged_date,
                    logged_time=logged_time,
                    inspection_fee=inspection_fee,
                    result=result,
                    next_inspection_due_date=next_inspection_due_date,
                    status=status
                )
                db.add(vehicle_inspection)
                
            db.flush()

        db.commit()
    except Exception as e:
        db.rollback()
        logger.error("Error parsing vehicle inspection data: %s", e)
        raise


if __name__ == "__main__":
    logger.info("Loading Vehicle Inspection Information")
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bat_file_key))
    inspection_df = pd.read_excel(excel_file, 'vehicle_inspections')
    parse_vehicle_inspection_information(db_session, inspection_df)
    db_session.close()
    logger.info("Vehicle Inspection Information Seeded Successfully ✅")