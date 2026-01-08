
# Standard library imports
from datetime import datetime

# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.utils.s3_utils import s3_utils
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.core.config import settings
from app.core.data_loader_config import data_loader_settings
from app.vehicles.models import Vehicle, VehicleInspection
from app.utils.general import get_safe_value , parse_date
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df
from app.seeder_loader.parser_registry import parser
import tempfile
import os

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="vehicle_inspections",
    sheet_names=[data_loader_settings.parser_vehicle_inspections_sheet],
    version="1.0",
    deprecated=False,
    description="Process vehicle inspections from Excel sheet"
)
def parse_vehicle_inspection_information(db: Session, df: pd.DataFrame) -> ParseResult:
    """
    Parses the vehicle inspection information from the excel file and upserts the data into the database.
    """
    result = ParseResult(sheet_name=data_loader_settings.parser_vehicle_inspections_sheet)
    created_inspections = 0
    updated_inspections = 0

    try:
        for idx, row in df.iterrows():
            try:
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
                # Renaming 'result' from row to 'inspection_result' to avoid conflict with ParseResult variable if needed, 
                # but here it is a column name so it is fine.
                inspection_result_val = get_safe_value(row, 'result')
                next_inspection_due_date = get_safe_value(row, 'next_inspection_due_date')
                status = get_safe_value(row , 'status')
    
                if not vehicle_vin:
                    logger.warning("Skipping row with missing VIN")
                    result.record_failed(idx, "Missing VIN")
                    continue

                def convert_time(time_str):
                    if pd.notnull(time_str):
                        try:
                            return time_str if isinstance(time_str, str) else time_str.strftime("%H:%M:%S")
                        except ValueError:
                            logger.warning("Invalid time format: %s. Skipping.", time_str)
                            return None
                    return None
    
    
                inspection_date = parse_date(inspection_date)
                odometer_reading_date = parse_date(odometer_reading_date)
                logged_date = parse_date(logged_date)
                next_inspection_due_date = parse_date(next_inspection_due_date)
    
                inspection_time = convert_time(inspection_time)
                odometer_reading_time = convert_time(odometer_reading_time)
                logged_time = convert_time(logged_time)
    
                # Get vehicle_id using VIN
                vehicle = db.query(Vehicle).filter_by(vin=vehicle_vin).first()
                if not vehicle:
                    logger.warning("No vehicle found for VIN: %s. Skipping.", vehicle_vin)
                    result.record_failed(idx, "Vehicle not found")
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
                    vehicle_inspection.result = inspection_result_val
                    vehicle_inspection.next_inspection_due_date = next_inspection_due_date
                    vehicle_inspection.status = status
                    updated_inspections += 1
                    result.record_updated(idx)
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
                        result=inspection_result_val,
                        next_inspection_due_date=next_inspection_due_date,
                        status=status
                    )
                    db.add(vehicle_inspection)
                    created_inspections += 1
                    result.record_inserted(idx)
                    
                db.flush()
            except Exception as row_error:
                logger.exception("Error parsing vehicle inspection row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        return result
    except Exception as e:
        logger.exception("Critical failure in parser vehicle_inspections: %s", e)
        raise RuntimeError(f"Parser vehicle_inspections failed: {e}") from e


if __name__ == "__main__":
    logger.info("Loading vehicle inspections configuration")
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
        data_df = pd.read_excel(excel_file, "vehicle_inspections")

        result = parse_vehicle_inspection_information(db_session, data_df)
        
        # Apply results
        updated_df = apply_parse_result_to_df(data_df, result)

        # Write back to temp file
        with pd.ExcelWriter(
            tmp_file_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            updated_df.to_excel(writer, sheet_name="vehicle_inspections", index=False)
            
        # Upload back to S3
        with open(tmp_file_path, 'rb') as f:
            s3_utils.upload_file(f, settings.bat_file_key)

        db_session.commit()
        logger.info("Vehicle inspections committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing vehicle inspections: %s", e)
        raise
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        db_session.close()
    logger.info("Vehicle Inspection Information Seeded Successfully ✅")