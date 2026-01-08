# Standard library imports
from datetime import datetime

# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.db import SessionLocal
from app.core.config import settings
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.vehicles.models import Vehicle, VehicleHackUp , HackUpTasks
from app.medallions.models import Medallion
from app.medallions.schemas import MedallionStatus
from app.vehicles.schemas import VehicleStatus , ProcessStatusEnum
from app.utils.general import get_safe_value , parse_date
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df
from app.seeder_loader.parser_registry import parser
import tempfile
import os

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="vehicle_hackups",
    sheet_names=[data_loader_settings.parser_vehicle_hackups_sheet],
    version="1.0",
    deprecated=False,
    description="Process vehicle hackups from Excel sheet"
)
def parse_vehicle_hackup_information(db: Session, df: pd.DataFrame) -> ParseResult:
    """
    Parses the vehicle hackup information from the excel file and upserts the data into the database.
    """
    result = ParseResult(sheet_name="vehicle_hackups")
    created_hackups = 0
    updated_hackups = 0
    
    try:
        for idx, row in df.iterrows():
            try:
                vehicle_vin = get_safe_value(row , "vin")
                status = get_safe_value(row , "status")

                if not vehicle_vin:
                    logger.warning("Skipping row with missing VIN")
                    result.record_failed(idx, "Missing VIN")
                    continue

                # Get Vehicle ID from VIN
                vehicle = db.query(Vehicle).filter_by(vin=vehicle_vin).first()
                if not vehicle:
                    logger.warning("No vehicle found for VIN: %s. Skipping.", vehicle_vin)
                    result.record_failed(idx, "Vehicle not found")
                    continue
                
                medallion = db.query(Medallion).filter_by(id=vehicle.medallion_id).first() if vehicle.medallion_id else None

                vehicle_id = vehicle.id

                if not medallion:
                    logger.warning("No medallion found for vehicle ID: %s. Skipping.", vehicle_id)
                    result.record_failed(idx, "Medallion not found")
                    continue

                # Check if vehicle hackup already exists
                vehicle_hackup = db.query(VehicleHackUp).filter_by(
                    vehicle_id=vehicle_id).first()

                if vehicle_hackup is not None:
                    # Update existing hackup details
                    logger.info("Updating existing vehicle installation for VIN: %s", vehicle_vin)
                    vehicle.vehicle_status = VehicleStatus.HACKED_UP
                    medallion.medallion_status = MedallionStatus.ACTIVE
                    updated_hackups += 1
                    result.record_updated(idx)
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
                    created_hackups += 1
                    result.record_inserted(idx)

                db.flush()
            except Exception as row_error:
                logger.exception("Error parsing vehicle hackup row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        
        return result
    except Exception as e:
        logger.exception("Critical failure in parser vehicle_hackups: %s", e)
        raise RuntimeError(f"Parser vehicle_hackups failed: {e}") from e


if __name__ == "__main__":
    logger.info("Loading vehicle hackups configuration")
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
        data_df = pd.read_excel(excel_file, "vehicle_hackups")

        result = parse_vehicle_hackup_information(db_session, data_df)
        
        # Apply results
        updated_df = apply_parse_result_to_df(data_df, result)

        # Write back to temp file
        with pd.ExcelWriter(
            tmp_file_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            updated_df.to_excel(writer, sheet_name="vehicle_hackups", index=False)
            
        # Upload back to S3
        with open(tmp_file_path, 'rb') as f:
            s3_utils.upload_file(f, settings.bat_file_key)

        db_session.commit()
        logger.info("Vehicle hackups committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing vehicle hackups: %s", e)
        raise
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        db_session.close()
    logger.info("Vehicle Hackup Information Seeded Successfully ✅")
