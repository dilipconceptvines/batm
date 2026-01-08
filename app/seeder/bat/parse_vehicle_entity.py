# Standard library imports
from datetime import datetime

# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoResultFound, IntegrityError

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.entities.models import Address
from app.vehicles.models import VehicleEntity
from app.utils.general import get_safe_value
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df
from app.seeder_loader.parser_registry import parser
import tempfile
import os

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="vehicle_entity",
    sheet_names=[data_loader_settings.parser_vehicle_entity_sheet],
    version="1.0",
    deprecated=False,
    description="Process vehicle entity from Excel sheet"
)
def parse_vehicle_entity(db: Session, df: pd.DataFrame) -> ParseResult:
    """Parse and load vehicle entities from dataframe into database."""
    result = ParseResult(sheet_name="vehicle_entity")
    created_count = 0
    updated_count = 0

    try:
        for idx, row in df.iterrows():
            try:
                # Use get_safe_value() to safely fetch values from DataFrame rows
                entity_name = get_safe_value(row, "entity_name")
                entity_address = get_safe_value(row, "entity_address_line_1")

                # Skip rows missing mandatory fields
                if not entity_name:
                    logger.warning("Skipping row with missing entity_name")
                    result.record_failed(idx, "Missing entity_name")
                    continue

                ein = get_safe_value(row, "ein")

                try:
                    address = db.query(Address).filter(
                        Address.address_line_1 == entity_address
                    ).first()
                    entity_address_id = address.id if address else None
                except NoResultFound:
                    logger.warning(
                        "Address '%s' not found in the database. Skipping entity '%s'.",
                        entity_address,
                        entity_name,
                    )
                    result.record_failed(idx, f"Address not found: {entity_address}")
                    continue

                # Check for existing records
                entity = db.query(VehicleEntity).filter(
                    VehicleEntity.entity_name == entity_name
                ).first()

                if entity:
                    # Update existing records
                    logger.info("Updating existing vehicle entity: %s", entity_name)
                    entity.ein = ein
                    entity.entity_address_id = entity_address_id
                    entity.entity_status = "Active"
                    updated_count += 1
                    result.record_updated(idx)
                else:
                    # Insert new ones
                    logger.info("Adding new vehicle entity: %s", entity_name)
                    entity = VehicleEntity(
                        entity_name=entity_name,
                        ein=ein,
                        entity_address_id=entity_address_id,
                        entity_status="Active",
                        is_active=True,
                        created_by=SUPERADMIN_USER_ID,
                        created_on=datetime.now(),
                    )
                    db.add(entity)
                    created_count += 1
                    result.record_inserted(idx)
                    logger.info("Vehicle entity '%s' added to the database.", entity_name)

                db.flush()
            except Exception as row_error:
                logger.exception("Error parsing vehicle entity row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        return result
    except Exception as e:
        logger.exception("Critical failure in parser vehicle_entity: %s", e)
        raise RuntimeError(f"Parser vehicle_entity failed: {e}") from e

if __name__ == "__main__":
    logger.info("Loading vehicle entity configuration")
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
        data_df = pd.read_excel(excel_file, "vehicle_entity")

        result = parse_vehicle_entity(db_session, data_df)
        
        # Apply results
        updated_df = apply_parse_result_to_df(data_df, result)

        # Write back to temp file
        with pd.ExcelWriter(
            tmp_file_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            updated_df.to_excel(writer, sheet_name="vehicle_entity", index=False)
            
        # Upload back to S3
        with open(tmp_file_path, 'rb') as f:
            s3_utils.upload_file(f, settings.bat_file_key)

        db_session.commit()
        logger.info("Vehicle entity committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing vehicle entity: %s", e)
        raise
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        db_session.close()



