# Standard library imports
from datetime import datetime

# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.entities.models import Address
from app.utils.general import get_safe_value
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df
from app.seeder_loader.parser_registry import parser
import tempfile
import os

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="address",
    sheet_names=[data_loader_settings.parser_address_sheet],
    version="1.0",
    deprecated=False,
    description="Process address from Excel sheet"
)
def parse_address(db: Session, df: pd.DataFrame) -> ParseResult:
    """Parse and load addresses from dataframe into database."""
    result = ParseResult(sheet_name="address")
    created_count = 0
    updated_count = 0

    try:
        for idx, row in df.iterrows():
            try:
                address_line_1 = get_safe_value(row, "address_line_1")
                if not address_line_1:
                    logger.warning("Skipping row with missing address_line_1")
                    result.record_failed(idx, "Missing address_line_1")
                    continue

                existing_address = (
                    db.query(Address).filter_by(address_line_1=address_line_1).first()
                )
                if existing_address:
                    logger.info("Address already exists: %s. Skipping.", address_line_1)
                    updated_count += 1
                    result.record_updated(idx)
                    continue

                new_address = Address(
                    address_line_1=get_safe_value(row, "address_line_1"),
                    address_line_2=get_safe_value(row, "address_line_2"),
                    city=get_safe_value(row, "city"),
                    state=get_safe_value(row, "state"),
                    zip=get_safe_value(row, "zip"),
                    is_active=True,
                    created_by=SUPERADMIN_USER_ID,
                    created_on=datetime.now(),
                )

                db.add(new_address)
                db.flush()
                created_count += 1
                result.record_inserted(idx)
                logger.info("Address '%s' added to the database.", address_line_1)
            except Exception as row_error:
                logger.exception("Error parsing address row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        logger.info("✅ Data successfully processed.")
        return result
    except Exception as e:
        logger.exception("Critical failure in parser address: %s", e)
        raise RuntimeError(f"Parser address failed: {e}") from e


if __name__ == "__main__":
    logger.info("Loading address configuration")
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
        data_df = pd.read_excel(excel_file, "address")

        result = parse_address(db_session, data_df)
        
        # Apply results
        updated_df = apply_parse_result_to_df(data_df, result)
        
        # Write back to temp file
        with pd.ExcelWriter(
            tmp_file_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            updated_df.to_excel(writer, sheet_name="address", index=False)
            
        # Upload back to S3
        with open(tmp_file_path, 'rb') as f:
            s3_utils.upload_file(f, settings.bat_file_key)

        db_session.commit()
        logger.info("Address committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing address: %s", e)
        raise
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        db_session.close()
            