# Standard library imports
from datetime import datetime, timezone

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
from app.utils.general import get_safe_value
from app.vehicles.models import Dealer
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df
from app.seeder_loader.parser_registry import parser
import tempfile
import os

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="dealers",
    sheet_names=[data_loader_settings.parser_dealers_sheet],
    version="1.0",
    deprecated=False,
    description="Process dealers from Excel sheet"
)
def parse_dealers(db: Session, df: pd.DataFrame) -> ParseResult:
    """Parse dealers"""
    result = ParseResult(sheet_name="dealers")
    created_dealers = 0
    updated_dealers = 0

    try:
        for idx, row in df.iterrows():
            try:
                dealer_name = get_safe_value(row, "dealer_name")
                if not dealer_name:
                    logger.warning("Skipping row with missing dealer_name")
                    result.record_failed(idx, "Missing dealer_name")
                    continue

                # Check if the dealer already exists
                dealer = db.query(Dealer).filter(Dealer.dealer_name == dealer_name).first()

                if dealer:
                    # Update existing dealer record
                    logger.info("Updating dealer: %s", dealer_name)
                    dealer.dealer_bank_name = get_safe_value(row, "dealer_bank_name")
                    dealer.dealer_bank_account_number = get_safe_value(row, "dealer_bank_account_number")
                    updated_dealers += 1
                    result.record_updated(idx)
                else:
                    # Create new dealer record
                    logger.info("Creating new dealer: %s", dealer_name)
                    new_dealer = Dealer(
                        dealer_name=dealer_name,
                        dealer_bank_name=get_safe_value(row, "dealer_bank_name"),
                        dealer_bank_account_number=get_safe_value(row, "dealer_bank_account_number"),
                    )
                    db.add(new_dealer)
                    created_dealers += 1
                    result.record_inserted(idx)
                db.flush()
            except Exception as row_error:
                logger.exception("Error parsing dealers row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        logger.info("✅ Data successfully processed.")
        return result
    except Exception as e:
        logger.exception("Critical failure in parser dealers: %s", e)
        raise RuntimeError(f"Parser dealers failed: {e}") from e


if __name__ == "__main__":
    logger.info("Loading dealers configuration")
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
        data_df = pd.read_excel(excel_file, "dealers")

        result = parse_dealers(db_session, data_df)
        
        # Apply results
        updated_df = apply_parse_result_to_df(data_df, result)
        
        # Write back to temp file
        with pd.ExcelWriter(
            tmp_file_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            updated_df.to_excel(writer, sheet_name="dealers", index=False)
            
        # Upload back to S3
        with open(tmp_file_path, 'rb') as f:
            s3_utils.upload_file(f, settings.bat_file_key)

        db_session.commit()
        logger.info("Dealers committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing dealers: %s", e)
        raise
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        db_session.close()

