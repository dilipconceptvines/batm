# Third party imports
import pandas as pd
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
import random

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.medallions.models import Medallion
from app.entities.services import entity_service
from app.medallions.services import medallion_service
from app.utils.general import generate_random_6_digit
from app.utils.general import get_safe_value , parse_date
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df
from app.seeder_loader.parser_registry import parser
import tempfile
import os

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="medallion",
    sheet_names=[data_loader_settings.parser_medallion_sheet],
    version="1.0",
    deprecated=False,
    description="Process medallion from Excel sheet"
)
def parse_medallions(db: Session, df: pd.DataFrame) -> ParseResult:
    """Parse and load medallions from dataframe into database."""
    result = ParseResult(sheet_name="medallion")
    created_medallions = 0
    updated_medallions = 0

    try:
        for idx, row in df.iterrows():
            try:
                # Use get_safe_value() to safely fetch values from DataFrame rows
                medallion_numbers = get_safe_value(row, "medallion_number")
                owner_type = get_safe_value(row, "owner_type")
                ein = get_safe_value(row, "ein")
                ssn = get_safe_value(row, "ssn")
                medallion_owner = None
                medallion_owner_type = None

                if not medallion_numbers:
                    logger.warning("Skipping row with missing medallion_number")
                    result.record_failed(idx, "Missing medallion_number")
                    continue

                if owner_type == "Ind":
                    individual = entity_service.get_individual(db=db, ssn=ssn)
                    if individual:
                        owner = medallion_service.get_medallion_owner(
                            db=db, individual_id=individual.id
                        )
                        medallion_owner = owner.id if owner else None
                        medallion_owner_type = "I"
                elif owner_type == "Corp":
                    corporation = entity_service.get_corporation(db=db, ein=ein)
                    if corporation:
                        owner = medallion_service.get_medallion_owner(
                            db=db, corporation_id=corporation.id
                        )
                        medallion_owner = owner.id if owner else None
                        medallion_owner_type = "C"

                medallion_type = get_safe_value(row, "medallion_type")
                medallion_status = get_safe_value(row, "medallion_status")
                medallion_renewal_date = get_safe_value(row, "medallion_renewal_date")
                validity_start_date = get_safe_value(row, "validity_start_date")
                validity_end_date = get_safe_value(row, "validity_end_date")
                last_renewal_date = get_safe_value(row, "last_renewal_date")
                fs6_status = get_safe_value(row, "fs6_status")
                fs6_date = get_safe_value(row, "fs6_date")


                medallion_renewal_date = parse_date(medallion_renewal_date)
                validity_start_date = parse_date(validity_start_date)
                validity_end_date = parse_date(validity_end_date)
                last_renewal_date = parse_date(last_renewal_date)
                fs6_date = parse_date(fs6_date)

                # Check for existing records
                medallion = (
                    db.query(Medallion)
                    .filter(Medallion.medallion_number == medallion_numbers)
                    .first()
                )

                if medallion is not None:
                    # Update existing records
                    logger.info("Updating existing medallion: %s", medallion_numbers)
                    medallion.medallion_type = medallion_type
                    medallion.owner_type = medallion_owner_type
                    medallion.medallion_renewal_date = medallion_renewal_date
                    medallion.validity_start_date = validity_start_date
                    medallion.validity_end_date = validity_end_date
                    medallion.last_renewal_date = last_renewal_date
                    medallion.fs6_status = fs6_status
                    medallion.fs6_date = fs6_date
                    medallion.owner_id = medallion_owner if medallion_owner else None
                    medallion.modified_by = SUPERADMIN_USER_ID
                    medallion.updated_on = datetime.now()
                    updated_medallions += 1
                    result.record_updated(idx)
                else:
                    # Insert new ones
                    logger.info("Inserting new medallion: %s", medallion_numbers)
                    medallion = Medallion(
                        medallion_number=medallion_numbers,
                        medallion_type=medallion_type,
                        owner_type=medallion_owner_type,
                        medallion_status=medallion_status,
                        medallion_renewal_date=medallion_renewal_date,
                        default_amount=generate_random_6_digit(),
                        validity_start_date=validity_start_date,
                        validity_end_date=validity_end_date,
                        last_renewal_date=last_renewal_date,
                        fs6_status=fs6_status,
                        fs6_date=fs6_date,
                        owner_id=medallion_owner if medallion_owner else None,
                        is_active=True,
                        created_by=SUPERADMIN_USER_ID,
                        created_on=datetime.now(),
                    )
                    db.add(medallion)
                    created_medallions += 1
                    result.record_inserted(idx)

                db.flush()
                logger.info("Medallion '%s' added to the database.", medallion_numbers)
            except Exception as row_error:
                logger.exception("Error parsing medallion row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        logger.info("✅ Data successfully processed.")
        return result
    except Exception as e:
        logger.exception("Critical failure in parser medallion: %s", e)
        raise RuntimeError(f"Parser medallion failed: {e}") from e

if __name__ == "__main__":
    logger.info("Loading medallion configuration")
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
        data_df = pd.read_excel(excel_file, "medallion")

        result = parse_medallions(db_session, data_df)
        
        # Apply results
        updated_df = apply_parse_result_to_df(data_df, result)

        # Write back to temp file
        with pd.ExcelWriter(
            tmp_file_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            updated_df.to_excel(writer, sheet_name="medallion", index=False)
            
        # Upload back to S3
        with open(tmp_file_path, 'rb') as f:
            s3_utils.upload_file(f, settings.bat_file_key)

        db_session.commit()
        logger.info("Medallion committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing medallion: %s", e)
        raise
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        db_session.close()

    
    