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
from app.medallions.models import MedallionOwner, Medallion
from app.entities.models import Individual, Corporation, Address
from app.utils.s3_utils import s3_utils
from app.utils.general import parse_date, get_safe_value
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df
from app.seeder_loader.parser_registry import parser
import tempfile
import os

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="medallion_owner",
    sheet_names=[data_loader_settings.parser_medallion_owner_sheet],
    version="1.0",
    deprecated=False,
    description="Process medallion owner from Excel sheet"
)
def parse_medallion_owner(db: Session, df: pd.DataFrame) -> ParseResult:
    """Parse medallion owner"""
    result = ParseResult(sheet_name=data_loader_settings.parser_medallion_owner_sheet)
    created_count = 0
    updated_count = 0

    try:
        for idx, row in df.iterrows():
            try:
                medallion_owner_type = get_safe_value(row, 'medallion_owner_type')
                primary_phone = get_safe_value(row, 'primary_phone')
                primary_email_address = get_safe_value(row, 'primary_email_address')
                medallion_owner_status = get_safe_value(row, 'medallion_owner_status')
                active_till = get_safe_value(row, 'active_till')
                primary_contact = get_safe_value(row, 'primary_contact')
                name = get_safe_value(row, 'corporation_name')
                primary_address_line1 = get_safe_value(row, 'primary_address_line1')
    
                active_till = parse_date(active_till)
                individual_id = None
                corporation_id = None
                primary_address_id = None
    
                # **Handle Address Lookup and Insertion**
                if primary_address_line1:
                    address = db.query(Address).filter_by(address_line_1=primary_address_line1).one_or_none()
                    if not address:
                        logger.info("Creating new address: %s", primary_address_line1)
                        address = Address(address_line_1=primary_address_line1)
                        db.add(address)
                        db.flush()  # Get new address ID
                    primary_address_id = address.id  # Assign the address ID
            
                # Determine the medallion owner
                owner = None
                if medallion_owner_type == 'I':
                    # Lookup individual owner by primary_contact
                    individual = db.query(Individual).filter_by(
                        first_name=primary_contact
                    ).one_or_none()
                    owner = db.query(MedallionOwner).filter(
                        MedallionOwner.medallion_owner_type == 'I',
                        MedallionOwner.individual.has(
                            first_name=primary_contact)
                    ).one_or_none()
                    if individual:
                        individual_id = individual.id
                    else:
                        logger.warning("No individual found with name '%s'. Skipping.", primary_contact)
                        result.record_failed(idx, f"No individual found with name {primary_contact}")
                        continue
                elif medallion_owner_type == 'C':
                    # Lookup corporation owner by corporation_name
                    corporation = db.query(Corporation).filter_by(
                        name=name
                    ).one_or_none()
                    owner = db.query(MedallionOwner).filter(
                        MedallionOwner.medallion_owner_type == 'C',
                        MedallionOwner.corporation.has(
                            name=name
                        )
                    ).one_or_none()
                    if corporation:
                        corporation_id = corporation.id
                    else:
                        logger.warning(
                            "Invalid owner type '%s' for medallion '%s'. Skipping.",
                            medallion_owner_type
                        )
                        result.record_failed(idx, f"Invalid owner type {medallion_owner_type}")
                        continue
    
                if not owner:
                    logger.info(
                        "Creating new medallion owner."
                    )
                    owner = MedallionOwner(
                        medallion_owner_type=medallion_owner_type,
                        primary_phone=primary_phone,
                        primary_email_address=primary_email_address,
                        medallion_owner_status=medallion_owner_status,
                        active_till=active_till,
                        individual_id = individual_id,
                        corporation_id = corporation_id,
                        primary_address_id=primary_address_id  # Store address ID
                    )
                    db.add(owner)
                    created_count += 1
                    result.record_inserted(idx)
                else:
                    # Update potentially? Logic above only creates if not exists. 
                    updated_count += 1
                    result.record_updated(idx) 
                
                db.flush()
            except Exception as row_error:
                logger.exception("Error parsing medallion owner row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        return result
    except Exception as e:
        logger.exception("Critical failure in parser medallion_owner: %s", e)
        raise RuntimeError(f"Parser medallion_owner failed: {e}") from e

if __name__ == "__main__":
    logger.info("Loading medallion owner configuration")
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
        data_df = pd.read_excel(excel_file, "medallion_owner")

        result = parse_medallion_owner(db_session, data_df)
        
        # Apply results
        updated_df = apply_parse_result_to_df(data_df, result)

        # Write back to temp file
        with pd.ExcelWriter(
            tmp_file_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            updated_df.to_excel(writer, sheet_name="medallion_owner", index=False)
            
        # Upload back to S3
        with open(tmp_file_path, 'rb') as f:
            s3_utils.upload_file(f, settings.bat_file_key)

        db_session.commit()
        logger.info("Medallion owner committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing medallion owner: %s", e)
        raise
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        db_session.close()

