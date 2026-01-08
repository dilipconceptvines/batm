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
from app.entities.models import Address, Corporation
from app.medallions.models import MedallionOwner
from app.medallions.services import medallion_service
from app.utils.general import get_safe_value
from app.entities.services import entity_service
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df
from app.seeder_loader.parser_registry import parser
import tempfile
import os

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="corporation",
    sheet_names=[data_loader_settings.parser_corporation_sheet],
    version="1.0",
    deprecated=False,
    description="Process corporation from Excel sheet"
)
def parse_corporation(db: Session, df: pd.DataFrame) -> ParseResult:
    """Parse and load corporations from dataframe into database."""
    result = ParseResult(sheet_name="corporation")
    created_count = 0
    updated_count = 0

    try:
        for idx, row in df.iterrows():
            try:
                # Use get_safe_value() to safely fetch values from DataFrame rows
                corporation_name = get_safe_value(row, "corporation_name")
                primary_address = get_safe_value(row, "primary_address")
                parent_company = get_safe_value(row, "parent_company")
                is_holding_entity = get_safe_value(row, "is_holding_co")

                # Skip rows missing mandatory fields
                if not corporation_name:
                    logger.warning("Skipping row with missing corporation_name")
                    result.record_failed(idx, "Missing corporation_name")
                    continue

                holding_entity = None
                if not is_holding_entity and parent_company:
                    holding_entity = entity_service.get_corporation(
                        db=db, name=parent_company, is_holding_entity=True
                    )

                # Lookup Address by address_line_1
                address = db.query(Address).filter_by(address_line_1=primary_address).first()
                # Check for existing records
                corporation = db.query(Corporation).filter_by(name=corporation_name).first()

                if corporation:
                    # Update existing records
                    logger.info("Updating existing corporation: %s", corporation_name)
                    corporation.ein = get_safe_value(row, "ein")
                    corporation.primary_address_id = address.id if address else None
                    corporation.primary_contact_number = get_safe_value(row, "primary_contact_number")
                    corporation.primary_email_address = get_safe_value(row, "primary_email_address")
                    corporation.is_active = get_safe_value(row, "is_active") == "True"
                    corporation.is_holding_entity = is_holding_entity
                    corporation.linked_pad_owner_id = holding_entity.id if holding_entity else None
                    corporation.is_llc = get_safe_value(row, "is_llc") == "Y"
                    corporation.modified_by = SUPERADMIN_USER_ID
                    corporation.updated_on = datetime.now(timezone.utc)
                    updated_count += 1
                    result.record_updated(idx)
                else:
                    # Insert new ones
                    logger.info("Creating new corporation: %s", corporation_name)
                    corporation = Corporation(
                        name=corporation_name,
                        registered_date=pd.to_datetime(get_safe_value(row, "registered_date"))
                        if not pd.isna(get_safe_value(row, "registered_date"))
                        else None,
                        ein=get_safe_value(row, "ein"),
                        primary_address_id=address.id if address else None,
                        primary_contact_number=get_safe_value(row, "primary_contact_number"),
                        primary_email_address=get_safe_value(row, "primary_email_address"),
                        is_active=get_safe_value(row, "is_active") == "True",
                        is_holding_entity=is_holding_entity,
                        linked_pad_owner_id=holding_entity.id if holding_entity else None,
                        is_llc=get_safe_value(row, "is_llc") == "Y",
                        created_by=SUPERADMIN_USER_ID,
                        created_on=datetime.now(),
                    )
                    db.add(corporation)
                    created_count += 1
                    result.record_inserted(idx)

                db.flush()

                medallion_owner = medallion_service.get_medallion_owner(
                    db=db, corporation_id=corporation.id
                )

                if medallion_owner:
                    medallion_owner.corporation_id = corporation.id
                    medallion_owner.medallion_owner_type = "C"
                    medallion_owner.primary_phone = get_safe_value(row, "primary_contact_number")
                    medallion_owner.primary_email_address = get_safe_value(
                        row, "primary_email_address"
                    )
                    medallion_owner.primary_address_id = address.id if address else None
                    medallion_owner.medallion_owner_status = "Y"
                    medallion_owner.modified_by = SUPERADMIN_USER_ID

                else:
                    medallion_owner = MedallionOwner(
                        medallion_owner_type="C",
                        primary_phone=get_safe_value(row, "primary_contact_number"),
                        primary_email_address=get_safe_value(row, "primary_email_address"),
                        primary_address_id=address.id if address else None,
                        corporation_id=corporation.id,
                        medallion_owner_status="Y",
                        is_active=True,
                        created_by=SUPERADMIN_USER_ID,
                    )

                    db.add(medallion_owner)
                
                db.flush()
                logger.info("Corporation owner '%s' processed.", corporation_name)
            except Exception as row_error:
                logger.exception("Error parsing corporation row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        logger.info("✅ Data successfully processed.")
        return result
    except Exception as e:
        logger.exception("Critical failure in parser corporation: %s", e)
        raise RuntimeError(f"Parser corporation failed: {e}") from e


if __name__ == "__main__":
    logger.info("Loading corporation configuration")
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
        data_df = pd.read_excel(excel_file, "corporation")

        result = parse_corporation(db_session, data_df)
        
        # Apply results
        updated_df = apply_parse_result_to_df(data_df, result)
        
        # Write back to temp file
        with pd.ExcelWriter(
            tmp_file_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            updated_df.to_excel(writer, sheet_name="corporation", index=False)
            
        # Upload back to S3
        with open(tmp_file_path, 'rb') as f:
            s3_utils.upload_file(f, settings.bat_file_key)

        db_session.commit()
        logger.info("Corporation committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing corporation: %s", e)
        raise
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        db_session.close()

