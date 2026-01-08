# Third party imports
import pandas as pd
import random
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoResultFound, IntegrityError

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.utils.general import get_safe_value
from app.entities.models import Entity, Address
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df
from app.seeder_loader.parser_registry import parser
import tempfile
import os

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="entity",
    sheet_names=[data_loader_settings.parser_entity_sheet],
    version="1.0",
    deprecated=False,
    description="Process entity from Excel sheet"
)
def parse_entity(db: Session, df: pd.DataFrame) -> ParseResult:
    """Parse entity"""
    result = ParseResult(sheet_name=data_loader_settings.parser_entity_sheet)
    created_count = 0
    updated_count = 0

    try:
        for idx, row in df.iterrows():
            try:
                entity_name = get_safe_value(row, 'entity_name')
                dos_id = get_safe_value(row, 'dos_id')
                entity_address_line_1 = get_safe_value(row, 'entity_address_line_1')
                num_corporations = get_safe_value(row, 'num_corporations') or 0
                president = get_safe_value(row, 'president')
                secretary = get_safe_value(row, 'secretary')
                corporate_officer = get_safe_value(row, 'corporate_officer')
                ein = get_safe_value(row, 'ein')
    
                if not entity_name:
                    logger.warning("Skipping row with missing entity_name")
                    result.record_failed(idx, "Missing entity_name")
                    continue

                # Lookup Address ID
                try:
                    logger.info("Looking up address %s", entity_address_line_1)
                    address = db.query(Address).filter_by(
                        address_line_1=entity_address_line_1).first()
                    entity_address_id = address.id if address else None
                    if not entity_address_id:
                         logger.warning(
                            "Address '%s' not found. Skipping entity '%s'.",
                            entity_address_line_1, entity_name
                        )
                         result.record_failed(idx, f"Address '{entity_address_line_1}' not found")
                         continue

                except NoResultFound:
                    logger.warning(
                        "Address '%s' not found in the database. Skipping entity '%s'.",
                        entity_address_line_1, entity_name
                    )
                    result.record_failed(idx, f"Address '{entity_address_line_1}' not found")
                    continue
    
                # Check if entity already exists
                entity = db.query(Entity).filter_by(
                    entity_name=entity_name).first()
    
                if entity:
                    # Update existing entity
                    logger.info("Updating existing entity: %s", entity_name)
                    entity.dos_id = dos_id
                    entity.entity_address_id = entity_address_id
                    entity.num_corporations = num_corporations
                    entity.president = president
                    entity.secretary = secretary
                    entity.corporate_officer = corporate_officer
                    entity.ein_ssn = ein
                    entity.is_corporation = False
                    entity.contact_person_id = random.randint(1,15)
                    entity.bank_id = random.randint(1,15)
                    updated_count += 1
                    result.record_updated(idx)
                else:
                    # Insert new entity
                    logger.info("Inserting new entity: %s", entity_name)
                    entity = Entity(
                        entity_name=entity_name,
                        dos_id=dos_id,
                        entity_address_id=entity_address_id,
                        num_corporations=num_corporations,
                        president=president,
                        secretary=secretary,
                        corporate_officer=corporate_officer,
                        ein_ssn = ein,
                        is_corporation = False,
                        contact_person_id = random.randint(1,15),
                        bank_id = random.randint(1,15)
                    )
                    db.add(entity)
                    created_count += 1
                    result.record_inserted(idx)
                db.flush()
                
            except Exception as row_error:
                logger.exception("Error parsing entity row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        logger.info("Entity data parsed successfully.")
        
        return result
    except Exception as e:
        logger.exception("Critical failure in parser entity: %s", e)
        raise RuntimeError(f"Parser entity failed: {e}") from e


if __name__ == "__main__":
    logger.info("Loading entity configuration")
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
        data_df = pd.read_excel(excel_file, "entity")

        result = parse_entity(db_session, data_df)
        
        # Apply results
        updated_df = apply_parse_result_to_df(data_df, result)

        # Write back to temp file
        with pd.ExcelWriter(
            tmp_file_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            updated_df.to_excel(writer, sheet_name="entity", index=False)
            
        # Upload back to S3
        with open(tmp_file_path, 'rb') as f:
            s3_utils.upload_file(f, settings.bat_file_key)

        db_session.commit()
        logger.info("Entity committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing entity: %s", e)
        raise
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        db_session.close()