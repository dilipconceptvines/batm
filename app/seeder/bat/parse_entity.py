# Third party imports
import pandas as pd
import random
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoResultFound

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.entities.models import Entity, Address

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def parse_entity(db: Session, df: pd.DataFrame):
    """Parse entity"""
    try:
        for _, row in df.iterrows():
            entity_name = row.get('entity_name')
            dos_id = row.get('dos_id')
            entity_address_line_1 = row.get('entity_address_line_1')
            num_corporations = row.get('num_corporations', 0)
            president = row.get('president')
            secretary = row.get('secretary')
            corporate_officer = row.get('corporate_officer')
            ein = row.get('ein')

            # Lookup Address ID
            try:
                logger.info("Looking up address %s", entity_address_line_1)
                address = db.query(Address).filter_by(
                    address_line_1=entity_address_line_1).first()
                entity_address_id = address.id
            except NoResultFound:
                logger.warning(
                    "Address '%s' not found in the database. Skipping entity '%s'.",
                    entity_address_line_1, entity_name
                )
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
        logger.info("Entity data parsed and committed successfully.")
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error("Error parsing entity data: %s", e)
        raise


if __name__ == "__main__":
    logger.info("Loading Entity information")
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bat_file_key))
    entity_df = pd.read_excel(excel_file, 'entity')
    parse_entity(db_session, entity_df)