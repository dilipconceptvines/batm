# Standard library imports
from datetime import datetime

# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoResultFound

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.entities.models import Address
from app.vehicles.models import VehicleEntity
from app.utils.general import get_safe_value

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def parse_vehicle_entity(db: Session, df: pd.DataFrame):
    """Parse and load vehicle entities from dataframe into database."""
    try:
        for _, row in df.iterrows():
            # Use get_safe_value() to safely fetch values from DataFrame rows
            entity_name = get_safe_value(row, 'entity_name')
            entity_address = get_safe_value(row, 'entity_address_line_1')
            
            # Skip rows missing mandatory fields
            if not entity_name:
                logger.warning("Skipping row with missing entity_name")
                continue

            ein = get_safe_value(row, "ein")

            try:
                address = db.query(Address).filter(
                    Address.address_line_1 == entity_address
                ).first()
                entity_address_id = address.id if address else None
            except NoResultFound:
                logger.warning("Address '%s' not found in the database. Skipping entity '%s'.", entity_address, entity_name)
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
                    created_on=datetime.now()
                )
                db.add(entity)
                db.flush()

                logger.info("Vehicle entity '%s' added to the database.", entity_name)

        db.commit()
        logger.info("✅ Data successfully processed.")
    except Exception as e:
        db.rollback()
        logger.error("Error parsing data: %s", e)
        raise
    finally:
        db.close()

if __name__ == "__main__":
    logger.info("Loading Entity information")
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bat_file_key))
    entity_df = pd.read_excel(excel_file, 'vehicle_entity')
    parse_vehicle_entity(db_session, entity_df)



