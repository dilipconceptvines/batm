# Standard library imports
from datetime import datetime

# Third party imports
import pandas as pd
from sqlalchemy.orm import Session

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.entities.models import Address, Entity
from app.vehicles.models import Vehicle
from app.medallions.models import MedallionOwner
from app.drivers.models import Driver
from app.leases.models import Lease
from app.utils.general import get_safe_value

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def parse_address(db: Session, df: pd.DataFrame):
    """Parse and load addresses from dataframe into database."""
    try:
        for _, row in df.iterrows():
            # Use get_safe_value() to safely fetch values from DataFrame rows
            address_line_1 = get_safe_value(row, 'address_line_1')
            
            # Skip rows missing mandatory fields
            if not address_line_1:
                logger.warning("Skipping row with missing address_line_1")
                continue

            # Check for existing records
            existing_address = db.query(Address).filter_by(
                address_line_1=address_line_1).first()
            if existing_address:
                logger.info("Address already exists: %s. Skipping.", address_line_1)
                continue

            # Insert new ones
            logger.info("Adding new address: %s", address_line_1)
            new_address = Address(
                address_line_1=get_safe_value(row, 'address_line_1'),
                address_line_2=get_safe_value(row, 'address_line_2'),
                city=get_safe_value(row, 'city'),
                state=get_safe_value(row, 'state'),
                zip=get_safe_value(row, 'zip'),
                is_active=True,
                created_by=SUPERADMIN_USER_ID,
                created_on=datetime.now()
            )

            # Add the new address to the session
            db.add(new_address)
            db.flush()  # Flush to get the new address ID
            
            logger.info("Address '%s' added to the database.", address_line_1)

        db.commit()
        logger.info("✅ Data successfully processed.")
    except Exception as e:
        db.rollback()
        logger.error("Error parsing data: %s", e)
        raise
    finally:
        db.close()


if __name__ == "__main__":
    logger.info("Loading Address from excel")
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bat_file_key))
    address_df = pd.read_excel(excel_file, 'address')
    parse_address(db=db_session, df=address_df)
            