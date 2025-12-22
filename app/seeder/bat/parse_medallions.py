# Third party imports
import pandas as pd
from datetime import datetime
from sqlalchemy.orm import Session
import random

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.medallions.models import Medallion
from app.entities.services import entity_service
from app.medallions.services import medallion_service
from app.utils.general import generate_random_6_digit
from app.utils.general import get_safe_value

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def parse_medallions(db: Session, df: pd.DataFrame):
    """Parse and load medallions from dataframe into database."""
    try:
        for _, row in df.iterrows():
            # Use get_safe_value() to safely fetch values from DataFrame rows
            medallion_numbers = get_safe_value(row, 'medallion_number')
            owner_type = get_safe_value(row, 'owner_type')
            ein = get_safe_value(row, 'ein')
            ssn = get_safe_value(row, 'ssn')
            medallion_owner = None
            medallion_owner_type = None
            
            if not medallion_numbers:
                logger.warning("Skipping row with missing medallion_number")
                continue

            if owner_type == "Ind":
                individual = entity_service.get_individual(db=db , ssn=ssn)
                if individual:
                    owner = medallion_service.get_medallion_owner(db=db , individual_id=individual.id)
                    medallion_owner = owner.id if owner else None
                    medallion_owner_type = "I"
            elif owner_type == "Corp":
                corporation = entity_service.get_corporation(db=db , ein=ein)
                if corporation:
                    owner = medallion_service.get_medallion_owner(db=db , corporation_id=corporation.id)
                    medallion_owner = owner.id if owner else None
                    medallion_owner_type = "C"



            medallion_type = get_safe_value(row, 'medallion_type')
            medallion_status = get_safe_value(row, 'medallion_status')
            medallion_renewal_date = get_safe_value(row, 'medallion_renewal_date')
            validity_start_date = get_safe_value(row, 'validity_start_date')
            validity_end_date = get_safe_value(row, 'validity_end_date')
            last_renewal_date = get_safe_value(row, 'last_renewal_date')
            fs6_status = get_safe_value(row, 'fs6_status')
            fs6_date = get_safe_value(row, 'fs6_date')

            # Convert dates to Python datetime objects
            def convert_date(date_str):
                if pd.notnull(date_str) and isinstance(date_str, pd.Timestamp):
                    try:
                        return date_str.to_pydatetime()
                    except ValueError:
                        logger.warning("Invalid date format: %s. Skipping.", date_str)
                        return None
                return None

            medallion_renewal_date = convert_date(medallion_renewal_date)
            validity_start_date = convert_date(validity_start_date)
            validity_end_date = convert_date(validity_end_date)
            last_renewal_date = convert_date(last_renewal_date)
            fs6_date = convert_date(fs6_date)

            # Check for existing records
            medallion = db.query(Medallion).filter(Medallion.medallion_number == medallion_numbers).first()

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
                    created_on=datetime.now()
                )
                db.add(medallion)

            db.flush()
            logger.info("Medallion '%s' added to the database.", medallion_numbers)

        db.commit()
        logger.info("✅ Data successfully processed.")
    except Exception as e:
        db.rollback()
        logger.error("Error parsing data: %s", e)
        raise
    finally:
        db.close()

if __name__ == "__main__":
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bat_file_key))
    medallion_df = pd.read_excel(excel_file, 'medallion')
    parse_medallions(db_session, medallion_df)
    db_session.close()

    
    