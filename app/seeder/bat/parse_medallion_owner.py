# Standard library imports
from datetime import datetime, timezone

# Third party imports
import pandas as pd
from sqlalchemy.orm import Session

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.medallions.models import MedallionOwner, Medallion
from app.entities.models import Individual, Corporation, Address
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def parse_medallion_owner(db: Session, df: pd.DataFrame):
    """Parse medallion owner"""
    try:
        for _, row in df.iterrows():
            medallion_owner_type = row.get('medallion_owner_type')
            primary_phone = row.get('primary_phone')
            primary_email_address = row.get('primary_email_address')
            medallion_owner_status = row.get('medallion_owner_status')
            active_till = row.get('active_till')
            primary_contact = row.get('primary_contact')
            name = row.get('corporation_name')
            primary_address_line1 = row.get('primary_address_line1')

            # Convert 'active_till' to Python datetime object
            if pd.notnull(active_till):
                try:
                    active_till = active_till.to_pydatetime()
                except ValueError:
                    logger.warning(
                        "Invalid date format for 'active_till': %s. Skipping row.",
                        active_till    
                    )
                    continue
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
                    continue

            if not owner:
                logger.info(
                    "Creating new medallion owner for medallion ."
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
                db.flush()  # Flush to get the new owner ID
            
        db.commit()
    except Exception as e:
        logger.error("Error parsing medallion owner: %s", e)
        db.rollback()
        raise

if __name__ == "__main__":
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bat_file_key))
    medallion_owner_df = pd.read_excel(excel_file, 'medallion_owner')
    parse_medallion_owner(db_session, medallion_owner_df)
    db_session.close()

