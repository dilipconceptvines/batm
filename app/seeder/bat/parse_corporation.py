# Standard library imports
from datetime import datetime, timezone

# Third party imports
import pandas as pd
from sqlalchemy.orm import Session

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.entities.models import Address, Corporation
from app.medallions.models import MedallionOwner
from app.medallions.services import medallion_service
from app.utils.general import get_safe_value
from app.entities.services import entity_service

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def parse_corporation(db: Session, df: pd.DataFrame):
    """Parse and load corporations from dataframe into database."""
    try:
        for i, row in df.iterrows():
            # Use get_safe_value() to safely fetch values from DataFrame rows
            corporation_name = get_safe_value(row, 'corporation_name')
            primary_address = get_safe_value(row, 'primary_address')
            parent_company = get_safe_value(row, 'parent_company')
            is_holding_entity = get_safe_value(row , "is_holding_co")
            
            # Skip rows missing mandatory fields
            if not corporation_name:
                logger.warning("Skipping row with missing corporation_name")
                continue

            holding_entity = None
            if not is_holding_entity and parent_company:
                holding_entity = entity_service.get_corporation(db=db , name = parent_company , is_holding_entity=True)
            # Lookup Address by address_line_1
            address = db.query(Address).filter_by(address_line_1=primary_address).first()
            # Check for existing records
            corporation = db.query(Corporation).filter_by(name=corporation_name).first()

            if corporation:
                # Update existing records
                logger.info("Updating existing corporation: %s", corporation_name)
                corporation.ein = get_safe_value(row, 'ein')
                corporation.primary_address_id = address.id if address else None
                corporation.primary_contact_number = get_safe_value(row, 'primary_contact_number')
                corporation.primary_email_address = get_safe_value(row, 'primary_email_address')
                corporation.is_active = get_safe_value(row, 'is_active') == 'True'
                corporation.is_holding_entity = is_holding_entity
                corporation.linked_pad_owner_id = holding_entity.id if holding_entity else None
                corporation.is_llc = get_safe_value(row, 'is_llc') == 'Y'
                corporation.modified_by = SUPERADMIN_USER_ID
                corporation.updated_on = datetime.now(timezone.utc)
            else:
                # Insert new ones
                logger.info("Creating new corporation: %s", corporation_name)
                corporation = Corporation(
                    name=corporation_name,
                    registered_date=pd.to_datetime(get_safe_value(row, 'registered_date')) if not pd.isna(get_safe_value(row, 'registered_date')) else None,
                    ein=get_safe_value(row, 'ein'),
                    primary_address_id=address.id if address else None,
                    primary_contact_number=get_safe_value(row, 'primary_contact_number'),
                    primary_email_address=get_safe_value(row, 'primary_email_address'),
                    is_active=get_safe_value(row, 'is_active') == 'True',
                    is_holding_entity= is_holding_entity,
                    linked_pad_owner_id= holding_entity.id if holding_entity else None,
                    is_llc=get_safe_value(row, 'is_llc') == 'Y',
                    created_by=SUPERADMIN_USER_ID,
                    created_on=datetime.now()
                )
                db.add(corporation)

            db.flush()

            medallion_owenr = medallion_service.get_medallion_owner(db=db , corporation_id=corporation.id)

            if medallion_owenr:
                medallion_owenr.corporation_id = corporation.id
                medallion_owenr.medallion_owner_type = 'C'
                medallion_owenr.primary_phone = get_safe_value(row, 'primary_contact_number')
                medallion_owenr.primary_email_address = get_safe_value(row, 'primary_email_address')
                medallion_owenr.primary_address_id = address.id if address else None
                medallion_owenr.medallion_owner_status = "Y"
                medallion_owenr.modified_by = SUPERADMIN_USER_ID

            else:
                medallion_owenr = MedallionOwner(
                    medallion_owner_type='C',
                    primary_phone=get_safe_value(row, 'primary_contact_number'),
                    primary_email_address=get_safe_value(row, 'primary_email_address'),
                    primary_address_id=address.id if address else None,
                    corporation_id=corporation.id,
                    medallion_owner_status="Y",
                    is_active=True,
                    created_by=SUPERADMIN_USER_ID,
                )

                db.add(medallion_owenr)
            db.flush()

            logger.info("Corporation owner '%s' added to the database.", corporation_name)
                
        db.commit()
        logger.info("✅ Data successfully processed.")
    except Exception as e:
        db.rollback()
        logger.error("Error parsing data: %s", e)
        raise
    finally:
        db.close()


if __name__ == "__main__":
    logger.info("Loading Corporation information")
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bat_file_key))
    corporation_df = pd.read_excel(excel_file, 'corporation')
    parse_corporation(db_session, corporation_df)

