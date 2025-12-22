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
from app.entities.models import Address, Individual, BankAccount
from app.medallions.models import MedallionOwner
from app.medallions.services import medallion_service
from app.utils.general import get_safe_value

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def get_address_id(db: Session, address_line_1: str):
    """
    Lookup address ID using address_line_1.

    Args:
        session: The database session
        address_line_1: The address line to lookup

    Returns:
        ID of the address if found, else None
    """
    try:
        logger.info("Looking up address %s", address_line_1)
        address = db.query(Address).filter_by(address_line_1=address_line_1).first()
        return address.id if address else None
    except NoResultFound:
        logger.warning("Address '%s' not found in the database.", address_line_1)
        return None

def parse_individuals(db: Session, df: pd.DataFrame):
    """Parse and load individuals from dataframe into database."""
    try:
        for _, row in df.iterrows():
            # Use get_safe_value() to safely fetch values from DataFrame rows
            primary_address_line_1 = get_safe_value(row, 'primary_address')
            
            # Skip rows missing mandatory fields

            first_name = get_safe_value(row, 'first_name')
            middle_name = get_safe_value(row, 'middle_name')
            last_name = get_safe_value(row, 'last_name')
            secondary_address_line_1 = get_safe_value(row, 'secondary_address')
            masked_ssn = get_safe_value(row, 'masked_ssn')
            dob = get_safe_value(row, 'dob')
            passport = get_safe_value(row, 'passport')
            passport_expiry_date = get_safe_value(row, 'passport_expiry_date')
            primary_contact_number = get_safe_value(row, 'primary_contact_number')
            additional_phone_number_1 = get_safe_value(row, 'additional_phone_number_1')
            additional_phone_number_2 = get_safe_value(row, 'additional_phone_number_2')
            primary_email_address = get_safe_value(row, 'primary_email_address')

            # Lookup Address ID
            primary_address_id = get_address_id(db, primary_address_line_1)
            secondary_address_id = get_address_id(db, secondary_address_line_1) if secondary_address_line_1 else None

            # Lookup Bank by bank account number
            bank_account_number = get_safe_value(row, 'bank_account_number')
            bank_account = None
            if bank_account_number:
                bank_account = db.query(BankAccount).filter_by(
                    bank_account_number=bank_account_number).first()

            # Check for existing records
            individual = db.query(Individual).filter_by(masked_ssn = masked_ssn).one_or_none()

            full_name = " ".join(filter(None, [part.strip() if part else None for part in [first_name , middle_name , last_name]]))

            if individual:
                # Update existing records
                logger.info("Updating existing individual: %s", full_name)
                individual.first_name = first_name
                individual.middle_name = middle_name
                individual.last_name = last_name
                individual.full_name = full_name
                individual.primary_address_id = primary_address_id
                individual.secondary_address_id = secondary_address_id
                individual.masked_ssn = masked_ssn
                individual.dob = dob
                individual.passport = passport
                individual.passport_expiry_date = pd.to_datetime(passport_expiry_date) if pd.notna(passport_expiry_date) else None
                individual.primary_contact_number = primary_contact_number
                individual.additional_phone_number_1 = additional_phone_number_1 if pd.notna(additional_phone_number_1) else None
                individual.additional_phone_number_2 = additional_phone_number_2 if pd.notna(additional_phone_number_2) else None
                individual.primary_email_address = primary_email_address
                individual.is_active = True
                individual.modified_by = SUPERADMIN_USER_ID
                individual.bank_account = bank_account if bank_account else None
            else:
                # Insert new ones
                logger.info("Inserting new individual: %s", first_name)
                individual = Individual(
                    first_name=first_name,
                    middle_name=middle_name if pd.notna(middle_name) else None,
                    last_name=last_name,
                    primary_address_id=primary_address_id,
                    secondary_address_id=secondary_address_id,
                    masked_ssn=masked_ssn,
                    dob=dob,
                    passport=passport,
                    passport_expiry_date=pd.to_datetime(passport_expiry_date) if pd.notna(passport_expiry_date) else None,
                    full_name=full_name,
                    primary_contact_number=primary_contact_number,
                    additional_phone_number_1=additional_phone_number_1 if pd.notna(additional_phone_number_1) else None,
                    additional_phone_number_2=additional_phone_number_2 if pd.notna(additional_phone_number_2) else None,
                    primary_email_address=primary_email_address,
                    is_active=True,
                    created_by=SUPERADMIN_USER_ID,
                    created_on=datetime.now(),
                    bank_account=bank_account if bank_account else None,
                )
                db.add(individual)
            db.flush()

            logger.info("Individual '%s' added to the database.", first_name)

            individual_owner = medallion_service.get_medallion_owner(db=db , individual_id=individual.id)
            if individual_owner:
                individual_owner.individual_id = individual.id
                individual_owner.medallion_owner_type = 'I'
                individual_owner.primary_phone = primary_contact_number
                individual_owner.primary_email_address = primary_email_address
                individual_owner.primary_address_id = primary_address_id
                individual_owner.medallion_owner_status = "Y"
                individual_owner.modified_by = SUPERADMIN_USER_ID
            else:
                individual_owner = MedallionOwner(
                    medallion_owner_type='I',
                    primary_phone=primary_contact_number,
                    primary_email_address=primary_email_address,
                    primary_address_id=primary_address_id,
                    individual_id=individual.id,
                    medallion_owner_status="Y",
                    is_active=True,
                    created_by=SUPERADMIN_USER_ID
                )
            
                db.add(individual_owner)
            db.flush()

            logger.info("Individual_owenr '%s' added to the database.", first_name)

        db.commit()
        logger.info("✅ Data successfully processed.")
    except Exception as e:
        db.rollback()
        logger.error("Error parsing data: %s", e)
        raise
    finally:
        db.close()

if __name__ == "__main__":
    logger.info("Loading Individual information")
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bat_file_key))
    individual_df = pd.read_excel(excel_file, 'Individual')
    parse_individuals(db_session, individual_df)