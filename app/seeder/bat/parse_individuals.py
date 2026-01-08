from datetime import datetime
# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoResultFound, IntegrityError

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.entities.models import Address, Individual, BankAccount
from app.medallions.models import MedallionOwner
from app.medallions.services import medallion_service
from app.utils.general import get_safe_value
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df
from app.seeder_loader.parser_registry import parser
import tempfile
import os

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

@parser(
    name="individuals",
    sheet_names=[data_loader_settings.parser_individuals_sheet],
    version="1.0",
    deprecated=False,
    description="Process individuals from Excel sheet"
)
def parse_individuals(db: Session, df: pd.DataFrame) -> ParseResult:
    """Parse and load individuals from dataframe into database."""
    result = ParseResult(sheet_name="Individual")
    try:
        for idx, row in df.iterrows():
            try:
                # Use get_safe_value() to safely fetch values from DataFrame rows
                primary_address_line_1 = get_safe_value(row, "primary_address")

                first_name = get_safe_value(row, "first_name")
                middle_name = get_safe_value(row, "middle_name")
                last_name = get_safe_value(row, "last_name")
                secondary_address_line_1 = get_safe_value(row, "secondary_address")
                masked_ssn = get_safe_value(row, "ssn")
                dob = get_safe_value(row, "dob")
                passport = get_safe_value(row, "passport")
                passport_expiry_date = get_safe_value(row, "passport_expiry_date")
                primary_contact_number = get_safe_value(row, "primary_contact_number")
                additional_phone_number_1 = get_safe_value(row, "additional_phone_number_1")
                additional_phone_number_2 = get_safe_value(row, "additional_phone_number_2")
                primary_email_address = get_safe_value(row, "primary_email_address")

                if not masked_ssn:
                    logger.warning("Skipping row with missing masked_ssn")
                    result.record_failed(idx, "Missing masked_ssn")
                    continue

                # Lookup Address ID
                primary_address_id = get_address_id(db, primary_address_line_1)
                secondary_address_id = (
                    get_address_id(db, secondary_address_line_1) if secondary_address_line_1 else None
                )

                # Lookup Bank by bank account number
                bank_account_number = get_safe_value(row, "bank_account_number")
                bank_account = None
                if bank_account_number:
                    bank_account = db.query(BankAccount).filter_by(
                        bank_account_number=bank_account_number
                    ).first()

                # Check for existing records
                individual = db.query(Individual).filter_by(masked_ssn=masked_ssn).one_or_none()

                full_name = " ".join(
                    filter(None, [part.strip() if part else None for part in [first_name, middle_name, last_name]])
                )

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
                    individual.passport_expiry_date = (
                        pd.to_datetime(passport_expiry_date) if pd.notna(passport_expiry_date) else None
                    )
                    individual.primary_contact_number = primary_contact_number
                    individual.additional_phone_number_1 = (
                        additional_phone_number_1 if pd.notna(additional_phone_number_1) else None
                    )
                    individual.additional_phone_number_2 = (
                        additional_phone_number_2 if pd.notna(additional_phone_number_2) else None
                    )
                    individual.primary_email_address = primary_email_address
                    individual.is_active = True
                    individual.modified_by = SUPERADMIN_USER_ID
                    individual.bank_account = bank_account if bank_account else None
                    result.record_updated(idx)
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
                        passport_expiry_date=pd.to_datetime(passport_expiry_date)
                        if pd.notna(passport_expiry_date)
                        else None,
                        full_name=full_name,
                        primary_contact_number=primary_contact_number,
                        additional_phone_number_1=additional_phone_number_1
                        if pd.notna(additional_phone_number_1)
                        else None,
                        additional_phone_number_2=additional_phone_number_2
                        if pd.notna(additional_phone_number_2)
                        else None,
                        primary_email_address=primary_email_address,
                        is_active=True,
                        created_by=SUPERADMIN_USER_ID,
                        created_on=datetime.now(),
                        bank_account=bank_account if bank_account else None,
                    )
                    db.add(individual)
                    result.record_inserted(idx)
                db.flush()

                logger.info("Individual '%s' added to the database.", first_name)

                individual_owner = medallion_service.get_medallion_owner(db=db, individual_id=individual.id)
                if individual_owner:
                    individual_owner.individual_id = individual.id
                    individual_owner.medallion_owner_type = "I"
                    individual_owner.primary_phone = primary_contact_number
                    individual_owner.primary_email_address = primary_email_address
                    individual_owner.primary_address_id = primary_address_id
                    individual_owner.medallion_owner_status = "Y"
                    individual_owner.modified_by = SUPERADMIN_USER_ID
                else:
                    individual_owner = MedallionOwner(
                        medallion_owner_type="I",
                        primary_phone=primary_contact_number,
                        primary_email_address=primary_email_address,
                        primary_address_id=primary_address_id,
                        individual_id=individual.id,
                        medallion_owner_status="Y",
                        is_active=True,
                        created_by=SUPERADMIN_USER_ID,
                    )

                    db.add(individual_owner)
                db.flush()

                logger.info("Individual_owenr '%s' added to the database.", first_name)
            except Exception as row_error:
                logger.exception("Error parsing individual row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        logger.info("✅ Data successfully processed.")
        return result
    except Exception as e:
        logger.exception("Critical failure in parser individuals: %s", e)
        raise RuntimeError(f"Parser individuals failed: {e}") from e

if __name__ == "__main__":
    logger.info("Loading individuals configuration")
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
        data_df = pd.read_excel(excel_file, "Individual")

        result = parse_individuals(db_session, data_df)
        
        # Apply results
        updated_df = apply_parse_result_to_df(data_df, result)

        # Write back to temp file
        with pd.ExcelWriter(
            tmp_file_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            updated_df.to_excel(writer, sheet_name="Individual", index=False)
            
        # Upload back to S3
        with open(tmp_file_path, 'rb') as f:
            s3_utils.upload_file(f, settings.bat_file_key)

        db_session.commit()
        logger.info("Individuals committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing individuals: %s", e)
        raise
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        db_session.close()