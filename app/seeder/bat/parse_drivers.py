# Standard library imports
from datetime import datetime

# Third party imports
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.entities.services import entity_service
from app.utils.s3_utils import s3_utils
from app.entities.models import Address, BankAccount
from app.drivers.models import DMVLicense, Driver, TLCLicense
from app.utils.general import get_safe_value , get_random_routing_number

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1


def parse_date(date_val):
    """
    Parses a date value into a datetime object.
    If the value is invalid or empty, returns None.
    """
    try:
        return date_val.to_pydatetime() if pd.notnull(date_val) else None
    except Exception:
        return None


def parse_drivers(db: Session, df: pd.DataFrame):
    """Parse and load drivers from dataframe into database."""
    try:
        # Clean column names and replace NaNs
        df.columns = df.columns.str.strip().str.lower()
        df = df.replace({np.nan: None})

        # Operation counters
        created_drivers = 0
        updated_drivers = 0
        created_dmv_licenses = 0
        updated_dmv_licenses = 0
        created_tlc_licenses = 0
        updated_tlc_licenses = 0
        created_addresses = 0
        updated_addresses = 0
        created_bank_accounts = 0
        updated_bank_accounts = 0

        for _, row in df.iterrows():
            # Use get_safe_value() to safely fetch values from DataFrame rows
            driver_id = get_safe_value(row, "driver_id")
            first_name = get_safe_value(row, "first_name")
            
            # Skip rows missing mandatory fields
            if not driver_id:
                logger.warning("Skipping row with missing driver_id")
                continue

            # Check for existing records
            driver = db.query(Driver).filter(Driver.driver_id == driver_id).first()

            if not driver:
                # Insert new ones
                logger.info("Adding new driver with ID: %s", driver_id)
                driver = Driver(
                    driver_id=driver_id,
                    is_active=True,
                    created_by=SUPERADMIN_USER_ID,
                    created_on=datetime.now()
                )
                db.add(driver)
                db.flush()
                created_drivers += 1
            else:
                # Update existing records
                logger.info("Updating existing driver with ID: %s", driver_id)
                updated_drivers += 1

            # Update Driver personal details
            driver.first_name = first_name
            driver.middle_name = get_safe_value(row, "middle_name")
            driver.last_name = get_safe_value(row, "last_name")
            driver.ssn = get_safe_value(row, "ssn")
            driver.full_name = " ".join(filter(None, [part.strip() if part else None for part in [driver.first_name, driver.middle_name, driver.last_name]]))
            driver.dob = parse_date(get_safe_value(row, "dob"))
            driver.phone_number_1 = get_safe_value(row, "phone_number_1")
            driver.phone_number_2 = get_safe_value(row, "phone_number_2")
            driver.email_address = get_safe_value(row, "email_address")
            driver.driver_status = get_safe_value(row, "driver_status")
            driver.drive_locked = get_safe_value(row, "driver_locked") or False

            # DMV License details
            dmv_license = driver.dmv_license or DMVLicense()
            dmv_license.dmv_license_number = get_safe_value(row, "dmv_license_number")
            dmv_license.dmv_license_issued_state = get_safe_value(row, "dmv_license_issued_state")
            dmv_license.is_dmv_license_active = get_safe_value(row, "is_dmv_license_active") == "True"
            dmv_license.dmv_license_expiry_date = parse_date(get_safe_value(row, "dmv_license_expiry_date"))

            if not driver.dmv_license:
                db.add(dmv_license)
                driver.dmv_license = dmv_license
                created_dmv_licenses += 1
            else:
                updated_dmv_licenses += 1

            # TLC License details
            tlc_license = driver.tlc_license or TLCLicense()
            tlc_license.tlc_license_number = get_safe_value(row, "tlc_license_number")
            tlc_license.tlc_issued_state = get_safe_value(row, "tlc_issued_state")
            tlc_license.is_tlc_license_active = get_safe_value(row, "is_tlc_license_active") == "True"
            tlc_license.tlc_license_expiry_date = parse_date(get_safe_value(row, "tlc_license_expiry_date"))

            if not driver.tlc_license:
                db.add(tlc_license)
                driver.tlc_license = tlc_license
                created_tlc_licenses += 1
            else:
                updated_tlc_licenses += 1

            # Address details
            primary_address_line_1 = get_safe_value(row, "primary_address_line_1")
            if primary_address_line_1:
                primary_address = entity_service.get_address(db=db, address_line_1=primary_address_line_1)
                if not primary_address:
                    primary_address = entity_service.upsert_address(db=db, address_data={"address_line_1": primary_address_line_1})
                    driver.primary_address_id = primary_address.id
                    created_addresses += 1
                else:
                    driver.primary_address_id = primary_address.id
                    updated_addresses += 1

            # Bank Account details
            if get_safe_value(row, "pay_to_mode") == "ACH":
                account_number = get_safe_value(row, "bank_account_number")
                if not account_number:
                    logger.warning("Skipping row with missing bank account number")
                    continue

                if isinstance(account_number, float):
                    account_number = int(account_number)
                # Convert to string and clean
                account_number = str(account_number).strip()
                account_number = "".join(filter(str.isdigit, account_number))

                logger.info("Adding new bank account with number: %s", account_number)
                bank_account = entity_service.get_bank_account(db=db, bank_account_number=account_number) if account_number else None
                if not bank_account:
                    bank_account = BankAccount(
                        bank_account_number=account_number,
                        bank_routing_number=get_random_routing_number(db),
                        bank_account_type = "S",
                        is_active=True,
                        created_by=SUPERADMIN_USER_ID
                    )
                    db.add(bank_account)
                    created_bank_accounts += 1
                else:
                    updated_bank_accounts += 1
                driver.driver_bank_account = bank_account
                driver.pay_to_mode = "ACH"
            else:
                driver.driver_bank_account = None
                driver.pay_to_mode = get_safe_value(row, "pay_to_mode")
                driver.pay_to = driver.full_name

        db.commit()
        logger.info("✅ Data successfully processed.")
        
        return {
            "drivers_created": created_drivers,
            "drivers_updated": updated_drivers,
            "dmv_licenses_created": created_dmv_licenses,
            "dmv_licenses_updated": updated_dmv_licenses,
            "tlc_licenses_created": created_tlc_licenses,
            "tlc_licenses_updated": updated_tlc_licenses,
            "addresses_created": created_addresses,
            "addresses_updated": updated_addresses,
            "bank_accounts_created": created_bank_accounts,
            "bank_accounts_updated": updated_bank_accounts,
        }
    except Exception as e:
        db.rollback()
        logger.error("Error parsing data: %s", e)
        raise
    finally:
        db.close()


if __name__ == "__main__":
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bat_file_key))
    drivers_df = pd.read_excel(excel_file, 'drivers')
    parse_drivers(db_session, drivers_df)
    db_session.close()
