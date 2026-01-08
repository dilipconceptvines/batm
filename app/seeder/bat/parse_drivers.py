# Standard library imports
from datetime import datetime

# Third party imports
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.entities.services import entity_service
from app.utils.s3_utils import s3_utils
from app.entities.models import Address, BankAccount
from app.drivers.models import DMVLicense, Driver, TLCLicense
from app.utils.general import get_safe_value , get_random_routing_number , parse_date
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df
from app.seeder_loader.parser_registry import parser
import tempfile
import os

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1


@parser(
    name="drivers",
    sheet_names=[data_loader_settings.parser_drivers_sheet],
    version="1.0",
    deprecated=False,
    description="Process drivers from Excel sheet"
)
def parse_drivers(db: Session, df: pd.DataFrame) -> ParseResult:
    """Parse and load drivers from dataframe into database."""
    result = ParseResult(sheet_name="drivers")
    
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

    try:
        df.columns = df.columns.str.strip().str.lower()
        df = df.replace({np.nan: None})

        for idx, row in df.iterrows():
            try:
                driver_id = get_safe_value(row, "driver_id")
                first_name = get_safe_value(row, "first_name")
                
                if not driver_id:
                    logger.warning("Skipping row with missing driver_id")
                    result.record_failed(idx, "Missing driver_id")
                    continue

                driver = db.query(Driver).filter(Driver.driver_id == driver_id).first()

                if not driver:
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
                    result.record_inserted(idx)
                else:
                    logger.info("Updating existing driver with ID: %s", driver_id)
                    updated_drivers += 1
                    result.record_updated(idx)

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

                if get_safe_value(row, "pay_to_mode") == "ACH":
                    account_number = get_safe_value(row, "bank_account_number")
                    if not account_number:
                        logger.warning("Skipping row with missing bank account number")
                        result.record_failed(idx, "Missing bank account number for ACH")
                        pass
                        continue

                    if isinstance(account_number, float):
                        account_number = int(account_number)
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

                db.flush()
            except Exception as row_error:
                logger.exception("Error parsing driver row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        logger.info("✅ Data successfully processed.")
        result.metadata = {
            "created": created_drivers,
            "updated": updated_drivers,
            "dmv_licenses_created": created_dmv_licenses,
            "dmv_licenses_updated": updated_dmv_licenses,
            "tlc_licenses_created": created_tlc_licenses,
            "tlc_licenses_updated": updated_tlc_licenses,
            "addresses_created": created_addresses,
            "addresses_updated": updated_addresses,
            "bank_accounts_created": created_bank_accounts,
            "bank_accounts_updated": updated_bank_accounts
        }
        return result
    except Exception as e:
        logger.exception("Critical failure in parser drivers: %s", e)
        raise RuntimeError(f"Parser drivers failed: {e}") from e


if __name__ == "__main__":
    logger.info("Loading drivers configuration")
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
        data_df = pd.read_excel(excel_file, "drivers")

        result = parse_drivers(db_session, data_df)
        
        # Apply results
        updated_df = apply_parse_result_to_df(data_df, result)

        # Write back to temp file
        with pd.ExcelWriter(
            tmp_file_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            updated_df.to_excel(writer, sheet_name="drivers", index=False)
            
        # Upload back to S3
        with open(tmp_file_path, 'rb') as f:
            s3_utils.upload_file(f, settings.bat_file_key)

        db_session.commit()
        logger.info("Drivers committed successfully")
        
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing drivers: %s", e)
        raise
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        db_session.close()
