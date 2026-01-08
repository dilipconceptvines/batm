from datetime import datetime
# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.entities.models import BankAccount, Address
from app.utils.general import get_safe_value
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df
from app.seeder_loader.parser_registry import parser
import tempfile
import os

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="bank_accounts",
    sheet_names=[data_loader_settings.parser_bank_accounts_sheet],
    version="1.0",
    deprecated=False,
    description="Process bank accounts from Excel sheet"
)
def parse_bank_accounts(db: Session, df: pd.DataFrame) -> ParseResult:
    """Parse and load bank accounts from dataframe into database."""
    result = ParseResult(sheet_name="bank_accounts")
    created_count = 0
    updated_count = 0

    try:
        for idx, row in df.iterrows():
            try:
                bank_name = get_safe_value(row, "bank_name")
                bank_account_number = get_safe_value(row, "bank_account_number")
                bank_address = get_safe_value(row, "bank_address")
                
                if not bank_account_number:
                    logger.warning("Skipping row with missing bank_account_number")
                    result.record_failed(idx, "Missing bank_account_number")
                    continue

                address = None
                if bank_address:
                    address = db.query(Address).filter(
                        Address.address_line_1 == bank_address).first()
                    if not address:
                        address = Address(
                            address_line_1=bank_address,
                            is_active=True,
                            created_by=SUPERADMIN_USER_ID,
                            created_on=datetime.now()
                        )
                        db.add(address)
                        db.flush()
                        
                        logger.info("Address '%s' added to the Address table.", bank_address)

                bank_account = db.query(BankAccount).filter(
                    BankAccount.bank_name == bank_name,
                    BankAccount.bank_account_number == bank_account_number
                ).first()

                if bank_account:
                    logger.info("Updating bank account for '%s' with account number '%s'.", bank_name, bank_account_number)
                    bank_account.bank_account_status = get_safe_value(row, "bank_account_status")
                    bank_account.bank_routing_number = get_safe_value(row, "bank_routing_number")
                    bank_account.bank_address_id = address.id if address else None
                    updated_count += 1
                    result.record_updated(idx)
                else:
                    logger.info("Adding new bank account for '%s' with account number '%s'.", bank_name, bank_account_number)
                    new_bank_account = BankAccount(
                        bank_name=bank_name,
                        bank_account_number=bank_account_number,
                        bank_account_status=get_safe_value(row, "bank_account_status"),
                        bank_routing_number=get_safe_value(row, "bank_routing_number"),
                        bank_address_id=address.id if address else None,
                        is_active=True,
                        created_by=SUPERADMIN_USER_ID,
                        created_on=datetime.now()
                    )
                    db.add(new_bank_account)
                    created_count += 1
                    result.record_inserted(idx)

                    logger.info("Bank account '%s' with account number '%s' added to the database.", bank_name, bank_account_number)
                
                db.flush()
            except Exception as row_error:
                logger.exception("Error parsing bank account row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        logger.info("✅ Data successfully processed.")
        result.metadata = {
            "created": created_count,
            "updated": updated_count
        }
        return result
    except Exception as e:
        logger.exception("Critical failure in parser bank_accounts: %s", e)
        raise RuntimeError(f"Parser bank_accounts failed: {e}") from e


if __name__ == "__main__":
    logger.info("Loading bank accounts configuration")
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
        data_df = pd.read_excel(excel_file, "bank_accounts")

        result = parse_bank_accounts(db_session, data_df)
        
        # Apply results
        updated_df = apply_parse_result_to_df(data_df, result)
        
        # Write back to temp file
        with pd.ExcelWriter(
            tmp_file_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            updated_df.to_excel(writer, sheet_name="bank_accounts", index=False)
            
        # Upload back to S3
        with open(tmp_file_path, 'rb') as f:
            s3_utils.upload_file(f, settings.bat_file_key)

        db_session.commit()
        logger.info("Bank accounts committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing bank accounts: %s", e)
        raise
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        db_session.close()