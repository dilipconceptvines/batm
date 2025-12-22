from datetime import datetime
# Third party imports
import pandas as pd
from sqlalchemy.orm import Session

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.entities.models import BankAccount, Address
from app.utils.general import get_safe_value

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def parse_bank_accounts(db: Session, df: pd.DataFrame):
    """Parse and load bank accounts from dataframe into database."""
    try:
        for _, row in df.iterrows():
            # Use get_safe_value() to safely fetch values from DataFrame rows
            bank_name = get_safe_value(row, 'bank_name')
            bank_account_number = get_safe_value(row, 'bank_account_number')
            bank_address = get_safe_value(row, 'bank_address')
            
            # Skip rows missing mandatory fields
            if not bank_account_number:
                logger.warning("Skipping row with bank_account_number")
                continue

            # Lookup or create the address
            address = None
            if bank_address:
                # Check if the address already exists
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

            # Check for existing records
            bank_account = db.query(BankAccount).filter(
                BankAccount.bank_name == bank_name,
                BankAccount.bank_account_number == bank_account_number
            ).first()

            if bank_account:
                # Update existing records
                logger.info("Updating bank account for '%s' with account number '%s'.", bank_name, bank_account_number)
                bank_account.bank_account_status = get_safe_value(row, 'bank_account_status')
                bank_account.bank_routing_number = get_safe_value(row, 'bank_routing_number')
                bank_account.bank_address_id = address.id if address else None
            else:
                # Insert new ones
                logger.info("Adding new bank account for '%s' with account number '%s'.", bank_name, bank_account_number)
                new_bank_account = BankAccount(
                    bank_name=bank_name,
                    bank_account_number=bank_account_number,
                    bank_account_status=get_safe_value(row, 'bank_account_status'),
                    bank_routing_number=get_safe_value(row, 'bank_routing_number'),
                    bank_address_id=address.id if address else None,
                    is_active=True,
                    created_by=SUPERADMIN_USER_ID,
                    created_on=datetime.now()
                )
                db.add(new_bank_account)
                db.flush()

                logger.info("Bank account '%s' with account number '%s' added to the database.", bank_name, bank_account_number)

        db.commit()
        logger.info("✅ Data successfully processed.")
    except Exception as e:
        db.rollback()
        logger.error("Error parsing data: %s", e)
        raise
    finally:
        db.close()


if __name__ == "__main__":
    logger.info("Loading Bank Account information")
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bat_file_key))
    bank_accounts_df = pd.read_excel(excel_file, 'bank_accounts')
    parse_bank_accounts(db_session, bank_accounts_df)