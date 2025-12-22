# Third party imports
import pandas as pd
from sqlalchemy.orm import Session

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.medallions.models import Medallion , MOLease

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def parse_mo_lease(db: Session, df: pd.DataFrame):
    """Parse medallion owner lease data"""
    try:
        for _, row in df.iterrows():
            medallion_number = row.get('medallion_number')
            contract_start_date = row.get('contract_start_date')
            contract_end_date = row.get('contract_end_date')
            contract_signed_mode = row.get('contract_signed_mode')
            mail_sent_date = row.get('mail_sent_date')
            mail_received_date = row.get('mail_received_date')
            lease_signed_flag = row.get('lease_signed_flag')
            lease_signed_date = row.get('lease_signed_date')
            in_house_lease = row.get('in_house_lease')
            med_active_exemption = row.get('med_active_exemption')
            payee = row.get('payee')

            # Convert dates to Python datetime objects
            def convert_date(date_str):
                if pd.notnull(date_str) and isinstance(date_str, pd.Timestamp):
                    try:
                        return date_str.to_pydatetime()
                    except ValueError:
                        logger.warning("Invalid date format: %s. Skipping.", date_str)
                        return None
                return None
            
            contract_start_date = convert_date(contract_start_date)
            contract_end_date = convert_date(contract_end_date)
            mail_sent_date = convert_date(mail_sent_date)
            mail_received_date = convert_date(mail_received_date)
            lease_signed_date = convert_date(lease_signed_date)

            # Check if medallion exists
            medallion = db.query(Medallion).filter_by(medallion_number=medallion_number).one_or_none()
            if not medallion:
                logger.warning("Medallion '%s' not found. Skipping.", medallion_number)
                continue

            mo_lease = MOLease(
                contract_start_date=contract_start_date,
                contract_end_date=contract_end_date,
                contract_signed_mode=contract_signed_mode,
                mail_sent_date=mail_sent_date,
                mail_received_date=mail_received_date,
                lease_signed_flag=lease_signed_flag,
                lease_signed_date=lease_signed_date,
                in_house_lease=in_house_lease,
                med_active_exemption=med_active_exemption,
                payee=payee,
                is_active=True
            )

            db.add(mo_lease)
            db.flush()  # Ensure the lease is added before linking to medallion

            medallion.mo_leases_id = mo_lease.id
            db.add(medallion)
            logger.info("Processed MO lease for medallion: %s", medallion_number)
            # Commit the changes to the database

        db.commit()
    except Exception as e:
        logger.error("Error parsing MO lease data: %s", e)
        db.rollback()
        raise

if __name__ == "__main__":
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bat_file_key))
    mo_lease_df = pd.read_excel(excel_file, 'mo_lease')
    parse_mo_lease(db_session, mo_lease_df)
    db_session.close()

