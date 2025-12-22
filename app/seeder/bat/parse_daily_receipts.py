import pandas as pd
from sqlalchemy.orm import Session

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.ledger.models import DailyReceipt , LedgerEntry
from app.ledger.schemas import LedgerSourceType

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

receipt_number = 0

def parse_daily_receipts(db: Session, df: pd.DataFrame):
    """Parse daily receipts"""
    try:
        for _, row in df.iterrows():
            # Convert date strings to datetime objects
            driver_id = row.get('driver_id')
            vehicle_id = row.get('vehicle_id')
            medallion_id = row.get('medallion_id')
            lease_id = row.get('lease_id')
            period_start = row.get('period_start')
            period_end = row.get('period_end')
            cc_earnings = row.get('cc_earnings')
            cash_earnings = row.get('cash_earnings')
            tips = row.get('tips')
            lease_due = row.get('lease_due')
            ezpass_due = row.get('ezpass_due')
            pvb_due = row.get('pvb_due')
            manual_fee = row.get('manual_fee')
            incentives = row.get("incentives")
            cash_paid = row.get('cash_paid')
            balance = row.get('balance')
            status = row.get('status')

            def convert_date(date_str):
                if pd.notnull(date_str) and isinstance(date_str, pd.Timestamp):
                    try:
                        return date_str.to_pydatetime()
                    except ValueError:
                        logger.warning("Invalid date format: %s. Skipping.", date_str)
                        return None
                return None
            
            period_start = convert_date(period_start)
            period_end = convert_date(period_end)
            
            global receipt_number
            receipt_number += 1

            dtr = DailyReceipt(
                driver_id=driver_id,
                vehicle_id=vehicle_id,
                medallion_id=medallion_id,
                lease_id=lease_id,
                receipt_number = str(receipt_number).zfill(12),
                period_start=period_start,
                period_end=period_end,
                cc_earnings=cc_earnings,
                cash_earnings=cash_earnings,
                tips=tips,
                lease_due=lease_due,
                ezpass_due=ezpass_due,
                pvb_due=pvb_due,
                curb_due = 0,
                manual_fee=manual_fee,
                incentives=incentives,
                cash_paid=cash_paid,
                balance=balance,
                status=status
            )
            db.add(dtr)
            db.flush()

            ledger = LedgerEntry(
                driver_id=driver_id,
                vehicle_id=vehicle_id,
                medallion_id=medallion_id,
                amount = float(cash_paid or 0) + float(balance or 0),
                debit = True ,
                description = f"Daily Receipt for Driver : {driver_id} , Vehicle : {vehicle_id} , Medallion : {medallion_id}",
                source_type = LedgerSourceType.DTR,
                source_id = dtr.id,
                created_by = SUPERADMIN_USER_ID
            )
            db.add(ledger)
            db.flush()
            dtr.ledger_snapshot_id = ledger.id
            logger.info("Processed daily receipt for driver: %s", driver_id)
        db.commit()
            
    except Exception as e:
        logger.error("Error parsing daily receipts: %s", e)
        db.rollback()
        raise

if __name__ == "__main__":
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bat_file_key))
    daily_receipts_df = pd.read_excel(excel_file, 'daily_receipts')
    parse_daily_receipts(db_session, daily_receipts_df)
    db_session.close()

