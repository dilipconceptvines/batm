import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.utils.general import parse_date
from app.ledger.models import DailyReceipt , LedgerEntry
from app.ledger.schemas import LedgerSourceType
from app.seeder_loader.parser_registry import parser
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df
import tempfile
import os

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

receipt_number = 0

@parser(
    name="daily_receipts",
    sheet_names=[data_loader_settings.parser_daily_receipts_sheet],
    version="1.0",
    deprecated=False,
    description="Process daily receipts from Excel sheet"
)
def parse_daily_receipts(db: Session, df: pd.DataFrame) -> ParseResult:
    """Parse daily receipts"""
    result = ParseResult(sheet_name=data_loader_settings.parser_daily_receipts_sheet)
    created_count = 0
    updated_count = 0

    try:
        global receipt_number
        for idx, row in df.iterrows():
            try:
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
                
                period_start = parse_date(period_start)
                period_end = parse_date(period_end)
                
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
                created_count += 1
                result.record_inserted(idx)
                logger.info("Processed daily receipt for driver: %s", driver_id)
            
            except Exception as row_error:
                logger.exception("Error parsing daily receipt row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))
        
        logger.info("✅ Data successfully processed.")
        return result
            
    except Exception as e:
        logger.exception("Critical failure in parser daily_receipts: %s", e)
        raise RuntimeError(f"Parser daily_receipts failed: {e}") from e

if __name__ == "__main__":
    logger.info("Loading daily receipts configuration")
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
        data_df = pd.read_excel(excel_file, "daily_receipts")

        # This parser also returns None and needs refactoring to return ParseResult
        result = parse_daily_receipts(db_session, data_df)
        
        if result:
            updated_df = apply_parse_result_to_df(data_df, result)

            # Write back to temp file
            with pd.ExcelWriter(
                tmp_file_path,
                engine="openpyxl",
                mode="a",
                if_sheet_exists="replace"
            ) as writer:
                updated_df.to_excel(writer, sheet_name="daily_receipts", index=False)
                
            # Upload back to S3
            with open(tmp_file_path, 'rb') as f:
                s3_utils.upload_file(f, settings.bat_file_key)

        db_session.commit()
        logger.info("Daily receipts committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing daily receipts: %s", e)
        raise
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        db_session.close()

