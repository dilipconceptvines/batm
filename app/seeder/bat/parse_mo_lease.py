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
from app.utils.general import parse_date
from app.medallions.models import Medallion , MOLease
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df
from app.seeder_loader.parser_registry import parser
import tempfile
import os

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="mo_lease",
    sheet_names=[data_loader_settings.parser_mo_lease_sheet],
    version="1.0",
    deprecated=False,
    description="Process mo lease from Excel sheet"
)
def parse_mo_lease(db: Session, df: pd.DataFrame) -> ParseResult:
    """Parse medallion owner lease data"""
    result = ParseResult(sheet_name=data_loader_settings.parser_mo_lease_sheet)
    created_count = 0
    updated_count = 0

    try:
        for idx, row in df.iterrows():
            try:
                medallion_number = get_safe_value(row, 'medallion_number')
                contract_start_date = get_safe_value(row, 'contract_start_date')
                contract_end_date = get_safe_value(row, 'contract_end_date')
                contract_signed_mode = get_safe_value(row, 'contract_signed_mode')
                mail_sent_date = get_safe_value(row, 'mail_sent_date')
                mail_received_date = get_safe_value(row, 'mail_received_date')
                lease_signed_flag = get_safe_value(row, 'lease_signed_flag')
                lease_signed_date = get_safe_value(row, 'lease_signed_date')
                in_house_lease = get_safe_value(row, 'in_house_lease')
                med_active_exemption = get_safe_value(row, 'med_active_exemption')
                payee = get_safe_value(row, 'payee')
                
                contract_start_date = parse_date(contract_start_date)
                contract_end_date = parse_date(contract_end_date)
                mail_sent_date = parse_date(mail_sent_date)
                mail_received_date = parse_date(mail_received_date)
                lease_signed_date = parse_date(lease_signed_date)
    
                # Check if medallion exists
                medallion = db.query(Medallion).filter_by(medallion_number=medallion_number).one_or_none()
                if not medallion:
                    logger.warning("Medallion '%s' not found. Skipping.", medallion_number)
                    result.record_failed(idx, f"Medallion {medallion_number} not found")
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
                db.flush()
                created_count += 1
                result.record_inserted(idx)
            except Exception as row_error:
                logger.exception("Error parsing MO lease row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        db.flush()
        return result
    except Exception as e:
        logger.exception("Critical failure in parser mo_lease: %s", e)
        raise RuntimeError(f"Parser mo_lease failed: {e}") from e

if __name__ == "__main__":
    logger.info("Loading mo lease configuration")
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
        data_df = pd.read_excel(excel_file, "mo_lease")

        result = parse_mo_lease(db_session, data_df)
        
        # Apply results
        updated_df = apply_parse_result_to_df(data_df, result)

        # Write back to temp file
        with pd.ExcelWriter(
            tmp_file_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            updated_df.to_excel(writer, sheet_name="mo_lease", index=False)
            
        # Upload back to S3
        with open(tmp_file_path, 'rb') as f:
            s3_utils.upload_file(f, settings.bat_file_key)

        db_session.commit()
        logger.info("Mo lease committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing mo lease: %s", e)
        raise
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        db_session.close()

