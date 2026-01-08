import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoResultFound, IntegrityError
from datetime import datetime

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.ezpass.services import ezpass_service
from app.curb.services import curb_service
from app.seeder_loader.parser_registry import parser
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df
import tempfile
import os

logger = get_logger(__name__)

SUPERADMIN_USER_ID = 1

@parser(
    name="ezpass",
    sheet_names=[data_loader_settings.parser_ezpass_sheet],
    version="1.0",
    deprecated=False,
    description="Process ezpass from Excel sheet"
)
def parse_ezpass(db: Session, df: pd.DataFrame) -> ParseResult:
    """Parse EZPass data"""
    result = ParseResult(sheet_name=data_loader_settings.parser_ezpass_sheet)
    created_count = 0
    updated_count = 0

    try:
        ezpass_data = []
        for idx, row in df.iterrows():
            try:
                ezpass = {}
                ezpass['TAG/PLATE NUMBER'] = row.get('TAG/PLATE NUMBER')
                trip = curb_service.get_curb_trip(db=db , cab_number= row.get('TAG/PLATE NUMBER'))
    
                # if Trip data is avaliable , then posting date and transaction date should be trip date
                ezpass['POSTING DATE'] = str(trip.start_date if trip else datetime.today().date())
                ezpass['TRANSACTION DATE'] = str(trip.start_date if trip else datetime.today().date())
                ezpass['AGENCY'] = row.get('AGENCY')
                ezpass['ACTIVITY'] = row.get('ACTIVITY')
                ezpass['PLAZA ID'] = row.get('PLAZA ID')
                ezpass['ENTRY TIME'] = row.get('ENTRY TIME')
                ezpass['ENTRY PLAZA'] = row.get('ENTRY PLAZA')
                ezpass['ENTRY LANE'] = row.get('ENTRY LANE')
                ezpass['EXIT TIME'] = row.get('EXIT TIME')
                ezpass['EXIT PLAZA'] = row.get('EXIT PLAZA')
                ezpass['EXIT LANE'] = row.get('EXIT LANE')
                ezpass['VEHICLE TYPE CODE'] = row.get('VEHICLE TYPE CODE')
                ezpass['AMOUNT'] = str(row.get('AMOUNT'))
                ezpass['PREPAID'] = row.get('PREPAID')
                ezpass['PLAN/RATE'] = row.get('PLAN/RATE')
                ezpass['FARE TYPE'] = row.get('FARE TYPE')
                ezpass['BALANCE'] = row.get('BALANCE')
    
                ezpass_data.append(ezpass)
                created_count += 1
                result.record_inserted(idx) # Using inserted as generic success here since it's a bulk process
                
            except Exception as row_error:
                logger.exception("Error parsing ezpass row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        if ezpass_data:
             ezpass_service.process_ezpass_data(db=db , rows=ezpass_data)
        
        logger.info("EZPass data parsed successfully.")
        db.flush()
        return result
    except Exception as e:
        logger.exception("Critical failure in parser ezpass: %s", e)
        raise RuntimeError(f"Parser ezpass failed: {e}") from e

if __name__ == "__main__":
    logger.info("Loading ezpass configuration")
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
        data_df = pd.read_excel(excel_file, "ezpass")

        result = parse_ezpass(db_session, data_df)
        
        # Apply results
        updated_df = apply_parse_result_to_df(data_df, result)

        # Write back to temp file
        with pd.ExcelWriter(
            tmp_file_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            updated_df.to_excel(writer, sheet_name="ezpass", index=False)
            
        # Upload back to S3
        with open(tmp_file_path, 'rb') as f:
            s3_utils.upload_file(f, settings.bat_file_key)

        db_session.commit()
        logger.info("Ezpass committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing ezpass: %s", e)
        raise
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        db_session.close()