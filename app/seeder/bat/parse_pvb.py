import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoResultFound, IntegrityError
from datetime import datetime

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.utils.general import get_safe_value
from app.utils.s3_utils import s3_utils
from app.pvb.services import pvb_service
from app.curb.services import curb_service
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df
from app.seeder_loader.parser_registry import parser
import tempfile
import os

logger = get_logger(__name__)

SUPERADMIN_USER_ID = 1

@parser(
    name="pvb",
    sheet_names=[data_loader_settings.parser_pvb_sheet],
    version="1.0",
    deprecated=False,
    description="Process pvb from Excel sheet"
)
def parse_pvb(db:Session , df: pd.DataFrame) -> ParseResult:
    """Parse PVB data"""
    result = ParseResult(sheet_name=data_loader_settings.parser_pvb_sheet)
    created_count = 0
    updated_count = 0 # PVB import might be insert-only or upsert. Assuming insert for now based on append.

    try:
        pvb_data = []
        
        for idx, row in df.iterrows():
            try:
                pvb = {}
                plate = get_safe_value(row, 'PLATE')
                if not plate:
                     plate = "TN0001"
                     logger.info("plate number is empty defaulting to %s" , plate) # Changed logging to info/warning
                
                pvb['PLATE'] = plate
                
                cab_number = get_safe_value(row, 'PLATE') # Check original uses row.get('PLATE') again for trip lookup?
                # Original: trip = curb_service.get_curb_trip(db=db , cab_number= row.get('PLATE'))
                # If row.get('PLATE') was empty/None, it used "TN0001" for pvb dict but original code used `row.get('PLATE')` for trip lookup which might be None.
                # I'll stick to using the `plate` value derived. Or the original raw value?
                # Original code:
                # pvb['PLATE'] = row.get('PLATE') if row.get("PLATE") else "TN0001"
                # trip = curb_service.get_curb_trip(db=db , cab_number= row.get('PLATE')) <-- raw value
                # If raw is None, get_curb_trip(None) -> None.
                
                trip = curb_service.get_curb_trip(db=db , cab_number= row.get('PLATE'))
                
                pvb['STATE'] = get_safe_value(row, 'STATE')
                pvb['TYPE'] = get_safe_value(row, 'TYPE')
                pvb['TERMINATED'] = get_safe_value(row, 'TERMINATED')
                pvb['SUMMONS'] = get_safe_value(row, 'SUMMONS')
                pvb['NON PROGRAM'] = get_safe_value(row, 'NON PROGRAM')
                pvb['ISSUE DATE'] = str(trip.start_date if trip else datetime.today().date())
                pvb['ISSUE TIME'] = get_safe_value(row, 'ISSUE TIME')
                pvb['SYS ENTRY'] = datetime.today().date()
                pvb["NEW ISSUE"] = get_safe_value(row, 'NEW ISSUE')
                pvb['VC'] = get_safe_value(row, 'VC')
                pvb['HEARING IND'] = get_safe_value(row, 'HEARING IND')
                pvb["PENALTY WARNING"] = get_safe_value(row, 'PENALTY WARNING')
                pvb['JUDGMENT'] = get_safe_value(row, 'JUDGMENT')
                pvb['FINE'] = get_safe_value(row, 'FINE')
                pvb['PENALTY'] = get_safe_value(row, 'PENALTY')
                pvb['INTEREST'] = get_safe_value(row, 'INTEREST')
                pvb['REDUCTION'] = get_safe_value(row, 'REDUCTION')
                pvb['PAYMENT'] = get_safe_value(row, 'PAYMENT')
                pvb['NG PMT'] = get_safe_value(row, 'NG PMT')
                pvb["AMOUNT DUE"] = get_safe_value(row, 'AMOUNT DUE')
                pvb['VIO COUNTY'] = get_safe_value(row, 'VIO COUNTY')
                pvb['FRONT OR OPP'] = get_safe_value(row, 'FRONT OR OPP')
                pvb['HOUSE NUMBER'] = get_safe_value(row, 'HOUSE NUMBER')
                pvb['STREET NAME'] = get_safe_value(row, 'STREET NAME')
                pvb['INTERSECT STREET'] = get_safe_value(row, 'INTERSECT STREET')
                pvb['GEO LOC'] = get_safe_value(row, 'GEO LOC')
                pvb['STREET CODE1'] = get_safe_value(row, 'STREET CODE1')
                pvb['STREET CODE2'] = get_safe_value(row, 'STREET CODE2')
                pvb['STREET CODE3'] = get_safe_value(row, 'STREET CODE3')
    
                pvb_data.append(pvb)
                created_count += 1
                result.record_inserted(idx) # Optimistically record as inserted since we do bulk import later.
            except Exception as row_error:
                logger.exception("Error parsing PVB row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        if pvb_data:
            pvb_service.import_pvb(db=db , rows=pvb_data)
            db.flush()
        return result
    except Exception as e:
        logger.exception("Critical failure in parser pvb: %s", e)
        raise RuntimeError(f"Parser pvb failed: {e}") from e

if __name__ == "__main__":
    logger.info("Loading pvb configuration")
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
        data_df = pd.read_excel(excel_file, "pvb")

        result = parse_pvb(db_session, data_df)
        
        # Apply results
        updated_df = apply_parse_result_to_df(data_df, result)

        # Write back to temp file
        with pd.ExcelWriter(
            tmp_file_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            updated_df.to_excel(writer, sheet_name="pvb", index=False)
            
        # Upload back to S3
        with open(tmp_file_path, 'rb') as f:
            s3_utils.upload_file(f, settings.bat_file_key)

        db_session.commit()
        logger.info("Pvb committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing pvb: %s", e)
        raise
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        db_session.close()