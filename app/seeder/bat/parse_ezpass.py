import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoResultFound
from datetime import datetime

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.ezpass.services import ezpass_service
from app.curb.services import curb_service

logger = get_logger(__name__)

SUPERADMIN_USER_ID = 1

def parse_ezpass(db:Session , df: pd.DataFrame):
    """Parse EZPass data"""
    try:
        ezpass_data = []
        for _, row in df.iterrows():
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

        ezpass_service.process_ezpass_data(db=db , rows=ezpass_data)
        logger.info("EZPass data parsed and committed successfully.")
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error("Error parsing EZPass data: %s", e)
        raise

if __name__ == "__main__":
    logger.info("Loading EZPass information")
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bat_file_key))
    ezpass_df = pd.read_excel(excel_file, 'ezpass')
    parse_ezpass(db_session, ezpass_df)