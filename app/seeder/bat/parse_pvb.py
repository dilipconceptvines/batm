import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoResultFound
from datetime import datetime

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.pvb.services import pvb_service
from app.curb.services import curb_service

logger = get_logger(__name__)

SUPERADMIN_USER_ID = 1

def parse_pvb(db:Session , df: pd.DataFrame):
    """Parse PVB data"""
    try:
        pvb_data = []
        
        for _, row in df.iterrows():
            pvb = {}

            pvb['PLATE'] = row.get('PLATE') if row.get("PLATE") else "TN0001"
            if not pvb['PLATE']:
                logger.info("plate number is empty %s" , row.get("PLATE"))
            trip = curb_service.get_curb_trip(db=db , cab_number= row.get('PLATE'))
            pvb['STATE'] = row.get('STATE')
            pvb['TYPE'] = row.get('TYPE')
            pvb['TERMINATED'] = row.get('TERMINATED')
            pvb['SUMMONS'] = row.get('SUMMONS')
            pvb['NON PROGRAM'] = row.get('NON PROGRAM')
            pvb['ISSUE DATE'] = str(trip.start_date if trip else datetime.today().date())
            pvb['ISSUE TIME'] = row.get('ISSUE TIME')
            pvb['SYS ENTRY'] = datetime.today().date()
            pvb["NEW ISSUE"] = row.get('NEW ISSUE')
            pvb['VC'] = row.get('VC')
            pvb['HEARING IND'] = row.get('HEARING IND')
            pvb["PENALTY WARNING"] = row.get('PENALTY WARNING')
            pvb['JUDGMENT'] = row.get('JUDGMENT')
            pvb['FINE'] = row.get('FINE')
            pvb['PENALTY'] = row.get('PENALTY')
            pvb['INTEREST'] = row.get('INTEREST')
            pvb['REDUCTION'] = row.get('REDUCTION')
            pvb['PAYMENT'] = row.get('PAYMENT')
            pvb['NG PMT'] = row.get('NG PMT')
            pvb["AMOUNT DUE"] = row.get('AMOUNT DUE')
            pvb['VIO COUNTY'] = row.get('VIO COUNTY')
            pvb['FRONT OR OPP'] = row.get('FRONT OR OPP')
            pvb['HOUSE NUMBER'] = row.get('HOUSE NUMBER')
            pvb['STREET NAME'] = row.get('STREET NAME')
            pvb['INTERSECT STREET'] = row.get('INTERSECT STREET')
            pvb['GEO LOC'] = row.get('GEO LOC')
            pvb['STREET CODE1'] = row.get('STREET CODE1')
            pvb['STREET CODE2'] = row.get('STREET CODE2')
            pvb['STREET CODE3'] = row.get('STREET CODE3')

            pvb_data.append(pvb)

        pvb_service.import_pvb(db=db , rows=pvb_data)
        logger.info("PVB data parsed and committed successfully.")
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error("Error parsing PVB data: %s", e)
        raise

if __name__ == "__main__":
    logger.info("Loading PVB information")
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bat_file_key))
    pvb_df = pd.read_excel(excel_file, 'pvb')
    parse_pvb(db_session, pvb_df)