# Third party imports
import pandas as pd
from sqlalchemy.orm import Session

# Local imports
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.core.config import settings
from app.bpm.models import CaseStatus
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def parse_case_status(db: Session, df: pd.DataFrame):
    """
    Parse the case status dataframe and insert into the database

    Args:
        session: The database session
        df: The case status dataframe
    """
    try:
        for _, row in df.iterrows():
            # Check if CaseStatus with the same name already exists
            case_status = db.query(
                CaseStatus).filter_by(name=row['name']).first()
            if case_status:
                case_status.name = row['name']
                case_status.created_by = SUPERADMIN_USER_ID
                logger.info(
                    "CaseStatus '%s' already exists. Updating.", row['name']
                )
            else:
                logger.info(
                    "CaseStatus '%s' does not exist. Adding.", row['name']
                )
                case_status = CaseStatus(
                    name=row['name'], created_by=SUPERADMIN_USER_ID)
                db.add(case_status)

        # Commit the session after adding all new case statuses
        db.commit()
    except Exception as e:
        logger.error("Error parsing case status: %s", e)
        raise


if __name__ == "__main__":
    logger.info("Loading Case Status")
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bpm_file_key))
    case_status_df = pd.read_excel(excel_file, 'CaseStatus')
    parse_case_status(db=db_session, df=case_status_df)
