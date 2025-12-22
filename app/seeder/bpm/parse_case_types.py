# Third party imports
import pandas as pd
from sqlalchemy.orm import Session

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.bpm.models import CaseType
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def parse_case_types(db: Session, df: pd.DataFrame):
    """Parse case types from the given dataframe"""
    try:
        for _, row in df.iterrows():
            # Check if CaseType with the same name already exists
            existing_case_type = db.query(
                CaseType).filter_by(name=row['name']).first()
            if existing_case_type:
                logger.info("CaseType '%s' already exists. Skipping.", row['name'])
                continue

            # Create and add new CaseType if it doesn't exist
            case_type = CaseType(
                name=row['name'], prefix=row['prefix'], created_by=SUPERADMIN_USER_ID)
            db.add(case_type)

        # Commit the session after adding all new case types
        db.commit()
    except Exception as e:
        logger.error("Error parsing case types: %s", e)
        raise


if __name__ == "__main__":
    logger.info("Parsing case types")
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bpm_file_key))
    case_types_df = pd.read_excel(excel_file, 'CaseTypes')
    parse_case_types(db=db_session, df=case_types_df)
