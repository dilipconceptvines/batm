# Third party imports
import pandas as pd
from sqlalchemy.orm import Session

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.bpm.models import CaseStep, CaseType
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def parse_case_step(db: Session, df: pd.DataFrame):
    """Parse case step"""
    try:
        for _, row in df.iterrows():
            # Fetch the related CaseType by case_type_prefix
            case_type = db.query(CaseType).filter_by(
                prefix=row['case_type_prefix']).first()
            if not case_type:
                logger.info(
                    "CaseType '%s' not found. Skipping row.", row['case_type_prefix']
                )
                continue

            # Check if CaseStep with the same name already exists
            logger.info("Checking '%s'", row['name'])
            case_step = db.query(CaseStep).filter_by(name=row['name']).first()
            if case_step:
                logger.info("CaseStep '%s' already exists. Updating.", row['name'])
                case_step.name = row['name']
                case_step.case_type_id = case_type.id
                case_step.weight = row['weight']
                case_step.created_by = SUPERADMIN_USER_ID
            else:
                logger.info("CaseStep '%s' does not exist. Creating.", row['name'])
                case_step = CaseStep(name=row['name'], case_type_id=case_type.id, weight=row['weight'], created_by=SUPERADMIN_USER_ID)
                db.add(case_step)

        # Commit the session after adding all new case types
        db.commit()
    except Exception as e:
        logger.error("Error parsing case step: %s", e)
        raise


if __name__ == "__main__":
    logger.info("Loading Case Step configuration")
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bpm_file_key))
    case_step_df = pd.read_excel(excel_file, 'CaseStep')
    parse_case_step(db=db_session, df=case_step_df)
