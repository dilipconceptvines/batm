# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.bpm.models import CaseStepConfig, CaseStepConfigPath
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def parse_case_step_config_paths(db: Session, df: pd.DataFrame):
    """Parse case step config paths"""
    try:
        for _, row in df.iterrows():
            # Step 1: Find the CaseStepConfig based on step_name
            case_step_config = db.query(CaseStepConfig).filter_by(
                step_name=row['step_name']).first()
            if not case_step_config:
                logger.info(
                    "CaseStepConfig with step_name '%s' not found. Skipping row.",
                    row['step_name']
                )
                continue

            # Step 2: Check if a CaseStepConfigPath entry already exists for this CaseStepConfig
            case_step_config_path = db.query(CaseStepConfigPath).filter_by(
                case_step_config_id=case_step_config.id).first()

            # If no entry exists, create one. Otherwise, update the existing path.
            if not case_step_config_path:
                schema_name = row['schema_name'] if not pd.isna(row['schema_name']) and not pd.isnull(row['schema_name']) else ""
                case_step_config_path = CaseStepConfigPath(
                    case_step_config_id=case_step_config.id, path=schema_name, is_active=True, created_by=SUPERADMIN_USER_ID)
                db.add(case_step_config_path)
                logger.info(
                    "Creating new path for step '%s' with path '%s'",
                    row['step_name'], row['schema_name']
                )
            else:
                if pd.isna(row.get('schema_name')) or pd.isnull(row.get('schema_name')):
                    case_step_config_path.path = ""
                else:
                    case_step_config_path.path = row.get('schema_name', '')
                logger.info(
                    "Updating path for step '%s' to '%s'",
                    row['step_name'], row['schema_name']
                )

            db.commit()

        # Commit all changes
        try:
            db.commit()
            logger.info("All paths have been updated successfully.")
        except IntegrityError:
            db.rollback()
            logger.info(
                "An error occurred while committing changes to CaseStepConfigPath entries.")
        finally:
            db.close()
    except Exception as e:
        logger.error("Error parsing case step config paths: %s", e)
        raise


if __name__ == "__main__":
    logger.info("Loading Case Step configuration paths")
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bpm_file_key))
    case_step_config_files_df = pd.read_excel(excel_file, 'CaseStepConfigFiles')
    parse_case_step_config_paths(db=db_session, df=case_step_config_files_df)
