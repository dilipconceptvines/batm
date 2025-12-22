# Third party imports
import pandas as pd
from sqlalchemy.orm import Session

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.bpm.models import CaseStep, CaseStepConfig, CaseType
from app.users.models import User, Role
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def parse_case_step_config(db: Session, df: pd.DataFrame):
    """Parse case step config"""
    try:
        # Populate CaseStepConfig table
        for _, row in df.iterrows():
            # Fetch the related CaseStep by case_step_name
            case_step = db.query(CaseStep).filter_by(
                name=row['case_step_name']).first()
            if not case_step:
                logger.info(
                    "CaseStep '%s' not found. Skipping row.", row['case_step_name']
                )
                continue

            # Fetch the related CaseType by case_type_prefix
            case_type = db.query(CaseType).filter_by(
                prefix=row['case_type_prefix']).first()
            if not case_step:
                logger.info(
                    "CaseType '%s' not found. Skipping row.", row['case_type_prefix']
                )
                continue

            # Fetch the related User by next_assignee_name
            next_assignee = None
            if pd.notna(row['next_assignee_name']):
                next_assignee = db.query(User).filter_by(
                    first_name=row['next_assignee_name']).first()
                if not next_assignee:
                    logger.info(
                        "User '%s' not found. Skipping row.", row['next_assignee_name']
                    )
                    continue

            # Check if CaseStepConfig with the same step_id exists to avoid duplicates
            case_step_config = db.query(CaseStepConfig).filter_by(
                step_id=row['step_id']).first()

            if case_step_config:
                logger.info(
                    "CaseStepConfig with step_id '%s' and step name '%s' already exists. Updating.",
                    row['step_id'], row['step_name']
                )
                case_step_config.step_id = row['step_id']
                case_step_config.case_step_id = case_step.id
                case_step_config.next_assignee_id = next_assignee.id if next_assignee else None
                case_step_config.next_step_id = str(int(float(
                    row['next_step_id']))) if pd.notna(row['next_step_id']) else ""
                case_step_config.case_type_id = case_type.id
                case_step_config.created_by = SUPERADMIN_USER_ID
                # case_step_config.step_name = row['step_name']
                case_step_config.roles.clear()
            else:
                logger.info(
                    "CaseStepConfig with step_id '%s' and step name '%s' does not exist. Creating.",
                    row['step_id'], row['step_name']
                )
                # Create a new CaseStepConfig instance
                case_step_config = CaseStepConfig(
                    step_id=row['step_id'],
                    case_step_id=case_step.id,
                    next_assignee_id=next_assignee.id if next_assignee else None,
                    next_step_id=str(int(float(
                        row['next_step_id']))) if pd.notna(row['next_step_id']) else "",
                    case_type_id=case_type.id,
                    created_by=SUPERADMIN_USER_ID,
                    step_name=row['step_name']
                )

            # Assign roles based on the user_roles column (comma-separated values)
            role_names = row['user_roles'].split(
                ',') if pd.notna(row['user_roles']) else []

            logger.info(
                "Roles present for step '%s' are '%s'.", row['step_id'], ",".join(
                    role_names)
            )
            for role_name in role_names:
                role_name = role_name.strip()
                role = db.query(Role).filter_by(name=role_name).first()
                if role:
                    case_step_config.roles.append(role)
                    logger.info(
                        "Adding '%s' to step '%s'.", role_name, row['step_id']
                    )
                else:
                    logger.info(
                        "Role '%s' not found. Skipping role assignment for '%s'.",
                        role_name, row['step_id']
                    )

            # Add CaseStepConfig to the session
            db.add(case_step_config)
            db.flush()
        # Commit the session after adding all new case step configurations
        db.commit()
    except Exception as e:
        logger.error("Error parsing case step config: %s", e)
        raise


if __name__ == "__main__":
    logger.info("Loading case step configuration")
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bpm_file_key))
    case_step_config_df = pd.read_excel(excel_file, 'CaseStepConfig')
    parse_case_step_config(db=db_session, df=case_step_config_df)
