# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.db import SessionLocal
from app.core.config import settings
from app.utils.logger import get_logger
from app.users.models import Role, User, user_role_association
from app.bpm.models import SLA, CaseStepConfig
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def parse_slas(db: Session, df: pd.DataFrame):
    """Parse SLAs"""
    try:
        for _, row in df.iterrows():
            step_id = row['step_id']
            user_name = row['user_name'] if pd.notna(row['user_name']) else None
            role_name = row['role_name']if pd.notna(row['role_name']) else None
            time_limit = row['time_limit']
            escalation_level = row['escalation_level']

            # Fetch the CaseStepConfig by step_id
            case_step_config = db.query(CaseStepConfig).filter(
                CaseStepConfig.step_id == step_id).first()

            if not case_step_config:
                logger.info(
                    "CaseStepConfig with step_id %s not found. Skipping this entry.", step_id)
                continue

            # Find user by name if provided
            user = None
            if user_name:
                user = db.query(User).filter(
                    User.first_name == user_name).first()
                if not user:
                    logger.info(
                        "User %s not found. Skipping this entry.", user_name)
                    continue

            # Find role by name if provided
            role = None
            if role_name:
                role = db.query(Role).filter(Role.name == role_name).first()
                if not role:
                    logger.info(
                        "Role %s not found. Skipping this entry.", role_name)
                    continue

            # Check if user and role association exists
            if user and role:
                association = db.query(user_role_association).filter_by(
                    user_id=user.id, role_id=role.id).first()
                if not association:
                    logger.info(
                        "No association found for User %s and Role %s. Skipping this entry.",
                        user_name, role_name)
                    continue
            # Check if the SLA already exists for the given step_id, user_id, or role_id
            existing_sla = db.query(SLA).filter(
                SLA.case_step_config_id == case_step_config.id,
                SLA.user_id == (user.id if user else None),
                SLA.role_id == (role.id if role else None)
            ).first()

            if existing_sla:
                # Update the existing SLA if found
                logger.info(
                    "Updating existing SLA for step_id %s with new time_limit %s.",
                    step_id, time_limit)
                existing_sla.time_limit = time_limit
                # Updated to the new escalation level
                existing_sla.escalation_level = escalation_level
                db.flush()
            else:
                # Create a new SLA if it does not exist
                sla = SLA(
                    name=f"SLA for {step_id} with {time_limit}",
                    case_step_config_id=case_step_config.id,
                    time_limit=time_limit,
                    escalation_level=escalation_level,
                    user_id=user.id if user else None,
                    role_id=role.id if role else None
                )
                try:
                    # Add and commit the SLA record to the session
                    db.add(sla)
                    db.flush()
                    logger.info(
                        "SLA successfully created for step_id %s with time_limit %s.",
                        step_id, time_limit)
                except IntegrityError:
                    db.rollback()
                    logger.info(
                        "Integrity error while processing SLA for step_id %s. Skipping this entry.",
                        step_id)
                except Exception as e:
                    db.rollback()
                    logger.info("Error occurred: %s. Skipping this entry.", e)
        db.commit()
    except Exception as e:
        logger.error("Error parsing SLAs: %s", e)
        raise


if __name__ == "__main__":
    logger.info("Loading Case Step SLA configuration")
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bpm_file_key))
    case_sla_df = pd.read_excel(excel_file, 'SLA')
    parse_slas(db_session, case_sla_df)
