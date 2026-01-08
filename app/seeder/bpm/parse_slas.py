# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.db import SessionLocal
from app.core.config import settings
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.users.models import Role, User, user_role_association
from app.bpm.models import SLA, CaseStepConfig
from app.utils.s3_utils import s3_utils
from app.seeder_loader.parser_registry import parser
from app.seeder.parsing_result import ParseResult

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="slas",
    sheet_names=[data_loader_settings.parser_slas_sheet],
    version="1.0",
    deprecated=False,
    description="Process SLAs from Excel sheet"
)
def parse_slas(db: Session, df: pd.DataFrame) -> ParseResult:
    """Parse SLAs"""
    result = ParseResult(sheet_name=data_loader_settings.parser_slas_sheet)
    created_count = 0
    updated_count = 0

    try:
        for idx, row in df.iterrows():
            try:
                step_id = row.get('step_id')
                if pd.isna(step_id):
                    result.record_failed(idx, "Missing mandatory field: step_id")
                    continue

                user_name = row['user_name'] if pd.notna(row.get('user_name')) else None
                role_name = row['role_name'] if pd.notna(row.get('role_name')) else None
                time_limit = row.get('time_limit')
                escalation_level = row.get('escalation_level')

                # Fetch the CaseStepConfig by step_id
                case_step_config = db.query(CaseStepConfig).filter(
                    CaseStepConfig.step_id == step_id).first()

                if not case_step_config:
                    logger.warning(
                        "CaseStepConfig with step_id %s not found. Skipping this entry.", step_id)
                    result.record_failed(idx, f"CaseStepConfig with step_id '{step_id}' not found")
                    continue

                # Find user by name if provided
                user = None
                if user_name:
                    user = db.query(User).filter(
                        User.first_name == user_name).first()
                    if not user:
                        logger.warning(
                            "User %s not found. Skipping this entry.", user_name)
                        result.record_failed(idx, f"User '{user_name}' not found")
                        continue

                # Find role by name if provided
                role = None
                if role_name:
                    role = db.query(Role).filter(Role.name == role_name).first()
                    if not role:
                        logger.warning(
                            "Role %s not found. Skipping this entry.", role_name)
                        result.record_failed(idx, f"Role '{role_name}' not found")
                        continue

                # Check if user and role association exists
                if user and role:
                    association = db.query(user_role_association).filter_by(
                        user_id=user.id, role_id=role.id).first()
                    if not association:
                        logger.warning(
                            "No association found for User %s and Role %s. Skipping this entry.",
                            user_name, role_name)
                        result.record_failed(idx, f"No association found for User '{user_name}' and Role '{role_name}'")
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
                    existing_sla.escalation_level = escalation_level
                    updated_count += 1
                    result.record_updated(idx)
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
                    db.add(sla)
                    created_count += 1
                    logger.info(
                        "SLA successfully created for step_id %s with time_limit %s.",
                        step_id, time_limit)
                    result.record_inserted(idx)

                db.flush()

            except IntegrityError as row_error:
                logger.exception("Integrity error parsing SLA row %s: %s", idx, row_error)
                result.record_failed(idx, f"Integrity error: {str(row_error)}")
            except Exception as row_error:
                logger.exception("Error parsing SLA row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        logger.info("✅ Data successfully processed.")
        return result
    except Exception as e:
        logger.exception("Critical failure in parser slas: %s", e)
        raise RuntimeError(f"Parser slas failed: {e}") from e


if __name__ == "__main__":
    logger.info("Loading SLAs configuration")
    db_session = SessionLocal()

    try:
        excel_file = pd.ExcelFile(
            s3_utils.download_file(settings.bpm_file_key)
        )
        data_df = pd.read_excel(excel_file, "SLA")

        parse_slas(db_session, data_df)

        db_session.commit()
        logger.info("SLAs committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing SLAs: %s", e)
        raise
    finally:
        db_session.close()
