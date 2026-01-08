# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.bpm.models import CaseStep, CaseStepConfig, CaseType
from app.users.models import User, Role
from app.utils.s3_utils import s3_utils
from app.seeder_loader.parser_registry import parser
from app.seeder.parsing_result import ParseResult

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="case_step_config",
    sheet_names=[data_loader_settings.parser_case_step_config_sheet],
    version="1.0",
    deprecated=False,
    description="Process case step config from Excel sheet"
)
def parse_case_step_config(db: Session, df: pd.DataFrame) -> ParseResult:
    """Parse case step config"""
    result = ParseResult(sheet_name=data_loader_settings.parser_case_step_config_sheet)
    created_count = 0
    updated_count = 0

    try:
        # Populate CaseStepConfig table
        for idx, row in df.iterrows():
            try:
                # Fetch the related CaseStep by case_step_name
                case_step_name = row.get('case_step_name')
                if not case_step_name:
                    result.record_failed(idx, "Missing mandatory field: case_step_name")
                    continue

                case_step = db.query(CaseStep).filter_by(
                    name=case_step_name).first()
                if not case_step:
                    logger.warning(
                        "CaseStep '%s' not found. Skipping row.", case_step_name
                    )
                    result.record_failed(idx, f"CaseStep '{case_step_name}' not found")
                    continue

                # Fetch the related CaseType by case_type_prefix
                case_type_prefix = row.get('case_type_prefix')
                case_type = db.query(CaseType).filter_by(
                    prefix=case_type_prefix).first()
                if not case_type:
                    logger.warning(
                        "CaseType '%s' not found. Skipping row.", case_type_prefix
                    )
                    result.record_failed(idx, f"CaseType '{case_type_prefix}' not found")
                    continue

                # Fetch the related User by next_assignee_name
                next_assignee = None
                next_assignee_name = row.get('next_assignee_name')
                if pd.notna(next_assignee_name):
                    next_assignee = db.query(User).filter_by(
                        first_name=next_assignee_name).first()
                    if not next_assignee:
                        logger.warning(
                            "User '%s' not found. Skipping row.", next_assignee_name
                        )
                        result.record_failed(idx, f"User '{next_assignee_name}' not found")
                        continue

                # Check if CaseStepConfig with the same step_id exists to avoid duplicates
                step_id = row.get('step_id')
                if not step_id:
                     result.record_failed(idx, "Missing mandatory field: step_id")
                     continue

                case_step_config = db.query(CaseStepConfig).filter_by(
                    step_id=step_id).first()

                next_step_val = row.get('next_step_id')
                next_step_id = str(int(float(next_step_val))) if pd.notna(next_step_val) else ""

                if case_step_config:
                    logger.info(
                        "CaseStepConfig with step_id '%s' and step name '%s' already exists. Updating.",
                        step_id, row.get('step_name')
                    )
                    case_step_config.step_id = step_id
                    case_step_config.case_step_id = case_step.id
                    case_step_config.next_assignee_id = next_assignee.id if next_assignee else None
                    case_step_config.next_step_id = next_step_id
                    case_step_config.case_type_id = case_type.id
                    case_step_config.created_by = SUPERADMIN_USER_ID
                    # case_step_config.step_name = row['step_name']
                    case_step_config.roles.clear()
                    updated_count += 1
                    result.record_updated(idx)
                else:
                    logger.info(
                        "CaseStepConfig with step_id '%s' and step name '%s' does not exist. Creating.",
                        step_id, row.get('step_name')
                    )
                    # Create a new CaseStepConfig instance
                    case_step_config = CaseStepConfig(
                        step_id=step_id,
                        case_step_id=case_step.id,
                        next_assignee_id=next_assignee.id if next_assignee else None,
                        next_step_id=next_step_id,
                        case_type_id=case_type.id,
                        created_by=SUPERADMIN_USER_ID,
                        step_name=row.get('step_name')
                    )
                    db.add(case_step_config) # Add initially to session so we can append roles
                    created_count += 1
                    result.record_inserted(idx)

                # Assign roles based on the user_roles column (comma-separated values)
                user_roles_str = row.get('user_roles')
                role_names = user_roles_str.split(
                    ',') if pd.notna(user_roles_str) else []

                logger.info(
                    "Roles present for step '%s' are '%s'.", step_id, ",".join(
                        role_names)
                )
                for role_name in role_names:
                    role_name = role_name.strip()
                    role = db.query(Role).filter_by(name=role_name).first()
                    if role:
                        case_step_config.roles.append(role)
                        logger.info(
                            "Adding '%s' to step '%s'.", role_name, step_id
                        )
                    else:
                        logger.warning(
                            "Role '%s' not found. Skipping role assignment for '%s'.",
                            role_name, step_id
                        )

                # Add CaseStepConfig to the session
                # If it was updated, it's already attached. If it was created, db.add was called.
                # Just flushing is enough.
                db.flush()

            except Exception as row_error:
                logger.exception("Error parsing case step config row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        logger.info("✅ Data successfully processed.")
        return result
    except Exception as e:
        logger.exception("Critical failure in parser case_step_config: %s", e)
        raise RuntimeError(f"Parser case_step_config failed: {e}") from e


if __name__ == "__main__":
    logger.info("Loading case step config configuration")
    db_session = SessionLocal()

    try:
        excel_file = pd.ExcelFile(
            s3_utils.download_file(settings.bpm_file_key)
        )
        data_df = pd.read_excel(excel_file, "CaseStepConfig")

        parse_case_step_config(db_session, data_df)

        db_session.commit()
        logger.info("Case step config committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing case step config: %s", e)
        raise
    finally:
        db_session.close()
