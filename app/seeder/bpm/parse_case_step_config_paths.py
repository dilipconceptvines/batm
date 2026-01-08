# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.bpm.models import CaseStepConfig, CaseStepConfigPath
from app.utils.s3_utils import s3_utils
from app.seeder_loader.parser_registry import parser
from app.seeder.parsing_result import ParseResult

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="case_step_config_paths",
    sheet_names=[data_loader_settings.parser_case_step_config_paths_sheet],
    version="1.0",
    deprecated=False,
    description="Process case step config paths from Excel sheet"
)
def parse_case_step_config_paths(db: Session, df: pd.DataFrame) -> ParseResult:
    """Parse case step config paths"""
    result = ParseResult(sheet_name=data_loader_settings.parser_case_step_config_paths_sheet)
    created_count = 0
    updated_count = 0

    try:
        for idx, row in df.iterrows():
            try:
                # Step 1: Find the CaseStepConfig based on step_name
                step_name = row.get('step_name')
                if not step_name:
                    result.record_failed(idx, "Missing mandatory field: step_name")
                    continue

                case_step_config = db.query(CaseStepConfig).filter_by(
                    step_name=step_name).first()
                if not case_step_config:
                    logger.warning(
                        "CaseStepConfig with step_name '%s' not found. Skipping row.",
                        step_name
                    )
                    result.record_failed(idx, f"CaseStepConfig with step_name '{step_name}' not found")
                    continue

                # Step 2: Check if a CaseStepConfigPath entry already exists for this CaseStepConfig
                case_step_config_path = db.query(CaseStepConfigPath).filter_by(
                    case_step_config_id=case_step_config.id).first()

                schema_name = row.get('schema_name')
                
                # If no entry exists, create one. Otherwise, update the existing path.
                if not case_step_config_path:
                    path_value = schema_name if pd.notna(schema_name) else ""
                    case_step_config_path = CaseStepConfigPath(
                        case_step_config_id=case_step_config.id, path=path_value, is_active=True, created_by=SUPERADMIN_USER_ID)
                    db.add(case_step_config_path)
                    logger.info(
                        "Creating new path for step '%s' with path '%s'",
                        step_name, path_value
                    )
                    created_count += 1
                    result.record_inserted(idx)
                else:
                    path_value = schema_name if pd.notna(schema_name) else ""
                    case_step_config_path.path = path_value
                    logger.info(
                        "Updating path for step '%s' to '%s'",
                        step_name, path_value
                    )
                    updated_count += 1
                    result.record_updated(idx)

                db.flush()

            except Exception as row_error:
                logger.exception("Error parsing case step config paths row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        logger.info("✅ Data successfully processed.")
        return result
    except Exception as e:
        logger.exception("Critical failure in parser case_step_config_paths: %s", e)
        raise RuntimeError(f"Parser case_step_config_paths failed: {e}") from e


if __name__ == "__main__":
    logger.info("Loading case step config paths configuration")
    db_session = SessionLocal()

    try:
        excel_file = pd.ExcelFile(
            s3_utils.download_file(settings.bpm_file_key)
        )
        data_df = pd.read_excel(excel_file, "CaseStepConfigFiles")

        parse_case_step_config_paths(db_session, data_df)

        db_session.commit()
        logger.info("Case step config paths committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing case step config paths: %s", e)
        raise
    finally:
        db_session.close()
