# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.bpm.models import CaseStep, CaseType
from app.utils.s3_utils import s3_utils
from app.seeder_loader.parser_registry import parser
from app.seeder.parsing_result import ParseResult

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="case_step",
    sheet_names=[data_loader_settings.parser_case_step_sheet],
    version="1.0",
    deprecated=False,
    description="Process case step from Excel sheet"
)
def parse_case_step(db: Session, df: pd.DataFrame) -> ParseResult:
    """Parse case step"""
    result = ParseResult(sheet_name=data_loader_settings.parser_case_step_sheet)
    created_count = 0
    updated_count = 0

    try:
        for idx, row in df.iterrows():
            try:
                name = row.get('name')
                if not name:
                    result.record_failed(idx, "Missing mandatory field: name")
                    continue

                case_type_prefix = row.get('case_type_prefix')
                weight = row.get('weight')

                # Fetch the related CaseType by case_type_prefix
                case_type = db.query(CaseType).filter_by(
                    prefix=case_type_prefix).first()
                if not case_type:
                    logger.warning(
                        "CaseType '%s' not found. Skipping row.", case_type_prefix
                    )
                    result.record_failed(idx, f"CaseType '{case_type_prefix}' not found")
                    continue

                # Check if CaseStep with the same name already exists
                logger.info("Checking '%s'", name)
                case_step = db.query(CaseStep).filter_by(name=name).first()
                if case_step:
                    logger.info("CaseStep '%s' already exists. Updating.", name)
                    case_step.name = name
                    case_step.case_type_id = case_type.id
                    case_step.weight = weight
                    case_step.created_by = SUPERADMIN_USER_ID
                    updated_count += 1
                    result.record_updated(idx)
                else:
                    logger.info("CaseStep '%s' does not exist. Creating.", name)
                    case_step = CaseStep(name=name, case_type_id=case_type.id, weight=weight, created_by=SUPERADMIN_USER_ID)
                    db.add(case_step)
                    created_count += 1
                    result.record_inserted(idx)

                db.flush()

            except Exception as row_error:
                logger.exception("Error parsing case step row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        logger.info("✅ Data successfully processed.")
        return result
    except Exception as e:
        logger.exception("Critical failure in parser case_step: %s", e)
        raise RuntimeError(f"Parser case_step failed: {e}") from e


if __name__ == "__main__":
    logger.info("Loading case step configuration")
    db_session = SessionLocal()

    try:
        excel_file = pd.ExcelFile(
            s3_utils.download_file(settings.bpm_file_key)
        )
        data_df = pd.read_excel(excel_file, "CaseStep")

        parse_case_step(db_session, data_df)

        db_session.commit()
        logger.info("Case step committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing case step: %s", e)
        raise
    finally:
        db_session.close()
