# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.bpm.models import CaseStepConfig, CaseTypeFirstStep, CaseType
from app.utils.s3_utils import s3_utils
from app.seeder_loader.parser_registry import parser
from app.seeder.parsing_result import ParseResult

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="case_first_step_config",
    sheet_names=[data_loader_settings.parser_case_first_step_config_sheet],
    version="1.0",
    deprecated=False,
    description="Process case first step config from Excel sheet"
)
def parse_case_first_step_config(db: Session, df: pd.DataFrame) -> ParseResult:
    """Parse case first step config"""
    result = ParseResult(sheet_name=data_loader_settings.parser_case_first_step_config_sheet)
    created_count = 0
    updated_count = 0

    try:
        # Populate CaseStepConfig table
        for idx, row in df.iterrows():
            try:
                # Step 1: Look up the CaseType by prefix
                prefix = row.get('prefix')
                if not prefix:
                    result.record_failed(idx, "Missing mandatory field: prefix")
                    continue

                case_type = db.query(CaseType).filter_by(
                    prefix=prefix).first()
                if not case_type:
                    logger.warning(
                        "CaseType with prefix '%s' not found. Skipping row.", prefix)
                    result.record_failed(idx, f"CaseType with prefix '{prefix}' not found")
                    continue

                # Step 2: Check if a CaseTypeFirstStep entry already exists for this case_type
                case_type_first_step = db.query(CaseTypeFirstStep).filter_by(
                    case_type_id=case_type.id
                ).first()

                first_step = None  # Default to None in case first_step is empty or invalid
                # Step 3: Look up the CaseStepConfig by name if first_step exists and is valid
                first_step_val = row.get('first_step')
                if pd.notna(first_step_val) and first_step_val:  # Check for NaN and empty value
                    try:
                        first_step_value = int(first_step_val)  # Convert to integer
                        first_step = db.query(CaseStepConfig).filter_by(
                            step_id=str(first_step_value)).first()
                        if not first_step:
                            logger.warning(
                                "CaseStepConfig with step '%s' not found. Skipping row.",
                                str(first_step_value)
                            )
                            result.record_failed(idx, f"CaseStepConfig with step '{first_step_value}' not found")
                            continue
                    except ValueError:  # Handle invalid conversion
                        logger.warning(
                            "Invalid value for 'first_step' in row. Skipping row."
                        )
                        result.record_failed(idx, f"Invalid value for 'first_step': {first_step_val}")
                        continue

                if case_type_first_step:
                    logger.info(
                        "CaseTypeFirstStep for prefix '%s' already exists. Updating.",
                        prefix
                    )
                    case_type_first_step.first_step_id = first_step.id if first_step else None
                    updated_count += 1
                    result.record_updated(idx)
                else:
                    # Step 4: Create a new CaseTypeFirstStep entry
                    case_type_first_step = CaseTypeFirstStep(
                        case_type_id=case_type.id,
                        first_step_id=first_step.id if first_step else None,  # NULL if first_step is None
                        is_active=True,  # Set default active status; adjust as necessary
                        created_by=SUPERADMIN_USER_ID,
                    )
                    logger.info(
                        "CaseTypeFirstStep for prefix '%s' and step '%s' creating.",
                        prefix,
                        str(first_step_val) if pd.notna(first_step_val) else 'NULL'
                    )
                    db.add(case_type_first_step)
                    created_count += 1
                    result.record_inserted(idx)

                # Add the new entry
                db.flush()

            except Exception as row_error:
                logger.exception("Error parsing case first step config row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        logger.info("✅ Data successfully processed.")
        return result
    except Exception as e:
        logger.exception("Critical failure in parser case_first_step_config: %s", e)
        raise RuntimeError(f"Parser case_first_step_config failed: {e}") from e


if __name__ == "__main__":
    logger.info("Loading case first step config configuration")
    db_session = SessionLocal()

    try:
        excel_file = pd.ExcelFile(
            s3_utils.download_file(settings.bpm_file_key)
        )
        data_df = pd.read_excel(excel_file, "CaseFirstStepConfig")

        parse_case_first_step_config(db_session, data_df)

        db_session.commit()
        logger.info("Case first step config committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing case first step config: %s", e)
        raise
    finally:
        db_session.close()
