# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.bpm.models import CaseType
from app.utils.s3_utils import s3_utils
from app.seeder_loader.parser_registry import parser
from app.seeder.parsing_result import ParseResult

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="case_types",
    sheet_names=[data_loader_settings.parser_case_types_sheet],
    version="1.0",
    deprecated=False,
    description="Process case types from Excel sheet"
)
def parse_case_types(db: Session, df: pd.DataFrame) -> ParseResult:
    """Parse case types from the given dataframe"""
    result = ParseResult(sheet_name=data_loader_settings.parser_case_types_sheet)
    created_count = 0
    updated_count = 0

    try:
        for idx, row in df.iterrows():
            try:
                name = row.get('name')
                if not name:
                    result.record_failed(idx, "Missing mandatory field: name")
                    continue

                prefix = row.get('prefix')

                # Check if CaseType with the same name already exists
                existing_case_type = db.query(
                    CaseType).filter_by(name=name).first()
                if existing_case_type:
                    logger.info("CaseType '%s' already exists. Updating.", name)
                    existing_case_type.prefix = prefix
                    updated_count += 1
                    result.record_updated(idx)
                else:
                    # Create and add new CaseType if it doesn't exist
                    case_type = CaseType(
                        name=name, prefix=prefix, created_by=SUPERADMIN_USER_ID)
                    db.add(case_type)
                    created_count += 1
                    result.record_inserted(idx)

                db.flush()

            except Exception as row_error:
                logger.exception("Error parsing case type row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        logger.info("✅ Data successfully processed.")
        return result
    except Exception as e:
        logger.exception("Critical failure in parser case_types: %s", e)
        raise RuntimeError(f"Parser case_types failed: {e}") from e


if __name__ == "__main__":
    logger.info("Loading case types configuration")
    db_session = SessionLocal()

    try:
        excel_file = pd.ExcelFile(
            s3_utils.download_file(settings.bpm_file_key)
        )
        data_df = pd.read_excel(excel_file, "CaseTypes")

        parse_case_types(db_session, data_df)

        db_session.commit()
        logger.info("Case types committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing case types: %s", e)
        raise
    finally:
        db_session.close()
