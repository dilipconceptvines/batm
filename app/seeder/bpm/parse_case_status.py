# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.db import SessionLocal
from app.core.config import settings
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.bpm.models import CaseStatus
from app.utils.s3_utils import s3_utils
from app.seeder_loader.parser_registry import parser
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df
import tempfile
import os

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="case_status",
    sheet_names=[data_loader_settings.parser_case_status_sheet],
    version="1.0",
    deprecated=False,
    description="Process case status from Excel sheet"
)
def parse_case_status(db: Session, df: pd.DataFrame) -> ParseResult:
    """
    Parse the case status dataframe and insert into the database

    Args:
        session: The database session
        df: The case status dataframe
    """
    result = ParseResult(sheet_name=data_loader_settings.parser_case_status_sheet)
    created_count = 0
    updated_count = 0

    try:
        for idx, row in df.iterrows():
            try:
                name = row['name']
                if pd.isna(name) or not name:
                     result.record_failed(idx, "Missing mandatory field: name")
                     continue

                # Check if CaseStatus with the same name already exists
                case_status = db.query(
                    CaseStatus).filter_by(name=name).first()
                if case_status:
                    case_status.name = name
                    case_status.created_by = SUPERADMIN_USER_ID
                    logger.info(
                        "CaseStatus '%s' already exists. Updating.", name
                    )
                    updated_count += 1
                    result.record_updated(idx)
                else:
                    logger.info(
                        "CaseStatus '%s' does not exist. Adding.", name
                    )
                    case_status = CaseStatus(
                        name=name, created_by=SUPERADMIN_USER_ID)
                    db.add(case_status)
                    created_count += 1
                    result.record_inserted(idx)

                db.flush()

            except Exception as row_error:
                logger.exception("Error parsing case status row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        logger.info("✅ Data successfully processed.")
        return result
    except Exception as e:
        logger.exception("Critical failure in parser case_status: %s", e)
        raise RuntimeError(f"Parser case_status failed: {e}") from e


if __name__ == "__main__":
    logger.info("Loading case status configuration")
    db_session = SessionLocal()

    tmp_file_path = None
    try:
        # Download file to temp
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
            tmp_file_path = tmp_file.name
            
        file_bytes = s3_utils.download_file(settings.bpm_file_key)
        if not file_bytes:
             raise Exception("Failed to download file from S3")
        
        with open(tmp_file_path, 'wb') as f:
            f.write(file_bytes)
            
        excel_file = pd.ExcelFile(tmp_file_path)
        data_df = pd.read_excel(excel_file, "CaseStatus")

        result = parse_case_status(db_session, data_df)
        
        # Apply results
        updated_df = apply_parse_result_to_df(data_df, result)
        
        # Write back to temp file
        with pd.ExcelWriter(
            tmp_file_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            updated_df.to_excel(writer, sheet_name="CaseStatus", index=False)
            
        # Upload back to S3
        with open(tmp_file_path, 'rb') as f:
            s3_utils.upload_file(f, settings.bpm_file_key)

        db_session.commit()
        logger.info("Case status committed successfully")
        
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing case status: %s", e)
        raise
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        db_session.close()
