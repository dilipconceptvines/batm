# Third party imports
import pandas as pd
import tempfile
import os
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.db import SessionLocal
from app.core.config import settings
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df

from app.seeder.bpm.parse_roles import process_roles
from app.seeder.bpm.parse_users_and_roles import process_users_and_roles
from app.seeder.bpm.parse_case_types import parse_case_types
from app.seeder.bpm.parse_case_status import parse_case_status
from app.seeder.bpm.parse_case_step import parse_case_step
from app.seeder.bpm.parse_case_step_config import parse_case_step_config
from app.seeder.bpm.parse_case_step_config_paths import parse_case_step_config_paths
from app.seeder.bpm.parse_case_first_step_config import parse_case_first_step_config
# from app.seeder.bpm.process_slas import process_sla_assignments

from app.bpm.models import *
from app.audit_trail.models import *
from app.exports.models import *

logger = get_logger(__name__)

# Ordered list of sheet parsers
SHEET_PARSERS = {
    "roles": process_roles,
    "users": process_users_and_roles,
    "CaseTypes": parse_case_types,
    "CaseStatus": parse_case_status,
    "CaseStep": parse_case_step,
    "CaseStepConfig": parse_case_step_config,
    "CaseStepConfigFiles": parse_case_step_config_paths,
    "CaseFirstStepConfig": parse_case_first_step_config,
    # "SLA": process_sla_assignments
}

def load_and_process_data(
        db: Session, key: str = settings.bpm_file_key
) -> pd.ExcelFile:
    """Load data from S3, process it, and write back results."""
    tmp_file_path = None
    try:
        # Download file to temp
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
            tmp_file_path = tmp_file.name
            
        file_bytes = s3_utils.download_file(key)
        if not file_bytes:
             raise Exception("Failed to download file from S3")
        
        with open(tmp_file_path, 'wb') as f:
            f.write(file_bytes)
            
        excel_data = pd.ExcelFile(tmp_file_path)

        aggregated_stats = {}
        dfs_to_write = {}
        
        for sheet_name, parser_func in SHEET_PARSERS.items():
            if sheet_name in excel_data.sheet_names:
                logger.info("Processing sheet: %s", sheet_name)
                sheet_df = excel_data.parse(sheet_name)
                result = parser_func(db, sheet_df)
                if isinstance(result, ParseResult):
                    aggregated_stats[sheet_name] = result.summary()
                    dfs_to_write[sheet_name] = apply_parse_result_to_df(sheet_df, result)
                elif isinstance(result, dict):
                    aggregated_stats.update(result)
            else:
                logger.warning("Sheet not found: %s", sheet_name)

        logger.info("All sheets processed successfully")
        
        # Write back results
        if dfs_to_write:
            with pd.ExcelWriter(
                tmp_file_path,
                engine="openpyxl",
                mode="a",
                if_sheet_exists="replace"
            ) as writer:
                for sheet_name, df in dfs_to_write.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            with open(tmp_file_path, 'rb') as f:
                s3_utils.upload_file(f, key)
            logger.info("Results uploaded back to S3")

        print("\n" + "="*50)
        print("BPM SEEDER REPORT")
        print("="*50)
        for key, value in aggregated_stats.items():
            print(f"{key}: {value}")
        print("="*50 + "\n")
        
        return excel_data
    except Exception as e:
        logger.error("Error loading data from S3: %s", e)
        raise e
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
    

if __name__ == "__main__":
    db = SessionLocal()
    try:
        load_and_process_data(db=db)
        db.commit()
        logger.info("✅ All BPM data committed successfully")
    except IntegrityError:
        db.rollback()
        logger.error("Session could not be committed due to integrity error")
        raise
    except Exception as e:
        db.rollback()
        logger.error("Error processing BPM data: %s", e)
        raise
    finally:
        db.close()
