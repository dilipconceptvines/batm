# Third party imports
import pandas as pd
from sqlalchemy.orm import Session

# Local imports
from app.core.db import SessionLocal
from app.core.config import settings
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils

from app.seeder.bpm.parse_users_and_roles import process_users_and_roles
from app.seeder.bpm.parse_case_types import parse_case_types
from app.seeder.bpm.parse_case_status import parse_case_status
from app.seeder.bpm.parse_case_step import parse_case_step
from app.seeder.bpm.parse_case_step_config import parse_case_step_config
from app.seeder.bpm.parse_case_step_config_paths import parse_case_step_config_paths
from app.seeder.bpm.parse_case_first_step_config import parse_case_first_step_config
# from app.seeder.bpm.process_slas import process_sla_assignments

logger = get_logger(__name__)

# Ordered list of sheet parsers
SHEET_PARSERS = {
    "users_roles": process_users_and_roles,
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
    """Load data from S3"""
    try:
        data = s3_utils.download_file(key)
        excel_data = pd.ExcelFile(data)

        # Iterate over required sheets in the defined order
        for sheet_name, parser_func in SHEET_PARSERS.items():
            if sheet_name == "users_roles":
                # Special case for user_roles that requires two sheets
                if "users" in excel_data.sheet_names and "roles" in excel_data.sheet_names:
                    logger.info("Processing users and roles")
                    users_df = excel_data.parse("users")
                    roles_df = excel_data.parse("roles")
                    parser_func(db, users_df, roles_df)
            elif sheet_name in excel_data.sheet_names:
                logger.info("Processing sheet: %s", sheet_name)
                sheet_df = excel_data.parse(sheet_name)
                parser_func(db, sheet_df)
            else:
                logger.warning("Sheet not found: %s", sheet_name)

        logger.info("All sheets processed successfully")
    except Exception as e:
        logger.error("Error loading data from S3: %s", e)
        raise e
    

if __name__ == "__main__":
    load_and_process_data(db=SessionLocal())
