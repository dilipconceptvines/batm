# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.bpm.models import CaseStepConfig, CaseTypeFirstStep, CaseType
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def parse_case_first_step_config(db: Session, df: pd.DataFrame):
    """Parse case first step config"""
    try:
        # Populate CaseStepConfig table
        for _, row in df.iterrows():
            # Step 1: Look up the CaseType by prefix
            case_type = db.query(CaseType).filter_by(
                prefix=row['prefix']).first()
            if not case_type:
                logger.info(
                    "CaseType with prefix '%s' not found. Skipping row.", row['prefix'])
                continue

            # Step 2: Check if a CaseTypeFirstStep entry already exists for this case_type
            case_type_first_step = db.query(CaseTypeFirstStep).filter_by(
                case_type_id=case_type.id
            ).first()

            first_step = None  # Default to None in case first_step is empty or invalid
            # Step 3: Look up the CaseStepConfig by name if first_step exists and is valid
            if not pd.isna(row.get('first_step')) and row['first_step']:  # Check for NaN and empty value
                try:
                    first_step_value = int(row['first_step'])  # Convert to integer
                    first_step = db.query(CaseStepConfig).filter_by(
                        step_id=str(first_step_value)).first()
                    if not first_step:
                        logger.info(
                            "CaseStepConfig with step '%s' not found. Skipping row.",
                            str(first_step_value)
                        )
                        continue
                except ValueError:  # Handle invalid conversion
                    logger.info(
                        "Invalid value for 'first_step' in row. Skipping row."
                    )
                    continue

            if case_type_first_step:
                logger.info(
                    "CaseTypeFirstStep for prefix '%s' already exists. Updating.",
                    row['prefix']
                )
                case_type_first_step.first_step_id = first_step.id if first_step else None
            else:
                # Step 4: Create a new CaseTypeFirstStep entry
                case_type_first_step = CaseTypeFirstStep(
                    case_type_id=case_type.id,
                    first_step_id=first_step.id if first_step else None,  # NULL if first_step is None
                    is_active=True,  # Set default active status; adjust as necessary
                    created_by=SUPERADMIN_USER_ID,
                )
                logger.info(
                    "CaseTypeFirstStep for prefix '%s' and step '%s' already exists. Inserting.",
                    row['prefix'],
                    str(row['first_step']) if not pd.isna(row.get('first_step')) else 'NULL'
                )

            # Add and commit the new entry
            db.add(case_type_first_step)

        # Commit all changes at once
        try:
            db.commit()
            print("All CaseTypeFirstStep entries created successfully.")
        except IntegrityError:
            db.rollback()
            logger.info(
                "An error occurred while committing CaseTypeFirstStep entries."
            )

        # Close the session
        db.close()
    except Exception as e:
        logger.error("Error parsing case first step config: %s", e)
        raise


if __name__ == "__main__":
    logger.info("Loading Case First Step Config")
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bpm_file_key))
    case_first_step_config_df = pd.read_excel(excel_file, 'CaseFirstStepConfig')
    parse_case_first_step_config(db=db_session, df=case_first_step_config_df)
