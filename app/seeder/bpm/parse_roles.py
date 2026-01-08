# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.db import SessionLocal
from app.core.config import settings
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.users.models import User, Role
from app.utils.s3_utils import s3_utils
from app.seeder_loader.parser_registry import parser
from app.seeder.parsing_result import ParseResult

logger = get_logger(__name__)


@parser(
    name="roles",
    sheet_names=[data_loader_settings.parser_roles_sheet],
    version="1.0",
    deprecated=False,
    description="Process roles from Excel sheet"
)
def process_roles(db: Session, roles_df: pd.DataFrame) -> ParseResult:
    """Process roles from Excel sheet"""
    result = ParseResult(sheet_name=data_loader_settings.parser_roles_sheet)
    created_count = 0
    updated_count = 0

    try:
        # Get or create superadmin user for audit fields
        admin_user = db.query(User).filter_by(first_name="superadmin").first()
        if not admin_user:
            logger.warning("Superadmin user not found. Creating roles without audit info.")
            admin_user_id = None
        else:
            admin_user_id = admin_user.id

        # Populate Role table
        for idx, row in roles_df.iterrows():
            try:
                name = row.get('name')
                if not name:
                     result.record_failed(idx, "Missing mandatory field: name")
                     continue

                role = db.query(Role).filter_by(name=name).first()

                if not role:
                    role = Role(
                        name=name,
                        description=row.get('description'),
                        created_by=admin_user_id,
                        modified_by=admin_user_id
                    )
                    db.add(role)
                    logger.info("Creating Role %s", role.name)
                    created_count += 1
                    result.record_inserted(idx)
                else:
                    logger.info("Role %s exists. Updating.", role.name)
                    role.description = row.get('description')
                    role.modified_by = admin_user_id
                    updated_count += 1
                    result.record_updated(idx)

                db.flush()

            except IntegrityError as row_error:
                logger.warning("Role %s already exists (integrity error)", row.get('name'))
                result.record_failed(idx, f"Integrity error: Role '{row.get('name')}' already exists")
            except Exception as row_error:
                logger.exception("Error parsing role row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        logger.info("✅ Data successfully processed.")
        return result
    except Exception as e:
        logger.exception("Critical failure in parser roles: %s", e)
        raise RuntimeError(f"Parser roles failed: {e}") from e


if __name__ == "__main__":
    logger.info("Loading roles configuration")
    db_session = SessionLocal()

    try:
        excel_file = pd.ExcelFile(
            s3_utils.download_file(settings.bpm_file_key)
        )
        data_df = pd.read_excel(excel_file, "roles")

        process_roles(db_session, data_df)

        db_session.commit()
        logger.info("Roles committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing roles: %s", e)
        raise
    finally:
        db_session.close()
