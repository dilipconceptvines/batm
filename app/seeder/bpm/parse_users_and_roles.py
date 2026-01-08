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
from app.audit_trail.models import AuditTrail
from app.bpm.models import SLA
from app.utils.security import get_password_hash
from app.utils.s3_utils import s3_utils
from app.seeder_loader.parser_registry import parser
from app.seeder.parsing_result import ParseResult

logger = get_logger(__name__)

@parser(
    name="users_and_roles",
    sheet_names=[data_loader_settings.parser_users_sheet, data_loader_settings.parser_roles_sheet],
    version="1.0",
    deprecated=False,
    description="Process users and roles from Excel sheet"
)
def process_users_and_roles(db: Session, users_df: pd.DataFrame) -> ParseResult:
    """Process users and roles"""
    # Note: This parser processes two sheets (users and roles), so we use a combined identifier
    result = ParseResult(sheet_name="users_roles")
    created_count = 0
    updated_count = 0

    try:
        # Create a superadmin user
        admin_role = db.query(Role).filter_by(name="superadmin").first()
        if not admin_role:
            logger.warning("Superadmin role not found")
            # This is a critical setup step, so we might want to log it as a failure but proceed if possible,
            # or maybe raising an error is better. Original code recorded failure at index 0.
            result.record_failed(0, "Superadmin role not found")
            # If admin_role is missing, we can't assign it later.

        admin_user = db.query(User).filter_by(first_name="superadmin").first()
        if not admin_user:
            admin_user = User(first_name="superadmin", middle_name="",
                            last_name="superadmin", email_address="superadmin@bat.com",
                            password=get_password_hash("bat@123"))
            logger.info("Creating User %s", admin_user.first_name)
            # We don't increment created_count for system users typically, but let's stick to row data counts.
        else:
            logger.info("User %s exists, omitting", admin_user.first_name)

        # Assign role if exists
        if admin_role:
             admin_user.roles = [admin_role]

        db.add(admin_user) # Ensure admin_user is attached
        db.flush()

        # Populate User table and associate roles
        for idx, row in users_df.iterrows():
            try:
                first_name = row.get('first_name')
                email_address = row.get('email_address')
                middle_name = row.get('middle_name')
                last_name = row.get('last_name')
                password = row.get('password')



                if not first_name or not email_address or not password:
                    result.record_failed(idx, "Missing mandatory field: first_name , email_address or password")
                    continue

                user = db.query(User).filter_by(email_address=email_address).first()
                if not user:
                    user = User(first_name=first_name,
                                middle_name= middle_name if pd.notna(middle_name) else None,
                                last_name= last_name if pd.notna(last_name) else None,
                                email_address=email_address,
                                password=get_password_hash(row.get('password', 'default')), # Verify if password mandatory
                                created_by=admin_user.id, modified_by=admin_user.id)
                    logger.info("Creating user %s", user.first_name)
                    created_count += 1
                    result.record_inserted(idx)
                else:
                    logger.info("User %s exists. Updating.", user.first_name)
                    user.first_name = first_name
                    user.middle_name = middle_name if pd.notna(middle_name) else None
                    user.last_name = last_name if pd.notna(last_name) else None
                    user.password = get_password_hash(row.get('password', 'default')) if pd.notna(row.get('password')) else user.password
                    user.modified_by = admin_user.id
                    updated_count += 1
                    result.record_updated(idx)

                role_names_str = row.get('roles')
                role_names = role_names_str.split(',') if pd.notna(role_names_str) else []

                all_roles = []
                for role_name in role_names:
                    role_name = role_name.strip()
                    logger.debug("Processing role: %s", role_name)
                    user_role = db.query(Role).filter_by(name=role_name).first()
                    if not user_role:
                        logger.warning("Role %s not found", role_name)
                        result.record_failed(idx, f"Role {role_name} not found")
                        continue

                    all_roles.append(user_role)

                if not all_roles:
                    logger.info("No valid roles found/defined for user %s", first_name)
                else:
                    user.roles = all_roles
                    db.add(user)

                db.flush()

            except Exception as row_error:
                logger.exception("Error parsing user row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        # Display users and their roles from the database for verification
        # This logging is a bit excessive for production but keeping it as per original logic's intent
        for user in db.query(User).all():
             logger.debug("User: %s, Roles: %s", user.first_name, [r.name for r in user.roles])

        logger.info("✅ Data successfully processed.")
        return result
    except Exception as e:
        logger.exception("Critical failure in parser users_and_roles: %s", e)
        raise RuntimeError(f"Parser users_and_roles failed: {e}") from e

if __name__ == "__main__":
    logger.info("Loading users and roles configuration")
    db_session = SessionLocal()

    try:
        excel_file = pd.ExcelFile(
            s3_utils.download_file(settings.bpm_file_key)
        )
        users_df = pd.read_excel(excel_file, "users")

        process_users_and_roles(db_session, users_df)

        db_session.commit()
        logger.info("Users and roles committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing users and roles: %s", e)
        raise
    finally:
        db_session.close()