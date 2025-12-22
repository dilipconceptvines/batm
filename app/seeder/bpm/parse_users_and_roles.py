# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.db import SessionLocal
from app.core.config import settings
from app.utils.logger import get_logger
from app.users.models import User, Role
from app.audit_trail.models import AuditTrail
from app.bpm.models import SLA
from app.utils.security import get_password_hash
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)

def process_users_and_roles(db: Session, users_df: pd.DataFrame, roles_df: pd.DataFrame):
    """Process users and roles"""
    try:
        # Create a superadmin user
        admin_role = db.query(Role).filter_by(name="superadmin").first()
        if not admin_role:
            admin_role = Role(name='superadmin', description='Superadmin User')
            logger.info("Creating Role %s", admin_role.name)
        else:
            logger.info("Role %s exists, omitting", admin_role.name)

        admin_user = db.query(User).filter_by(first_name="superadmin").first()
        if not admin_user:
            admin_user = User(first_name="superadmin", middle_name="",
                            last_name="superadmin", email_address="superadmin@bat.com",
                            password=get_password_hash("bat@123"))
            logger.info("Creating User %s", admin_user.first_name)
        else:
            logger.info("User %s exists, omitting", admin_user.first_name)

# Assign role
        admin_user.roles = [admin_role]
        db.flush()

        # Populate Role table
        roles_dict = {}
        for _, row in roles_df.iterrows():
            role = db.query(Role).filter_by(
                name=row['name']).first()
            if not role:
                role = Role(name=row['name'], description=row['description'],
                            created_by=admin_user.id, modified_by=admin_user.id)
                db.add(role)
                logger.info("Creating Role %s", role.name)
            else:
                logger.info("Role %s exists, omitting", role.name)
                continue
            try:
                db.flush()
                roles_dict[role.name] = role
            except IntegrityError:
                db.rollback()
                role = db.query(Role).filter_by(name=row['name']).one()
                # Store existing role in dictionary if already created
                roles_dict[role.name] = role
        logger.info(roles_dict)

        # Populate User table and associate roles
        for _, row in users_df.iterrows():
            user = db.query(User).filter_by(
                first_name=row['first_name']).first()
            if not user:
                user = User(first_name=row['first_name'], middle_name=row['middle_name'],
                            last_name=row['last_name'], email_address=row['email_address'],
                            password=get_password_hash(row['password']),
                            created_by=admin_user.id, modified_by=admin_user.id)
                logger.info("Creating user %s", user.first_name)
            else:
                logger.info("User %s exists, omitting", user.first_name)
                continue
            role_names = row['roles'].split(',') if pd.notna(row['roles']) else []

            all_roles = []
            for role_name in role_names:
                role_name = role_name.strip()
                logger.info(role_name)
                if role_name in roles_dict:
                    all_roles.append(roles_dict[role_name])

            logger.info("All roles %s ", all_roles)
            if not all_roles:
                logger.info("No roles defined for users")
            else:
                user.roles = all_roles
                db.add(user)
                db.flush()

        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            logger.error("Session could not be committed")

        # Display users and their roles from the database for verification
        for user in db.query(User).all():
            logger.info("User: %s, Email: %s", user.first_name, user.email_address)
            logger.info("Roles:")
            for role in user.roles:
                logger.info("  - %s: %s", role.name, role.description)
    except Exception as e:
        logger.error("Error processing users and roles: %s", e)
        raise

if __name__ == "__main__":
    logger.info("Loading user and role configuration")
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bpm_file_key))

    roles_data_df = pd.read_excel(excel_file, 'roles')
    users_data_df = pd.read_excel(excel_file, 'users')
    process_users_and_roles(db_session, users_data_df, roles_data_df)
