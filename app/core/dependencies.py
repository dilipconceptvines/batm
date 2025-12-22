## app/core/dependencies.py

from fastapi import Depends
from sqlalchemy.orm import Session

from app.core.db import SessionLocal, set_current_user
from app.users.models import User
from app.users.utils import get_current_user
from app.utils.logger import get_logger

logger = get_logger(__name__)


def get_db_with_current_user(
    logged_in_user: User = Depends(get_current_user),
):
    """
    Get database session with automatic current user tracking for audit fields.

    This dependency:
    1. Gets the current logged-in user
    2. Creates a database session
    3. Sets the current user in the session for audit tracking
    4. Yields the session
    5. Commits and closes the session

    Usage in routers:
        from app.core.dependencies import get_db_with_current_user
        from sqlalchemy.orm import Session
        from fastapi import Depends

        @router.post("/endpoint")
        def my_endpoint(
            db: Session = Depends(get_db_with_current_user)
        ):
            # created_by and modified_by will be automatically set
            driver = Driver(name="Test")
            db.add(driver)
            db.commit()

    Args:
        logged_in_user: The currently logged-in user (injected by FastAPI)

    Yields:
        Database session with current user tracking enabled
    """
    db = SessionLocal()
    try:
        # Set current user in session for audit tracking
        if logged_in_user and hasattr(logged_in_user, "id"):
            set_current_user(db, logged_in_user.id)

        yield db
        logger.info("Committing DB transaction with user tracking")
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error("Error in DB transaction", error_message=str(e))
        raise e
    finally:
        db.close()
