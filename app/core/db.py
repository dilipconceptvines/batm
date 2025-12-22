# app/core/db.py

from fastapi import Depends
from sqlalchemy import MetaData, create_engine, event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from app.core.config import settings
from app.utils.logger import get_logger

# --- Configure logging ---
logger = get_logger(__name__)

# --- Create database engine ---
engine = create_engine(settings.db_url)

# --- Create sessionmaker ---
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# --- Create declarative base ---
Base = declarative_base()


# --- Helper functions for current user tracking ---
def set_current_user(db: Session, user_id: int):
    """
    Set the current user ID in the session info for audit tracking.

    Args:
        db: SQLAlchemy session
        user_id: ID of the currently logged-in user
    """
    db.info["current_user_id"] = user_id


def get_current_user_id(db: Session) -> int:
    """
    Get the current user ID from the session info.

    Args:
        db: SQLAlchemy session

    Returns:
        User ID if set, None otherwise
    """
    return db.info.get("current_user_id")


# --- Event listeners for automatic audit field population ---
@event.listens_for(Session, "before_flush")
def before_flush(session, flush_context, instances):
    """
    Automatically populate created_by and modified_by fields before flushing.
    """
    current_user_id = get_current_user_id(session)

    if current_user_id is None:
        return  # No user set, skip auto-population

    for obj in session.new:
        # Set created_by for new objects
        if hasattr(obj, "created_by") and obj.created_by is None:
            obj.created_by = current_user_id
        # # Also set modified_by for new objects
        # if hasattr(obj, "modified_by"):
        #     obj.modified_by = current_user_id

    for obj in session.dirty:
        # Only update modified_by for modified objects
        if hasattr(obj, "modified_by"):
            obj.modified_by = current_user_id


# --- Synchronous database session ---


def get_db():
    """
    Method for obtaining database session object.

    Note: This does NOT automatically set the current user.
    Use get_db_with_current_user from app.core.dependencies instead for automatic audit tracking.
    """
    db = SessionLocal()
    try:
        yield db
        logger.debug("Committing DB transaction")
        db.commit()
    finally:
        db.close()


def generate_schema_description() -> str:
    """
    Generate a schema description for the database
    """
    try:
        meta = MetaData()
        meta.reflect(bind=engine)

        table_descriptions = []

        for table in meta.sorted_tables:
            column_descriptions = []
            for col in table.columns:
                col_desc = f"{col.name} {col.type}"
                if col.foreign_keys:
                    fk = list(col.foreign_keys)[0]
                    col_desc += f" → {fk.column.table.name}.{fk.column.name}"
                column_descriptions.append(col_desc)

            table_description = (
                f"Table: {table.name} ({', '.join(column_descriptions)})"
            )
            table_descriptions.append(table_description)

        return "\n".join(table_descriptions)
    except Exception as e:
        logger.error("Error generating schema description", error_message=str(e))
        raise e


# --- Asynchronous database setup ---
async_engine = create_async_engine(settings.async_db_url, echo=False, future=True)
AsyncSessionLocal = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


async def get_async_db():
    """
    Async method for obtaining database session object
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            logger.debug("Committing async DB transaction")
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.error("Error in async DB transaction", error_message=str(e))
            raise e
        finally:
            await session.close()
