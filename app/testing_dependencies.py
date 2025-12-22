import logging
import os

import pytest
from dotenv import find_dotenv, load_dotenv
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from .core.db import get_db
from .main import bat_app as fast_api_app

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

env_file = find_dotenv(f'.env{os.getenv("ENV", "")}')
logger.info("Fetching env_file %s", env_file)
load_dotenv(env_file)

TEST_DATABASE_FILE = os.getenv("TEST_DATABASE_FILE")


# Replace with your actual database URL
DATABASE_URL = f"sqlite:///{TEST_DATABASE_FILE}"


engine = create_engine(DATABASE_URL, connect_args={
                       "check_same_thread": False})  # For SQLite
TestSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
TestBase = declarative_base()


@pytest.fixture(scope="module")
def db_session():
    db = TestSessionLocal()
    try:
        yield db
        logger.info("Committing Test DB Transaction")
        db.commit()
    finally:
        TestBase.metadata.drop_all(bind=engine)
        db.close()

# Simulating Redis client for test cases


class RedisClient:
    def __init__(self):
        self.storage = {}

    def get(self, key: str) -> str:
        return self.storage.get(key)

    def set(self, key: str, value: str):
        self.storage[key] = value


@pytest.fixture(scope="module")
def get_redis_db():
    # Return mock redis client for the
    return RedisClient()


@pytest.fixture(scope="module")
def client(db_session):

    # Override FastAPI's dependency to use the test database session
    def override_get_db():
        yield db_session

    fast_api_app.dependency_overrides[get_db] = override_get_db
    yield TestClient(fast_api_app)
