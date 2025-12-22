## app/core/redis.py

# Third party imports
import redis

# Local imports
from app.core.config import settings

# Synchronous redis connection
def get_redis_db():
    """
    Method for obtaining redis session object
    """
    redis_session = redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        username=settings.redis_username,
        password=settings.redis_password,
        decode_responses=True,
    )
    try:
        yield redis_session
    finally:
        redis_session.close()