# app/utils/security.py

from passlib.context import CryptContext

from app.utils.logger import get_logger

logger = get_logger(__name__)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def verify_password(password: str, hashed_password: str):
    """
    Verify a password
    Args:
        password: str
        hashed_password: str
    Returns:
        bool
    """
    return pwd_context.verify(password, hashed_password)


def get_password_hash(password: str):
    """
    Get a password hash
    Args:
        password: str
    Returns:
        str
    """
    return pwd_context.hash(password)