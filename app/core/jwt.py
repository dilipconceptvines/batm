# app/core/jwt.py

from datetime import datetime, timedelta, timezone
from typing import Optional

from jose import JWTError, jwt , ExpiredSignatureError
from fastapi import HTTPException, status

from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

# --- JWT Token Management ---

def create_access_token(
    data: dict,
    expires_delta: Optional[timedelta] = None
):
    """Create an access token"""
    try:
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.now(timezone.utc) + expires_delta
        else:
            expire = datetime.now(timezone.utc) + timedelta(minutes=settings.access_token_expire_minutes)
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, settings.secret_key, algorithm=settings.algorithm)
        return encoded_jwt
    except Exception as e:
        logger.error("Error creating access token", error_message=str(e))
        raise e
    
def create_refresh_token(data: dict):
    """Create a refresh token"""
    try:
        to_encode = data.copy()
        expire = datetime.now(timezone.utc) + timedelta(days=settings.refresh_token_expire_days)
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, settings.secret_key, algorithm=settings.algorithm)
        return encoded_jwt
    except Exception as e:
        logger.error("Error creating refresh token", error_message=str(e))
        raise e
    
def verify_token(token: str):
    """Verify a token"""
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        return payload
    except ExpiredSignatureError as ese:
        logger.error("Token has expired.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        ) from ese
    except JWTError as e:
        logger.error("Error verifying token", error_message=str(e))
        raise e
    
def create_password_reset_token(email: str) -> str:
    """
    Creates a password reset token for the given email.
    The token expires in 1 hour.
    """
    try:
        expire = datetime.now(timezone.utc) + timedelta(hours=1)
        to_encode = {"exp": expire, "sub": email, "scope": "password_reset"}
        encoded_jwt = jwt.encode(to_encode, settings.secret_key, algorithm=settings.algorithm)
        return encoded_jwt
    except Exception as e:
        logger.error("Error creating password reset token", error_message=str(e))
        raise e
    
def verify_password_reset_token(token: str) -> str:
    """
    Verifies a password reset token and returns the email.
    Raises HTTPException on failure.
    """
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        if payload.get("scope") != "password_reset":
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token scope")
        return payload.get("sub")
    except ExpiredSignatureError as ese:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Password reset link has expired") from ese
    except JWTError as je:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid password reset link") from je
    

    
