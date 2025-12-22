# app/users/utils.py

from typing import List

from fastapi import HTTPException, status, Depends
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_async_db
from app.core.jwt import verify_token
from app.users.repository import UserRepository
from app.users.models import User
from app.utils.logger import get_logger

logger = get_logger(__name__)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

async def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_async_db)
) -> User:
    """
    Async dependency to get the current authenticated user from the JWT token.
    Uses the repository directly for the efficiency.
    """

    payload = verify_token(token)
    email = payload.get("sub")
    if email is None:
        logger.error("Token payload missing 'sub' field")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials."
        )
    
    repo = UserRepository(db)
    user = await repo.get_user_by_email(email)

    if user is None or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive."
        )
    
    return user


class RoleChecker:
    """
    RBAC dependency that checks if the current user has any of the allowed roles.
    """
    def __init__(self, allowed_roles: List[str]):
        self.allowed_roles = [role.lower() for role in allowed_roles]

    def __call__(self, current_user: User = Depends(get_current_user)):
        """Check if the current user has at least one of the allowed roles."""
        user_roles = {role.name.lower() for role in current_user.roles}

        if not user_roles.intersection(self.allowed_roles):
            logger.warning("User does not have required roles", user_id=current_user.id, required_roles=self.allowed_roles)
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You do not have permission to perform this action."
            )
        
