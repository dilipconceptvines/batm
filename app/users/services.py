# app/users/services.py

from datetime import datetime, timezone
from typing import Optional, Tuple, List

from fastapi import Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_async_db
from app.core.config import settings
from app.core.jwt import create_password_reset_token, verify_password_reset_token
from app.users.models import User
from app.users.repository import UserRepository
from app.users.schemas import (
    LoginRequest, PasswordChangeRequest, ForgotPasswordRequest, ResetPasswordRequest
)
from app.utils.security import get_password_hash, verify_password
from app.utils.logger import get_logger
from app.utils.email_service import email_service

logger = get_logger(__name__)

def get_user_repository(db: AsyncSession = Depends(get_async_db)) -> UserRepository:
    """Dependency to get UserRepository instance."""
    return UserRepository(db)


class UserService:
    """
    Business logic layer for user-related operations.
    Depends on the UserRepository for data access.
    """

    def __init__(self, repo: UserRepository = Depends(get_user_repository)):
        self.repo = repo

    async def authenticate_user(self, login_data: LoginRequest) -> Optional[User]:
        """Authenticate user by email and password."""
        user = await self.repo.get_user_by_email(login_data.email_id)

        if not user or not user.is_active or not verify_password(login_data.password, user.password):
            logger.warning("Authentication failed for email", email=login_data.email_id)
            return None
        
        # --- Platform validation logic ---
        platform = (login_data.platform or "").lower()
        user_roles = [role.name.lower() for role in user.roles]
        is_runner = "runner" in user_roles

        if is_runner and platform != "mobile":
            logger.warning("Runner role can only login via mobile", email=login_data.email_id)
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Runners can only log in via mobile platform."
            )
        if not is_runner and platform == "mobile":
            logger.warning("Non-runner role cannot login via mobile", email=login_data.email_id)
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only runners can log in via mobile platform."
            )
        
        user.last_login = datetime.now(timezone.utc)
        await self.repo.update(user)
        return user
    
    async def change_password(self, user: User, password_data: PasswordChangeRequest) -> None:
        """Change User's password."""
        if not verify_password(password_data.current_password, user.password):
            logger.warning("Current password does not match", user_id=user.id)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Current password is incorrect."
            )
        
        user.password = get_password_hash(password_data.new_password)
        await self.repo.update(user)

    async def search_users(
        self, search: Optional[str], roles : Optional[str], sort_by: str, sort_order: str, skip: int, limit: int
    ) -> Tuple[List[User], int]:
        """
        Calls the repository to search for users.
        Future business logic related to user search would go here.
        """
        return await self.repo.search_users(
            search=search,
            roles=roles,
            sort_by=sort_by,
            sort_order=sort_order,
            skip=skip,
            limit=limit
        )
    
    async def request_password_reset(self, request_data: ForgotPasswordRequest):
        """
        Handles the logic for a user requesting a password reset.
        Generates a token and sends the reset email.
        """
        user = await self.repo.get_user_by_email(request_data.email)
        if not user:
            # IMPORTANT: Do not reveal if the user exists
            # This prevents email enumeration attacks
            logger.warning("Password reset requested for non-existent email", email=request_data.email)
            return # Silently succeed
        
        # --- Generate a short-lived password reset token ---
        token = create_password_reset_token(user.email_address)

        # --- Construct the reset link for the email template ---
        # --- The frontend_url should be configured in your .env file ---
        frontend_url = settings.app_base_url or "http://localhost:3000"
        reset_link = f"{frontend_url}/reset-password?token={token}"

        logger.info("Sending password reset email", email=user.email_address)

        try:
            await email_service.send_templated_email(
                to_emails=[user.email_address],
                subject="Your Password Reset Request",
                template_name="password_reset.html",
                context={
                    "user_name": user.first_name or "User",
                    "reset_link": reset_link,
                    "valid_for_minutes": settings.access_token_expire_minutes
                }
            )
        except Exception as e:
            logger.error("Failed to send password reset email", email=user.email_address, error=str(e))
            # Do not raise an exception to the user, as it could leak information
            # The system should log this failure for monitoring.

    async def reset_password(self, request_data: ResetPasswordRequest):
        """
        Handles the logic for resetting a password using a valid token.
        """
        email = verify_password_reset_token(request_data.token)
        if not email:
            logger.warning("Invalid or expired password reset token used")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid token"
            )
        
        user = await self.repo.get_user_by_email(email)
        if not user or not user.is_active:
            logger.warning("Password reset attempted for non-existent or inactive user", email=email)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found or is inactive"
            )
        
        user.password = get_password_hash(request_data.new_password)
        await self.repo.update(user)
        logger.info("Password successfully reset for user", email=email)



