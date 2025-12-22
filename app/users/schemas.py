# app/users/schemas.py

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, EmailStr


class TokenResponse(BaseModel):
    """Schema for token response."""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class LoginRequest(BaseModel):
    """Schema for login request."""
    email_id: EmailStr
    password: str
    platform: Optional[str] = "web"


class RoleBase(BaseModel):
    """Base schema for role."""
    name: str
    description: Optional[str] = None


class RoleCreate(RoleBase):
    """Schema for creating a role."""
    pass


class RoleUpdate(RoleBase):
    """Schema for updating a role."""
    name: Optional[str] = None
    description: Optional[str] = None


class RoleResponse(RoleBase):
    """Schema for role response."""
    id: int
    is_active: bool
    is_archived: bool

    class Config:
        """Pydantic configuration."""
        from_attributes = True


class UserBase(BaseModel):
    """Base schema for user."""
    email_address: EmailStr
    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    last_name: Optional[str] = None


class UserCreate(UserBase):
    """Schema for creating a new user."""
    password: str
    roles: List[int] = []


class UserResponse(UserBase):
    """Schema for user response."""
    id: int
    is_active: bool
    is_archived: bool
    last_login: Optional[datetime] = None
    roles: List[RoleResponse] = []

    class Config:
        """Pydantic configuration."""
        from_attributes = True


class PasswordChangeRequest(BaseModel):
    """Schema for password change request."""
    current_password: str
    new_password: str


class PaginatedUserResponse(BaseModel):
    """Schema for a paginated list of users."""
    items: List[UserResponse]
    total_items: int
    page: int
    per_page: int
    total_pages: int


class ForgotPasswordRequest(BaseModel):
    """Schema for forgot password request."""
    email: EmailStr


class ResetPasswordRequest(BaseModel):
    """Schema for reset password request."""
    token: str
    new_password: str



