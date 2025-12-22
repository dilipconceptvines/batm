# app/user/router.py

import math
from typing import List, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, status, Query
from sqlalchemy import select

from app.core.jwt import create_access_token, create_refresh_token, verify_token
from app.users.models import User, Role
from app.users.schemas import (
    LoginRequest, TokenResponse, UserResponse, PasswordChangeRequest, RoleCreate,
    RoleResponse, PaginatedUserResponse, ForgotPasswordRequest, ResetPasswordRequest
)
from app.users.services import UserService
from app.users.utils import get_current_user, RoleChecker
from app.utils.logger import get_logger

# RBAC Dependencies for endpoint protection
allow_admin = RoleChecker(["Admin"])
allow_admin_or_manager = RoleChecker(["Admin", "Accident Manager"])

router = APIRouter(tags=["Users"])
logger = get_logger(__name__)

@router.post("/login", response_model=TokenResponse)
async def login(
    login_request: LoginRequest,
    user_service: UserService = Depends(),
):
    """Authenticate user and return access & refresh tokens."""
    user = await user_service.authenticate_user(login_request)

    if not user:
        logger.warning("Failed login attempt", email=login_request.email_id)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password."
        )
    
    access_token = create_access_token(data={"sub": user.email_address, "id": user.id})
    refresh_token = create_refresh_token(data={"sub": user.email_address, "id": user.id})

    return {"access_token": access_token, "refresh_token": refresh_token, "token_type": "bearer"}

@router.get("/user", response_model=UserResponse)
async def get_user_me(
    current_user: User = Depends(get_current_user),
):
    """Get details of the currently authenticated user."""
    return current_user

@router.post("/user/change-password", status_code=status.HTTP_204_NO_CONTENT)
async def change_password(
    password_data: PasswordChangeRequest,
    current_user: User = Depends(get_current_user),
    user_service: UserService = Depends(),
):
    """Allows an authenticated user to change their own password."""
    await user_service.change_password(current_user, password_data)
    return

@router.post("/refresh", response_model=TokenResponse)
async def refresh_access_token(
    authorization: str = Header(..., alias="Authorization"),
    user_service: UserService = Depends(),
):
    """Refresh the access token using a valid refresh token."""
    old_refresh_token = authorization.replace("Bearer ", "")
    payload = verify_token(old_refresh_token)
    email = payload.get("sub")

    user = await user_service.repo.get_user_by_email(email)
    if not user:
        logger.warning("Invalid refresh token attempt", email=email)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token."
        )
    
    new_access_token = create_access_token(data={"sub": user.email_address, "id": user.id})
    new_refresh_token = create_refresh_token(data={"sub": user.email_address, "id": user.id})

    return {"access_token": new_access_token, "refresh_token": new_refresh_token, "token_type": "bearer"}

# --- Admin/Manager endpoints for roles ---

@router.get("/roles", response_model=List[RoleResponse], dependencies=[Depends(allow_admin_or_manager)])
async def list_roles(
    user_service: UserService = Depends(),
):
    """List all available roles in the system."""
    # This logic would be in the service/repository in a larger app
    roles = await user_service.repo.db.execute(select(Role))
    return roles.scalars().all()

@router.post("/roles", response_model=RoleResponse, status_code=status.HTTP_201_CREATED, dependencies=[Depends(allow_admin)])
async def create_role(
    role_data: RoleCreate,
    user_service: UserService = Depends(),
):
    """Create a new role (Admin only)."""
    # This logic would be in the service/repository
    existing_role = await user_service.repo.get_role_by_name(role_data.name)

    if existing_role:
        logger.warning("Attempt to create duplicate role", role=role_data.name)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Role already exists."
        )
    
    new_role = Role(**role_data.model_dump())
    await user_service.repo.db.add(new_role)
    await user_service.repo.db.flush()
    await user_service.repo.db.refresh(new_role)
    return new_role

@router.get("/users", response_model=PaginatedUserResponse, dependencies=[Depends(allow_admin_or_manager)])
async def search_users(
    search: Optional[str] = Query(None, description="Search term for name or email"),
    roles: Optional[str] = Query(None, description="Filter by role name"),
    sort_by: str = Query("name", description="Sort by field (name, email, created_on)"),
    sort_order: str = Query("asc", description="Sort order (asc, desc)"),
    skip: int = Query(0, ge=0, description="Number of records to skip for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of records to return"),
    user_service: UserService = Depends()
):
    """
    Get a paginated list of users with optional search and sorting.
    Accessible only by users with 'Admin' or 'Manager' roles.
    """
    users, total_items = await user_service.search_users(
        search=search,
        roles=roles,
        sort_by=sort_by,
        sort_order=sort_order,
        skip=skip,
        limit=limit
    )

    page = (skip // limit) + 1
    total_pages = math.ceil(total_items / limit) if limit > 0 else 0

    return {
        "items": users,
        "total_items": total_items,
        "page": page,
        "per_page": limit,
        "total_pages": total_pages,
    }

@router.get("/logout")
async def logout(authorization: str = Header(..., alias="Authorization")):
    """Logout user"""
    return {"message": "Logout successful"}

@router.post("/forgot-password", status_code=status.HTTP_202_ACCEPTED)
async def forgot_password(
    request_data: ForgotPasswordRequest,
    user_service: UserService = Depends(),
):
    """
    Initiates the password reset process.
    Takes an email and sends a reset link if the user exists.
    Always returns a successful response to prevent email enumeration.
    """
    await user_service.request_password_reset(request_data)
    return {"message": "If an account with that email exists, a password reset link has been sent."}

@router.post("/reset-password", status_code=status.HTTP_204_NO_CONTENT)
async def reset_password(
    request_data: ResetPasswordRequest,
    user_service: UserService = Depends(),
):
    """
    Resets the user's password using a valid token.
    """
    await user_service.reset_password(request_data)
    return


