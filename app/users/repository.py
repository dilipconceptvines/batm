# app/users/repository.py

from typing import List, Optional, Tuple

from sqlalchemy import select, func, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.users.models import Role, User


class UserRepository:
    """
    Data Access Layer for User and Role models.
    Handles all database interactions.
    """

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_user_by_email(self, email: str) -> Optional[User]:
        """Fetch a user by email address."""
        stmt = select(User).options(selectinload(User.roles)).where(User.email_address == email)
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()
    
    async def get_user_by_id(self, user_id: int) -> Optional[User]:
        """Fetch a user by ID."""
        stmt = select(User).options(selectinload(User.roles)).where(User.id == user_id)
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()
    
    async def get_role_by_id(self, role_id: int) -> Optional[Role]:
        """Fetch a role by ID."""
        stmt = select(Role).where(Role.id == role_id)
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()
    
    async def get_role_by_name(self, name: str) -> Optional[Role]:
        """Fetch a role by its name."""
        stmt = select(Role).where(Role.name == name)
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()
    
    async def get_roles_by_ids(self, role_ids: List[int]) -> List[Role]:
        """Fetch multiple roles by their IDs."""
        stmt = select(Role).where(Role.id.in_(role_ids))
        result = await self.db.execute(stmt)
        return result.scalars().all()
    
    async def create(self, user: User) -> User:
        """Create a new user."""
        self.db.add(user)
        await self.db.commit()
        await self.db.refresh(user)
        return user
    
    async def update(self, user: User) -> User:
        """Update an existing user."""
        self.db.add(user)
        await self.db.commit()
        await self.db.refresh(user)
        return user
    
    async def search_users(
        self, search: Optional[str], roles : Optional[str], sort_by: str, sort_order: str, skip: int, limit: int
    ) -> Tuple[List[User], int]:
    
        """
        Searches, sorts, and paginates users from the database.

        Returns a tuple containing the list of users and the total count of matching users.
        """
        stmt = select(User).options(selectinload(User.roles))

        # Apply search filter if a term in provided
        if search:
            search_term = f"%{search.lower()}%"
            stmt = stmt.filter(
                or_(
                    func.lower(User.first_name).ilike(search_term),
                    func.lower(User.last_name).ilike(search_term),
                    func.lower(User.email_address).ilike(search_term),
                )
            )

        if roles:
            role = [role.strip() for role in roles.split(',')]
            stmt = stmt.join(User.roles).filter(or_(*[Role.name.ilike(f"%{r}%") for r in role]))

        # First, get the total count of items that match the filter
        count_stmt = select(func.count()).select_from(stmt.subquery())
        total_count_result = await self.db.execute(count_stmt)
        total_count = total_count_result.scalar_one()

        # Define valid sortable columns to prevent arbitrary column sorting
        sortable_columns = {
            "name": User.first_name,
            "email": User.email_address,
            "created_on": User.created_on,
        }
        sort_column = sortable_columns.get(sort_by, User.first_name)

        # Apply sorting
        if sort_order.lower() == "desc":
            stmt = stmt.order_by(sort_column.desc())
        else:
            stmt = stmt.order_by(sort_column.asc())

        # Apply pagination
        stmt = stmt.offset(skip).limit(limit)

        # Execute the final query to get the paginated list of users
        result = await self.db.execute(stmt)
        users = result.scalars().all()

        return users, total_count
