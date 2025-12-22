### app/medallions/crud.py

# Standard Library Imports
from typing import List, Union

# Third Party Imports
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc

# Local Imports
from app.utils.logger import get_logger
from app.medallions.models import (
    Medallion, MedallionRenewal, MedallionStorage,
    MedallionOwner
)

logger = get_logger(__name__)

class MedallionCRUD:
    """
    CRUD operations for Medallions
    """
    async def get_medallion(
            self,
            db: AsyncSession,
            lookup_id: str,
            lookup_value: str,
            multiple: bool = False
    ) -> Union[List[Medallion], Medallion, None]:
        """
        Get a medallion by lookup ID and value

        Args:
            db: Database session
            lookup_id: ID of the lookup
            lookup_value: Value of the lookup
            multiple: Whether to return multiple results

        Returns:
            Medallion or list of medallions
        """
        try:
            # Construct the base query
            query = select(Medallion)

            # Add filters
            if lookup_id and lookup_value:
                if lookup_id == "owner_id":
                    query = query.where(Medallion.owner_id == int(lookup_value))
                if lookup_id == "medallion_number":
                    query = query.where(Medallion.medallion_number == lookup_value)
                elif lookup_id == "id":
                    query = query.where(Medallion.id == int(lookup_value))

            # Execute the query
            result = await db.execute(query)
            if multiple:
                return result.scalars().all()
            return result.scalar_one_or_none()
            
        except Exception as e:
            logger.error("Error getting medallion: %s", str(e))
            raise e
        
    async def get_medallion_renewal(
            self,
            db: AsyncSession,
            lookup_type: str,
            lookup_value: str
    ):
        """
        Get a medallion renewal by lookup type and value

        Args:
            db: Database session
            lookup_type: Type of the lookup
            lookup_value: Value of the lookup

        Returns:
            Medallion renewal
        """
        try:
            # Construct the base query
            query = select(MedallionRenewal)

            # Add filters
            if lookup_type == "medallion_number":
                query = query.where(MedallionRenewal.medallion_number == lookup_value)
            elif lookup_type == "id":
                query = query.where(MedallionRenewal.id == int(lookup_value))

            # Execute the query
            result = await db.execute(query)
            return result.scalar_one_or_none()
            
        except Exception as e:
            logger.error("Error getting medallion renewal: %s", str(e))
            raise e
                
    async def get_medallion_storage(
            self,
            db: AsyncSession,
            lookup_type: str,
            lookup_value: str
    ):
        """
        Get a medallion storage by lookup type and value

        Args:
            db: Database session
            lookup_type: Type of the lookup
            lookup_value: Value of the lookup

        Returns:
            Medallion storage
        """
        try:
            # Construct the base query
            query = select(MedallionStorage)

            # Add filters
            if lookup_type == "medallion_number":
                query = query.where(MedallionStorage.medallion_number == lookup_value)
            elif lookup_type == "id":
                query = query.where(MedallionStorage.id == int(lookup_value))
                
            # Execute the query
            result = await db.execute(query)
            return result.scalar_one_or_none()
            
        except Exception as e:
            logger.error("Error getting medallion storage: %s", str(e))
            raise e
            
    async def get_medallion_owner(
        self,
        db: AsyncSession,
        lookup_type: str,
        lookup_value: str
    ):
        """
        Get a medallion owner by lookup type and value
        
        Args:
            db: Database session
            lookup_type: Type of the lookup
            lookup_value: Value of the lookup

        Returns:
            Medallion owner
        """
        try:
            if lookup_type == "id":
                owner = await db.execute(
                    select(MedallionOwner).where(MedallionOwner.id == int(lookup_value))
                )
                return owner.scalar_one_or_none()
            elif lookup_type == "medallion_id":
                medallion = await db.execute(
                    select(Medallion).where(Medallion.id == lookup_value)
                )
                medallion = medallion.scalar_one_or_none()
                return medallion.owner if medallion else None
            elif lookup_type == "address_id":
                owner = await db.execute(
                    select(MedallionOwner).where(MedallionOwner.primary_address_id == lookup_value).order_by(
                        desc(MedallionOwner.created_on)
                    )
                )
                return owner.scalar_one_or_none()
            else:
                raise ValueError(f"Invalid lookup type: {lookup_type}")
            
        except Exception as e:
            logger.error("Error getting medallion owner: %s", str(e))
            raise e
                
            
medallion_crud = MedallionCRUD()
