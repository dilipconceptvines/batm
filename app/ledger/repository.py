# app/ledger/repository.py

from datetime import date
from decimal import Decimal
from typing import List, Optional, Tuple

from sqlalchemy import case, func, or_, select
from sqlalchemy.orm import Session, joinedload

from app.drivers.models import Driver
from app.ledger.exceptions import BalanceNotFoundError, PostingNotFoundError
from app.ledger.models import (
    BalanceStatus,
    EntryType,
    LedgerBalance,
    LedgerPosting,
    PostingCategory,
    PostingStatus,
)
from app.medallions.models import Medallion
from app.utils.logger import get_logger

logger = get_logger(__name__)


class LedgerRepository:
    """
    Data Access Layer for the Centralized Ledger.
    Handles all database interactions for LedgerPosting and LedgerBalance models.
    """

    def __init__(self, db: Session):
        self.db = db

    def create_posting(self, posting: LedgerPosting) -> LedgerPosting:
        """
        Adds a new LedgerPosting record to the session.
        The caller is responsible for committing the transaction.
        """
        self.db.add(posting)
        self.db.commit()
        self.db.refresh(posting)
        logger.info("Created new LedgerPosting", posting_id=posting.id, category=posting.category, amount=posting.amount)
        return posting

    def get_posting_by_id(self, posting_id: str) -> LedgerPosting:
        """
        Fetches a single ledger posting by its unique ID.
        Raises PostingNotFoundError if not found.
        """
        stmt = select(LedgerPosting).where(LedgerPosting.id == posting_id)
        result = self.db.execute(stmt)
        posting = result.scalar_one_or_none()
        if not posting:
            raise PostingNotFoundError(posting_id=posting_id)
        return posting
    
    def get_posting_by_reference_id(self, reference_id: str) -> LedgerPosting:
        """
        Fetches a single ledger posting by its reference_id.
        Raises PostingNotFoundError if not found.
        """
        stmt = select(LedgerPosting).where(LedgerPosting.reference_id == reference_id).order_by(LedgerPosting.created_on.desc())
        result = self.db.execute(stmt)
        posting = result.scalars().first()
        if not posting:
            raise PostingNotFoundError(reference_id=reference_id)
        return posting

    def update_posting_status(
        self, posting: LedgerPosting, status: PostingStatus
    ) -> LedgerPosting:
        """Updates the status of an existing LedgerPosting (e.g., to VOIDED)."""
        posting.status = status
        self.db.flush()
        self.db.refresh(posting)
        logger.info("Updated LedgerPosting status", posting_id=posting.id, new_status=status.value)
        return posting

    def create_balance(self, balance: LedgerBalance) -> LedgerBalance:
        """
        Adds a new LedgerBalance record to the session.
        The caller is responsible for committing the transaction.
        """
        self.db.add(balance)
        self.db.flush()
        self.db.refresh(balance)
        logger.info("Created new LedgerBalance", balance_id=balance.id, category=balance.category, amount=balance.balance)
        return balance

    def get_balance_by_reference_id(self, reference_id: str) -> Optional[LedgerBalance]:
        """
        Fetches a single LedgerBalance by its reference_id.
        Returns None if not found.
        """
        stmt = select(LedgerBalance).where(LedgerBalance.reference_id == reference_id).order_by(LedgerBalance.created_on.desc())
        result = self.db.execute(stmt)
        return result.scalars().first()

    def get_balance_by_id(self, balance_id: str) -> LedgerBalance:
        """
        Fetches a single LedgerBalance by its unique ID.
        Raises BalanceNotFoundError if not found.
        """
        stmt = select(LedgerBalance).where(LedgerBalance.id == balance_id)
        result = self.db.execute(stmt)
        balance = result.scalar_one_or_none()
        if not balance:
            raise BalanceNotFoundError(balance_id)
        return balance

    def update_balance(
        self, balance: LedgerBalance, new_balance: Decimal, status: Optional[BalanceStatus] = None
    ) -> LedgerBalance:
        """
        Updates the balance and optionally the status of a LedgerBalance record.
        """
        balance.balance = new_balance
        if status:
            balance.status = status
        self.db.flush()
        self.db.refresh(balance)
        logger.info("Updated LedgerBalance", balance_id=balance.id, new_balance=new_balance, status=status)
        return balance

    def get_open_balances_for_driver(
        self, driver_id: int, lease_id: Optional[int] = None
    ) -> List[LedgerBalance]:
        """
        Fetches all OPEN balances for a driver, correctly ordered by:
        1. Category hierarchy (as defined in the payment priority)
        2. Created_on date (oldest first within each category)
        """
        # Define the payment hierarchy order
        category_order = case(
            (LedgerBalance.category == PostingCategory.TAXES, 1),
            (LedgerBalance.category == PostingCategory.EZPASS, 2),
            (LedgerBalance.category == PostingCategory.LEASE, 3),
            (LedgerBalance.category == PostingCategory.PVB, 4),
            (LedgerBalance.category == PostingCategory.TLC, 5),
            (LedgerBalance.category == PostingCategory.REPAIR, 6),
            (LedgerBalance.category == PostingCategory.LOAN, 7),
            (LedgerBalance.category == PostingCategory.MISC, 8),
            (LedgerBalance.category == PostingCategory.DEPOSIT, 9),
            else_=99,
        )

        stmt = (
            select(LedgerBalance)
            .where(
                LedgerBalance.driver_id == driver_id,
                LedgerBalance.status == BalanceStatus.OPEN,
            )
            .order_by(category_order, LedgerBalance.created_on)
        )

        if lease_id:
            stmt = stmt.where(LedgerBalance.lease_id == lease_id)

        result = self.db.execute(stmt)
        return list(result.scalars().all())
    
    def get_balance_by_lease_and_category(
        self, 
        lease_id: int, 
        category: PostingCategory
    ) -> Optional[LedgerBalance]:
        """
        Get the balance record for a specific lease and category.
        Used for finding the LEASE balance when applying excess payments.
        """
        return (
            self.db.query(LedgerBalance)
            .filter(
                LedgerBalance.lease_id == lease_id,
                LedgerBalance.category == category,
                LedgerBalance.status == BalanceStatus.OPEN
            )
            .order_by(LedgerBalance.id.desc())  # Get oldest first
            .first()
        )

    def list_postings(
        self,
        page: Optional[int] = None,
        per_page: Optional[int] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "desc",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        status: Optional[PostingStatus] = None,
        category: Optional[PostingCategory] = None,
        entry_type: Optional[EntryType] = None,
        driver_name: Optional[str] = None,
        lease_id: Optional[int] = None,
        vehicle_vin: Optional[str] = None,
        medallion_no: Optional[str] = None,
        include_all: bool = False,
    ) -> Tuple[List[LedgerPosting], int]:
        """
        Fetches a filtered, sorted, and paginated list of LedgerPosting records.
        """
        stmt = (
            select(LedgerPosting)
            .options(
                joinedload(LedgerPosting.driver),
                joinedload(LedgerPosting.vehicle),
                joinedload(LedgerPosting.medallion),
            )
        )

        # Apply filters
        if start_date:
            stmt = stmt.where(LedgerPosting.created_on >= start_date)
        if end_date:
            end_of_day = date(end_date.year, end_date.month, end_date.day)
            stmt = stmt.where(LedgerPosting.created_on <= end_of_day)
        if status:
            stmt = stmt.where(LedgerPosting.status == status)
        if category:
            stmt = stmt.where(LedgerPosting.category == category)
        if entry_type:
            stmt = stmt.where(LedgerPosting.entry_type == entry_type)
        if lease_id:
            stmt = stmt.where(LedgerPosting.lease_id == lease_id)
        if vehicle_vin:
            stmt = stmt.where(LedgerPosting.vin == vehicle_vin)
        if medallion_no:
            stmt = stmt.where(Medallion.medallion_number.ilike(f"%{medallion_no}%"))
        if driver_name:
            stmt = stmt.join(Driver, LedgerPosting.driver_id == Driver.id).where(
                or_(
                    Driver.first_name.ilike(f"%{driver_name}%"),
                    Driver.last_name.ilike(f"%{driver_name}%"),
                    func.concat(Driver.first_name, " ", Driver.last_name).ilike(f"%{driver_name}%"),
                )
            )

        # Count total items
        count_stmt = select(func.count()).select_from(stmt.subquery())
        total_items = self.db.execute(count_stmt).scalar()

        # Apply sorting
        if sort_by:
            order_column = getattr(LedgerPosting, sort_by, LedgerPosting.created_on)
            if sort_order == "asc":
                stmt = stmt.order_by(order_column.asc())
            else:
                stmt = stmt.order_by(order_column.desc())
        else:
            stmt = stmt.order_by(LedgerPosting.created_on.desc())

        # Apply pagination unless include_all is True
        if not include_all and page and per_page:
            offset = (page - 1) * per_page
            stmt = stmt.offset(offset).limit(per_page)

        result = self.db.execute(stmt)
        postings = list(result.scalars().all())

        return postings, total_items

    def list_balances(
        self,
        page: Optional[int] = None,
        per_page: Optional[int] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "desc",
        driver_name: Optional[str] = None,
        lease_id: Optional[int] = None,
        status: Optional[BalanceStatus] = None,
        category: Optional[PostingCategory] = None,
        include_all: bool = False,
    ) -> Tuple[List[LedgerBalance], int]:
        """
        Fetches a filtered, sorted, and paginated list of LedgerBalance records.
        """
        stmt = (
            select(LedgerBalance)
            .options(
                joinedload(LedgerBalance.driver),
                joinedload(LedgerBalance.vehicle),
            )
        )

        # Apply filters
        if lease_id:
            stmt = stmt.where(LedgerBalance.lease_id == lease_id)
        if status:
            stmt = stmt.where(LedgerBalance.status == status)
        if category:
            stmt = stmt.where(LedgerBalance.category == category)
        if driver_name:
            stmt = stmt.join(Driver, LedgerBalance.driver_id == Driver.id).where(
                or_(
                    Driver.first_name.ilike(f"%{driver_name}%"),
                    Driver.last_name.ilike(f"%{driver_name}%"),
                    func.concat(Driver.first_name, " ", Driver.last_name).ilike(f"%{driver_name}%"),
                )
            )

        # Count total items
        count_stmt = select(func.count()).select_from(stmt.subquery())
        total_items = self.db.execute(count_stmt).scalar()

        # Apply sorting
        if sort_by:
            order_column = getattr(LedgerBalance, sort_by, LedgerBalance.created_on)
            if sort_order == "asc":
                stmt = stmt.order_by(order_column.asc())
            else:
                stmt = stmt.order_by(order_column.desc())
        else:
            stmt = stmt.order_by(LedgerBalance.created_on.desc())

        # Apply pagination unless include_all is True
        if not include_all and page and per_page:
            offset = (page - 1) * per_page
            stmt = stmt.offset(offset).limit(per_page)

        result = self.db.execute(stmt)
        balances = list(result.scalars().all())

        return balances, total_items
    
    def get_balance_by_lease_category(
        self, 
        lease_id: int, 
        category: PostingCategory
    ) -> Optional[LedgerBalance]:
        """
        Get the balance record for a specific lease and category.
        Alias for get_balance_by_lease_and_category for backward compatibility.
        """
        return self.get_balance_by_lease_and_category(lease_id, category)
 