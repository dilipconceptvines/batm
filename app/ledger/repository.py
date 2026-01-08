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
from app.leases.models import Lease
from app.vehicles.models import Vehicle
from app.utils.logger import get_logger
from app.utils.general import apply_multi_filter

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
    
    def get_posting_by_reference_id(
    self,
    reference_id: str,
    is_last: bool = False
    ) -> LedgerPosting:
        """
        Fetches a single ledger posting by its reference_id.

        - is_last=False → latest posting
        - is_last=True  → oldest posting

        Raises PostingNotFoundError if not found.
        """

        order_clause = (
            LedgerPosting.created_on.asc()
            if not is_last
            else LedgerPosting.created_on.desc()
        )

        stmt = (
            select(LedgerPosting)
            .where(LedgerPosting.reference_id == reference_id)
            .order_by(order_clause)
            .limit(1)
        )

        posting = self.db.execute(stmt).scalar_one_or_none()

        if not posting:
            return None

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
            (LedgerBalance.category == PostingCategory.MISCELLANEOUS_EXPENSE, 8),
            (LedgerBalance.category == PostingCategory.MISCELLANEOUS_CREDIT, 8),
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
        sort_order: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        posting_id: Optional[str] = None,
        from_amount: Optional[str] = None,
        to_amount: Optional[str] = None,
        status: Optional[PostingStatus] = None,
        category: Optional[PostingCategory] = None,
        entry_type: Optional[EntryType] = None,
        reference_id: Optional[str] = None,
        driver_name: Optional[str] = None,
        lease_id: Optional[str] = None,
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

        join_driver = False
        join_vehicle = False
        join_medallion = False
        join_lease = False

        # Apply filters
        if start_date:
            stmt = stmt.where(LedgerPosting.created_on >= start_date)
        if end_date:
            end_of_day = date(end_date.year, end_date.month, end_date.day)
            stmt = stmt.where(LedgerPosting.created_on <= end_of_day)
        if posting_id:
            stmt = apply_multi_filter(stmt, LedgerPosting.id, posting_id)
        if from_amount:
            stmt = stmt.where(LedgerPosting.amount >= from_amount)
        if to_amount:
            stmt =  stmt.where(LedgerPosting.amount <= to_amount)
        if status:
            stmt = stmt.where(LedgerPosting.status == status)
        if category:
            stmt = stmt.where(LedgerPosting.category == category)
        if entry_type:
            stmt = stmt.where(LedgerPosting.entry_type == entry_type)
        
        if reference_id:
            stmt = apply_multi_filter(stmt, LedgerPosting.reference_id, reference_id)

        if lease_id:
            if not join_lease:
                stmt = stmt.join(Lease, LedgerPosting.lease_id == Lease.id)
                join_lease = True
            stmt = apply_multi_filter(stmt, Lease.lease_id, lease_id)

        if vehicle_vin:
            if not join_vehicle:
                stmt = stmt.join(Vehicle, LedgerPosting.vehicle_id == Vehicle.id)
                join_vehicle = True
            stmt = apply_multi_filter(stmt, Vehicle.vin, vehicle_vin)

        if medallion_no:
            if not join_medallion:
                stmt = stmt.join(Medallion, LedgerPosting.medallion_id == Medallion.id)
                join_medallion = True
            stmt = apply_multi_filter(stmt, Medallion.medallion_number, medallion_no)
        if driver_name:
            if not join_driver:
                stmt = stmt.join(Driver, LedgerPosting.driver_id == Driver.id)
                join_driver = True
            stmt = apply_multi_filter(stmt, Driver.full_name, driver_name)

        # Count total items
        count_stmt = select(func.count()).select_from(stmt.subquery())
        total_items = self.db.execute(count_stmt).scalar()

        map_sorting = {
            "posting_id": LedgerPosting.id,
            "reference_id": LedgerPosting.reference_id,
            "category": LedgerPosting.category,
            "entry_type": LedgerPosting.entry_type,
            "status": LedgerPosting.status,
            "driver_id": LedgerPosting.driver_id,
            "amount": LedgerPosting.amount,
            "driver_name": Driver.full_name,
            "vehicle_vin": Vehicle.vin,
            "medallion_no": Medallion.medallion_number,
            "lease_id": LedgerPosting.lease_id,
            "date": LedgerPosting.created_on,
        }

        # Apply sorting
        if sort_by:
            order_column = map_sorting.get(sort_by, LedgerPosting.created_on)
            if sort_by == "driver_name":
                if not join_driver:
                    stmt = stmt.join(Driver, LedgerPosting.driver_id == Driver.id)
                    join_driver = True
                order_column = Driver.full_name

            elif sort_by == "driver_id":
                if not join_driver:
                    stmt = stmt.join(Driver, LedgerPosting.driver_id == Driver.id)
                    join_driver = True
                order_column = Driver.driver_id

            elif sort_by == "vehicle_vin":
                if not join_vehicle:
                    stmt = stmt.join(Vehicle, LedgerPosting.vehicle_id == Vehicle.id)
                    join_vehicle = True
                order_column = Vehicle.vin

            elif sort_by == "lease_id":
                if not join_lease:
                    stmt = stmt.join(Lease, LedgerPosting.lease_id == Lease.id)
                    join_lease = True
                order_column = Lease.lease_id
            elif sort_by == "medallion_no":
                if not join_medallion:
                    stmt = stmt.join(Medallion, LedgerPosting.medallion_id == Medallion.id)
                    join_medallion = True
                order_column = Medallion.medallion_number

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
        sort_order: Optional[str] = None,
        balance_id: Optional[str] = None,
        reference_id: Optional[str] = None,
        from_original_amount: Optional[float] = None,
        to_original_amount: Optional[float] = None,
        from_prior_balance: Optional[float] = None,
        to_prior_balance: Optional[float] = None,
        from_balance: Optional[float] = None,
        to_balance: Optional[float] = None,
        driver_name: Optional[str] = None,
        lease_id: Optional[str] = None,
        vehicle_vin: Optional[str] = None,
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
                joinedload(LedgerBalance.lease),
                joinedload(LedgerBalance.medallion),
            )
        )

        join_lease = False
        join_vehicle = False
        join_driver = False

        # Apply filters
        if lease_id:
            if not join_lease:
                stmt = stmt.join(Lease, LedgerBalance.lease_id == Lease.id)
                join_lease = True            
            stmt = apply_multi_filter(stmt, Lease.lease_id, lease_id)

        if status:
            stmt = stmt.where(LedgerBalance.status == status)

        if category:
            stmt = stmt.where(LedgerBalance.category == category)

        if balance_id:
            stmt = apply_multi_filter(stmt, LedgerBalance.id, balance_id)

        if reference_id:
            stmt = apply_multi_filter(stmt, LedgerBalance.reference_id, reference_id)

        if from_original_amount:
            stmt = stmt.where(LedgerBalance.original_amount >= from_original_amount)

        if to_original_amount:
            stmt = stmt.where(LedgerBalance.original_amount <= to_original_amount)

        if from_prior_balance:
            stmt = stmt.where(LedgerBalance.prior_balance >= from_prior_balance)

        if to_prior_balance:
            stmt = stmt.where(LedgerBalance.prior_balance <= to_prior_balance)

        if from_balance:
            stmt = stmt.where(LedgerBalance.balance >= from_balance)

        if to_balance:
            stmt = stmt.where(LedgerBalance.balance <= to_balance)

        if driver_name:
            if not join_driver:
                stmt = stmt.join(Driver, LedgerBalance.driver_id == Driver.id)
                join_driver = True
            stmt = apply_multi_filter(stmt, Driver.full_name, driver_name)

        if vehicle_vin:
            if not join_vehicle:
                stmt = stmt.join(Vehicle, LedgerBalance.vehicle_id == Vehicle.id)
                join_vehicle = True
            stmt = apply_multi_filter(stmt, Vehicle.vin, vehicle_vin)

        # Count total items
        count_stmt = select(func.count()).select_from(stmt.subquery())
        total_items = self.db.execute(count_stmt).scalar()

        sorting_map = {
            "balance_id": LedgerBalance.id,
            "reference_id": LedgerBalance.reference_id,
            "category": LedgerBalance.category,
            "status": LedgerBalance.status,
            "driver_id": LedgerBalance.driver_id,
            "original_amount": LedgerBalance.original_amount,
            "balance": LedgerBalance.balance,
            "prior_balance": LedgerBalance.prior_balance,
            "driver_name": Driver.full_name,
            "vehicle_vin": Vehicle.vin,
            "lease_id": LedgerBalance.lease_id,
            "created_on": LedgerBalance.created_on,
        }

        # Apply sorting
        if sort_by and sort_order:
            order_column = sorting_map.get(sort_by, LedgerBalance.created_on)
            if sort_by == "driver_name":
                if not join_driver:
                    stmt = stmt.join(Driver, LedgerBalance.driver_id == Driver.id)
                    join_driver = True
                order_column = Driver.full_name

            elif sort_by == "driver_id":
                if not join_driver:
                    stmt = stmt.join(Driver, LedgerBalance.driver_id == Driver.id)
                    join_driver = True
                order_column = Driver.driver_id

            elif sort_by == "vehicle_vin":
                if not join_vehicle:
                    stmt = stmt.join(Vehicle, LedgerBalance.vehicle_id == Vehicle.id)
                    join_vehicle = True
                order_column = Vehicle.vin

            elif sort_by == "lease_id":
                if not join_lease:
                    stmt = stmt.join(Lease, LedgerBalance.lease_id == Lease.id)
                    join_lease = True
                order_column = Lease.lease_id

            if sort_order == "asc":
                stmt = stmt.order_by(order_column.asc())
            else:
                stmt = stmt.order_by(order_column.desc())
        else:
            stmt = stmt.order_by(LedgerBalance.updated_on.desc() , LedgerBalance.created_on.desc())

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
 