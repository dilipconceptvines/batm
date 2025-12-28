# app/deposits/repository.py

from datetime import date
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.deposits.models import Deposit, DepositStatus
from app.utils.logger import get_logger

logger = get_logger(__name__)


class DepositRepository:
    """
    Data Access Layer for Security Deposits.
    Handles all database interactions for Deposit model.
    """

    def __init__(self, db: Session):
        self.db = db

    def get_by_deposit_id(self, deposit_id: str) -> Optional[Deposit]:
        """
        Fetches a single deposit by its unique deposit_id.
        Returns None if not found.
        """
        stmt = select(Deposit).where(Deposit.deposit_id == deposit_id)
        result = self.db.execute(stmt)
        return result.scalar_one_or_none()

    def get_by_lease_id(self, lease_id: int) -> Optional[Deposit]:
        """
        Fetches a single deposit by its lease_id.
        Returns None if not found.
        """
        stmt = select(Deposit).where(Deposit.lease_id == lease_id)
        result = self.db.execute(stmt)
        return result.scalar_one_or_none()

    def get_unpaid_deposits_by_start_date(self, start_date: date) -> List[Deposit]:
        """
        Fetches deposits that are unpaid (Pending or Partially Paid) for a given lease start date.
        Used for reminder automation.
        """
        stmt = select(Deposit).where(
            Deposit.lease_start_date == start_date,
            Deposit.deposit_status.in_([DepositStatus.PENDING, DepositStatus.PARTIALLY_PAID])
        )
        result = self.db.execute(stmt)
        return list(result.scalars().all())

    def get_deposits_by_hold_expiry_date(self, expiry_date: date) -> List[Deposit]:
        """
        Fetches deposits that are in Held status and have the specified hold expiry date.
        Used for auto-application after 30 days.
        """
        stmt = select(Deposit).where(
            Deposit.hold_expiry_date == expiry_date,
            Deposit.deposit_status == DepositStatus.HELD
        )
        result = self.db.execute(stmt)
        return list(result.scalars().all())

    def get_overdue_refunds(self, alert_date: date) -> List[Deposit]:
        """
        Fetches deposits that are in Held status and have hold expiry dates more than 5 days past the alert date.
        Used for refund overdue alerts.
        """
        from datetime import timedelta
        overdue_threshold = alert_date - timedelta(days=5)

        stmt = select(Deposit).where(
            Deposit.hold_expiry_date < overdue_threshold,
            Deposit.deposit_status == DepositStatus.HELD
        )
        result = self.db.execute(stmt)
        return list(result.scalars().all())

    def create(self, deposit: Deposit) -> Deposit:
        """
        Adds a new Deposit record to the session.
        The caller is responsible for committing the transaction.
        """
        self.db.add(deposit)
        self.db.flush()
        self.db.refresh(deposit)
        logger.info("Created new Deposit", deposit_id=deposit.deposit_id, lease_id=deposit.lease_id)
        return deposit

    def update(self, deposit: Deposit) -> Deposit:
        """
        Updates an existing Deposit record.
        The caller is responsible for committing the transaction.
        """
        self.db.flush()
        self.db.refresh(deposit)
        logger.info("Updated Deposit", deposit_id=deposit.deposit_id, status=deposit.deposit_status)
        return deposit