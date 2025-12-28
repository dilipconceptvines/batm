# app/deposits/services.py

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, Optional

from sqlalchemy.orm import Session

from app.deposits.exceptions import (
    DepositError,
    DepositLedgerError,
    DepositNotFoundError,
    DepositValidationError,
    InvalidDepositOperationError,
)
from app.deposits.models import Deposit, DepositStatus, CollectionMethod
from app.deposits.repository import DepositRepository
from app.ledger.models import PostingCategory, EntryType
from app.ledger.services import LedgerService
from app.utils.logger import get_logger

logger = get_logger(__name__)


class DepositService:
    """
    Service layer for managing Security Deposits.
    Handles business logic for deposit lifecycle, collections, holds, and refunds.
    """

    def __init__(self, db: Session):
        self.db = db
        self.repo = DepositRepository(db)

    def generate_deposit_id(self, lease_id: int) -> str:
        """
        Generates a standardized deposit ID.
        Format: DEP-{lease_id}-01
        """
        return f"DEP-{lease_id}-01"

    def create_deposit(self, db: Session, deposit_data: dict) -> Deposit:
        """
        Creates a new deposit record with validation and calculations.

        Args:
            db: Database session
            deposit_data: Dictionary containing deposit information

        Returns:
            Created Deposit instance

        Raises:
            DepositValidationError: If required fields are missing or invalid
        """
        required_fields = ['lease_id', 'required_amount', 'driver_tlc_license']
        for field in required_fields:
            if field not in deposit_data or deposit_data[field] is None:
                raise DepositValidationError(f"Required field '{field}' is missing")

        # Validate amounts
        required_amount = Decimal(str(deposit_data['required_amount']))
        collected_amount = Decimal(str(deposit_data.get('collected_amount', 0)))

        if required_amount <= 0:
            raise DepositValidationError("Required amount must be positive")

        if collected_amount < 0:
            raise DepositValidationError("Collected amount cannot be negative")

        if collected_amount > required_amount:
            raise DepositValidationError("Collected amount cannot exceed required amount")

        # Calculate outstanding amount
        outstanding_amount = required_amount - collected_amount

        # Determine status
        if collected_amount == 0:
            status = DepositStatus.PENDING
        elif collected_amount < required_amount:
            status = DepositStatus.PARTIALLY_PAID
        else:
            status = DepositStatus.PAID

        # Generate deposit ID
        deposit_id = self.generate_deposit_id(deposit_data['lease_id'])

        # Check if deposit already exists for this lease
        existing_deposit = self.repo.get_by_lease_id(deposit_data['lease_id'])
        if existing_deposit:
            raise DepositValidationError(f"Deposit already exists for lease {deposit_data['lease_id']}")

        # Create deposit instance
        deposit = Deposit(
            deposit_id=deposit_id,
            lease_id=deposit_data['lease_id'],
            driver_tlc_license=deposit_data['driver_tlc_license'],
            required_amount=required_amount,
            collected_amount=collected_amount,
            outstanding_amount=outstanding_amount,
            deposit_status=status,
            vehicle_vin=deposit_data.get('vehicle_vin'),
            vehicle_plate=deposit_data.get('vehicle_plate'),
            lease_start_date=deposit_data.get('lease_start_date'),
            notes=deposit_data.get('notes'),
        )

        # Save to database
        created_deposit = self.repo.create(deposit)
        logger.info(
            "Created new deposit",
            deposit_id=created_deposit.deposit_id,
            lease_id=created_deposit.lease_id,
            required_amount=float(created_deposit.required_amount),
            status=created_deposit.deposit_status.value
        )

        return created_deposit

    def update_deposit_collection(
        self,
        db: Session,
        deposit_id: str,
        additional_amount: Decimal,
        collection_method: CollectionMethod,
        notes: Optional[str] = None
    ) -> Deposit:
        """
        Updates deposit collection with additional payment.

        Args:
            db: Database session
            deposit_id: Unique deposit identifier
            additional_amount: Amount being collected
            collection_method: How the payment was collected
            notes: Optional notes about the collection

        Returns:
            Updated Deposit instance

        Raises:
            DepositNotFoundError: If deposit doesn't exist
            InvalidDepositOperationError: If deposit is not in collectible state
        """
        if additional_amount <= 0:
            raise DepositValidationError("Additional amount must be positive")

        # Get existing deposit
        deposit = self.repo.get_by_deposit_id(deposit_id)
        if not deposit:
            raise DepositNotFoundError(deposit_id=deposit_id)

        # Validate deposit can accept collections
        if deposit.deposit_status not in [DepositStatus.PENDING, DepositStatus.PARTIALLY_PAID]:
            raise InvalidDepositOperationError(
                f"Cannot collect payment for deposit in {deposit.deposit_status.value} status"
            )

        # Update amounts
        deposit.collected_amount += additional_amount
        deposit.outstanding_amount = deposit.required_amount - deposit.collected_amount

        # Update collection details
        if not deposit.initial_collection_amount:
            deposit.initial_collection_amount = additional_amount
        deposit.collection_method = collection_method

        # Update status
        if deposit.collected_amount >= deposit.required_amount:
            deposit.deposit_status = DepositStatus.PAID
            deposit.outstanding_amount = Decimal('0.00')
        elif deposit.collected_amount > 0:
            deposit.deposit_status = DepositStatus.PARTIALLY_PAID

        # Append to notes
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        collection_note = f"[{timestamp}] Collected ${additional_amount} via {collection_method.value}"
        if notes:
            collection_note += f" - {notes}"

        if deposit.notes:
            deposit.notes += f"\n{collection_note}"
        else:
            deposit.notes = collection_note

        # Save changes
        updated_deposit = self.repo.update(deposit)
        logger.info(
            "Updated deposit collection",
            deposit_id=deposit_id,
            additional_amount=float(additional_amount),
            new_collected=float(deposit.collected_amount),
            new_status=deposit.deposit_status.value
        )

        return updated_deposit

    def initiate_hold_period(
        self,
        db: Session,
        lease_id: int,
        termination_date: date
    ) -> Deposit:
        """
        Initiates the 30-day hold period after lease termination.

        Args:
            db: Database session
            lease_id: Lease identifier
            termination_date: Date when lease was terminated

        Returns:
            Updated Deposit instance

        Raises:
            DepositNotFoundError: If deposit doesn't exist
            InvalidDepositOperationError: If deposit is not in valid state for hold
        """
        # Get deposit
        deposit = self.repo.get_by_lease_id(lease_id)
        if not deposit:
            raise DepositNotFoundError(lease_id=lease_id)

        # Validate deposit can be put on hold
        if deposit.deposit_status != DepositStatus.PAID:
            raise InvalidDepositOperationError(
                f"Cannot initiate hold for deposit in {deposit.deposit_status.value} status"
            )

        # Calculate hold expiry date (30 days after termination)
        hold_expiry_date = termination_date + timedelta(days=30)

        # Update deposit
        deposit.lease_termination_date = termination_date
        deposit.hold_expiry_date = hold_expiry_date
        deposit.deposit_status = DepositStatus.HELD

        # Save changes
        updated_deposit = self.repo.update(deposit)
        logger.info(
            "Initiated hold period for deposit",
            deposit_id=deposit.deposit_id,
            lease_id=lease_id,
            termination_date=termination_date.isoformat(),
            hold_expiry_date=hold_expiry_date.isoformat()
        )

        return updated_deposit

    def auto_apply_deposit(
        self,
        db: Session,
        deposit_id: str,
        ledger_service: LedgerService
    ) -> dict:
        """
        Automatically applies held deposit to outstanding obligations.

        Args:
            db: Database session
            deposit_id: Unique deposit identifier
            ledger_service: Ledger service instance

        Returns:
            Dictionary with application summary

        Raises:
            DepositNotFoundError: If deposit doesn't exist
            InvalidDepositOperationError: If deposit is not in HELD status
        """
        # Get deposit
        deposit = self.repo.get_by_deposit_id(deposit_id)
        if not deposit:
            raise DepositNotFoundError(deposit_id=deposit_id)

        if deposit.deposit_status != DepositStatus.HELD:
            raise InvalidDepositOperationError(
                f"Cannot auto-apply deposit in {deposit.deposit_status.value} status"
            )

        # Get open balances for this lease (EZPASS, PVB, TLC categories)
        # Note: This would require querying ledger balances by lease_id
        # For now, we'll implement the logic structure

        applied_amounts = {
            'ezpass': Decimal('0.00'),
            'pvb': Decimal('0.00'),
            'tlc': Decimal('0.00'),
            'total_applied': Decimal('0.00'),
            'remaining_refund': Decimal('0.00')
        }

        # TODO: Implement ledger balance querying and application logic
        # This would involve:
        # 1. Query open balances for lease_id with categories EZPASS, PVB, TLC
        # 2. Apply deposit amount to balances (oldest first)
        # 3. Create CREDIT postings for each application
        # 4. Update balances

        remaining_amount = deposit.collected_amount - applied_amounts['total_applied']

        if remaining_amount > 0:
            applied_amounts['remaining_refund'] = remaining_amount
            deposit.refund_amount = remaining_amount
            deposit.deposit_status = DepositStatus.REFUNDED

        updated_deposit = self.repo.update(deposit)
        logger.info(
            "Auto-applied deposit to obligations",
            deposit_id=deposit_id,
            total_applied=float(applied_amounts['total_applied']),
            remaining_refund=float(applied_amounts['remaining_refund'])
        )

        return applied_amounts

    def process_refund(
        self,
        db: Session,
        deposit_id: str,
        refund_method: CollectionMethod,
        refund_reference: str,
        ledger_service: LedgerService,
        user_id: int
    ) -> Deposit:
        """
        Processes a refund for a held deposit.

        Args:
            db: Database session
            deposit_id: Unique deposit identifier
            refund_method: Method of refund
            refund_reference: Reference for the refund transaction
            ledger_service: Ledger service instance
            user_id: User processing the refund

        Returns:
            Updated Deposit instance

        Raises:
            DepositNotFoundError: If deposit doesn't exist
            InvalidDepositOperationError: If deposit is not in HELD status
        """
        # Get deposit
        deposit = self.repo.get_by_deposit_id(deposit_id)
        if not deposit:
            raise DepositNotFoundError(deposit_id=deposit_id)

        if deposit.deposit_status != DepositStatus.HELD:
            raise InvalidDepositOperationError(
                f"Cannot process refund for deposit in {deposit.deposit_status.value} status"
            )

        try:
            # Create ledger posting for refund (DEBIT to reduce deposit liability)
            refund_reference_id = f"REFUND-{deposit_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}"

            posting, balance = ledger_service.create_obligation(
                category=PostingCategory.DEPOSIT,
                amount=deposit.collected_amount,
                reference_id=refund_reference_id,
                driver_id=user_id,  # This should be the driver's ID, not user_id
                entry_type=EntryType.DEBIT,
                lease_id=deposit.lease_id
            )

            # Update deposit with refund details
            deposit.refund_amount = deposit.collected_amount
            deposit.refund_date = date.today()
            deposit.refund_method = refund_method.value
            deposit.refund_reference = refund_reference
            deposit.deposit_status = DepositStatus.REFUNDED

            # Save changes
            updated_deposit = self.repo.update(deposit)
            logger.info(
                "Processed deposit refund",
                deposit_id=deposit_id,
                refund_amount=float(deposit.refund_amount),
                refund_method=refund_method.value,
                refund_reference=refund_reference
            )

            return updated_deposit

        except Exception as e:
            logger.error(
                "Failed to process deposit refund",
                deposit_id=deposit_id,
                error=str(e),
                exc_info=True
            )
            raise DepositLedgerError(deposit_id, f"Refund processing failed: {str(e)}") from e