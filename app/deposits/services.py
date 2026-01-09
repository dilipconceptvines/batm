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
        Creates a new deposit record during lease creation.
        
        SIMPLIFIED: Only captures required_amount. No collection at creation.
        All payments must be made through interim payments.

        Args:
            db: Database session
            deposit_data: Dictionary containing deposit information
                Required keys:
                    - lease_id: int
                    - required_amount: Decimal
                Optional keys:
                    - driver_tlc_license: str
                    - vehicle_vin: str
                    - vehicle_plate: str
                    - lease_start_date: date
                    - notes: str

        Returns:
            Created Deposit instance

        Raises:
            DepositValidationError: If required fields are missing or invalid
        """
        # Validate required fields
        required_fields = ['lease_id', 'required_amount']
        for field in required_fields:
            if field not in deposit_data or deposit_data[field] is None:
                raise DepositValidationError(f"Required field '{field}' is missing")

        # Validate amount
        required_amount = Decimal(str(deposit_data['required_amount']))

        if required_amount < 0:
            raise DepositValidationError("Required amount cannot be negative")

        # Generate deposit ID
        deposit_id = self.generate_deposit_id(deposit_data['lease_id'])

        # Check if deposit already exists for this lease
        existing_deposit = self.repo.get_by_lease_id(deposit_data['lease_id'])
        if existing_deposit:
            raise DepositValidationError(f"Deposit already exists for lease {deposit_data['lease_id']}")

        # Create deposit instance
        # ALWAYS starts with collected_amount = 0, status = PENDING (or PAID if required = 0)
        collected_amount = Decimal('0.00')
        outstanding_amount = required_amount
        
        # Determine initial status
        if required_amount == 0:
            status = DepositStatus.PAID  # Waived deposit
        else:
            status = DepositStatus.PENDING

        deposit = Deposit(
            deposit_id=deposit_id,
            lease_id=deposit_data['lease_id'],
            driver_tlc_license=deposit_data.get('driver_tlc_license'),
            required_amount=required_amount,
            collected_amount=collected_amount,  # Always 0 at creation
            outstanding_amount=outstanding_amount,
            deposit_status=status,
            initial_collection_amount=None,  # Set when first payment made
            collection_method=None,  # Set when first payment made
            vehicle_vin=deposit_data.get('vehicle_vin'),
            vehicle_plate=deposit_data.get('vehicle_plate'),
            lease_start_date=deposit_data.get('lease_start_date'),
            notes=deposit_data.get('notes'),
        )

        # Save to database
        created_deposit = self.repo.create(deposit)
        logger.info(
            "Created new deposit (no collection at creation)",
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
        Updates deposit collection with payment from interim payment.
        
        This is the ONLY way deposits are collected per client requirements.

        Args:
            db: Database session
            deposit_id: Unique deposit identifier
            additional_amount: Amount being collected
            collection_method: How the payment was collected (from interim payment)
            notes: Optional notes about the collection

        Returns:
            Updated Deposit instance

        Raises:
            DepositNotFoundError: If deposit doesn't exist
            InvalidDepositOperationError: If deposit is not in collectible state
            DepositValidationError: If validation fails
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

        # Validate amount doesn't exceed outstanding
        if additional_amount > deposit.outstanding_amount:
            raise DepositValidationError(
                f"Payment amount ${additional_amount} exceeds outstanding deposit amount ${deposit.outstanding_amount}"
            )

        # Update amounts
        deposit.collected_amount += additional_amount
        deposit.outstanding_amount = deposit.required_amount - deposit.collected_amount

        # Set initial_collection_amount on FIRST payment only
        if not deposit.initial_collection_amount:
            deposit.initial_collection_amount = additional_amount

        # Update collection method (track most recent method)
        deposit.collection_method = collection_method

        # Update status based on new collected amount
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
            "Updated deposit collection via interim payment",
            deposit_id=deposit_id,
            additional_amount=float(additional_amount),
            new_collected=float(deposit.collected_amount),
            new_outstanding=float(deposit.outstanding_amount),
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
            lease_id: Lease ID being terminated
            termination_date: Date of termination

        Returns:
            Updated Deposit instance

        Raises:
            DepositNotFoundError: If deposit doesn't exist
        """
        deposit = self.repo.get_by_lease_id(lease_id)
        if not deposit:
            raise DepositNotFoundError(lease_id=lease_id)

        # Allow holding even if not fully paid (partial deposits can still be held)
        if deposit.deposit_status not in [DepositStatus.PAID, DepositStatus.PARTIALLY_PAID]:
            logger.warning(
                "Holding deposit that has not been collected",
                deposit_id=deposit.deposit_id,
                current_status=deposit.deposit_status.value,
                collected_amount=float(deposit.collected_amount)
            )

        # Calculate hold expiry date (30 days from termination)
        hold_expiry_date = termination_date + timedelta(days=30)

        # Update deposit
        deposit.deposit_status = DepositStatus.HELD
        deposit.lease_termination_date = termination_date
        deposit.hold_expiry_date = hold_expiry_date

        # Save changes
        updated_deposit = self.repo.update(deposit)
        logger.info(
            "Initiated deposit hold period",
            deposit_id=deposit.deposit_id,
            collected_amount=float(deposit.collected_amount),
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