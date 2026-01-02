# app/ledger/services.py

from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone, timedelta

from fastapi import Depends
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.ledger.exceptions import (
    BalanceNotFoundError,
    InvalidLedgerOperationError,
    LedgerError,
    PostingNotFoundError,
)
from app.ledger.models import (
    BalanceStatus,
    EntryType,
    LedgerBalance,
    LedgerPosting,
    PostingCategory,
    PostingStatus,
)
from app.ledger.repository import LedgerRepository
from app.ledger.schemas import LedgerBalanceResponse, LedgerPostingResponse
from app.utils.logger import get_logger

logger = get_logger(__name__)


def get_ledger_repository(db: Session = Depends(get_db)) -> LedgerRepository:
    """Dependency injector to get an instance of LedgerRepository."""
    return LedgerRepository(db)


class LedgerService:
    """
    Business Logic Layer for the Centralized Ledger.
    This service is the single entry point for all ledger operations.
    """

    def __init__(self, repo: LedgerRepository = Depends(get_ledger_repository)):
        self.repo = repo

    def create_obligation(
        self,
        category: PostingCategory,
        amount: Decimal,
        reference_id: str,
        driver_id: int,
        entry_type: EntryType = EntryType.DEBIT,
        lease_id: Optional[int] = None,
        vehicle_id: Optional[int] = None,
        medallion_id: Optional[int] = None,
    ) -> tuple[LedgerPosting, LedgerBalance]:
        """
        Creates a new financial obligation.
        This is an atomic operation that creates both a DEBIT posting and an OPEN balance.
        
        Returns:
            tuple: (LedgerPosting, LedgerBalance) - The created posting and balance objects
        """
        if amount <= 0:
            raise InvalidLedgerOperationError("Obligation amount must be positive.")

        try:
            posting = LedgerPosting(
                category=category,
                amount=amount,
                entry_type=entry_type,
                status=PostingStatus.POSTED,
                reference_id=reference_id,
                driver_id=driver_id,
                lease_id=lease_id,
                vehicle_id=vehicle_id,
                medallion_id=medallion_id,
            )
            self.repo.create_posting(posting)

            balance_ledger = self.repo.get_balance_by_reference_id(reference_id)
            
            if balance_ledger:
                amount = Decimal(str(amount))  # MUST convert before arithmetic
                balance = balance_ledger.balance  # already Decimal

                new_balance = (balance - amount) if entry_type == EntryType.CREDIT.value else (balance + amount)

                new_balance = self.repo.update_balance(
                    balance_ledger,
                    new_balance,
                    BalanceStatus.OPEN
                )
            else:
                balance = LedgerBalance(
                    category=category,
                    reference_id=reference_id,
                    original_amount=amount,
                    balance=amount,
                    status=BalanceStatus.OPEN,
                    driver_id=driver_id,
                    lease_id=lease_id,
                    vehicle_id=vehicle_id,
                    medallion_id=medallion_id,
                )
                new_balance = self.repo.create_balance(balance)

            self.repo.db.commit()
            logger.info(
                "Successfully created obligation.",
                category=category.value,
                amount=amount,
                reference_id=reference_id,
                driver_id=driver_id,
            )
            return posting, new_balance
        except SQLAlchemyError as e:
            self.repo.db.rollback()
            logger.error("Failed to create obligation.", error=str(e), exc_info=True)
            raise LedgerError(f"Failed to create obligation: {str(e)}") from e

    def create_manual_credit(
        self,
        category: PostingCategory,
        amount: Decimal,
        reference_id: str,
        driver_id: int,
        lease_id: Optional[int] = None,
        vehicle_id: Optional[int] = None,
        medallion_id: Optional[int] = None,
        description: Optional[str] = None,
        user_id: Optional[int] = None,
    ) -> LedgerPosting:
        """
        Creates a manual credit posting to reduce an existing balance.
        
        This method is used for manual adjustments, reassignments, or corrections
        where a credit needs to be applied to an existing balance. It creates a CREDIT
        posting and updates the related balance.
        
        Args:
            category: The posting category (EZPASS, REPAIR, LEASE, etc.)
            amount: Credit amount (positive value that will be applied as credit)
            reference_id: Unique reference identifier for this credit
            driver_id: Driver receiving the credit
            lease_id: Optional lease ID
            vehicle_id: Optional vehicle ID  
            medallion_id: Optional medallion ID
            description: Optional description for the credit
            user_id: Optional user ID who created the credit
            
        Returns:
            LedgerPosting: The created credit posting
            
        Raises:
            LedgerError: If credit creation fails
            InvalidLedgerOperationError: If amount is invalid
        """
        if amount <= 0:
            raise InvalidLedgerOperationError("Credit amount must be positive.")

        try:
            # Create the CREDIT posting
            credit_posting = LedgerPosting(
                category=category,
                amount=amount,
                entry_type=EntryType.CREDIT,
                status=PostingStatus.POSTED,
                reference_id=reference_id,
                driver_id=driver_id,
                lease_id=lease_id,
                vehicle_id=vehicle_id,
                medallion_id=medallion_id,
                description=description,
                created_by=user_id
            )
            self.repo.create_posting(credit_posting)

            # Update existing balance if one exists for the original reference
            # Extract original reference if this is a reassignment reversal
            original_ref = reference_id
            if reference_id.startswith("REASSIGN-REV-"):
                original_ref = reference_id.replace("REASSIGN-REV-", "")
            
            existing_balance = self.repo.get_balance_by_reference_id(original_ref)
            if existing_balance:
                # Reduce the balance by the credit amount
                new_balance_amount = Decimal(existing_balance.balance) - amount
                new_status = BalanceStatus.CLOSED if new_balance_amount <= 0 else BalanceStatus.OPEN
                
                self.repo.update_balance(existing_balance, new_balance_amount, new_status)
                
                logger.info(
                    f"Updated existing balance for reference {original_ref}",
                    original_balance=float(existing_balance.balance),
                    credit_amount=float(amount),
                    new_balance=float(new_balance_amount),
                    new_status=new_status.value
                )
                
                # Notify if balance is fully paid
                if new_balance_amount <= 0:
                    self._notify_balance_paid(original_ref, category)

            logger.info(
                "Successfully created manual credit.",
                category=category.value,
                amount=float(amount),
                reference_id=reference_id,
                driver_id=driver_id
            )
            return credit_posting
            
        except SQLAlchemyError as e:
            self.repo.db.rollback()
            logger.error("Failed to create manual credit.", error=str(e), exc_info=True)
            raise LedgerError(f"Failed to create manual credit: {str(e)}") from e

    def apply_interim_payment(
        self,
        payment_amount: Decimal,
        allocations: Dict[str, Decimal],
        driver_id: int,
        lease_id: Optional[int] = None,
        payment_method: str = "CASH",
    ) -> List[LedgerPosting]:
        """
        Applies an interim payment by creating CREDIT postings and updating balances.
        
        EXCESS HANDLING STRATEGY:
        1. Calculate excess = payment_amount - sum(allocations)
        2. If excess > 0:
           a. Query lease_schedule for upcoming/current installments
           b. Apply excess to installments in chronological order
           c. Create ledger posting for each installment reference_id
           d. Update installment status if fully paid
           e. Create balance entries if they don't exist
        3. If no lease installments available:
           - FAIL transaction with clear error message
           - Do NOT silently drop excess
        
        Args:
            payment_amount: Total payment received.
            allocations: Dict mapping reference_id to amount to allocate.
            driver_id: Driver making payment
            lease_id: Lease for the payment (Required for excess handling)
            payment_method: Payment Method (Cash, Check, ACH)

        Returns:
            List of created LedgerPosting records (including excess if any)
        """
        if payment_amount <= 0:
            raise InvalidLedgerOperationError("Payment must be positive.")
        
        total_allocated = sum(allocations.values())
        if total_allocated > payment_amount:
            raise InvalidLedgerOperationError(
                f"Total allocated amount ({total_allocated}) exceeds payment amount {payment_amount}"
            )
        
        created_postings = []

        try:
            # Step 1: Process explicit allocations
            for reference_id, allocation_amount in allocations.items():
                if allocation_amount <= 0:
                    continue

                balance = self.repo.get_balance_by_reference_id(reference_id)
                if not balance:
                    raise BalanceNotFoundError(f"Balance with reference ID {reference_id} not found.")
                
                if balance.status != BalanceStatus.OPEN:
                    raise InvalidLedgerOperationError(
                        f"Cannot apply payment to a balance with status {balance.status}."
                    )
                
                # Create CREDIT posting for this allocation
                credit_posting = LedgerPosting(
                    category=balance.category,
                    amount=allocation_amount,
                    entry_type=EntryType.CREDIT,
                    status=PostingStatus.POSTED,
                    reference_id=reference_id,  # ✅ Use original reference_id
                    driver_id=driver_id,
                    lease_id=lease_id,
                    vehicle_id=balance.vehicle_id,
                    medallion_id=balance.medallion_id,
                    description=f"Interim payment via {payment_method}",
                    payment_source="INTERIM_PAYMENT",
                    payment_method=payment_method
                )
                self.repo.create_posting(credit_posting)
                created_postings.append(credit_posting)

                # Update balance
                new_balance_amount = Decimal(balance.balance) - Decimal(allocation_amount)
                new_status = BalanceStatus.CLOSED if new_balance_amount <= 0 else BalanceStatus.OPEN
                self.repo.update_balance(balance, new_balance_amount, new_status)

                # Notify source module if balance is fully paid
                if new_balance_amount <= 0:
                    self._notify_balance_paid(balance.reference_id, balance.category)

            # Handle excess amount (auto allocate to LEASE per specification Section 3.2.3)
            excess_amount = payment_amount - total_allocated

            if excess_amount > 0:
                if not lease_id:
                    raise InvalidLedgerOperationError(
                        f"Cannot process payment with excess amount ${excess_amount} - lease_id required for excess handling"
                    )
                
                logger.info(
                    f"Applying ${excess_amount} excess to LEASE balance",
                    driver_id=driver_id,
                    lease_id=lease_id
                )
                
                # Per specification: "Excess automatically applied to Lease"
                # Find or create LEASE category balance for this lease
                from app.leases.models import Lease
                lease = self.repo.db.query(Lease).filter(Lease.id == lease_id).first()
                if not lease:
                    raise InvalidLedgerOperationError(f"Lease {lease_id} not found for excess allocation")
                
                # Get or create LEASE balance
                lease_reference_id = lease.lease_id  # Use lease.lease_id as reference
                lease_balance = self.repo.get_balance_by_reference_id(lease_reference_id)
                
                if not lease_balance:
                    # Create LEASE balance if it doesn't exist
                    lease_balance = LedgerBalance(
                        category=PostingCategory.LEASE,
                        reference_id=lease_reference_id,
                        original_amount=Decimal("0.00"),
                        balance=Decimal("0.00"),
                        status=BalanceStatus.CLOSED,
                        driver_id=driver_id,
                        lease_id=lease_id,
                        vehicle_id=lease.vehicle_id,
                        medallion_id=lease.medallion_id
                    )
                    self.repo.create_balance(lease_balance)
                    logger.info(f"Created LEASE balance for excess allocation: {lease_reference_id}")
                
                # Apply excess to LEASE balance
                excess_posting = LedgerPosting(
                    category=PostingCategory.LEASE,
                    amount=excess_amount,
                    entry_type=EntryType.CREDIT,
                    status=PostingStatus.POSTED,
                    reference_id=lease_reference_id,
                    driver_id=driver_id,
                    lease_id=lease_id,
                    vehicle_id=lease_balance.vehicle_id,
                    medallion_id=lease_balance.medallion_id,
                    description=f"Excess from interim payment auto-applied to lease via {payment_method}",
                    payment_source="INTERIM_PAYMENT_EXCESS",
                    payment_method=payment_method
                )
                self.repo.create_posting(excess_posting)
                created_postings.append(excess_posting)
                
                # Update LEASE balance if it has outstanding amount
                if lease_balance.balance > 0:
                    new_lease_balance = Decimal(lease_balance.balance) - excess_amount
                    new_status = BalanceStatus.CLOSED if new_lease_balance <= 0 else BalanceStatus.OPEN
                    self.repo.update_balance(lease_balance, new_lease_balance, new_status)
                    logger.info(f"Applied ${excess_amount} excess to LEASE balance {lease_reference_id}")
                else:
                    logger.info(f"LEASE balance {lease_reference_id} has no outstanding amount, excess posting created for future offset")

            return created_postings

        except Exception as e:
            logger.error("Failed to apply interim payment.", error=str(e), exc_info=True)
            raise LedgerError(f"Failed to apply interim payment: {str(e)}") from e

    def _notify_balance_paid(self, reference_id: str, category: PostingCategory):
        """
        Notify source modules when a balance is fully paid.
        This enables status synchronization.
        """
        try:
            if category == PostingCategory.REPAIR:
                from app.repairs.services import RepairService
                repair_service = RepairService(self.repo.db)
                repair_service.mark_installment_paid(reference_id)
                
            elif category == PostingCategory.LOAN:
                from app.loans.services import LoanService
                loan_service = LoanService(self.repo.db)
                loan_service.mark_installment_paid(reference_id)
                
            # Add other categories as needed (EZPASS, PVB, TLC, etc.)
            
        except Exception as e:
            # Don't fail the payment if notification fails
            logger.error(
                f"Failed to notify source module about paid balance",
                reference_id=reference_id,
                category=category.value,
                error=str(e),
                exc_info=True
            )

    def _apply_excess_to_lease_schedule(
        self,
        lease_id: int,
        excess_amount: Decimal,
        driver_id: int,
        payment_method: str
    ) -> List[LedgerPosting]:
        """
        Apply excess payment to lease schedule installments in chronological order.
        
        ALGORITHM:
        1. Query lease_schedule for installments with status 'Scheduled' or 'Posted'
        2. Order by installment_due_date ASC (earliest first)
        3. For each installment:
           a. Check if there's an existing ledger_balance for this installment
           b. If not, create one (happens if installment not yet posted to ledger)
           c. Apply min(excess_remaining, installment_balance) to this installment
           d. Create CREDIT posting with reference_id = installment.installment_id
           e. Update ledger balance
           f. If balance reaches 0, mark installment as PAID
           g. Continue to next installment if excess remains
        4. If excess STILL remains after all installments: FAIL with error
        
        RETURNS: List of created LedgerPosting objects
        RAISES: InvalidLedgerOperationError if excess cannot be fully allocated
        """
        from app.leases.models import LeaseSchedule
        
        created_postings = []
        remaining_excess = excess_amount
        
        # Step 1: Get upcoming/current lease installments (not yet fully paid)
        upcoming_installments = (
            self.repo.db.query(LeaseSchedule)
            .filter(
                LeaseSchedule.lease_id == lease_id,
                LeaseSchedule.installment_status.in_(['Scheduled', 'Posted']),
                LeaseSchedule.installment_amount > 0
            )
            .order_by(LeaseSchedule.installment_due_date.asc())
            .limit(10)  # Reasonable limit - excess unlikely to cover 10+ weeks
            .all()
        )
        
        if not upcoming_installments:
            raise InvalidLedgerOperationError(
                f"Cannot apply excess ${excess_amount} - no scheduled lease installments found for lease {lease_id}. "
                f"This may indicate the lease has ended or all installments are already paid."
            )
        
        logger.info(
            f"Applying ${excess_amount} excess to {len(upcoming_installments)} lease installments",
            lease_id=lease_id
        )
        
        # Step 2: Apply excess to installments chronologically
        for installment in upcoming_installments:
            if remaining_excess <= 0:
                break
            
            # Step 3: Get or create ledger balance for this installment
            balance = self.repo.get_balance_by_reference_id(str(installment.id))
            
            if not balance:
                # Installment not yet posted to ledger - create balance entry
                balance = self._create_lease_installment_balance(
                    installment=installment,
                    driver_id=driver_id,
                    lease_id=lease_id
                )
            
            # Skip if already fully paid
            if balance.status == BalanceStatus.CLOSED:
                continue
            
            # Step 4: Calculate payment for this installment
            installment_outstanding = Decimal(str(balance.balance))
            payment_for_installment = min(remaining_excess, installment_outstanding)
            
            # Step 5: Create CREDIT posting
            posting = LedgerPosting(
                category=PostingCategory.LEASE,
                amount=payment_for_installment,
                entry_type=EntryType.CREDIT,
                status=PostingStatus.POSTED,
                reference_id=str(installment.id),  # ✅ CRITICAL: Use actual installment ID
                driver_id=driver_id,
                lease_id=lease_id,
                vehicle_id=balance.vehicle_id,
                medallion_id=balance.medallion_id,
                description=f"Excess interim payment via {payment_method} - Installment #{installment.installment_number}",
                payment_source="INTERIM_PAYMENT",
                payment_method=payment_method
            )
            self.repo.create_posting(posting)
            created_postings.append(posting)
            
            # Step 6: Update balance
            new_balance = installment_outstanding - payment_for_installment
            new_status = BalanceStatus.CLOSED if new_balance <= 0 else BalanceStatus.OPEN
            self.repo.update_balance(balance, new_balance, new_status)
            
            # Step 7: Update installment status if fully paid
            if new_balance <= 0:
                installment.installment_status = 'Paid'
                self.repo.db.add(installment)
            
            remaining_excess -= payment_for_installment
            
            logger.info(
                f"Applied ${payment_for_installment} excess to lease installment {installment.id}",
                installment_number=installment.installment_number,
                remaining_balance=float(new_balance),
                remaining_excess=float(remaining_excess)
            )
        
        # Step 8: Final validation - all excess must be allocated
        if remaining_excess > 0.01:  # Allow for small floating point differences
            raise InvalidLedgerOperationError(
                f"Unable to fully allocate excess payment. ${remaining_excess} remains after processing all available lease installments. "
                f"This should not happen - contact system administrator."
            )
        
        return created_postings


    def _create_lease_installment_balance(
        self,
        installment: "LeaseSchedule",
        driver_id: int,
        lease_id: int
    ) -> "LedgerBalance":
        """
        Create a ledger balance entry for a lease installment that hasn't been posted yet.
        This allows interim payments to prepay future installments.
        """
        balance = LedgerBalance(
            category=PostingCategory.LEASE,
            reference_id=str(installment.id),
            driver_id=driver_id,
            lease_id=lease_id,
            vehicle_id=installment.lease.vehicle_id if installment.lease else None,
            medallion_id=installment.lease.medallion_id if installment.lease else None,
            original_amount=Decimal(str(installment.installment_amount)),
            balance=Decimal(str(installment.installment_amount)),
            status=BalanceStatus.OPEN,
            posted_on=datetime.now(timezone.utc)
        )
        
        self.repo.db.add(balance)
        self.repo.db.flush()
        
        logger.info(
            f"Created ledger balance for lease installment {installment.id}",
            amount=float(installment.installment_amount)
        )
        
        return balance

    def apply_weekly_earnings(
        self, driver_id: int, earnings_amount: Decimal, lease_id: Optional[int] = None
    ) -> Dict[str, Decimal]:
        """
        Applies weekly earnings to open balances according to payment hierarchy.
        Returns a dictionary of reference_id: amount_applied.
        """
        if earnings_amount <= 0:
            return []

        remaining_earnings = earnings_amount
        created_postings = []
        try:
            earnings_posting = LedgerPosting(
                category=PostingCategory.EARNINGS,
                amount=-earnings_amount,
                entry_type=EntryType.CREDIT,
                reference_id=f"EARNINGS-{datetime.now(timezone.utc).strftime('%Y%m%d')}",
                driver_id=driver_id,
                lease_id=lease_id,
            )
            self.repo.create_posting(earnings_posting)
            created_postings.append(earnings_posting)

            open_balances = self.repo.get_open_balances_for_driver(driver_id)

            for balance in open_balances:
                if remaining_earnings <= 0:
                    break

                payment_amount = min(remaining_earnings, balance.balance)
                new_balance_amount = balance.balance - payment_amount
                new_status = BalanceStatus.CLOSED if new_balance_amount <= 0 else BalanceStatus.OPEN
                self.repo.update_balance(
                    balance=balance,
                    new_balance=new_balance_amount,
                    status=new_status,
                )
                remaining_earnings -= payment_amount

            self.repo.db.commit()
            logger.info("Successfully applied weekly earnings.", driver_id=driver_id, total_earnings=earnings_amount)
            return created_postings
        except (SQLAlchemyError, LedgerError) as e:
            self.repo.db.rollback()
            logger.error("Failed to apply weekly earnings.", driver_id=driver_id, error=str(e), exc_info=True)
            raise

    def void_posting(
        self,
        posting_id: str,
        reason: str,
        user_id: int
    ) -> Tuple[LedgerPosting, LedgerPosting]:
        """
        Voids a posting by creating a reversal and notifying source modules.
        
        NEW: Notifies source modules when payments are reversed so they can
        update installment status back to POSTED.
        """
        try:
            # Get original posting
            original = self.repo.get_posting_by_posting_id(posting_id)
            
            if not original:
                raise PostingNotFoundError(f"Posting {posting_id} not found")
            
            if original.status == PostingStatus.VOIDED:
                raise InvalidLedgerOperationError(f"Posting {posting_id} is already voided")
            
            # Mark original as voided
            original.status = PostingStatus.VOIDED
            original.voided_at = datetime.now(timezone.utc)
            original.voided_by = user_id
            original.void_reason = reason
            
            # Create reversal posting (opposite type)
            reversal_type = EntryType.DEBIT if original.entry_type == EntryType.CREDIT else EntryType.CREDIT
            reversal_amount = -original.amount if original.entry_type == EntryType.CREDIT else original.amount
            
            reversal = LedgerPosting(
                category=original.category,
                amount=reversal_amount,
                entry_type=reversal_type,
                status=PostingStatus.POSTED,
                reference_id=f"VOID-{original.posting_id}",
                driver_id=original.driver_id,
                lease_id=original.lease_id,
                vehicle_id=original.vehicle_id,
                medallion_id=original.medallion_id,
                description=f"Reversal of {original.posting_id}: {reason}"
            )
            
            self.repo.create_posting(reversal)
            
            # Link them
            original.voided_by_posting_id = reversal.posting_id
            
            # Update the related balance
            balance = self.repo.get_balance_by_reference_id(original.reference_id)
            if balance:
                # Reverse the effect of the original posting
                if original.entry_type == EntryType.CREDIT:
                    # Original was a payment (reduced balance), so add it back
                    new_balance = balance.balance + abs(original.amount)
                else:
                    # Original was an obligation (increased balance), so subtract it
                    new_balance = balance.balance - abs(original.amount)
                
                # Reopen if necessary
                new_status = BalanceStatus.OPEN if new_balance > 0 else BalanceStatus.CLOSED
                
                self.repo.update_balance(balance, new_balance, new_status)
                
                # NEW: Notify source module if payment was voided
                if original.entry_type == EntryType.CREDIT and new_balance > 0:
                    self._notify_balance_reopened(original.reference_id, original.category)
            
            self.repo.db.commit()
            
            logger.info(
                f"Successfully voided posting {posting_id}",
                reversal_posting_id=reversal.posting_id,
                user_id=user_id
            )
            
            return original, reversal
            
        except Exception as e:
            self.repo.db.rollback()
            logger.error(f"Failed to void posting {posting_id}", error=str(e), exc_info=True)
            raise

    def _notify_balance_reopened(self, reference_id: str, category: PostingCategory):
        """
        Notify source modules when a payment is voided and balance is reopened.
        """
        try:
            if category == PostingCategory.REPAIR:
                from app.repairs.services import RepairService
                repair_service = RepairService(self.repo.db)
                repair_service.mark_installment_reopened(reference_id)
                
            elif category == PostingCategory.LOAN:
                from app.loans.services import LoanService
                loan_service = LoanService(self.repo.db)
                loan_service.mark_installment_reopened(reference_id)
                
            # Add other categories as needed
            
        except Exception as e:
            # Don't fail the void if notification fails
            logger.error(
                f"Failed to notify source module about reopened balance",
                reference_id=reference_id,
                category=category.value,
                error=str(e),
                exc_info=True
            )

    def list_postings(
        self, **kwargs
    ) -> Tuple[List[LedgerPostingResponse], int]:
        """
        Fetches and formats a list of ledger postings.
        """
        postings, total_items = self.repo.list_postings(**kwargs)

        # Map SQLAlchemy models to Pydantic response models
        response_items = [
            LedgerPostingResponse(
                posting_id=p.id,
                status=p.status,
                date=p.created_on,
                category=p.category,
                type=p.entry_type,
                amount=p.amount,
                driver_name=p.driver.full_name if p.driver else None,
                lease_id=p.lease_id,
                vehicle_vin=p.vin,
                medallion_no=p.medallion.medallion_number if p.medallion else None,
                reference_id=p.reference_id,
            )
            for p in postings
        ]

        return response_items, total_items

    def list_balances(
        self, **kwargs
    ) -> Tuple[List[LedgerBalanceResponse], int]:
        """
        Fetches and formats a list of ledger balances.
        """
        balances, total_items = self.repo.list_balances(**kwargs)

        # Map SQLAlchemy models to Pydantic response models
        response_items = [
            LedgerBalanceResponse(
                balance_id=b.id,
                category=b.category,
                status=b.status,
                reference_id=b.reference_id,
                driver_name=b.driver.full_name if b.driver else None,
                lease_id=b.lease_id,
                vehicle_vin=b.vin,
                original_amount=b.original_amount,
                prior_balance=b.prior_balance,
                balance=b.balance,
            )
            for b in balances
        ]

        return response_items, total_items