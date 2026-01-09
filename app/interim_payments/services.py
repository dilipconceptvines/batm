### app/interim_payments/services.py

from datetime import datetime, timezone, timedelta

from sqlalchemy.orm import Session

from app.bpm.services import bpm_service
from app.deposits.services import DepositService
from app.interim_payments.exceptions import (
    InterimPaymentLedgerError,
    InvalidAllocationError,
    InterimPaymentNotFoundError,
    InvalidOperationError,
    InterimPaymentError
)
from app.interim_payments.validators import InterimPaymentValidator
from app.interim_payments.models import (
    InterimPayment, PaymentStatus, InterimPaymentAllocation
)
from app.interim_payments.repository import InterimPaymentRepository
from app.interim_payments.schemas import InterimPaymentCreate
from app.ledger.models import (
    PostingCategory, LedgerPosting, EntryType, PostingStatus,
    LedgerBalance
)
from app.ledger.services import LedgerService
from app.ledger.repository import LedgerRepository
from app.utils.logger import get_logger

logger = get_logger(__name__)


class InterimPaymentService:
    """
    Service layer for managing the lifecycle of Interim Payments, including
    creation via BPM, validation, and integration with the Centralized Ledger.
    """

    def __init__(self, db: Session):
        self.db = db
        self.repo = InterimPaymentRepository(db)
        # Use an async session for the ledger service as required by its repository
        self.ledger_repository = LedgerRepository(db)
        self.ledger_service = LedgerService(self.ledger_repository)
        self.deposit_service = DepositService(db)
        self.validator = InterimPaymentValidator(db)

    def _generate_next_payment_id(self) -> str:
        """Generates a new, unique Interim Payment ID in the format INTPAY-YYYY-#####."""
        current_year = datetime.now(timezone.utc).year
        last_id_record = self.repo.get_last_payment_id_for_year(current_year)

        sequence = 1
        if last_id_record:
            # last_id_record is a tuple, access the string with [0]
            last_sequence_str = last_id_record[0].split('-')[-1]
            sequence = int(last_sequence_str) + 1

        return f"INTPAY-{current_year}-{str(sequence).zfill(5)}"

    async def create_interim_payment(
        self, 
        case_no: str, 
        payment_data: InterimPaymentCreate, 
        user_id: int
    ) -> InterimPayment:
        """
        Creates a new Interim Payment from the BPM workflow.
        This operation validates the payment, creates the master record,
        and posts the allocations to the Centralized Ledger.
        
        FIXED: Uses structured validation and allocation storage
        """
        try:
            # --- Comprehensive Validation ---
            self.validator.validate_payment_creation(
                driver_id=payment_data.driver_id,
                lease_id=payment_data.lease_id,
                total_amount=payment_data.total_amount,
                payment_method=payment_data.payment_method.value,
                allocations=[alloc.model_dump() for alloc in payment_data.allocations]
            )
            
            # Validate deposit allocations separately
            for alloc in payment_data.allocations:
                if alloc.category.upper() == "DEPOSIT":
                    self._validate_deposit_allocation(alloc, payment_data.lease_id)

            # --- Create Master Interim Payment Record ---
            payment_id = self._generate_next_payment_id()
            new_payment = InterimPayment(
                payment_id=payment_id,
                case_no=case_no,
                driver_id=payment_data.driver_id,
                lease_id=payment_data.lease_id,
                payment_date=payment_data.payment_date,
                total_amount=payment_data.total_amount,
                payment_method=payment_data.payment_method,
                notes=payment_data.notes,
                allocations=[alloc.model_dump() for alloc in payment_data.allocations],  # Keep for backward compatibility
                status=PaymentStatus.ACTIVE,  # FIXED: Set initial status
                created_by=user_id,
            )
            created_payment = self.repo.create_payment(new_payment)
            self.db.flush()

            # --- Process Deposit Allocations ---
            deposit_allocations = [
                alloc for alloc in payment_data.allocations 
                if alloc.category.upper() == "DEPOSIT"
            ]
            non_deposit_allocations = [
                alloc for alloc in payment_data.allocations 
                if alloc.category.upper() != "DEPOSIT"
            ]
            
            for alloc in deposit_allocations:
                try:
                    deposit_id = alloc.reference_id
                    
                    deposit = self.deposit_service.update_deposit_collection(
                        db=self.db,
                        deposit_id=deposit_id,
                        additional_amount=alloc.amount,
                        collection_method=payment_data.payment_method,
                        notes=f"Interim payment {payment_id}"
                    )
                    
                    await self.ledger_service.create_manual_credit(
                        category=PostingCategory.DEPOSIT,
                        amount=alloc.amount,
                        reference_id=deposit_id,
                        driver_id=payment_data.driver_id,
                        lease_id=payment_data.lease_id,
                        description=f"Deposit payment via interim payment {payment_id}"
                    )
                    
                    logger.info(
                        f"Applied ${alloc.amount} to deposit {deposit_id} "
                        f"via interim payment {payment_id}"
                    )
                    
                except Exception as e:
                    logger.error(
                        f"Failed to process deposit allocation for deposit {alloc.reference_id}: {e}"
                    )
                    raise InterimPaymentLedgerError(
                        payment_id, 
                        f"Deposit allocation failed: {str(e)}"
                    ) from e

            # --- Apply Non-Deposit Payments to Ledger ---
            # FIXED: Create structured allocation records
            allocation_dict = {
                alloc.reference_id: alloc.amount 
                for alloc in non_deposit_allocations
            }

            created_postings = await self.ledger_service.apply_interim_payment(
                payment_amount=payment_data.total_amount,
                allocations=allocation_dict,
                driver_id=payment_data.driver_id,
                lease_id=payment_data.lease_id,
                payment_method=payment_data.payment_method.value,
            )
            
            # --- Create Structured Allocation Records (NEW) ---
            for alloc in payment_data.allocations:
                # Get the ledger balance
                balance = self.db.query(LedgerBalance).filter(
                    LedgerBalance.reference_id == alloc.reference_id,
                    LedgerBalance.driver_id == payment_data.driver_id,
                    LedgerBalance.lease_id == payment_data.lease_id
                ).first()
                
                if balance:
                    # Create allocation record with before/after snapshots
                    balance_before = balance.balance + alloc.amount  # Before payment
                    balance_after = balance.balance  # After payment
                    
                    allocation_record = InterimPaymentAllocation(
                        interim_payment_id=created_payment.id,
                        ledger_balance_id=str(balance.id),
                        category=alloc.category,
                        reference_id=alloc.reference_id,
                        allocated_amount=alloc.amount,
                        balance_before=balance_before,
                        balance_after=balance_after,
                        created_by=user_id
                    )
                    self.db.add(allocation_record)

            # --- Link to BPM Case ---
            bpm_service.create_case_entity(
                self.db, case_no, "interim_payment", "id", str(created_payment.id)
            )
            
            self.db.commit()
            logger.info(
                f"Successfully created Interim Payment {payment_id} and applied to ledger.",
                total_amount=float(payment_data.total_amount),
                allocations_count=len(payment_data.allocations)
            )

            return created_payment

        except (InvalidAllocationError, InterimPaymentLedgerError):
            self.db.rollback()
            raise
        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to create interim payment: {e}", exc_info=True)
            raise InterimPaymentError(f"Failed to create interim payment: {str(e)}") from e
        
    def _validate_deposit_allocation(
        self, 
        alloc, 
        lease_id: int
    ) -> None:
        """Validate deposit-specific allocation rules"""
        try:
            deposit_id = alloc.reference_id
            deposit = self.deposit_service.repo.get_by_deposit_id(deposit_id)
            
            if not deposit:
                raise InvalidAllocationError(
                    f"Deposit with ID {deposit_id} not found."
                )
            
            if deposit.lease_id != lease_id:
                raise InvalidAllocationError(
                    f"Deposit {deposit_id} belongs to lease {deposit.lease_id}, "
                    f"not lease {lease_id}."
                )
            
            outstanding_amount = deposit.required_amount - deposit.collected_amount
            if alloc.amount > outstanding_amount:
                raise InvalidAllocationError(
                    f"Allocation amount ${alloc.amount} exceeds "
                    f"outstanding deposit amount ${outstanding_amount}."
                )
                
        except InvalidAllocationError:
            raise
        except Exception as e:
            raise InvalidAllocationError(
                f"Error validating deposit allocation: {str(e)}"
            ) from e

    async def void_interim_payment(
        self, 
        payment_id: str, 
        reason: str, 
        user_id: int
    ) -> InterimPayment:
        """
        Voids an interim payment and reverses all ledger postings.
        
        FIXED: Now properly handles status field and validation
        
        ALGORITHM:
        1. Retrieve interim payment record
        2. Validate payment is ACTIVE (not already voided)
        3. Find all ledger postings for this payment's allocations
        4. For each posting:
           a. Call ledger_service.void_posting() to create reversal
           b. Reopen affected ledger balances
           c. Update source module statuses (repairs, loans)
        5. Mark interim_payment as VOIDED
        6. Generate voided receipt (optional)

        ATOMIC: All postings reversed or none (transaction rollback on error)

        Args:
            payment_id: Unique payment identifier (e.g., "INTPAY-2025-00123")
            reason: Reason for voiding (minimum 10 characters)
            user_id: ID of user performing the void

        Returns:
            Updated InterimPayment with status=VOIDED

        Raises:
            InterimPaymentNotFoundError: Payment not found
            InvalidOperationError: Payment already voided
            InterimPaymentError: Failed to void payment
        """
        try:
            # Validate void operation
            self.validator.validate_void_operation(payment_id, reason)
            
            # Step 1: Get payment record
            payment = self.repo.get_payment_by_payment_id(payment_id)

            if not payment:
                raise InterimPaymentNotFoundError(payment_id)

            logger.info(
                f"Voiding interim payment {payment_id}",
                reason=reason,
                user_id=user_id,
                total_amount=float(payment.total_amount),
                allocations_count=len(payment.allocations) if payment.allocations else 0
            )

            # Step 3: Get all postings for this payment
            reference_ids = [
                alloc['reference_id'] 
                for alloc in (payment.allocations or [])
            ]

            # Query ledger postings created around payment date
            time_window_start = payment.payment_date - timedelta(hours=12)
            time_window_end = payment.payment_date + timedelta(hours=12)

            postings_to_void = (
                self.db.query(LedgerPosting)
                .filter(
                    LedgerPosting.reference_id.in_(reference_ids),
                    LedgerPosting.driver_id == payment.driver_id,
                    LedgerPosting.entry_type == EntryType.CREDIT,
                    LedgerPosting.status == PostingStatus.POSTED,
                    LedgerPosting.created_on >= time_window_start,
                    LedgerPosting.created_on <= time_window_end
                )
                .all()
            )

            if not postings_to_void:
                raise InvalidOperationError(
                    f"No ledger postings found for payment {payment_id}. "
                    f"Payment may have already been reversed or postings were "
                    f"created outside expected time window."
                )

            logger.info(
                f"Found {len(postings_to_void)} ledger postings to void",
                posting_ids=[p.id for p in postings_to_void]
            )

            # Step 4: Void each posting
            voided_postings = []
            for posting in postings_to_void:
                try:
                    original, reversal = self.ledger_service.void_posting(
                        posting_id=str(posting.id),
                        reason=f"Reversal of interim payment {payment_id}: {reason}",
                        user_id=user_id
                    )
                    voided_postings.append((original, reversal))

                    logger.info(
                        f"Voided posting {posting.id}",
                        category=posting.category.value,
                        amount=float(posting.amount),
                        reference_id=posting.reference_id
                    )
                except Exception as e:
                    logger.error(f"Failed to void posting {posting.id}: {e}")
                    raise InvalidOperationError(
                        f"Failed to void posting {posting.id}: {str(e)}"
                    ) from e

            # Step 5: Mark payment as VOIDED
            payment.status = PaymentStatus.VOIDED
            payment.voided_at = datetime.now(timezone.utc)
            payment.voided_by = user_id
            payment.void_reason = reason

            self.db.commit()

            logger.info(
                f"Successfully voided interim payment {payment_id}",
                postings_voided=len(voided_postings),
                voided_by_user_id=user_id,
                total_amount_reversed=float(payment.total_amount)
            )

            return payment

        except (InterimPaymentNotFoundError, InvalidOperationError):
            self.db.rollback()
            raise
        except Exception as e:
            self.db.rollback()
            logger.error(
                f"Failed to void interim payment {payment_id}: {e}",
                exc_info=True
            )
            raise InterimPaymentError(f"Failed to void payment: {str(e)}") from e