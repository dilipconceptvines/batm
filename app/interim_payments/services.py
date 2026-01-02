### app/interim_payments/services.py

from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import List, Tuple

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
from app.interim_payments.models import InterimPayment, PaymentStatus
from app.interim_payments.repository import InterimPaymentRepository
from app.interim_payments.schemas import InterimPaymentCreate
from app.ledger.models import PostingCategory, LedgerPosting, EntryType, PostingStatus
from app.ledger.services import LedgerService
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
        self.ledger_service = LedgerService(db)
        self.deposit_service = DepositService(db)

    def _generate_next_payment_id(self) -> str:
        """Generates a new, unique Interim Payment ID in the format INTPAY-YYYY-#####."""
        current_year = datetime.utcnow().year
        last_id_record = self.repo.get_last_payment_id_for_year(current_year)

        sequence = 1
        if last_id_record:
            # last_id_record is a tuple, access the string with [0]
            last_sequence_str = last_id_record[0].split('-')[-1]
            sequence = int(last_sequence_str) + 1

        return f"INTPAY-{current_year}-{str(sequence).zfill(5)}"

    async def create_interim_payment(self, case_no: str, payment_data: InterimPaymentCreate, user_id: int) -> InterimPayment:
        """
        Creates a new Interim Payment from the BPM workflow.
        This operation validates the payment, creates the master record,
        and posts the allocations to the Centralized Ledger.
        """
        try:
            # --- Validation ---
            total_allocated = sum(alloc.amount for alloc in payment_data.allocations)
            if total_allocated > payment_data.total_amount:
                raise InvalidAllocationError("Total allocated amount cannot exceed the total payment amount.")
            
            # Validate deposit allocations
            for alloc in payment_data.allocations:
                if alloc.category.upper() == "DEPOSIT":
                    try:
                        deposit_id = alloc.reference_id
                        deposit = self.deposit_service.repo.get_by_deposit_id(deposit_id)
                        
                        if not deposit:
                            raise InvalidAllocationError(f"Deposit with ID {deposit_id} not found.")
                        
                        if deposit.lease_id != payment_data.lease_id:
                            raise InvalidAllocationError(f"Deposit {deposit_id} belongs to lease {deposit.lease_id}, not lease {payment_data.lease_id}.")
                        
                        outstanding_amount = deposit.required_amount - deposit.collected_amount
                        if alloc.amount > outstanding_amount:
                            raise InvalidAllocationError(f"Allocation amount ${alloc.amount} exceeds outstanding deposit amount ${outstanding_amount}.")
                            
                    except InvalidAllocationError:
                        raise
                    except Exception as e:
                        raise InvalidAllocationError(f"Error validating deposit allocation: {str(e)}")

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
                allocations=[alloc.model_dump() for alloc in payment_data.allocations],
                created_by=user_id,
            )
            created_payment = self.repo.create_payment(new_payment)

            # --- Process Deposit Allocations ---
            deposit_allocations = [alloc for alloc in payment_data.allocations if alloc.category.upper() == "DEPOSIT"]
            non_deposit_allocations = [alloc for alloc in payment_data.allocations if alloc.category.upper() != "DEPOSIT"]
            
            # Process deposit allocations
            for alloc in deposit_allocations:
                try:
                    # Extract deposit_id from reference_id
                    deposit_id = alloc.reference_id
                    
                    # Update deposit collection
                    deposit = self.deposit_service.update_deposit_collection(
                        db=self.db,
                        deposit_id=deposit_id,
                        additional_amount=alloc.amount,
                        collection_method=payment_data.payment_method,
                        notes=f"Interim payment {payment_id}"
                    )
                    
                    # Create ledger posting for deposit
                    await self.ledger_service.create_manual_credit(
                        category=PostingCategory.DEPOSIT,
                        amount=alloc.amount,
                        reference_id=deposit_id,
                        driver_id=payment_data.driver_id,
                        lease_id=payment_data.lease_id,
                        description=f"Deposit payment via interim payment {payment_id}"
                    )
                    
                    logger.info(f"Applied ${alloc.amount} to deposit {deposit_id} via interim payment {payment_id}")
                    
                except Exception as e:
                    logger.error(f"Failed to process deposit allocation for deposit {alloc.reference_id}: {e}")
                    raise InterimPaymentLedgerError(payment_id, f"Deposit allocation failed: {str(e)}") from e

            # --- Apply Non-Deposit Payments to Ledger ---
            allocation_dict = {alloc.reference_id: alloc.amount for alloc in non_deposit_allocations}

            # The ledger service handles the creation of credit postings and balance updates
            await self.ledger_service.apply_interim_payment(
                payment_amount=payment_data.total_amount,
                allocations=allocation_dict,
                driver_id=payment_data.driver_id,
                lease_id=payment_data.lease_id,
                payment_method=payment_data.payment_method.value,
            )

            # --- Link to BPM Case ---
            bpm_service.create_case_entity(
                self.db, case_no, "interim_payment", "id", str(created_payment.id)
            )
            
            self.db.commit()
            logger.info(f"Successfully created Interim Payment {payment_id} and applied to ledger.")
            return created_payment

        except InvalidAllocationError as e:
            self.db.rollback()
            logger.warning(f"Invalid allocation for interim payment: {e}")
            raise
        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to create interim payment: {e}", exc_info=True)
            raise InterimPaymentLedgerError(payment_id if 'payment_id' in locals() else 'N/A', str(e)) from e

    def void_interim_payment(
        self,
        payment_id: str,
        reason: str,
        user_id: int
    ) -> InterimPayment:
        """
        Void an entire interim payment by reversing all associated ledger postings.

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
            # Step 1: Get payment record
            payment = self.repo.get_payment_by_payment_id(payment_id)

            if not payment:
                raise InterimPaymentNotFoundError(payment_id)

            # Step 2: Validate status
            if payment.status == PaymentStatus.VOIDED:
                raise InvalidOperationError(
                    f"Payment {payment_id} is already voided on {payment.voided_at.isoformat()}"
                )

            # Validate reason
            if not reason or len(reason.strip()) < 10:
                raise InvalidOperationError("Void reason must be at least 10 characters")

            logger.info(
                f"Voiding interim payment {payment_id}",
                reason=reason,
                user_id=user_id,
                total_amount=float(payment.total_amount),
                allocations_count=len(payment.allocations) if payment.allocations else 0
            )

            # Step 3: Get all postings for this payment
            # Find postings by reference_ids from allocations AND payment date
            reference_ids = [alloc['reference_id'] for alloc in (payment.allocations or [])]

            # Query ledger postings created around payment date
            # (to avoid voiding postings from other payments with same reference_id)
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
                    f"Payment may have already been reversed or postings were created outside expected time window."
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
                    raise InvalidOperationError(f"Failed to void posting {posting.id}: {str(e)}") from e

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

            # Step 6: Generate voided receipt (optional - implement if needed)
            # self._generate_voided_receipt(payment)

            return payment

        except (InterimPaymentNotFoundError, InvalidOperationError):
            # Re-raise known errors
            raise
        except Exception as e:
            self.db.rollback()
            logger.error(
                f"Failed to void interim payment {payment_id}: {e}",
                exc_info=True
            )
            raise InterimPaymentError(f"Failed to void payment: {str(e)}") from e