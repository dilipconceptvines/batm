### app/interim_payments/services.py

from datetime import datetime

from sqlalchemy.orm import Session

from app.bpm.services import bpm_service
from app.interim_payments.exceptions import (
    InterimPaymentLedgerError,
    InvalidAllocationError,
)
from app.interim_payments.models import InterimPayment
from app.interim_payments.repository import InterimPaymentRepository
from app.interim_payments.schemas import InterimPaymentCreate
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

            # --- Apply Payments to Ledger ---
            allocation_dict = {alloc.reference_id: alloc.amount for alloc in payment_data.allocations}

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