### app/misc_expenses/services.py

from datetime import datetime
from decimal import Decimal

from sqlalchemy.orm import Session

from app.bpm.services import bpm_service
from app.interim_payments.exceptions import InvalidAllocationError
from app.misc_expenses.exceptions import (
    MiscellaneousExpenseLedgerError,
    MiscellaneousExpenseValidationError,
)
from app.misc_expenses.models import MiscellaneousExpense, MiscellaneousExpenseStatus
from app.misc_expenses.repository import MiscellaneousExpenseRepository
from app.misc_expenses.schemas import MiscellaneousExpenseCreate
from app.ledger.models import PostingCategory, EntryType
from app.ledger.services import LedgerService
from app.ledger.repository import LedgerRepository
from app.utils.logger import get_logger

logger = get_logger(__name__)


class MiscellaneousExpenseService:
    """
    Service layer for managing Miscellaneous Expenses, including creation
    and immediate integration with the Centralized Ledger.
    """

    def __init__(self, db: Session):
        self.db = db
        self.repo = MiscellaneousExpenseRepository(db)
        # The ledger service is designed to be used asynchronously
        self.ledger_repo = LedgerRepository(db)
        self.ledger_service = LedgerService(self.ledger_repo)

    def _generate_next_expense_id(self) -> str:
        """Generates a unique Miscellaneous Expense ID in the format MISC-YYYY-#####."""
        current_year = datetime.utcnow().year
        last_id_record = self.repo.get_last_expense_id_for_year(current_year)

        sequence = 1
        if last_id_record:
            # last_id_record is a tuple, access the string with [0]
            last_sequence_str = last_id_record[0].split('-')[-1]
            sequence = int(last_sequence_str) + 1
            
        return f"MISC-{current_year}-{str(sequence).zfill(5)}"

    def create_misc_expense(self, case_no: str, expense_data: MiscellaneousExpenseCreate, user_id: int) -> MiscellaneousExpense:
        """
        Create miscellaneous payment (expense or credit)
        
        - EXPENSE: Posts DEBIT to ledger (charge to driver)
        - CREDIT: Posts CREDIT to ledger (payment to driver)
        """
        try:
            # --- Validation ---
            if expense_data.amount <= 0:
                raise MiscellaneousExpenseValidationError("Expense amount must be greater than zero.")
            
            # Additional validation can be added here (e.g., check if driver has an active lease)

            # --- Create Master Expense Record ---
            expense_id = self._generate_next_expense_id()

            # Determine ledger entry type based on payment type
            if expense_data.payment_type == "EXPENSE":
                # Expense = DEBIT (charge to driver, increases what they owe)
                entry_type = EntryType.DEBIT
                posting_category = PostingCategory.MISCELLANEOUS_EXPENSE
                logger.info(f"Creating EXPENSE (DEBIT) - {expense_id}")
            else: # PaymentType.CREDIT
                # Credit = CREDIT (payment to driver, increases what we owe them)
                entry_type = EntryType.CREDIT
                posting_category = PostingCategory.MISCELLANEOUS_CREDIT
                logger.info(f"Creating CREDIT (CREDIT) - {expense_id}")

            # Post to ledger
            reference_id = f"MISC-PAY-{expense_id}"

            try:
                posting, balance = self.ledger_service.create_obligation(
                    category=posting_category,
                    amount=expense_data.amount,
                    entry_type=entry_type,
                    reference_id=reference_id,
                    driver_id=expense_data.driver_id,
                    lease_id=expense_data.lease_id,
                    vehicle_id=expense_data.vehicle_id,
                    medallion_id=expense_data.medallion_id,
                )
                
                ledger_posting_id = posting.id
                logger.info(
                    f"Ledger posting created for {expense_data.payment_type.value}",
                    payment_id=expense_id,
                    posting_id=ledger_posting_id,
                    entry_type=entry_type.value
                )
                
            except Exception as ledger_error:
                logger.error(
                    "Ledger posting failed for miscellaneous payment",
                    payment_id=expense_id,
                    error=str(ledger_error)
                )
                raise MiscellaneousExpenseLedgerError(
                    expense_id=expense_id,reason=f"Failed to post to ledger: {str(ledger_error)}"
                ) from ledger_error

            new_expense = MiscellaneousExpense(
                expense_id=expense_id,
                payment_type=expense_data.payment_type,
                driver_id=expense_data.driver_id,
                lease_id=expense_data.lease_id,
                vehicle_id=expense_data.vehicle_id,
                medallion_id=expense_data.medallion_id,
                expense_date=expense_data.expense_date,
                category=expense_data.category,
                reference_number=expense_data.reference_number,
                amount=expense_data.amount,
                notes=expense_data.notes,
                created_by=user_id,
                ledger_posting_ref=str(ledger_posting_id),
                status=MiscellaneousExpenseStatus.OPEN,
            )
            
            self.db.add(new_expense)
            self.db.commit()
            self.db.refresh(new_expense)

            logger.info(
                f"Miscellaneous {expense_data.payment_type.value} created successfully",
                payment_id=new_expense.expense_id,
                amount=float(new_expense.amount),
                category=new_expense.category
            )

            return new_expense

        except (MiscellaneousExpenseValidationError, InvalidAllocationError) as e:
            self.db.rollback()
            logger.warning(f"Validation error for miscellaneous expense: {e}")
            raise
        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to create miscellaneous expense: {e}", exc_info=True)
            raise MiscellaneousExpenseLedgerError(expense_id if 'expense_id' in locals() else 'N/A', str(e)) from e