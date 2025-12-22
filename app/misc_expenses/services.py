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
from app.misc_expenses.models import MiscellaneousExpense
from app.misc_expenses.repository import MiscellaneousExpenseRepository
from app.misc_expenses.schemas import MiscellaneousExpenseCreate
from app.ledger.models import PostingCategory
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
        Creates a new Miscellaneous Expense from the BPM workflow.
        This operation validates the data, creates the master record, and
        immediately posts the charge as an obligation to the ledger.
        """
        try:
            # --- Validation ---
            if expense_data.amount <= 0:
                raise MiscellaneousExpenseValidationError("Expense amount must be greater than zero.")
            
            # Additional validation can be added here (e.g., check if driver has an active lease)

            # --- Create Master Expense Record ---
            expense_id = self._generate_next_expense_id()
            new_expense = MiscellaneousExpense(
                expense_id=expense_id,
                case_no=case_no,
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
            )
            
            # --- Post to Ledger ---
            # This is an atomic operation that creates a DEBIT posting and an OPEN balance
            balance = self.ledger_service.create_obligation(
                category=PostingCategory.MISC,
                amount=new_expense.amount,
                reference_id=new_expense.expense_id, # Use the unique expense ID as the reference
                driver_id=new_expense.driver_id,
                lease_id=new_expense.lease_id,
                vehicle_id=new_expense.vehicle_id,
                medallion_id=new_expense.medallion_id,
            )
            
            # Link the ledger posting reference back to the expense record
            new_expense.ledger_posting_ref = balance.reference_id # balance.reference_id holds the posting ID
            
            # Now that the ledger posting is successful, save the expense record
            created_expense = self.repo.create_expense(new_expense)

            # --- Link to BPM Case ---
            bpm_service.create_case_entity(
                self.db, case_no, "miscellaneous_expense", "id", str(created_expense.id)
            )
            
            self.db.commit()
            logger.info(f"Successfully created Miscellaneous Expense {expense_id} and posted to ledger.")
            return created_expense

        except (MiscellaneousExpenseValidationError, InvalidAllocationError) as e:
            self.db.rollback()
            logger.warning(f"Validation error for miscellaneous expense: {e}")
            raise
        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to create miscellaneous expense: {e}", exc_info=True)
            raise MiscellaneousExpenseLedgerError(expense_id if 'expense_id' in locals() else 'N/A', str(e)) from e