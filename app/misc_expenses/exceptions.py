### app/misc_expenses/exceptions.py

class MiscellaneousExpenseError(Exception):
    """Base exception for all miscellaneous expense processing errors."""
    pass

class ExpenseNotFoundError(MiscellaneousExpenseError):
    """Raised when a specific miscellaneous expense cannot be found."""
    def __init__(self, expense_id: str):
        self.expense_id = expense_id
        super().__init__(f"MiscellaneousExpense with Expense ID '{expense_id}' not found.")

class InvalidExpenseOperationError(MiscellaneousExpenseError):
    """Raised for logical errors, such as trying to modify a recovered expense."""
    pass

class MiscellaneousExpenseValidationError(MiscellaneousExpenseError):
    """Raised for general validation errors during manual expense creation."""
    pass

class MiscellaneousExpenseLedgerError(MiscellaneousExpenseError):
    """Raised when an expense fails to post correctly to the ledger."""
    def __init__(self, expense_id: str, reason: str):
        self.expense_id = expense_id
        self.reason = reason
        super().__init__(f"Failed to post Miscellaneous Expense '{expense_id}' to ledger: {reason}")