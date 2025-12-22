### app/loans/exceptions.py

class LoanError(Exception):
    """Base exception for all driver loan processing errors."""
    pass

class LoanNotFoundError(LoanError):
    """Raised when a specific driver loan cannot be found."""
    def __init__(self, loan_id: str):
        self.loan_id = loan_id
        super().__init__(f"DriverLoan with Loan ID '{loan_id}' not found.")

class InvalidLoanOperationError(LoanError):
    """Raised for logical errors, such as trying to modify a closed loan."""
    pass

class LoanScheduleGenerationError(LoanError):
    """Raised when the system fails to generate a valid repayment schedule."""
    pass

class LoanLedgerPostingError(LoanError):
    """Raised when a loan installment fails to post to the ledger."""
    def __init__(self, installment_id: str, reason: str):
        self.installment_id = installment_id
        self.reason = reason
        super().__init__(f"Failed to post loan installment '{installment_id}' to ledger: {reason}")

class LoanValidationError(LoanError):
    """Raised for general validation errors during manual loan creation."""
    pass