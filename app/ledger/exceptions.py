# app/ledger/exceptions.py

class LedgerError(Exception):
    """Base exception for ledger-related errors."""
    pass

class PostingNotFoundError(LedgerError):
    """Raised when a specific ledger posting cannot be found."""
    def __init__(self, posting_id: str):
        self.posting_id = posting_id
        super().__init__(f"LedgerPosting with ID '{posting_id}' not found.")

class BalanceNotFoundError(LedgerError):
    """Raised when a specific ledger balance cannot be found."""
    def __init__(self, reference_id: str):
        self.reference_id = reference_id
        super().__init__(f"LedgerBalance with reference_id '{reference_id}' not found.")

class InvalidLedgerOperationError(LedgerError):
    """Raised when an invalid operation is attempted (e.g., applying payment to a closed balance)."""
    pass