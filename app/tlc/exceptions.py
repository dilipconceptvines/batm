### app/tlc/exceptions.py

class TLCError(Exception):
    """Base exception for all TLC violation processing errors."""
    pass

class TLCViolationNotFoundError(TLCError):
    """Raised when a specific TLC violation cannot be found."""
    def __init__(self, summons_no: str):
        self.summons_no = summons_no
        super().__init__(f"TLCViolation with Summons No '{summons_no}' not found.")

class InvalidTLCActionError(TLCError):
    """Raised for logical errors, such as trying to modify a posted violation incorrectly."""
    pass

class TLCLedgerPostingError(TLCError):
    """Raised when a violation fails to post to the ledger."""
    def __init__(self, summons_no: str, reason: str):
        self.summons_no = summons_no
        self.reason = reason
        super().__init__(f"Failed to post TLC violation '{summons_no}' to ledger: {reason}")

class TLCValidationError(TLCError):
    """Raised for general validation errors during manual TLC violation creation."""
    pass