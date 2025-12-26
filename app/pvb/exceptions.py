### app/pvb/exceptions.py

class PVBError(Exception):
    """Base exception for all PVB processing-related errors."""
    pass

class PVBCSVParseError(PVBError):
    """Raised when there is an error parsing an uploaded PVB CSV file."""
    def __init__(self, message: str, row_number: int = None):
        self.row_number = row_number
        if row_number:
            super().__init__(f"PVB CSV parsing error on row {row_number}: {message}")
        else:
            super().__init__(f"PVB CSV parsing error: {message}")


class ReassignmentError(PVBError):
    """
    Raised when violation reassignment fails.
    
    This exception is used for all reassignment validation and processing errors including:
    - Source/target driver or lease not found
    - Violation not in valid status for reassignment
    - No-op reassignment (source equals target)
    - Ledger balance not found
    - Ledger reversal or posting failures
    """
    
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(f"Reassignment failed: {reason}")


class PVBAssociationError(PVBError):
    """Raised when a PVB violation cannot be associated with a valid lease/driver."""
    def __init__(self, summons_number: str, reason: str):
        self.summons_number = summons_number
        self.reason = reason
        super().__init__(f"Failed to associate PVB violation '{summons_number}': {reason}")

class PVBLedgerPostingError(PVBError):
    """Raised when a successfully associated violation fails to post to the ledger."""
    def __init__(self, summons_number: str, reason: str):
        self.summons_number = summons_number
        self.reason = reason
        super().__init__(f"Failed to post PVB violation '{summons_number}' to ledger: {reason}")

class PVBImportInProgressError(PVBError):
    """Raised when an attempt is made to start a new import while one is already running."""
    def __init__(self):
        super().__init__("A PVB import is already in progress. Please wait for it to complete.")

class PVBValidationError(PVBError):
    """Raised for general validation errors during manual PVB creation."""
    pass