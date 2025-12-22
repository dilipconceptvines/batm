### app/ezpass/exceptions.py

class EZPassError(Exception):
    """Base exception for all EZPass processing-related errors."""
    pass

class CSVParseError(EZPassError):
    """Raised when there is an error parsing the uploaded CSV file."""
    def __init__(self, message: str, row_number: int = None):
        self.row_number = row_number
        if row_number:
            super().__init__(f"CSV parsing error on row {row_number}: {message}")
        else:
            super().__init__(f"CSV parsing error: {message}")

class AssociationError(EZPassError):
    """Raised when an EZPass transaction cannot be associated with a valid lease/driver."""
    def __init__(self, transaction_id: str, reason: str):
        self.transaction_id = transaction_id
        self.reason = reason
        super().__init__(f"Failed to associate EZPass transaction '{transaction_id}': {reason}")


class ReassignmentError(EZPassError):
    """
    Raised when transaction reassignment fails.
    
    This exception is used for all reassignment validation and processing errors including:
    - Source/target driver or lease not found
    - Transaction not in valid status for reassignment
    - No-op reassignment (source equals target)
    - Ledger balance not found
    - Ledger reversal or posting failures
    """
    
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(f"Reassignment failed: {reason}")


class ValidationError(EZPassError):
    """Raised when data validation fails."""
    
    def __init__(self, field: str, reason: str):
        self.field = field
        self.reason = reason
        super().__init__(f"Validation error for {field}: {reason}")


class LedgerPostingError(EZPassError):
    """Raised when a successfully associated transaction fails to post to the ledger."""
    def __init__(self, transaction_id: str, reason: str):
        self.transaction_id = transaction_id
        self.reason = reason
        super().__init__(f"Failed to post EZPass transaction '{transaction_id}' to ledger: {reason}")

class ImportInProgressError(EZPassError):
    """Raised when an attempt is made to start a new import while one is already running."""
    def __init__(self):
        super().__init__("An EZPass import is already in progress. Please wait for it to complete.")