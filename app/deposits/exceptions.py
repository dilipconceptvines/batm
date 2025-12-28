# app/deposits/exceptions.py

class DepositError(Exception):
    """Base exception for all deposit processing errors."""
    pass

class DepositNotFoundError(DepositError):
    """Raised when a specific deposit cannot be found."""
    def __init__(self, deposit_id: str = None, lease_id: int = None):
        if deposit_id:
            self.deposit_id = deposit_id
            super().__init__(f"Deposit with ID '{deposit_id}' not found.")
        elif lease_id:
            self.lease_id = lease_id
            super().__init__(f"Deposit for lease ID '{lease_id}' not found.")
        else:
            super().__init__("Deposit not found.")

class InvalidDepositOperationError(DepositError):
    """Raised for logical errors in deposit operations."""
    pass

class DepositValidationError(DepositError):
    """Raised when deposit data validation fails."""
    pass

class DepositLedgerError(DepositError):
    """Raised when deposit ledger operations fail."""
    def __init__(self, deposit_id: str, reason: str):
        self.deposit_id = deposit_id
        self.reason = reason
        super().__init__(f"Deposit ledger operation failed for '{deposit_id}': {reason}")