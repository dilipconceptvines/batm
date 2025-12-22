### app/repairs/exceptions.py

class RepairError(Exception):
    """Base exception for all vehicle repair processing errors."""
    pass

class InvoiceNotFoundError(RepairError):
    """Raised when a specific repair invoice cannot be found."""
    def __init__(self, repair_id: str):
        self.repair_id = repair_id
        super().__init__(f"RepairInvoice with Repair ID '{repair_id}' not found.")

class InvalidRepairOperationError(RepairError):
    """Raised for logical errors, such as trying to modify a closed invoice."""
    pass

class PaymentScheduleGenerationError(RepairError):
    """Raised when the system fails to generate a valid payment schedule."""
    pass

class RepairLedgerPostingError(RepairError):
    """Raised when an installment fails to post to the ledger."""
    def __init__(self, installment_id: str, reason: str):
        self.installment_id = installment_id
        self.reason = reason
        super().__init__(f"Failed to post repair installment '{installment_id}' to ledger: {reason}")