### app/interim_payments/exceptions.py

class InterimPaymentError(Exception):
    """Base exception for all interim payment processing errors."""
    pass

class PaymentNotFoundError(InterimPaymentError):
    """Raised when a specific interim payment cannot be found."""
    def __init__(self, payment_id: str):
        self.payment_id = payment_id
        super().__init__(f"InterimPayment with Payment ID '{payment_id}' not found.")

class InvalidAllocationError(InterimPaymentError):
    """Raised for logical errors during the payment allocation process, such as over-allocating funds."""
    pass

class InterimPaymentValidationError(InterimPaymentError):
    """Raised for general validation errors during manual payment creation."""
    pass

class InterimPaymentLedgerError(InterimPaymentError):
    """Raised when an interim payment fails to post correctly to the ledger."""
    def __init__(self, payment_id: str, reason: str):
        self.payment_id = payment_id
        self.reason = reason
        super().__init__(f"Failed to post Interim Payment '{payment_id}' to ledger: {reason}")