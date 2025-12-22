# app/dtr/exceptions.py

"""
Custom exceptions for DTR module
"""


class DTRException(Exception):
    """Base exception for DTR module"""
    pass


class DTRValidationError(DTRException):
    """Raised when DTR validation fails"""
    pass


class DTRGenerationError(DTRException):
    """Raised when DTR generation fails"""
    pass


class DTRNotFoundError(DTRException):
    """Raised when DTR is not found"""
    pass


class DTRAlreadyExistsError(DTRException):
    """Raised when trying to create duplicate DTR"""
    pass


class DTRStatusError(DTRException):
    """Raised when DTR is in wrong status for operation"""
    pass


class MidWeekTerminationError(DTRException):
    """Raised when mid-week termination handling fails"""
    pass


class PendingChargesError(DTRException):
    """Raised when charges are still pending"""
    pass


# app/driver_payments/exceptions.py

"""
Custom exceptions for Driver Payments module
"""


class DriverPaymentException(Exception):
    """Base exception for driver payments"""
    pass


class ACHBatchError(DriverPaymentException):
    """Base exception for ACH batch operations"""
    pass


class MissingBankInformationError(ACHBatchError):
    """Raised when driver is missing bank information"""
    pass


class CompanyBankConfigError(ACHBatchError):
    """Raised when company bank configuration is missing or invalid"""
    pass


class NACHAGenerationError(ACHBatchError):
    """Raised when NACHA file generation fails"""
    pass


class ACHBatchNotFoundError(ACHBatchError):
    """Raised when ACH batch is not found"""
    pass


class ACHBatchReversalError(ACHBatchError):
    """Raised when batch reversal fails"""
    pass


class InvalidBatchStatusError(ACHBatchError):
    """Raised when batch is in invalid status for operation"""
    pass