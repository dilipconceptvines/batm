# app/curb/exceptions.py

"""
CURB Module Custom Exceptions

Provides specific exception types for clear error handling throughout
the CURB data processing pipeline.
"""


class CurbError(Exception):
    """Base exception for all CURB module errors"""
    pass


class CurbApiError(CurbError):
    """
    Raised when CURB API communication fails
    
    Examples:
    - Network timeout
    - Invalid response format
    - Authentication failure
    - SOAP envelope parsing errors
    """
    pass


class CurbAccountNotFoundError(CurbError):
    """Raised when a specified CURB account doesn't exist or is inactive"""
    pass


class CurbTripNotFoundError(CurbError):
    """Raised when a specific trip cannot be found in the database"""
    pass


class CurbDataParsingError(CurbError):
    """
    Raised when XML data from CURB API cannot be parsed
    
    Examples:
    - Malformed XML structure
    - Missing required fields
    - Invalid data types
    """
    pass


class CurbLedgerPostingError(CurbError):
    """
    Raised when posting to the ledger fails
    
    Examples:
    - Ledger service unavailable
    - Invalid trip data for posting
    - Duplicate posting attempt
    """
    pass


class CurbReconciliationError(CurbError):
    """
    Raised when trip reconciliation with CURB API fails
    
    Examples:
    - Reconciliation API call failure
    - Invalid reconciliation ID format
    - Trips already reconciled
    """
    pass


class CurbS3Error(CurbError):
    """
    Raised when S3 data lake operations fail
    
    Examples:
    - Upload failure
    - Download failure
    - File not found in S3
    - Invalid S3 path
    """
    pass