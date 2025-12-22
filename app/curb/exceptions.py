### app/curb/exceptions.py

class CurbError(Exception):
    """Base exception for all CURB integration-related errors."""
    pass

class CurbApiError(CurbError):
    """Raised when the CURB API returns an error or unexpected response."""
    def __init__(self, message: str, status_code: int = None):
        self.status_code = status_code
        super().__init__(f"CURB API Error: {message}")

class ReconciliationError(CurbError):
    """Raised when an error occurs during the reconciliation process with the CURB API."""
    pass

class DataMappingError(CurbError):
    """Raised when CURB data cannot be successfully mapped to internal system entities."""
    def __init__(self, field: str, value: str):
        self.field = field
        self.value = value
        super().__init__(f"Could not map CURB data: No match found for {field}='{value}'")

class TripProcessingError(CurbError):
    """Raised during the ledger posting phase if a trip cannot be processed."""
    def __init__(self, trip_id: str, reason: str):
        self.trip_id = trip_id
        self.reason = reason
        super().__init__(f"Failed to process trip {trip_id} for ledger posting. Reason: {reason}")