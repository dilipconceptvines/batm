## app/notifications/schemas.py

# Standard library imports
from enum import Enum as PyEnum
from typing import Optional

# Third party imports
from pydantic import BaseModel


class NotificationChannel(str, PyEnum):
    """
    Notification delivery channels
    """
    EMAIL = "EMAIL"
    SMS = "SMS"


class NotificationEvent(str, PyEnum):
    """
    Notification event types with their descriptions
    """
    DMV_EXPIRY_PRIOR = "DMV License Expiry Reminder"
    DMV_EXPIRY_POST = "DMV License Expired Notification"
    TLC_EXPIRY_PRIOR = "TLC License Expiry Reminder"
    TLC_EXPIRY_POST = "TLC License Expired Notification"
    DMV_EXPIRY_DATE = "DMV License Expiring Today"
    TLC_EXPIRY_DATE = "TLC License Expiring Today"


class NotificationStatus(str, PyEnum):
    """
    Notification delivery status
    """
    SENT = "SENT"
    DELIVERED = "DELIVERED"
    FAILED_TECHNICAL = "FAILED_TECHNICAL"
    NOT_SENT_MISSING_EMAIL = "NOT_SENT_MISSING_EMAIL"
    NOT_SENT_MISSING_PHONE = "NOT_SENT_MISSING_PHONE"
    NOT_SENT_INVALID_EMAIL = "NOT_SENT_INVALID_EMAIL"
    NOT_SENT_INVALID_PHONE = "NOT_SENT_INVALID_PHONE"


class NotificationLogDetail(BaseModel):
    """
    Notification log details for tracking email/SMS delivery attempts.
    """
    event_key: str
    event_name: str
    channel: str  # "EMAIL" or "SMS"
    attempt_datetime: str

    driver_id: str
    driver_name: str = ""
    driver_tlc_number: str = ""
    driver_status: str = ""
    driver_email: str = ""
    driver_phone: str = ""

    dmv_license_number: str = ""
    dmv_expiry_date: str = ""
    dmv_remaining_days: int = 0
    has_dmv_expired: bool = False

    tlc_license_number: str = ""
    tlc_expiry_date: str = ""
    tlc_remaining_days: int = 0
    has_tlc_expired: bool = False

    lease_id: str = ""
    lease_status: str = ""
    medallion_number: str = ""
    vehicle_id: str = ""

    email_status: Optional[str] = None  # NotificationStatus
    sms_status: Optional[str] = None  # NotificationStatus

    process_error: Optional[str] = None  # Error during data collection
    email_error: Optional[str] = None  # Error during email sending
    sms_error: Optional[str] = None  # Error during SMS sending
