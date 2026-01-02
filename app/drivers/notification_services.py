from datetime import date, datetime

from sqlalchemy.orm import Session

from app.core.config import settings
from app.drivers.services import driver_service
from app.notifications.schemas import (
    NotificationEvent,
    NotificationLogDetail,
)
from app.utils.logger import get_logger

logger = get_logger(__name__)


def build_notification_log_from_driver(driver):
    """
    Helper function to extract all notification-relevant data from a driver.
    Handles all null checks in one place.

    Args:
        driver: Driver object

    Returns:
        Dictionary with all driver-related notification data
    """
    # Get driver's active lease info
    active_lease = None
    if driver and driver.lease_drivers:
        for lease_driver in driver.lease_drivers:
            if lease_driver.is_active and lease_driver.lease:
                active_lease = lease_driver.lease
                break

    return {
        "driver_id": driver.driver_id if driver else "",
        "driver_name": driver.full_name if driver and driver.full_name else "",
        "driver_tlc_number": (
            driver.tlc_license.tlc_license_number
            if driver and driver.tlc_license
            else ""
        ),
        "driver_status": driver.driver_status if driver else "",
        "driver_email": driver.email_address if driver else "",
        "driver_phone": (
            driver.phone_number_1 or driver.phone_number_2 if driver else ""
        ),
        "lease_id": active_lease.lease_id if active_lease else "",
        "lease_status": active_lease.lease_status if active_lease else "",
        "medallion_number": (
            active_lease.medallion.medallion_number
            if active_lease and active_lease.medallion
            else ""
        ),
        "vehicle_id": (
            active_lease.vehicle.vin if active_lease and active_lease.vehicle else ""
        ),
    }


def get_dmv_license_expiry_notifications(
    db: Session,
    target_date: date,
    run_at_tz: datetime,
    event: NotificationEvent = NotificationEvent.DMV_EXPIRY_PRIOR,
):
    """
    Collect drivers with DMV licenses expiring on the given target date.
    Handles PRIOR (future dates), DATE (today), and POST (past dates) notifications.
    Only processes drivers with status: Registered, Active, or Suspended.

    Args:
        db: Database session
        target_date: Target expiry date to check (can be past, present, or future)
        run_at_tz: Timezone-aware datetime when notification is running
        event: NotificationEvent enum for event key and name

    Returns:
        List of NotificationLogDetail objects (without channel and status - to be filled by send functions)
    """
    from app.drivers.models import Driver

    # Allowed driver statuses for notifications
    allowed_statuses = ["Registered", "Active", "Suspended"]

    logger.info(
        "Collecting DMV License expiry notifications",
        target_date=str(target_date),
        allowed_statuses=allowed_statuses,
    )

    dmv_licenses = driver_service.get_dmv_license(
        db=db, expiry_to=target_date, multiple=True
    )

    # Filter by driver status and exact expiry date match
    dmv_licenses = [
        dmv
        for dmv in dmv_licenses
        if dmv.driver
        and dmv.driver.driver_status in allowed_statuses
        and dmv.dmv_license_expiry_date
        and dmv.dmv_license_expiry_date.date() == target_date
    ]

    logger.info(
        f"Found {len(dmv_licenses)} DMV licenses expiring on {target_date} with allowed statuses"
    )

    notifications = []
    attempt_datetime = run_at_tz.isoformat()
    today = run_at_tz.date()

    for dmv in dmv_licenses:
        try:
            driver = dmv.driver
            remaining_days = (dmv.dmv_license_expiry_date.date() - today).days
            has_dmv_expired = remaining_days < 0

            # Get all driver-related data using helper function
            driver_data = build_notification_log_from_driver(driver)

            # Format date using common_date_format from settings if available
            if settings.common_date_format and dmv.dmv_license_expiry_date:
                formatted_dmv_date = dmv.dmv_license_expiry_date.strftime(settings.common_date_format)
            else:
                formatted_dmv_date = str(dmv.dmv_license_expiry_date) if dmv.dmv_license_expiry_date else ""

            # Create notification log entry (channel and status will be filled by send functions)
            notification = NotificationLogDetail(
                event_key=event.name,
                event_name=event.value,
                channel="",  # Will be set by send function
                attempt_datetime=attempt_datetime,
                dmv_license_number=dmv.dmv_license_number or "",
                dmv_expiry_date=formatted_dmv_date,
                dmv_remaining_days=remaining_days,
                has_dmv_expired=has_dmv_expired,
                email_status=None,
                sms_status=None,
                process_error=None,
                email_error=None,
                sms_error=None,
                **driver_data,  # Unpack driver data
            )

            notifications.append(notification)

        except Exception as inner_exc:
            logger.error(
                f"Failed collecting notification for driver {dmv.driver.driver_id}: {inner_exc}"
            )
            # Get whatever driver data we can for error logging
            driver_data = build_notification_log_from_driver(dmv.driver)

            # Format date for error case
            if settings.common_date_format and dmv.dmv_license_expiry_date:
                error_dmv_date = dmv.dmv_license_expiry_date.strftime(settings.common_date_format)
            else:
                error_dmv_date = str(dmv.dmv_license_expiry_date) if dmv.dmv_license_expiry_date else ""

            # Create a notification entry with the error
            error_notification = NotificationLogDetail(
                event_key=event.name,
                event_name=event.value,
                channel="",
                attempt_datetime=attempt_datetime,
                dmv_license_number=dmv.dmv_license_number or "",
                dmv_expiry_date=error_dmv_date,
                dmv_remaining_days=0,
                has_dmv_expired=False,
                email_status=None,
                sms_status=None,
                process_error=str(inner_exc),
                email_error=None,
                sms_error=None,
                **driver_data,  # Unpack driver data
            )
            notifications.append(error_notification)

    logger.info(f"Collected {len(notifications)} DMV notification entries")

    return notifications


def get_tlc_license_expiry_notifications(
    db: Session,
    target_date: date,
    run_at_tz: datetime,
    event: NotificationEvent = NotificationEvent.TLC_EXPIRY_PRIOR,
):
    """
    Collect drivers with TLC licenses expiring on the given target date.
    Handles PRIOR (future dates), DATE (today), and POST (past dates) notifications.
    Only processes drivers with status: Registered, Active, or Suspended.

    Args:
        db: Database session
        target_date: Target expiry date to check (can be past, present, or future)
        run_at_tz: Timezone-aware datetime when notification is running
        event: NotificationEvent enum for event key and name

    Returns:
        List of NotificationLogDetail objects (without channel and status - to be filled by send functions)
    """
    from app.drivers.models import Driver

    # Allowed driver statuses for notifications
    allowed_statuses = ["Registered", "Active", "Suspended"]

    logger.info(
        "Collecting TLC License expiry notifications",
        target_date=str(target_date),
        allowed_statuses=allowed_statuses,
    )

    tlc_licenses = driver_service.get_tlc_license(
        db=db, expiry_to=target_date, multiple=True
    )

    # Filter by driver status and exact expiry date match
    tlc_licenses = [
        tlc
        for tlc in tlc_licenses
        if tlc.driver
        and tlc.driver.driver_status in allowed_statuses
        and tlc.tlc_license_expiry_date
        and tlc.tlc_license_expiry_date.date() == target_date
    ]

    logger.info(
        f"Found {len(tlc_licenses)} TLC licenses expiring on {target_date} with allowed statuses"
    )

    notifications = []
    attempt_datetime = run_at_tz.isoformat()
    today = run_at_tz.date()

    for tlc in tlc_licenses:
        try:
            driver = tlc.driver
            remaining_days = (tlc.tlc_license_expiry_date.date() - today).days
            has_tlc_expired = remaining_days < 0

            # Get all driver-related data using helper function
            driver_data = build_notification_log_from_driver(driver)

            # Format date using common_date_format from settings if available
            if settings.common_date_format and tlc.tlc_license_expiry_date:
                formatted_tlc_date = tlc.tlc_license_expiry_date.strftime(settings.common_date_format)
            else:
                formatted_tlc_date = str(tlc.tlc_license_expiry_date) if tlc.tlc_license_expiry_date else ""

            # Create notification log entry (channel and status will be filled by send functions)
            notification = NotificationLogDetail(
                event_key=event.name,
                event_name=event.value,
                channel="",  # Will be set by send function
                attempt_datetime=attempt_datetime,
                tlc_license_number=tlc.tlc_license_number or "",
                tlc_expiry_date=formatted_tlc_date,
                tlc_remaining_days=remaining_days,
                has_tlc_expired=has_tlc_expired,
                email_status=None,
                sms_status=None,
                process_error=None,
                email_error=None,
                sms_error=None,
                **driver_data,  # Unpack driver data
            )

            notifications.append(notification)

        except Exception as inner_exc:
            logger.error(
                f"Failed collecting notification for driver {tlc.driver.driver_id}: {inner_exc}"
            )
            # Get whatever driver data we can for error logging
            driver_data = build_notification_log_from_driver(tlc.driver)

            # Format date for error case
            if settings.common_date_format and tlc.tlc_license_expiry_date:
                error_tlc_date = tlc.tlc_license_expiry_date.strftime(settings.common_date_format)
            else:
                error_tlc_date = str(tlc.tlc_license_expiry_date) if tlc.tlc_license_expiry_date else ""

            # Create a notification entry with the error
            error_notification = NotificationLogDetail(
                event_key=event.name,
                event_name=event.value,
                channel="",
                attempt_datetime=attempt_datetime,
                tlc_license_number=tlc.tlc_license_number or "",
                tlc_expiry_date=error_tlc_date,
                tlc_remaining_days=0,
                has_tlc_expired=False,
                email_status=None,
                sms_status=None,
                process_error=str(inner_exc),
                email_error=None,
                sms_error=None,
                **driver_data,  # Unpack driver data
            )
            notifications.append(error_notification)

    logger.info(f"Collected {len(notifications)} TLC notification entries")

    return notifications
