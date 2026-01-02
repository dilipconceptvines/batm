## app/notifications/services.py

from typing import List

from app.core.config import settings
from app.notifications.schemas import (
    NotificationChannel,
    NotificationLogDetail,
    NotificationStatus,
)
from app.utils.email_service import send_templated_email
from app.utils.logger import get_logger
from app.utils.sms_service import send_templated_sms

logger = get_logger(__name__)


def send_notification_emails(
    notifications: List[NotificationLogDetail],
    email_subject_template: str = None,
    email_template_s3_key: str = None,
    default_cc_emails: str = "",
    override_emails: str = "",
):
    """
    Send emails for collected notifications and update email_status.
    Uses all fields from NotificationLogDetail as template variables.

    Args:
        notifications: List of NotificationLogDetail objects
        email_subject_template: Email subject template string (supports {field_name} placeholders)
        email_template_s3_key: S3 key for email template (template can access all notification fields)
        default_cc_emails: Comma-separated list of default CC email addresses
        override_emails: Comma-separated list of emails to send to instead of driver emails (bypasses driver email)

    Returns:
        List of NotificationLogDetail objects with updated email_status and channel
    """
    email_subject_template = (
        email_subject_template or settings.dmv_license_expiry_reminder_subject_template
    )
    email_template_s3_key = (
        email_template_s3_key or settings.driver_license_expiry_email_template
    )

    # Parse override emails
    override_email_list = (
        [email.strip() for email in override_emails.split(",") if email.strip()]
        if override_emails
        else []
    )

    # Parse CC emails
    cc_email_list = (
        [email.strip() for email in default_cc_emails.split(",") if email.strip()]
        if default_cc_emails
        else []
    )

    results = []

    for notification in notifications:
        email_log = notification.model_copy()
        email_log.channel = NotificationChannel.EMAIL.value

        # Determine recipient emails
        if override_email_list:
            # Use override emails instead of driver email
            to_emails = override_email_list
        elif notification.driver_email:
            # Use driver email
            to_emails = [notification.driver_email]
        else:
            # No email to send to
            email_log.email_status = NotificationStatus.NOT_SENT_MISSING_EMAIL.value
            results.append(email_log)
            continue

        try:
            # Convert notification object to dict for template access
            notification_dict = notification.model_dump()

            # Format subject using template with all notification fields available
            subject = email_subject_template.format(**notification_dict)

            # Add formatted subject to template data
            template_data = {
                "subject": subject,
                **notification_dict,  # Make all notification fields available to template
            }

            success = send_templated_email(
                to_emails=to_emails,
                cc_emails=cc_email_list if cc_email_list else None,
                subject_template=subject,
                template_s3_key=email_template_s3_key,
                template_data=template_data,
            )

            email_log.email_status = (
                NotificationStatus.SENT.value
                if success
                else NotificationStatus.FAILED_TECHNICAL.value
            )
        except Exception as e:
            logger.error(f"Error sending email to {notification.driver_id}: {e}")
            email_log.email_status = NotificationStatus.FAILED_TECHNICAL.value
            email_log.email_error = str(e)

        results.append(email_log)

    logger.info(f"Sent {len(results)} emails")
    return results


def send_notification_sms(
    notifications: List[NotificationLogDetail],
    sms_template_s3_key: str = None,
    override_phones: str = "",
):
    """
    Send SMS for collected notifications and update sms_status.
    Uses all fields from NotificationLogDetail as template variables.

    Args:
        notifications: List of NotificationLogDetail objects
        sms_template_s3_key: S3 key for SMS template (template can access all notification fields)
        override_phones: Comma-separated list of phone numbers to send to instead of driver phones (bypasses driver phone)

    Returns:
        List of NotificationLogDetail objects with updated sms_status and channel
    """
    sms_template_s3_key = (
        sms_template_s3_key or settings.driver_license_expiry_sms_template
    )

    # Parse override phones
    override_phone_list = (
        [phone.strip() for phone in override_phones.split(",") if phone.strip()]
        if override_phones
        else []
    )

    results = []

    for notification in notifications:
        sms_log = notification.model_copy()
        sms_log.channel = NotificationChannel.SMS.value

        # Determine recipient phone numbers
        if override_phone_list:
            # Use override phones instead of driver phone
            phone_numbers = override_phone_list
        elif notification.driver_phone:
            # Use driver phone
            phone_numbers = [notification.driver_phone]
        else:
            # No phone to send to
            sms_log.sms_status = NotificationStatus.NOT_SENT_MISSING_PHONE.value
            results.append(sms_log)
            continue

        try:
            # Convert notification object to dict for template access
            notification_dict = notification.model_dump()

            # Make all notification fields available to SMS template
            template_data = notification_dict

            # Send to all phone numbers (override or driver)
            all_success = True
            for phone_number in phone_numbers:
                success = send_templated_sms(
                    phone_number=phone_number,
                    template_s3_key=sms_template_s3_key,
                    template_data=template_data,
                    sender_id=None,
                )
                if not success:
                    all_success = False

            sms_log.sms_status = (
                NotificationStatus.SENT.value
                if all_success
                else NotificationStatus.FAILED_TECHNICAL.value
            )
        except Exception as e:
            logger.error(f"Error sending SMS to {notification.driver_id}: {e}")
            sms_log.sms_status = NotificationStatus.FAILED_TECHNICAL.value
            sms_log.sms_error = str(e)

        results.append(sms_log)

    logger.info(f"Sent {len(results)} SMS")
    return results


def log_notification_details(notifications: List[NotificationLogDetail]):
    """
    Log all notification details for tracking and debugging.

    Args:
        notifications: List of NotificationLogDetail objects
    """
    for notification in notifications:
        log_data = {
            "event_key": notification.event_key,
            "event_name": notification.event_name,
            "channel": notification.channel,
            "attempt_datetime": notification.attempt_datetime,
            "driver_id": notification.driver_id,
            "driver_tlc_number": notification.driver_tlc_number,
            "driver_status": notification.driver_status,
            "driver_email": notification.driver_email,
            "driver_phone": notification.driver_phone,
            "lease_id": notification.lease_id,
            "lease_status": notification.lease_status,
            "medallion_number": notification.medallion_number,
            "vehicle_id": notification.vehicle_id,
            "email_status": notification.email_status,
            "email_error": notification.email_error,
            "sms_status": notification.sms_status,
            "sms_error": notification.sms_error,
            "process_error": notification.process_error,
        }

        logger.info("Notification log entry", **log_data)
