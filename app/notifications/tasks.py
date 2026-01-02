import os
from datetime import datetime
from zoneinfo import ZoneInfo

from celery import shared_task

from app.core.config import settings
from app.drivers.models import Driver, TLCLicense
from app.dtr.models import DTR, DTRStatus
from app.entities.models import (  # Import Address and BankAccount for relationships
    Address,
    BankAccount,
)
from app.leases.models import Lease
from app.medallions.models import Medallion

# IMPORTANT: import handler modules so decorators execute
from app.notifications.registry import (
    NotificationContext,
    get_notification_handler,
    import_notification_jobs,
)
from app.scheduler.events_config import load_events_yaml
from app.tlc.models import TLCViolation
from app.uploads.models import Document
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)

# Import all notification handlers so @notification decorators are executed
import_notification_jobs()


def run_notifications(event_key: str):
    from app.notifications.schemas import NotificationChannel
    from app.notifications.services import (
        log_notification_details,
        send_notification_emails,
        send_notification_sms,
    )

    logger.info(f"Loading events yaml from {settings.events_config_path}")
    yaml_content = s3_utils.download_file(settings.events_config_path)
    if not yaml_content:
        raise ValueError("YAML has no content")
    cfg = load_events_yaml(yaml_content)

    event = cfg.events.get(event_key)
    if not event:
        raise ValueError(f"Unknown event_key={event_key}")

    params = event.get("kwargs", {}) or {}

    run_at = datetime.now(tz=ZoneInfo(cfg.timezone))

    ctx = NotificationContext(
        event_key=event_key,
        run_at=run_at,
        params=params,
    )

    # Call the notification handler to collect notifications
    handler = get_notification_handler(event_key)
    notifications = handler(ctx)

    logger.info(f"Notification handler completed for {event_key}, collected {len(notifications)} notifications")

    # Get configuration for sending
    channels = params.get("channels", "EMAIL,SMS")
    email_subject_template = params.get("email_subject_template")
    email_template_s3_key = params.get("email_template_key")
    sms_template_s3_key = params.get("sms_template_key")
    default_cc_emails = params.get("default_cc_emails", "")
    override_emails = params.get("override_emails", "")
    override_phones = params.get("override_phones", "")

    # Parse channels from comma-separated string
    channel_list = [c.strip().upper() for c in channels.split(",")]

    all_results = []

    # Send notifications based on configured channels
    if NotificationChannel.EMAIL.value in channel_list:
        email_results = send_notification_emails(
            notifications,
            email_subject_template=email_subject_template,
            email_template_s3_key=email_template_s3_key,
            default_cc_emails=default_cc_emails,
            override_emails=override_emails,
        )
        all_results.extend(email_results)

    if NotificationChannel.SMS.value in channel_list:
        sms_results = send_notification_sms(
            notifications,
            sms_template_s3_key=sms_template_s3_key,
            override_phones=override_phones,
        )
        all_results.extend(sms_results)

    # Log all notification details
    log_notification_details(all_results)

    logger.info(f"Notification processing completed for {event_key}, sent {len(all_results)} notifications")
    return [r.model_dump() for r in all_results]


@shared_task(name="app.tasks.notifications.evaluate_notification")
def evaluate_notification(event_key: str) -> None:
    logger.info("Running notifications now")
    run_notifications(event_key)
