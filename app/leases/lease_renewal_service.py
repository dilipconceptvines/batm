# app/leases/lease_renewal_service.py

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import boto3
from dateutil.relativedelta import relativedelta
from sqlalchemy import and_
from sqlalchemy.orm import Session

from app.audit_trail.schemas import AuditTrailType
from app.audit_trail.services import audit_trail_service
from app.core.config import settings
from app.leases.models import Lease, LeaseConfiguration, LeaseDriver, LeaseSchedule
from app.leases.schemas import (
    DOV_DEFAULT_TOTAL_SEGMENTS,
    LeaseStatus,
    LeaseType,
    get_lease_renewal_period_config_key,
)
from app.leases.services import lease_service
from app.utils.logger import get_logger

logger = get_logger(__name__)


def get_renewal_period_months(lease_type: str) -> int:
    """
    Get the renewal period in months based on lease type from configuration.

    Args:
        lease_type: Type of lease (DOV, MEDALLION, LONG TERM, SHIFT, SHORT TERM)

    Returns:
        Renewal period in months
    """
    config_key = get_lease_renewal_period_config_key(lease_type)
    return getattr(settings, config_key, 6)


def get_s3_template(bucket_name: str, template_key: str) -> Optional[str]:
    """
    Fetch an HTML email template from S3 and return it as a decoded string.
    (No gzip handling.)

    Args:
        bucket_name: S3 bucket name
        template_key: S3 object key for the template

    Returns:
        HTML template content as string or None if error
    """
    try:
        logger.info(
            f"Fetching S3 template from bucket: {bucket_name}, key: {template_key}"
        )
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region,
        )

        response = s3_client.get_object(Bucket=bucket_name, Key=template_key)
        body = response["Body"].read()

        # Try to honor charset from ContentType; default to utf-8
        content_type = response.get("ContentType", "") or ""
        charset = "utf-8"
        if "charset=" in content_type.lower():
            # e.g. "text/html; charset=utf-8"
            try:
                charset = (
                    content_type.lower()
                    .split("charset=", 1)[1]
                    .split(";", 1)[0]
                    .strip()
                ) or "utf-8"
            except Exception:
                charset = "utf-8"

        html = body.decode(charset, errors="replace")
        logger.info(f"Successfully fetched template from S3: {template_key}")
        return html

    except Exception as e:
        logger.error(
            f"Error fetching template from S3 bucket '{bucket_name}' with key '{template_key}': {str(e)}"
        )
        return None


def render_template(template: str, data: Dict[str, Any]) -> str:
    """
    Simple template rendering by replacing placeholders.

    Args:
        template: Template string with {{variable}} placeholders
        data: Dictionary of variable values

    Returns:
        Rendered template string
    """
    rendered = template
    for key, value in data.items():
        placeholder = "{{" + key + "}}"
        rendered = rendered.replace(placeholder, str(value) if value else "")
    return rendered


def render_admin_template(template: str, renewal_summary: Dict[str, Any]) -> str:
    """
    Render the admin notification template with renewal data.

    Args:
        template: Template HTML string
        renewal_summary: Dictionary containing renewal results

    Returns:
        Rendered HTML string
    """
    html = template.replace(
        "{{renewal_date}}", renewal_summary.get("renewal_date", "N/A")
    )
    html = html.replace(
        "{{total_leases_found}}", str(renewal_summary.get("total_leases_found", 0))
    )
    html = html.replace(
        "{{leases_renewed}}", str(renewal_summary.get("leases_renewed", 0))
    )
    html = html.replace("{{errors}}", str(renewal_summary.get("errors", 0)))
    html = html.replace(
        "{{generation_timestamp}}", datetime.now().strftime("%B %d, %Y at %I:%M %p")
    )

    # Build renewed leases rows
    renewed_leases = renewal_summary.get("renewed_leases", [])
    if renewed_leases:
        rows_html = ""
        for lease in renewed_leases:
            renewal_weeks = lease.get("renewal_weeks", "N/A")
            rows_html += f"""
                <tr>
                    <td>{lease.get("lease_id", "N/A")}</td>
                    <td>{lease.get("lease_type", "N/A")}</td>
                    <td>{lease.get("lease_start_date", "N/A")}</td>
                    <td>{lease.get("old_end_date", "N/A")}</td>
                    <td>{lease.get("last_renewed_date", "N/A")}</td>
                    <td>{lease.get("new_end_date", "N/A")}</td>
                    <td><span class="badge badge-success">{renewal_weeks} weeks</span></td>
                    <td>{lease.get("action", "N/A")}</td>
                </tr>
            """
        html = html.replace("{{renewed_leases_rows}}", rows_html)
        html = html.replace("{{#if_renewed_leases}}", "")
        html = html.replace("{{/if_renewed_leases}}", "")
        html = html.replace("{{#if_no_renewals}}", "<!--")
        html = html.replace("{{/if_no_renewals}}", "-->")
    else:
        html = html.replace("{{#if_renewed_leases}}", "<!--")
        html = html.replace("{{/if_renewed_leases}}", "-->")
        html = html.replace("{{#if_no_renewals}}", "")
        html = html.replace("{{/if_no_renewals}}", "")

    # Build error rows
    error_details = renewal_summary.get("error_details", [])
    if error_details:
        error_rows_html = ""
        for error in error_details:
            error_rows_html += f"""
                <tr>
                    <td>{error.get("lease_id", "N/A")}</td>
                    <td>{error.get("error", "N/A")}</td>
                </tr>
            """
        html = html.replace("{{error_rows}}", error_rows_html)
        html = html.replace("{{#if_errors}}", "")
        html = html.replace("{{/if_errors}}", "")
    else:
        html = html.replace("{{#if_errors}}", "<!--")
        html = html.replace("{{/if_errors}}", "-->")

    return html


def send_email_via_ses(
    to_email: str,
    subject: str,
    body_html: str,
    body_text: str,
    sender_email: str,
    configuration_set: Optional[str] = None,
) -> bool:
    """
    Send email via AWS SES.

    Args:
        to_email: Recipient email address
        subject: Email subject
        body_html: HTML body content
        body_text: Plain text body content
        sender_email: Sender email address
        configuration_set: Optional SES configuration set

    Returns:
        True if successful, False otherwise
    """
    try:
        ses_client = boto3.client(
            "ses",
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region,
        )

        message = {
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {
                "Text": {"Data": body_text, "Charset": "UTF-8"},
                "Html": {"Data": body_html, "Charset": "UTF-8"},
            },
        }

        params = {
            "Source": sender_email,
            "Destination": {"ToAddresses": [to_email]},
            "Message": message,
        }

        if configuration_set:
            params["ConfigurationSetName"] = configuration_set

        response = ses_client.send_email(**params)
        logger.info(f"Email sent to {to_email}, MessageId: {response['MessageId']}")
        return True
    except Exception as e:
        logger.error(f"Error sending email to {to_email}: {str(e)}")
        return False


def send_sms_via_sns(
    phone_number: str, message: str, sender_id: Optional[str] = None
) -> bool:
    """
    Send SMS via AWS SNS.

    Args:
        phone_number: Phone number in E.164 format (e.g., +1234567890)
        message: SMS message content
        sender_id: Optional sender ID

    Returns:
        True if successful, False otherwise
    """
    try:
        sns_client = boto3.client(
            "sns",
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region,
        )

        params = {
            "PhoneNumber": phone_number,
            "Message": message,
            "MessageAttributes": {
                "AWS.SNS.SMS.SMSType": {
                    "DataType": "String",
                    "StringValue": "Transactional",
                }
            },
        }

        if sender_id:
            params["MessageAttributes"]["AWS.SNS.SMS.SenderID"] = {
                "DataType": "String",
                "StringValue": sender_id,
            }

        response = sns_client.publish(**params)
        logger.info(f"SMS sent to {phone_number}, MessageId: {response['MessageId']}")
        return True
    except Exception as e:
        logger.error(f"Error sending SMS to {phone_number}: {str(e)}")
        return False


def normalize_phone_number(phone: str) -> Optional[str]:
    """
    Normalize US phone number to E.164 format.

    Args:
        phone: Phone number in various formats

    Returns:
        Phone number in E.164 format (+1XXXXXXXXXX) or None
    """
    if not phone:
        return None

    # Remove all non-digit characters
    digits = "".join(filter(str.isdigit, phone))

    # Add country code if not present
    if len(digits) == 10:
        return f"+1{digits}"
    elif len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"

    return None


def send_admin_notification(renewal_summary: Dict[str, Any]) -> bool:
    """
    Send email notification to admin with lease renewal summary.

    Args:
        renewal_summary: Dictionary containing renewal results

    Returns:
        True if successful, False otherwise
    """
    try:
        # Fetch template from S3
        template_key = settings.admin_renewal_notification_template_key
        template = get_s3_template(settings.s3_bucket_name, template_key)

        if not template:
            error_msg = f"Failed to fetch admin renewal notification template from S3 bucket '{settings.s3_bucket_name}' with key '{template_key}'"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Render template
        html_body = render_admin_template(template, renewal_summary)

        # Log the complete HTML email content for verification
        logger.info("\n" + "=" * 80)
        logger.info("ADMIN NOTIFICATION EMAIL - HTML CONTENT")
        logger.info("=" * 80)
        logger.info(f"To: {settings.aws_admin_email}")
        logger.info(f"From: {settings.aws_ses_sender_email}")
        logger.info(
            f"Subject: Lease Auto-Renewal Summary - {renewal_summary['renewal_date']}"
        )
        logger.info("-" * 80)
        logger.info("HTML BODY:")
        logger.info("-" * 80)
        logger.info(html_body)
        logger.info("=" * 80 + "\n")

        # Build email subject
        subject = f"Lease Auto-Renewal Summary - {renewal_summary['renewal_date']}"

        # Plain text body
        text_body = f"""
Lease Auto-Renewal Summary

Renewal Date: {renewal_summary["renewal_date"]}
Total Leases Found: {renewal_summary["total_leases_found"]}
Leases Renewed: {renewal_summary["leases_renewed"]}
Errors: {renewal_summary["errors"]}

Renewed Leases:
"""
        for lease in renewal_summary.get("renewed_leases", []):
            text_body += f"""
- {lease.get("lease_id")}: {lease.get("action")}
  Lease Start Date: {lease.get("lease_start_date")}
  Old End Date: {lease.get("old_end_date")}
  Last Renewed: {lease.get("last_renewed_date")}
  New End Date: {lease.get("new_end_date")}
  Renewal Period: {lease.get("renewal_weeks", "N/A")} weeks
"""

        if renewal_summary.get("error_details"):
            text_body += "\nErrors:\n"
            for error in renewal_summary["error_details"]:
                text_body += f"- {error.get('lease_id')}: {error.get('error')}\n"

        # Log the plain text version as well
        logger.info("\n" + "=" * 80)
        logger.info("ADMIN NOTIFICATION EMAIL - PLAIN TEXT CONTENT")
        logger.info("=" * 80)
        logger.info(text_body)
        logger.info("=" * 80 + "\n")

        # Log email configuration status
        logger.info("Email Configuration Status:")
        logger.info(f"  Admin Email: {settings.aws_admin_email or 'NOT CONFIGURED'}")
        logger.info(
            f"  Sender Email: {settings.aws_ses_sender_email or 'NOT CONFIGURED'}"
        )
        logger.info(f"  AWS Region: {settings.aws_region or 'NOT CONFIGURED'}")
        logger.info(
            f"  Configuration Set: {settings.aws_ses_configuration_set or 'NOT CONFIGURED'}"
        )
        logger.info(f"  S3 Bucket: {settings.s3_bucket_name or 'NOT CONFIGURED'}")
        logger.info(
            f"  Template Key: {settings.admin_renewal_notification_template_key or 'NOT CONFIGURED'}"
        )

        # Send email
        return send_email_via_ses(
            to_email=settings.aws_admin_email,
            subject=subject,
            body_html=html_body,
            body_text=text_body,
            sender_email=settings.aws_ses_sender_email,
            configuration_set=settings.aws_ses_configuration_set,
        )

    except Exception as e:
        logger.error(f"Error sending admin notification: {str(e)}")
        return False


def renew_lease(db: Session, lease: Lease, renewal_date) -> Dict[str, Any]:
    """
    Renew a lease based on its type and return renewal details.

    Args:
        db: Database session
        lease: The lease to renew
        renewal_date: The date on which renewal is happening

    Returns:
        Dictionary with renewal details
    """
    lease_type = lease.lease_type if lease.lease_type else ""

    # Get duration in weeks from the lease (typically 26 weeks for 6 months)
    duration_weeks = lease.duration_in_weeks or 26

    # Calculate new term dates based on weeks
    # Note: We keep lease_start_date unchanged and only update last_renewed_date and lease_end_date
    new_end_date = renewal_date + timedelta(weeks=duration_weeks)

    result = {
        "lease_id": lease.lease_id,
        "lease_type": lease_type,
        "renewal_weeks": duration_weeks,
        "lease_start_date": lease.lease_start_date.isoformat()
        if lease.lease_start_date
        else None,
        "old_end_date": lease.lease_end_date.isoformat()
        if lease.lease_end_date
        else None,
        "last_renewed_date": renewal_date.isoformat(),
        "new_end_date": new_end_date.isoformat(),
        "action": None,
    }

    # Initialize segment tracking if not set (for all lease types)
    if lease.current_segment is None:
        lease.current_segment = 1

    # Set total_segments based on lease type (always enforce correct value)
    if lease_type == LeaseType.DOV.value:
        # For DOV leases, set total segments to 8 (4 years at 6 months each) if not already set
        if lease.total_segments is None:
            lease.total_segments = DOV_DEFAULT_TOTAL_SEGMENTS
    else:
        # For all other lease types, explicitly set to None (no segment limit)
        lease.total_segments = None

    if lease_type == LeaseType.DOV.value:
        current_segment = lease.current_segment
        total_segments = lease.total_segments

        if current_segment < total_segments:
            lease.current_segment = current_segment + 1
            lease.last_renewed_date = renewal_date
            lease.lease_end_date = new_end_date

            if lease.current_segment >= total_segments:
                lease.is_auto_renewed = False
                result["action"] = (
                    f"Renewed to segment {lease.current_segment}/{total_segments} - FINAL SEGMENT, auto_renew disabled"
                )
            else:
                result["action"] = (
                    f"Renewed to segment {lease.current_segment}/{total_segments}"
                )

            result["new_segment"] = lease.current_segment
            result["total_segments"] = total_segments
        else:
            lease.is_auto_renewed = False
            result["action"] = (
                "Already at final segment, auto_renew disabled, no renewal"
            )
            result["new_segment"] = current_segment
            result["total_segments"] = total_segments

    elif lease_type in [
        LeaseType.LONG_TERM.value,
        LeaseType.MEDALLION.value,
        LeaseType.SHIFT.value,
        LeaseType.SHORT_TERM.value,
    ]:
        # Increment segment for all lease types
        lease.current_segment = lease.current_segment + 1
        lease.last_renewed_date = renewal_date
        lease.lease_end_date = new_end_date
        result["action"] = (
            f"Renewed for {duration_weeks} weeks - Segment {lease.current_segment}"
        )
        result["new_segment"] = lease.current_segment
        result["total_segments"] = (
            lease.total_segments
        )  # Will be None for non-DOV leases

    else:
        result["action"] = f"Unknown lease type: {lease_type}, no renewal performed"
        return result

    # Flush the lease changes first
    db.flush()
    db.refresh(lease)

    # Create audit trail for lease renewal
    try:
        audit_description = f"Lease auto-renewed. New end date: {result['new_end_date']}"
        audit_trail_service.create_audit_trail(
            db=db,
            description=audit_description,
            case=None,
            user=None,  # Automated renewal, no user
            meta_data={
                "lease_id": lease.id,
            },
            audit_type=AuditTrailType.AUTOMATED,
        )
        logger.info(f"Created audit trail for lease renewal: {lease.lease_id}")
    except Exception as e:
        logger.error(f"Error creating audit trail for lease {lease.lease_id}: {str(e)}")
        # Don't fail the renewal if audit trail creation fails

    # Create new lease schedule for the renewed lease
    try:
        configs = (
            db.query(LeaseConfiguration)
            .filter(LeaseConfiguration.lease_id == lease.id)
            .all()
        )

        def get_config_value(key: str) -> float:
            config = next((c for c in configs if c.lease_breakup_type == key), None)
            return float(config.lease_limit) if config and config.lease_limit else 0.0

        if lease_type == LeaseType.DOV.value:
            vehicle_weekly_amount = get_config_value("total_vehicle_lease")
            medallion_weekly_amount = get_config_value("total_medallion_lease_payment")
        elif lease_type == LeaseType.MEDALLION.value:
            vehicle_weekly_amount = 0.0
            medallion_weekly_amount = get_config_value("total_medallion_lease_payment")
        elif lease_type in [LeaseType.LONG_TERM.value, LeaseType.SHIFT.value]:
            vehicle_weekly_amount = 0.0
            medallion_weekly_amount = get_config_value("total_medallion_lease_payment")
        else:
            vehicle_weekly_amount = 0.0
            medallion_weekly_amount = 0.0

        lease_service.create_or_update_lease_schedule(
            db=db,
            lease=lease,
            vehicle_weekly_amount=vehicle_weekly_amount,
            medallion_weekly_amount=medallion_weekly_amount,
            override_start_date=lease.last_renewed_date,  # Use renewal date as start for new schedule
        )
        logger.info(f"Created new lease schedule for renewed lease: {lease.lease_id}")
    except Exception as e:
        logger.error(
            f"Error creating lease schedule for lease {lease.lease_id}: {str(e)}"
        )
        result["schedule_error"] = str(e)

    return result


def get_total_lease_amount(db: Session, lease: Lease) -> float:
    """
    Calculate total lease amount for the renewal period from lease configurations.

    Args:
        db: Database session
        lease: The lease object

    Returns:
        Total lease amount for the renewal period
    """
    configs = (
        db.query(LeaseConfiguration)
        .filter(LeaseConfiguration.lease_id == lease.id)
        .all()
    )

    def get_config_value(key: str) -> float:
        config = next((c for c in configs if c.lease_breakup_type == key), None)
        return float(config.lease_limit) if config and config.lease_limit else 0.0

    lease_type = lease.lease_type if lease.lease_type else ""

    if lease_type == LeaseType.DOV.value:
        total_weekly = get_config_value("total_vehicle_lease") + get_config_value(
            "total_medallion_lease_payment"
        )
    elif lease_type == LeaseType.MEDALLION.value:
        total_weekly = get_config_value("total_medallion_lease_payment")
    elif lease_type in [LeaseType.LONG_TERM.value, LeaseType.SHIFT.value]:
        total_weekly = get_config_value("total_medallion_lease_payment")
    else:
        total_weekly = (
            get_config_value("total_vehicle_lease")
            or get_config_value("total_medallion_lease_payment")
            or 0.0
        )

    duration_weeks = lease.duration_in_weeks or 26
    total_amount = total_weekly * duration_weeks

    return total_amount


def send_renewal_reminders(
    db: Session,
    lease: Lease,
    days_until_expiry: int,
    email_template: str,
    sms_template: str,
) -> Dict[str, Any]:
    """
    Send renewal reminders to all drivers associated with a lease.

    Args:
        db: Database session
        lease: The lease that's expiring
        days_until_expiry: Number of days until lease expires
        email_template: Email template content
        sms_template: SMS template content

    Returns:
        Dictionary with reminder sending results
    """
    result = {
        "lease_id": lease.lease_id,
        "lease_type": lease.lease_type,
        "expiry_date": lease.lease_end_date.isoformat()
        if lease.lease_end_date
        else None,
        "days_until_expiry": days_until_expiry,
        "emails_sent": 0,
        "sms_sent": 0,
        "errors": [],
        "email_body": [],
        "sms_body": [],
    }

    # Get all active drivers for this lease
    lease_drivers = (
        db.query(LeaseDriver)
        .filter(
            and_(
                LeaseDriver.lease_id == lease.id,
                LeaseDriver.date_terminated.is_(None),  # Active drivers only
            )
        )
        .all()
    )

    if not lease_drivers:
        result["errors"].append("No active drivers found for this lease")
        return result

    # Calculate total lease amount
    total_lease_amount = get_total_lease_amount(db, lease)

    # Prepare template data
    template_data = {
        "lease_id": lease.lease_id or "",
        "lease_type": lease.lease_type or "",
        "expiry_date": lease.lease_end_date.strftime("%B %d, %Y")
        if lease.lease_end_date
        else "",
        "days_until_expiry": str(days_until_expiry),
        "medallion_number": lease.medallion.medallion_number if lease.medallion else "",
        "vehicle_vin": lease.vehicle.vin if lease.vehicle else "",
        "total_lease_amount": f"{total_lease_amount:,.2f}",
    }

    for lease_driver in lease_drivers:
        try:
            driver = lease_driver.driver
            if not driver:
                result["errors"].append(
                    f"Driver not found for lease_driver {lease_driver.id}"
                )
                continue

            # Add driver-specific data to template
            driver_data = {
                **template_data,
                "driver_name": f"{driver.first_name or ''} {driver.last_name or ''}".strip(),
                "driver_id": driver.driver_id or "",
            }

            # Send email if driver has email address
            if driver.email_address:
                email_subject = f"Lease Renewal Reminder - {lease.lease_id}"
                email_body_html = render_template(email_template, driver_data)
                email_body_text = email_body_html

                logger.info(f"\n{'=' * 80}")
                logger.info(f"EMAIL CONTENT FOR DRIVER {driver.driver_id}:")
                logger.info(f"{'=' * 80}")
                logger.info(f"To: {driver.email_address}")
                logger.info(f"Subject: {email_subject}")
                logger.info(f"Body:\n{email_body_html}")
                logger.info(f"{'=' * 80}\n")
                result["email_body"].append(
                    {"email_subject": email_subject, "email_body": email_body_html}
                )
                if send_email_via_ses(
                    to_email=driver.email_address,
                    subject=email_subject,
                    body_html=email_body_html,
                    body_text=email_body_text,
                    sender_email=settings.aws_ses_sender_email,
                    configuration_set=settings.aws_ses_configuration_set,
                ):
                    result["emails_sent"] += 1
            else:
                result["errors"].append(
                    f"No email address for driver {driver.driver_id}"
                )

            # Send SMS if driver has phone number
            phone = normalize_phone_number(driver.phone_number_1)
            if phone:
                sms_body = render_template(sms_template, driver_data)

                logger.info(f"\n{'=' * 80}")
                logger.info(f"SMS CONTENT FOR DRIVER {driver.driver_id}:")
                logger.info(f"{'=' * 80}")
                logger.info(f"To: {phone}")
                logger.info(f"Message:\n{sms_body}")
                logger.info(f"{'=' * 80}\n")

                result["sms_body"].append(sms_body)
                if send_sms_via_sns(
                    phone_number=phone,
                    message=sms_body,
                    sender_id=settings.aws_sns_sender_id,
                ):
                    result["sms_sent"] += 1
            else:
                result["errors"].append(
                    f"No valid phone number for driver {driver.driver_id}"
                )

        except Exception as e:
            error_msg = (
                f"Error sending reminders for driver {lease_driver.driver_id}: {str(e)}"
            )
            logger.error(error_msg)
            result["errors"].append(error_msg)

    return result


def process_auto_renewals(db: Session, renewal_date) -> Dict[str, Any]:
    """
    Process auto-renewals for leases expiring on the given date.

    Args:
        db: Database session
        renewal_date: Date to process renewals for

    Returns:
        Dictionary with renewal results
    """
    logger.info(f"Processing auto-renewals for date: {renewal_date}")

    # Find all active leases with auto-renewal enabled that are expiring on the renewal date
    leases = (
        db.query(Lease)
        .filter(
            and_(
                Lease.is_auto_renewed == True,
                Lease.lease_status == LeaseStatus.ACTIVE.value,
                Lease.lease_end_date == renewal_date,
            )
        )
        .all()
    )

    logger.info(f"Found {len(leases)} leases eligible for auto-renewal")

    renewed_leases = []
    errors = []

    for lease in leases:
        try:
            logger.info(
                f"Processing lease: {lease.lease_id} (Type: {lease.lease_type})"
            )
            result = renew_lease(db, lease, renewal_date)
            renewed_leases.append(result)
            logger.info(
                f"Successfully renewed lease: {lease.lease_id} - {result['action']}"
            )
        except Exception as e:
            error_msg = f"Error renewing lease {lease.lease_id}: {str(e)}"
            logger.error(error_msg)
            errors.append({"lease_id": lease.lease_id, "error": str(e)})

    # Prepare response
    response = {
        "success": True,
        "renewal_date": renewal_date.isoformat(),
        "total_leases_found": len(leases),
        "leases_renewed": len(renewed_leases),
        "errors": len(errors),
        "renewed_leases": renewed_leases,
        "error_details": errors,
    }

    # Always send and log admin notification (even if there are no renewals)
    logger.info("Sending admin notification...")
    notification_sent = send_admin_notification(response)
    response["admin_notification_sent"] = notification_sent

    return response


def process_renewal_reminders(
    db: Session, check_date, reminder_days: int = 30
) -> Dict[str, Any]:
    """
    Process renewal reminders for leases expiring in the given number of days.

    Args:
        db: Database session
        check_date: Date to check from
        reminder_days: Number of days before expiry to send reminders

    Returns:
        Dictionary with reminder results
    """
    logger.info(f"Reminder configuration: {reminder_days} days before expiry")
    logger.info(f"Check date: {check_date}")

    # Fetch templates from S3
    bucket_name = settings.s3_bucket_name
    if not bucket_name:
        raise ValueError("S3 bucket name not configured")

    email_template = get_s3_template(
        bucket_name, settings.lease_renewal_email_template_key
    )
    sms_template = get_s3_template(bucket_name, settings.lease_renewal_sms_template_key)

    if not email_template or not sms_template:
        raise ValueError("Failed to fetch templates from S3")

    logger.info("Templates fetched successfully from S3")

    # Calculate the target expiry date
    target_expiry_date = check_date + timedelta(days=reminder_days)

    # Find all active leases expiring around the target date
    date_window_start = target_expiry_date
    date_window_end = target_expiry_date + timedelta(days=1)

    leases = (
        db.query(Lease)
        .filter(
            and_(
                Lease.lease_status == LeaseStatus.ACTIVE.value,
                Lease.lease_end_date >= date_window_start,
                Lease.lease_end_date < date_window_end,
            )
        )
        .all()
    )

    logger.info(f"Found {len(leases)} leases expiring around {target_expiry_date}")

    reminder_results = []
    total_emails_sent = 0
    total_sms_sent = 0
    errors = []
    email_body = []
    sms_body = []

    for lease in leases:
        try:
            logger.info(
                f"Processing lease: {lease.lease_id} (Expires: {lease.lease_end_date})"
            )
            result = send_renewal_reminders(
                db=db,
                lease=lease,
                days_until_expiry=reminder_days,
                email_template=email_template,
                sms_template=sms_template,
            )
            reminder_results.append(result)
            total_emails_sent += result["emails_sent"]
            total_sms_sent += result["sms_sent"]

            email_body.append(result["email_body"])
            sms_body.append(result["sms_body"])
            if result["errors"]:
                errors.extend(result["errors"])

            logger.info(
                f"Lease {lease.lease_id}: {result['emails_sent']} emails, {result['sms_sent']} SMS sent"
            )
        except Exception as e:
            error_msg = f"Error processing lease {lease.lease_id}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)

    return {
        "success": True,
        "check_date": check_date.isoformat(),
        "reminder_days": reminder_days,
        "target_expiry_date": target_expiry_date.isoformat(),
        "total_leases_processed": len(leases),
        "total_emails_sent": total_emails_sent,
        "total_sms_sent": total_sms_sent,
        "reminder_results": reminder_results,
        "errors": errors,
        "templates": {
            "email_template_raw": email_template,
            "sms_template_raw": sms_template,
            "email_template_sample": email_body,
            "sms_template_sample": sms_body,
        },
    }
