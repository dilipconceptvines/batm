# app/leases/lease_expiry_service.py

from datetime import datetime
from typing import Any, Dict

import boto3
from sqlalchemy import and_
from sqlalchemy.orm import Session

from app.audit_trail.schemas import AuditTrailType
from app.audit_trail.services import audit_trail_service
from app.core.config import settings
from app.leases.models import Lease
from app.leases.schemas import LeaseStatus
from app.utils.logger import get_logger
from app.vehicles.schemas import VehicleStatus

logger = get_logger(__name__)


def get_s3_template(bucket_name: str, template_key: str) -> str | None:
    """
    Fetch an HTML email template from S3 and return it as a decoded string.

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


def render_admin_expiry_template(template: str, expiry_summary: Dict[str, Any]) -> str:
    """
    Render the admin expiry notification template with expiry and termination data.

    Args:
        template: Template HTML string
        expiry_summary: Dictionary containing expiry and termination results

    Returns:
        Rendered HTML string
    """
    html = template.replace(
        "{{expiry_date}}", expiry_summary.get("expiry_check_date", "N/A")
    )
    html = html.replace(
        "{{total_leases_found}}", str(expiry_summary.get("total_leases_found", 0))
    )
    html = html.replace(
        "{{leases_terminated}}", str(expiry_summary.get("leases_terminated", 0))
    )
    html = html.replace(
        "{{leases_expired}}", str(expiry_summary.get("leases_expired", 0))
    )
    html = html.replace("{{errors}}", str(expiry_summary.get("errors", 0)))
    html = html.replace(
        "{{generation_timestamp}}", datetime.now().strftime("%B %d, %Y at %I:%M %p")
    )

    # Build terminated leases rows
    terminated_leases = expiry_summary.get("terminated_leases", [])
    terminated_rows_html = ""
    for lease in terminated_leases:
        terminated_rows_html += f"""
            <tr>
                <td>{lease.get("lease_id", "N/A")}</td>
                <td>{lease.get("lease_type", "N/A")}</td>
                <td>{lease.get("lease_start_date", "N/A")}</td>
                <td>{lease.get("termination_date", "N/A")}</td>
                <td>{lease.get("termination_reason", "N/A")}</td>
                <td><span class="badge badge-warning">Terminated</span></td>
                <td>{lease.get("days_since_termination", "N/A")} days</td>
            </tr>
        """

    # Build expired leases rows
    expired_leases = expiry_summary.get("expired_leases", [])
    expired_rows_html = ""
    for lease in expired_leases:
        expired_rows_html += f"""
            <tr>
                <td>{lease.get("lease_id", "N/A")}</td>
                <td>{lease.get("lease_type", "N/A")}</td>
                <td>{lease.get("lease_start_date", "N/A")}</td>
                <td>{lease.get("lease_end_date", "N/A")}</td>
                <td>{lease.get("last_renewed_date", "N/A")}</td>
                <td><span class="badge badge-danger">Expired</span></td>
                <td>{lease.get("days_overdue", "N/A")} days</td>
            </tr>
        """

    # Handle conditional rendering
    if terminated_leases or expired_leases:
        html = html.replace("{{terminated_leases_rows}}", terminated_rows_html)
        html = html.replace("{{expired_leases_rows}}", expired_rows_html)
        html = html.replace("{{#if_expired_leases}}", "")
        html = html.replace("{{/if_expired_leases}}", "")
        html = html.replace("{{#if_no_expiries}}", "<!--")
        html = html.replace("{{/if_no_expiries}}", "-->")
    else:
        html = html.replace("{{#if_expired_leases}}", "<!--")
        html = html.replace("{{/if_expired_leases}}", "-->")
        html = html.replace("{{#if_no_expiries}}", "")
        html = html.replace("{{/if_no_expiries}}", "")

    # Build error rows
    error_details = expiry_summary.get("error_details", [])
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
    configuration_set: str | None = None,
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


def send_admin_expiry_notification(expiry_summary: Dict[str, Any]) -> bool:
    """
    Send email notification to admin with lease expiry summary.

    Args:
        expiry_summary: Dictionary containing expiry results

    Returns:
        True if successful, False otherwise
    """
    try:
        # Fetch template from S3
        template_key = getattr(
            settings,
            "admin_expiry_notification_template_key",
            "email_sms_templates/admin_expiry_notification.html",
        )
        template = get_s3_template(settings.s3_bucket_name, template_key)

        if not template:
            error_msg = f"Failed to fetch admin expiry notification template from S3 bucket '{settings.s3_bucket_name}' with key '{template_key}'"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Render template
        html_body = render_admin_expiry_template(template, expiry_summary)

        # Log the complete HTML email content for verification
        logger.info("\n" + "=" * 80)
        logger.info("ADMIN EXPIRY NOTIFICATION EMAIL - HTML CONTENT")
        logger.info("=" * 80)
        logger.info(f"To: {settings.aws_admin_email}")
        logger.info(f"From: {settings.aws_ses_sender_email}")
        logger.info(
            f"Subject: Lease Expiry Summary - {expiry_summary['expiry_check_date']}"
        )
        logger.info("-" * 80)
        logger.info("HTML BODY:")
        logger.info("-" * 80)
        logger.info(html_body)
        logger.info("=" * 80 + "\n")

        # Build email subject
        subject = f"Lease Expiry & Termination Summary - {expiry_summary['expiry_check_date']}"

        # Plain text body
        text_body = f"""
Lease Expiry & Termination Summary

Check Date: {expiry_summary["expiry_check_date"]}
Total Leases Found: {expiry_summary["total_leases_found"]}
Leases Terminated: {expiry_summary.get("leases_terminated", 0)}
Leases Expired: {expiry_summary.get("leases_expired", 0)}
Errors: {expiry_summary["errors"]}
"""

        # Add terminated leases section
        terminated_leases = expiry_summary.get("terminated_leases", [])
        if terminated_leases:
            text_body += "\nTerminated Leases:\n"
            for lease in terminated_leases:
                text_body += f"""
- {lease.get("lease_id")}: Marked as TERMINATED
  Lease Type: {lease.get("lease_type")}
  Lease Start Date: {lease.get("lease_start_date")}
  Termination Date: {lease.get("termination_date")}
  Termination Reason: {lease.get("termination_reason", "N/A")}
  Days Since Termination: {lease.get("days_since_termination", "N/A")} days
"""

        # Add expired leases section
        expired_leases = expiry_summary.get("expired_leases", [])
        if expired_leases:
            text_body += "\nExpired Leases:\n"
            for lease in expired_leases:
                text_body += f"""
- {lease.get("lease_id")}: Marked as EXPIRED
  Lease Type: {lease.get("lease_type")}
  Lease Start Date: {lease.get("lease_start_date")}
  Lease End Date: {lease.get("lease_end_date")}
  Last Renewed: {lease.get("last_renewed_date")}
  Days Overdue: {lease.get("days_overdue", "N/A")} days
"""

        if expiry_summary.get("error_details"):
            text_body += "\nErrors:\n"
            for error in expiry_summary["error_details"]:
                text_body += f"- {error.get('lease_id')}: {error.get('error')}\n"

        # Log the plain text version as well
        logger.info("\n" + "=" * 80)
        logger.info("ADMIN EXPIRY NOTIFICATION EMAIL - PLAIN TEXT CONTENT")
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
        logger.info(f"  Template Key: {template_key or 'NOT CONFIGURED'}")

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
        logger.error(f"Error sending admin expiry notification: {str(e)}")
        return False


def mark_lease_as_terminated(db: Session, lease: Lease, check_date) -> Dict[str, Any]:
    """
    Mark a lease as terminated and return termination details.

    Args:
        db: Database session
        lease: The lease to mark as terminated
        check_date: The date on which termination check is happening

    Returns:
        Dictionary with termination details
    """
    lease_type = lease.lease_type if lease.lease_type else ""

    # Calculate days since termination
    days_since_termination = (
        (check_date - lease.termination_date).days if lease.termination_date else 0
    )

    result = {
        "lease_id": lease.lease_id,
        "lease_type": lease_type,
        "lease_start_date": lease.lease_start_date.isoformat()
        if lease.lease_start_date
        else None,
        "lease_end_date": lease.lease_end_date.isoformat()
        if lease.lease_end_date
        else None,
        "termination_date": lease.termination_date.isoformat()
        if lease.termination_date
        else None,
        "termination_reason": lease.termination_reason,
        "days_since_termination": days_since_termination,
        "previous_status": lease.lease_status,
        "new_status": LeaseStatus.TERMINATED.value,
        "action_type": "terminated",
    }

    # Update lease status to TERMINATED
    lease.lease_status = LeaseStatus.TERMINATED.value

    # Make vehicle available (set to HACKED_UP status)
    if lease.vehicle:
        lease.vehicle.vehicle_status = VehicleStatus.HACKED_UP.value
        logger.info(
            f"Vehicle {lease.vehicle.vin} for lease {lease.lease_id} set to available (HACKED_UP)"
        )

    logger.info(
        f"Lease {lease.lease_id} marked as TERMINATED (was {result['previous_status']})"
    )

    # Create audit trail for lease termination
    try:
        audit_description = f"Lease terminated. Termination date: {result['termination_date']}"
        audit_trail_service.create_audit_trail(
            db=db,
            description=audit_description,
            case=None,
            user=None,  # Automated termination, no user
            meta_data={
                "lease_id": lease.id,
            },
            audit_type=AuditTrailType.AUTOMATED,
        )
        logger.info(f"Created audit trail for lease termination: {lease.lease_id}")
    except Exception as e:
        logger.error(f"Error creating audit trail for lease {lease.lease_id}: {str(e)}")
        # Don't fail the termination if audit trail creation fails

    return result


def mark_lease_as_expired(db: Session, lease: Lease, check_date) -> Dict[str, Any]:
    """
    Mark a lease as expired and return expiry details.

    Args:
        db: Database session
        lease: The lease to mark as expired
        check_date: The date on which expiry check is happening

    Returns:
        Dictionary with expiry details
    """
    lease_type = lease.lease_type if lease.lease_type else ""

    # Calculate days overdue
    days_overdue = (
        (check_date - lease.lease_end_date).days if lease.lease_end_date else 0
    )

    result = {
        "lease_id": lease.lease_id,
        "lease_type": lease_type,
        "lease_start_date": lease.lease_start_date.isoformat()
        if lease.lease_start_date
        else None,
        "lease_end_date": lease.lease_end_date.isoformat()
        if lease.lease_end_date
        else None,
        "last_renewed_date": lease.last_renewed_date.isoformat()
        if lease.last_renewed_date
        else None,
        "days_overdue": days_overdue,
        "previous_status": lease.lease_status,
        "new_status": LeaseStatus.EXPIRED.value,
        "action_type": "expired",
    }

    # Update lease status to EXPIRED
    lease.lease_status = LeaseStatus.EXPIRED.value

    # Make vehicle available (set to HACKED_UP status)
    if lease.vehicle:
        lease.vehicle.vehicle_status = VehicleStatus.HACKED_UP.value
        logger.info(
            f"Vehicle {lease.vehicle.vin} for lease {lease.lease_id} set to available (HACKED_UP)"
        )

    logger.info(
        f"Lease {lease.lease_id} marked as EXPIRED (was {result['previous_status']})"
    )

    # Create audit trail for lease expiry
    try:
        audit_description = f"Lease expired. End date: {result['lease_end_date']}"
        audit_trail_service.create_audit_trail(
            db=db,
            description=audit_description,
            case=None,
            user=None,  # Automated expiry, no user
            meta_data={
                "lease_id": lease.id,
            },
            audit_type=AuditTrailType.AUTOMATED,
        )
        logger.info(f"Created audit trail for lease expiry: {lease.lease_id}")
    except Exception as e:
        logger.error(f"Error creating audit trail for lease {lease.lease_id}: {str(e)}")
        # Don't fail the expiry if audit trail creation fails

    return result


def process_lease_expiries(db: Session, check_date) -> Dict[str, Any]:
    """
    Process lease expiries and terminations.

    Priority:
    1. Check for leases with termination_date < check_date → mark as TERMINATED
    2. Check for leases with lease_end_date < check_date and is_auto_renewed = False → mark as EXPIRED

    Args:
        db: Database session
        check_date: Date to check expiries for

    Returns:
        Dictionary with expiry and termination results
    """
    logger.info(f"Processing lease expiries and terminations for date: {check_date}")

    # Find all active leases that need to be checked
    # Either they have a termination_date or they're past their end date
    from sqlalchemy import or_

    leases = (
        db.query(Lease)
        .filter(
            and_(
                Lease.lease_status == LeaseStatus.ACTIVE.value,
                or_(
                    # Leases with termination date
                    Lease.termination_date < check_date,
                    # Leases past end date with no auto-renewal
                    and_(
                        Lease.lease_end_date < check_date,
                        Lease.is_auto_renewed == False,
                    ),
                ),
            )
        )
        .all()
    )

    logger.info(f"Found {len(leases)} leases eligible for processing")

    terminated_leases = []
    expired_leases = []
    errors = []

    for lease in leases:
        try:
            # Priority 1: Check termination_date first
            if lease.termination_date and lease.termination_date < check_date:
                logger.info(
                    f"Processing lease for termination: {lease.lease_id} (Termination Date: {lease.termination_date})"
                )
                result = mark_lease_as_terminated(db, lease, check_date)
                terminated_leases.append(result)
                logger.info(
                    f"Successfully marked lease as terminated: {lease.lease_id}"
                )
            # Priority 2: Check lease_end_date for expiry
            elif lease.lease_end_date < check_date and not lease.is_auto_renewed:
                logger.info(
                    f"Processing lease for expiry: {lease.lease_id} (End Date: {lease.lease_end_date})"
                )
                result = mark_lease_as_expired(db, lease, check_date)
                expired_leases.append(result)
                logger.info(
                    f"Successfully marked lease as expired: {lease.lease_id} ({result['days_overdue']} days overdue)"
                )
        except Exception as e:
            error_msg = f"Error processing lease {lease.lease_id}: {str(e)}"
            logger.error(error_msg)
            errors.append({"lease_id": lease.lease_id, "error": str(e)})

    # Prepare response
    response = {
        "success": True,
        "expiry_check_date": check_date.isoformat(),
        "total_leases_found": len(leases),
        "leases_terminated": len(terminated_leases),
        "leases_expired": len(expired_leases),
        "errors": len(errors),
        "terminated_leases": terminated_leases,
        "expired_leases": expired_leases,
        "error_details": errors,
    }

    # Always send and log admin notification (even if there are no expiries)
    logger.info("Sending admin expiry notification...")
    notification_sent = send_admin_expiry_notification(response)
    response["admin_notification_sent"] = notification_sent

    return response
