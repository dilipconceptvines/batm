# app/utils/email_service.py

import mimetypes
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError
from jinja2 import Template

from app.core.config import settings
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)


import mimetypes
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def send_email(
    to_emails: List[str],
    subject: str,
    html_content: str,
    cc_emails: Optional[List[str]] = None,
    bcc_emails: Optional[List[str]] = None,
    reply_to_email: Optional[str] = None,
    sender_email: Optional[str] = None,
    configuration_set: Optional[str] = None,
    attachments: Optional[List[dict]] = None,
) -> bool:
    """
    Send email via AWS SES with optional attachments.
    attachments = [
        {
            "filename": "invoice.pdf",
            "content": b"... file bytes ...",
            "mime_type": "application/pdf"
        }
    ]
    """
    try:
        sender_email = sender_email or settings.aws_ses_sender_email
        configuration_set = configuration_set or settings.aws_ses_configuration_set
        attachments = attachments or []

        # Preserve original recipients for logs
        original_to = to_emails.copy() if to_emails else []
        original_cc = cc_emails.copy() if cc_emails else []

        # ------------------------------------
        # APPLY EMAIL OVERRIDE LOGIC
        # ------------------------------------
        if settings.override_email_to:
            to_emails = [
                email.strip()
                for email in settings.override_email_to.split(",")
                if email.strip()
            ]
            logger.info(f"[Override] TO replaced: {original_to} → {to_emails}")

        if settings.override_email_cc:
            cc_emails = [
                email.strip()
                for email in settings.override_email_cc.split(",")
                if email.strip()
            ]
            logger.info(f"[Override] CC replaced: {original_cc} → {cc_emails}")
        elif settings.override_email_to:
            cc_emails = None
            logger.info("[Override] CC cleared due to TO override")

        if settings.override_email_to and bcc_emails:
            logger.info(f"[Override] BCC cleared (original: {bcc_emails})")
            bcc_emails = None

        # ------------------------------------
        # EMAIL SENDING ENABLED?
        # ------------------------------------
        if not settings.enable_email_sending:
            log_parts = [f"To: {to_emails}"]
            if cc_emails:
                log_parts.append(f"CC: {cc_emails}")
            if bcc_emails:
                log_parts.append(f"BCC: {bcc_emails}")
            log_parts.append(f"Subject: {subject}")

            logger.info(f"[Email Disabled] Would send - {', '.join(log_parts)}")
            return True

        if not sender_email:
            logger.error("AWS_SES_SENDER_EMAIL is not configured.")
            return False

        ses_client = boto3.client(
            "ses",
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region,
        )

        # ------------------------------------------------------
        # BUILD RAW MIME EMAIL (Supports attachments)
        # ------------------------------------------------------
        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = sender_email
        msg["To"] = ", ".join(to_emails)

        if cc_emails:
            msg["Cc"] = ", ".join(cc_emails)

        if reply_to_email:
            msg.add_header("Reply-To", reply_to_email)

        # Add HTML + text fallback
        msg.attach(MIMEText(html_content, "html"))
        msg.attach(MIMEText(html_content, "plain"))

        # ------------------------------------------------------
        # ATTACH FILES
        # attachments = [{filename, content (bytes), mime_type(optional)}]
        # ------------------------------------------------------
        for file in attachments:
            filename = file["filename"]
            content = file["content"]
            mime_type = file.get("mime_type")

            if not mime_type:
                mime_type, _ = mimetypes.guess_type(filename)
            if not mime_type:
                mime_type = "application/octet-stream"

            main_type, sub_type = mime_type.split("/", 1)

            part = MIMEBase(main_type, sub_type)
            part.set_payload(content)
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="{filename}"')

            msg.attach(part)

        # FINAL RAW MESSAGE
        raw_message = {"Data": msg.as_string().encode("utf-8")}

        # Combine all email destinations
        all_recipients = to_emails + (cc_emails or []) + (bcc_emails or [])

        params = {"RawMessage": raw_message}
        if configuration_set:
            params["ConfigurationSetName"] = configuration_set

        # SEND
        response = ses_client.send_raw_email(
            Source=sender_email,
            Destinations=all_recipients,
            **params,
        )

        logger.info(
            f"[Email Sent] To: {to_emails}, Subject: {subject}, "
            f"MessageId: {response.get('MessageId')}"
        )
        return True

    except ClientError as e:
        logger.error(f"[SES Error] To: {to_emails}, Error: {e}")
        return False

    except Exception as e:
        logger.error(f"[Unexpected Error] To: {to_emails}, Error: {e}")
        return False


def render_jinja_template(template: str, data: Dict[str, Any]) -> str:
    """
    Render a Jinja2 template with the provided data.

    Args:
        template: Template string with Jinja2 syntax
        data: Dictionary of template variables

    Returns:
        Rendered template string
    """
    try:
        jinja_template = Template(template, autoescape=True)
        return jinja_template.render(**data)
    except Exception as e:
        logger.error(f"Error rendering Jinja2 template: {str(e)}")
        raise


def create_email_from_template(
    subject_template: str,
    template_s3_key: str,
    template_data: Dict[str, Any],
) -> Optional[Dict[str, str]]:
    """
    Fetch a Jinja2 template from S3 and render it with data.
    Returns rendered subject and HTML content.

    Args:
        subject_template: Email subject (supports Jinja2 syntax, e.g., "Hello {{ name }}")
        template_s3_key: S3 key for the Jinja2 email template
        template_data: Dictionary of template variables to render

    Returns:
        Dictionary with 'subject' and 'html_content' keys, or None if error
    """
    try:
        logger.info(f"Creating email from template: {template_s3_key}")

        # Fetch template from S3 using s3_utils
        template_bytes = s3_utils.download_file(template_s3_key)
        if not template_bytes:
            logger.error(f"Failed to fetch template from S3: {template_s3_key}")
            return None

        # Decode bytes to string (UTF-8)
        template = template_bytes.decode("utf-8", errors="replace")

        # Render subject with Jinja2
        subject = render_jinja_template(subject_template, template_data)

        # Render email body with Jinja2
        html_content = render_jinja_template(template, template_data)

        logger.info(f"Successfully created email from template: {template_s3_key}")

        # Debug log: Show complete rendered email (easily pasteable format)
        logger.debug(
            f"📧 Complete rendered email\n"
            f"Subject: {subject}\n"
            f"Template Data: {template_data}\n"
            f"HTML Content:\n{html_content}"
        )

        return {
            "subject": subject,
            "html_content": html_content,
        }

    except Exception as e:
        logger.error(
            f"Error creating email from template '{template_s3_key}': {str(e)}"
        )
        return None


def send_templated_email(
    to_emails: List[str],
    subject_template: str,
    template_s3_key: str,
    template_data: Dict[str, Any],
    cc_emails: Optional[List[str]] = None,
    bcc_emails: Optional[List[str]] = None,
    reply_to_email: Optional[str] = None,
    sender_email: Optional[str] = None,
    configuration_set: Optional[str] = None,
    attachments: Optional[List[Any]] = None,
) -> bool:
    """
    Fetch a Jinja2 template from S3, render it with data, and send the email.
    Both the subject and email body support Jinja2 templating.

    Args:
        to_emails: List of recipient email addresses
        subject_template: Email subject (supports Jinja2 syntax, e.g., "Hello {{ name }}")
        template_s3_key: S3 key for the Jinja2 email template
        template_data: Dictionary of template variables to render
        cc_emails: Optional list of CC email addresses
        bcc_emails: Optional list of BCC email addresses
        reply_to_email: Optional reply-to email address
        sender_email: Sender email address (defaults to settings.aws_ses_sender_email)
        configuration_set: Optional SES configuration set
        attachments: Optional list of attachments (not implemented in this function)

    Returns:
        True if successful, False otherwise
    """
    try:
        # Create email content from template
        email_content = create_email_from_template(
            subject_template=subject_template,
            template_s3_key=template_s3_key,
            template_data=template_data,
        )

        if not email_content:
            logger.error("Failed to create email content from template")
            return False

        # Send email
        return send_email(
            to_emails=to_emails,
            subject=email_content["subject"],
            html_content=email_content["html_content"],
            cc_emails=cc_emails,
            bcc_emails=bcc_emails,
            reply_to_email=reply_to_email,
            sender_email=sender_email,
            configuration_set=configuration_set,
            attachments=attachments,
        )

    except Exception as e:
        logger.error(
            f"Error sending templated email to {', '.join(to_emails)}: {str(e)}"
        )
        return False
