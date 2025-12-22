# app/utils/email_service.py

import asyncio
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError
from jinja2 import Environment, FileSystemLoader

from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class EmailService:
    """
    A centralized service for sending emails via Amazon SES,
    with support for Jinja2 templating and attachments.
    """
    def __init__(self):
        self.ses_client = boto3.client(
            "ses",
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region,
        )
        self.sender = settings.aws_ses_sender_email

        # Initialize Jinja2 environment for email templates
        template_path = Path(__file__).parent.parent / "templates" / "emails"
        self.jinja_env = Environment(loader=FileSystemLoader(template_path), autoescape=True)

    def _render_template(self, template_name: str, context: Dict[str, Any]) -> str:
        """Render an HTML email template using Jinja2."""
        try:
            template = self.jinja_env.get_template(template_name)
            return template.render(context)
        except Exception as e:
            logger.error("Error rendering email template", template=template_name, error_message=str(e))
            raise

    async def _send_email_async(
        self,
        to_emails: List[str],
        subject: str,
        html_body: str,
        attachments: Optional[List[Dict[str, Any]]] = None,
        cc_emails: Optional[List[str]] = None,
        bcc_emails: Optional[List[str]] = None,
    ):
        """
        Constructs and sends a multipart email using a synchronous boto3 call
        in an asyncio-safe manner.
        """
        msg = MIMEMultipart("mixed")
        msg["Subject"] = subject
        msg["From"] = self.sender
        msg["To"] = ", ".join(to_emails)
        if cc_emails:
            msg["Cc"] = ", ".join(cc_emails)

        # --- Attach the HTML body ---
        msg_body = MIMEMultipart("alternative")
        html_part = MIMEText(html_body, "html")
        msg_body.attach(html_part)
        msg.attach(msg_body)

        # --- Attach files ---
        if attachments:
            for attachment in attachments:
                part = MIMEApplication(attachment["data"])
                part.add_header(
                    "Content-Disposition",
                    "attachment",
                    filename=attachment["filename"],
                )
                msg.attach(part)

        all_recipients = to_emails + (cc_emails or []) + (bcc_emails or [])

        try:
            # Run the synchronous Boto3 call in a separate thread
            await asyncio.to_thread(
                self.ses_client.send_raw_email,
                Source=self.sender,
                Destinations=all_recipients,
                RawMessage={"Data": msg.as_string()},
            )
            logger.info("Email sent successfully", subject=subject, to=f"{', '.join(all_recipients)}")
        except ClientError as e:
            logger.error("Failed to send email", subject=subject, error_message=str(e))
            raise
        except Exception as e:
            logger.error("Unexpected error sending email", subject=subject, error_message=str(e))
            raise

    async def send_templated_email(
        self,
        *,
        to_emails: List[str],
        subject: str,
        template_name: str,
        context: Dict[str, Any],
        attachments: Optional[List[Dict[str, Any]]] = None,
        cc_emails: Optional[List[str]] = None,
        bcc_emails: Optional[List[str]] = None,
    ):
        """
        Renders an email from a template, and sends it with optional attachments.

        Args:
            to_emails (List[str]): List of recipient email addresses.
            subject (str): Subject of the email.
            template_name (str): Name of the Jinja2 template file.
            context (Dict[str, Any]): Context variables for rendering the template.
            attachments (Optional[List[Dict[str, Any]]]): List of attachments, each a dict with 'filename' and 'data'.
            cc_emails (Optional[List[str]]): List of CC email addresses.
            bcc_emails (Optional[List[str]]): List of BCC email addresses.
        """
        if not self.sender:
            logger.error("Email sending is disabled: AWS_SES_SENDER_EMAIL is not configured")
            return
        
        html_body = self._render_template(template_name, context)
        await self._send_email_async(
            to_emails=to_emails,
            subject=subject,
            html_body=html_body,
            attachments=attachments,
            cc_emails=cc_emails,
            bcc_emails=bcc_emails,
        )


# Create a single, reusable instance of the service
email_service = EmailService()
