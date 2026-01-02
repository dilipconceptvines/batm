# app/utils/sms_service.py

from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError
from jinja2 import Template

from app.core.config import settings
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)


def normalize_phone_number(phone: str) -> Optional[str]:
    """
    Normalize US phone number to E.164 format.

    Args:
        phone: Phone number in various formats

    Returns:
        Phone number in E.164 format (+1XXXXXXXXXX) or None if invalid
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

    logger.warning(f"Invalid phone number format: {phone} (digits: {digits})")
    return None


def send_sms(
    phone_number: str,
    message: str,
    sender_id: Optional[str] = None,
) -> bool:
    """
    Send SMS via AWS SNS.

    Args:
        phone_number: Phone number in E.164 format (e.g., +1234567890, +919876543210)
        message: SMS message content
        sender_id: Optional sender ID (defaults to settings.aws_sns_sender_id)

    Returns:
        True if successful, False otherwise
    """
    try:
        # Ensure phone number is in E.164 format
        if not phone_number.startswith('+'):
            phone_number = f'+{phone_number}'

        # Apply SMS override if configured (do this BEFORE the enable check so we can see it in logs)
        original_phone_number = phone_number
        if settings.override_sms_to:
            phone_number = settings.override_sms_to
            # Ensure override number is also in E.164 format
            if not phone_number.startswith('+'):
                phone_number = f'+{phone_number}'
            logger.info(
                f"SMS override active - Original TO: {original_phone_number} → "
                f"Override TO: {phone_number}"
            )

        # Check if SMS sending is enabled
        if not settings.enable_sms_sending:
            logger.warning(
                f"SMS sending is disabled via ENABLE_SMS_SENDING flag. "
                f"Would have sent to: {phone_number}, Message: {message}"
            )
            return True  # Return True to not break workflows

        sender_id = sender_id or settings.aws_sns_sender_id

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
        logger.info(
            f"SMS sent successfully - To: {phone_number}, MessageId: {response['MessageId']}"
        )
        return True

    except ClientError as e:
        logger.error(f"Failed to send SMS - To: {phone_number}, Error: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error sending SMS - To: {phone_number}, Error: {str(e)}")
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


def create_sms_from_template(
    template_s3_key: str,
    template_data: Dict[str, Any],
) -> Optional[str]:
    """
    Fetch a Jinja2 template from S3 and render it with data.
    Returns rendered SMS content.

    Args:
        template_s3_key: S3 key for the Jinja2 SMS template
        template_data: Dictionary of template variables to render

    Returns:
        Rendered SMS message string, or None if error
    """
    try:
        logger.info(f"Creating SMS from template: {template_s3_key}")

        # Fetch template from S3 using s3_utils
        template_bytes = s3_utils.download_file(template_s3_key)
        if not template_bytes:
            logger.error(f"Failed to fetch template from S3: {template_s3_key}")
            return None

        # Decode bytes to string (UTF-8)
        template = template_bytes.decode("utf-8", errors="replace")

        # Render SMS body with Jinja2
        sms_content = render_jinja_template(template, template_data)

        logger.info(f"Successfully created SMS from template: {template_s3_key}")

        # Debug log: Show complete rendered SMS
        logger.debug(
            "📱 Complete rendered SMS",
            sms_content=sms_content,
            template_data=template_data,
        )

        return sms_content

    except Exception as e:
        logger.error(f"Error creating SMS from template '{template_s3_key}': {str(e)}")
        return None


def send_templated_sms(
    phone_number: str,
    template_s3_key: str,
    template_data: Dict[str, Any],
    sender_id: Optional[str] = None,
) -> bool:
    """
    Fetch a Jinja2 template from S3, render it with data, and send the SMS.

    Args:
        phone_number: Phone number to send SMS to (will be normalized to E.164)
        template_s3_key: S3 key for the Jinja2 SMS template
        template_data: Dictionary of template variables to render
        sender_id: Optional sender ID

    Returns:
        True if successful, False otherwise
    """
    try:
        # Create SMS content from template
        sms_content = create_sms_from_template(
            template_s3_key=template_s3_key,
            template_data=template_data,
        )

        if not sms_content:
            logger.error("Failed to create SMS content from template")
            return False

        # Send SMS
        return send_sms(
            phone_number=phone_number,
            message=sms_content,
            sender_id=sender_id,
        )

    except Exception as e:
        logger.error(f"Error sending templated SMS to {phone_number}: {str(e)}")
        return False
