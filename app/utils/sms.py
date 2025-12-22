### app/utils/sms.py

import boto3
from botocore.exceptions import ClientError
from typing import Optional
from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

class SMSService:
    def __init__(self):
        self.client = boto3.client(
            'sns',
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region
        )
        self.sender_id = settings.aws_sns_sender_id

    def send_sms(
        self,
        phone_number: str,
        message: str,
        message_attributes: Optional[dict] = None
    ) -> bool:
        """
        Send SMS using AWS SNS
        
        Args:
            phone_number: Recipient phone number (E.164 format)
            message: SMS message content
            message_attributes: Optional SNS message attributes
            
        Returns:
            bool: True if SMS sent successfully, False otherwise
        """
        try:
            # Ensure phone number is in E.164 format
            if not phone_number.startswith('+'):
                phone_number = f'+{phone_number}'

            # Prepare message attributes
            attributes = {
                'AWS.SNS.SMS.SendererId': {
                    'DataType': 'String',
                    'StringValue': self.sender_id
                },
                'AWS.SNS.SMS.SMSType': {
                    'DataType': 'String',
                    'StringValue': 'Transactional'
                }
            }
            
            # Add custom message attributes if provided
            if message_attributes:
                attributes.update(message_attributes)

            # Send SMS
            response = self.client.publish(
                PhoneNumber=phone_number,
                Message=message,
                MessageAttributes=attributes
            )

            logger.info(
                "SMS sent successfully to %s. MessageId: %s",
                phone_number, response['MessageId']
            )
            return True

        except ClientError as e:
            logger.error(
                "Failed to send SMS to %s. Error: %s",
                phone_number, str(e)
            )
            return False

        except Exception as e:
            logger.error(
                "Unexpected error sending SMS to %s. Error: %s",
                phone_number, str(e)
            )
            return False

    def send_bulk_sms(
        self,
        phone_numbers: list[str],
        message: str,
        message_attributes: Optional[dict] = None
    ) -> dict:
        """
        Send bulk SMS using AWS SNS
        
        Args:
            phone_numbers: List of recipient phone numbers (E.164 format)
            message: SMS message content
            message_attributes: Optional SNS message attributes
            
        Returns:
            dict: Dictionary with phone numbers as keys and success status as values
        """
        results = {}
        for phone_number in phone_numbers:
            success = self.send_sms(
                phone_number=phone_number,
                message=message,
                message_attributes=message_attributes
            )
            results[phone_number] = success
        return results


# Create a singleton instance
sms_service = SMSService()

def send_sms(
    phone_number: str,
    message: str,
    message_attributes: Optional[dict] = None
) -> bool:
    """
    Utility function to send SMS
    
    Args:
        phone_number: Recipient phone number (E.164 format)
        message: SMS message content
        message_attributes: Optional SNS message attributes
        
    Returns:
        bool: True if SMS sent successfully, False otherwise
    """
    return sms_service.send_sms(
        phone_number=phone_number,
        message=message,
        message_attributes=message_attributes
    )

def send_bulk_sms(
    phone_numbers: list[str],
    message: str,
    message_attributes: Optional[dict] = None
) -> dict:
    """
    Utility function to send bulk SMS
    
    Args:
        phone_numbers: List of recipient phone numbers (E.164 format)
        message: SMS message content
        message_attributes: Optional SNS message attributes
        
    Returns:
        dict: Dictionary with phone numbers as keys and success status as values
    """
    return sms_service.send_bulk_sms(
        phone_numbers=phone_numbers,
        message=message,
        message_attributes=message_attributes
    )