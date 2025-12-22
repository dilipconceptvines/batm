### app/utils/lambda_utils.py

import json

import boto3
from botocore.exceptions import ClientError

from app.utils.logger import get_logger
from app.core.config import settings

logger = get_logger(__name__)

class LambdaInvocationError(Exception):
    """Custom exception for Lambda invocation errors"""
    def __init__(self, message: str, status_code: int = None, error_details: dict = None):
        self.message = message
        self.status_code = status_code
        self.error_details = error_details or {}
        super().__init__(self.message)


def invoke_lambda_function(function_name: str, payload: dict) -> dict:
    """
    Invokes an AWS Lambda function and returns the response.

    Args:
        function_name (str): Name or ARN of the Lambda function to invoke
        payload (dict): Data to be passed to the Lambda function

    Returns:
        dict: Response from the Lambda function

    Raises:
        LambdaInvocationError: If the Lambda invocation fails or returns an error
    """
    try:
        lambda_client = boto3.client(
            'lambda',
            region_name=settings.aws_region,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
        )

        logger.info("Invoking Lambda function: %s with payload: %s", function_name, payload)
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )

        status_code = response['StatusCode']

        # Parse the response payload
        response_payload = json.loads(response['Payload'].read())

        # Check if the invocation was successful (200 or 202)
        if status_code not in [200, 202]:
            error_msg = f"Lambda invocation failed with status code: {status_code}"
            logger.error(error_msg)
            raise LambdaInvocationError(
                message=error_msg,
                status_code=status_code,
                error_details=response_payload
            )

        # Check if Lambda function returned an error (handled exception from Lambda)
        if 'errorMessage' in response_payload:
            error_message = response_payload.get('errorMessage', 'Unknown error')
            error_type = response_payload.get('errorType', 'Unknown')

            logger.error(
                "Lambda function returned error - Type: %s, Message: %s",
                error_type,
                error_message
            )

            raise LambdaInvocationError(
                message=f"PDF generation failed: {error_message}",
                status_code=400,
                error_details={
                    'error_type': error_type,
                    'error_message': error_message,
                    'stack_trace': response_payload.get('stackTrace', [])
                }
            )

        # Check if Lambda returned a status code in the body (API Gateway style)
        if 'statusCode' in response_payload:
            body_status = response_payload['statusCode']
            if body_status >= 400:
                body = response_payload.get('body', '{}')
                try:
                    body_dict = json.loads(body) if isinstance(body, str) else body
                    error_message = body_dict.get('message', body_dict.get('error', 'PDF generation failed'))
                except:
                    error_message = str(body)

                logger.error(
                    "Lambda returned error status %s: %s",
                    body_status,
                    error_message
                )

                raise LambdaInvocationError(
                    message=error_message,
                    status_code=body_status,
                    error_details={'response_body': response_payload}
                )

        return response_payload

    except LambdaInvocationError:
        # Re-raise our custom exception
        raise
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_msg = e.response.get('Error', {}).get('Message', str(e))
        logger.error("AWS Lambda ClientError - Code: %s, Message: %s", error_code, error_msg)
        raise LambdaInvocationError(
            message=f"AWS service error: {error_msg}",
            status_code=500,
            error_details={'error_code': error_code, 'aws_error': error_msg}
        ) from e
    except Exception as e:
        error_msg = f"Unexpected error invoking Lambda function: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise LambdaInvocationError(
            message=error_msg,
            status_code=500,
            error_details={'exception_type': type(e).__name__}
        ) from e