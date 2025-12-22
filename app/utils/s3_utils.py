### app/utils/s3_utils.py

# Standard library imports
import json
import os
from typing import Optional, BinaryIO, Dict, Any

# Third party imports
import boto3
from botocore.exceptions import ClientError

# Local imports
from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class S3Utils:
    """Utility class for interacting with s3"""
    def __init__(self):
        """Initialize S3 client with AWS credentials"""
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region
        )
        self.bucket_name = settings.s3_bucket_name

    def upload_file(self, file_obj: BinaryIO, key: str, content_type: Optional[str] = None) -> bool:
        """
        Upload a file to S3
        
        Args:
            file_obj: File object to upload
            key: S3 key (path) where the file will be stored
            content_type: Optional content type of the file
            
        Returns:
            bool: True if upload was successful, False otherwise
        """
        try:
            extra_args = {}
            # get file extension and set content type
            file_extension = os.path.splitext(key)[1]
            if file_extension == ".pdf":
                extra_args['ContentType'] = "application/pdf"
            elif file_extension == ".docx":
                extra_args['ContentType'] = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            elif file_extension == ".doc":
                extra_args['ContentType'] = "application/msword"
            if content_type:
                extra_args['ContentType'] = content_type

            self.s3_client.upload_fileobj(
                file_obj,
                self.bucket_name,
                key,
                ExtraArgs=extra_args
            )
            return True
        except ClientError as e:
            print(f"Error uploading file to S3: {e}")
            return False

    def download_file(self, key: str) -> Optional[bytes]:
        """
        Download a file from S3
        
        Args:
            key: S3 key (path) of the file to download
            
        Returns:
            bytes: File content if successful, None otherwise
        """
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=key
            )
            return response['Body'].read()
        except ClientError as e:
            print(f"Error downloading file from S3: {e}")
            return None

    def generate_presigned_url(self, key: str, expiration: int = 3600) -> Optional[str]:
        """
        Generate a presigned URL for temporary access to an S3 object
        
        Args:
            key: S3 key (path) of the file
            expiration: URL expiration time in seconds (default: 1 hour)
            
        Returns:
            str: Presigned URL if successful, None otherwise
        """
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': key
                },
                ExpiresIn=expiration
            )
            return url
        except ClientError as e:
            print(f"Error generating presigned URL: {e}")
            return None

    def delete_file(self, key: str) -> bool:
        """
        Delete a file from S3
        
        Args:
            key: S3 key (path) of the file to delete
            
        Returns:
            bool: True if deletion was successful, False otherwise
        """
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=key
            )
            return True
        except ClientError as e:
            print(f"Error deleting file from S3: {e}")
            return False
        
    def get_file_metadata(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves and parses the custom metadata of an S3 object.

        This function specifically looks for 'document-type' and a JSON string
        in 'structured-data' written by the document processing Lambda.

        Args:
            key: The S3 key (path) of the file.

        Returns:
            A dictionary containing the parsed metadata if found, otherwise None.
        """
        try:
            # head_object is more efficient than get_object for retrieving only metadata
            response = self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=key
            )
            
            # boto3 automatically normalizes 'x-amz-meta-*' keys to lowercase
            custom_metadata = response.get('Metadata', {})

            if not custom_metadata:
                return None

            # Prepare the final structured result
            processed_data = {
                "document_type": custom_metadata.get('document-type', 'unknown'),
                "extracted_data": {}
            }

            # The 'structured-data' was stored as a JSON string, so we parse it
            structured_data_str = custom_metadata.get('structured-data')
            if structured_data_str:
                try:
                    parse = json.loads(structured_data_str)
                    if not parse:
                        parse = {}

                    processed_data['extracted_data'] = parse
                except (json.JSONDecodeError, TypeError):
                    logger.error(f"Failed to parse structured-data JSON from S3 metadata for key: {key}")
                    processed_data['extracted_data'] = {"error": "Invalid JSON in metadata", "raw_data": structured_data_str}
            
            return processed_data
            
        except ClientError as e:
            # A '404' Not Found error is common and not a system failure
            if e.response['Error']['Code'] == '404':
                logger.warning(f"Metadata requested for non-existent S3 key: {key}")
            else:
                logger.error(f"Error getting file metadata from S3 for key {key}: {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred in get_file_metadata for key {key}: {e}", exc_info=True)
            return None
        
    def list_files(self, prefix: str = "") -> list[str]:
        """
        List all files in S3 under a given prefix

        Args:
            prefix: S3 prefix (folder path) to list files from

        Returns:
            list: List of S3 keys (file paths)
        """
        try:
            file_keys = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        file_keys.append(obj['Key'])

            return file_keys
        except ClientError as e:
            logger.error(f"Error listing files from S3: {e}", exc_info=True)
            return []
        

s3_utils = S3Utils()
