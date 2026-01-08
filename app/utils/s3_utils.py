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
        
    def list_files(self, prefix: str = "", max_keys: Optional[int] = None, continuation_token: Optional[str] = None) -> Dict[str, Any]:
        """
        List files in S3 under a given prefix with optional pagination

        Args:
            prefix: S3 prefix (folder path) to list files from
            max_keys: Maximum number of keys to return (for pagination)
            continuation_token: Token for fetching next page

        Returns:
            dict: Contains 'keys' (list of S3 keys), 'is_truncated' (bool), and 'next_continuation_token' (str or None)
        """
        try:
            params = {
                'Bucket': self.bucket_name,
                'Prefix': prefix
            }

            if max_keys:
                params['MaxKeys'] = max_keys

            if continuation_token:
                params['ContinuationToken'] = continuation_token

            response = self.s3_client.list_objects_v2(**params)

            file_keys = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    file_keys.append(obj['Key'])

            return {
                'keys': file_keys,
                'is_truncated': response.get('IsTruncated', False),
                'next_continuation_token': response.get('NextContinuationToken')
            }
        except ClientError as e:
            logger.error(f"Error listing files from S3: {e}", exc_info=True)
            return {
                'keys': [],
                'is_truncated': False,
                'next_continuation_token': None
            }

    def list_all_files(self, prefix: str = "") -> list[str]:
        """
        List all files in S3 under a given prefix (no pagination limit)

        Args:
            prefix: S3 prefix (folder path) to list files from

        Returns:
            list: List of all S3 keys (file paths)
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
        

    def list_folders_with_metadata(self, prefix: str = "", limit: int = 10) -> list[Dict[str, Any]]:
        """
        List folders under a prefix with metadata (latest modification date and file count).
        Returns folders sorted by most recent activity (newest first).

        Args:
            prefix: S3 prefix (base path) to list folders from
            limit: Maximum number of folders to return (default: 10)

        Returns:
            list: List of dicts with folder_name, last_modified, and file_count
        """
        try:
            # Use paginator to get all objects
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

            # Dictionary to track folder info: {folder_name: {files: [metadata], count: int}}
            folders_data = {}

            for page in pages:
                if 'Contents' not in page:
                    continue

                for obj in page['Contents']:
                    key = obj['Key']

                    # Extract folder name from key
                    # Key format: "data-loaders/folder-name/file.xlsx"
                    # Remove prefix and get folder name
                    relative_path = key[len(prefix):] if key.startswith(prefix) else key

                    # Skip if no folder structure (file at root level)
                    if '/' not in relative_path:
                        continue

                    folder_name = relative_path.split('/')[0]

                    # Skip empty folder names
                    if not folder_name:
                        continue

                    # Initialize folder if not exists
                    if folder_name not in folders_data:
                        folders_data[folder_name] = {
                            'last_modified': obj['LastModified'],
                            'count': 0
                        }

                    # Update folder metadata
                    folders_data[folder_name]['count'] += 1

                    # Track the most recent modification date
                    if obj['LastModified'] > folders_data[folder_name]['last_modified']:
                        folders_data[folder_name]['last_modified'] = obj['LastModified']

            # Convert to list and sort by last_modified (newest first)
            folder_list = [
                {
                    'folder_name': folder_name,
                    'last_modified': data['last_modified'],
                    'file_count': data['count']
                }
                for folder_name, data in folders_data.items()
            ]

            # Sort by last_modified descending (newest first)
            folder_list.sort(key=lambda x: x['last_modified'], reverse=True)

            # Return only the requested number of folders
            return folder_list[:limit]

        except ClientError as e:
            logger.error(f"Error listing folders from S3: {e}", exc_info=True)
            return []


s3_utils = S3Utils()
