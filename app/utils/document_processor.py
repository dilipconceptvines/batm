# app/utils/document_processor.py

import json
import time
from typing import Dict, Any

import boto3
from botocore.exceptions import ClientError

from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class DocumentProcessor:
    """
    A utility class to process documents from S3 using AWS Textract
    and Bedrock.
    """

    def __init__(self):
        """
        Initializes the AWS services for Textract and Bedrock.
        """
        try:
            self.s3_client = boto3.client(
                "s3",
                region_name=settings.aws_region,
                aws_access_key_id=settings.aws_access_key_id,
                aws_secret_access_key=settings.aws_secret_access_key
            )
            self.textract_client = boto3.client(
                "textract",
                region_name=settings.aws_region,
                aws_access_key_id=settings.aws_access_key_id,
                aws_secret_access_key=settings.aws_secret_access_key
            )
            self.bedrock_client = boto3.client(
                "bedrock-runtime",
                region_name=settings.aws_region,
                aws_access_key_id=settings.aws_access_key_id,
                aws_secret_access_key=settings.aws_secret_access_key
            )
            self.bucket_name = settings.s3_bucket_name
        except ClientError as e:
            logger.error("Error initializing DocumentProcessor: %s", str(e), exc_info=True)
            raise

    def _get_raw_text_from_s3(self, s3_key: str) -> str:
        """
        Starts a Textract job to extract text from a document in S3 and gets the result.

        Args:
            s3_key (str): The S3 key of the document to process.

        Returns:
            str: The raw text extracted from the document.
        """
        try:
            logger.info(f"Starting textract job for s://{self.bucket_name}/{s3_key}")
            response = self.textract_client.start_document_text_detection(
                DocumentLocation={"S3Object": {"Bucket": self.bucket_name, "Name": s3_key}}
            )
            job_id = response["JobId"]

            # Poll for the job to complete
            while True:
                result = self.textract_client.get_document_text_detection(JobId=job_id)
                status = result["JobStatus"]
                if status in ["SUCCEEDED", "FAILED"]:
                    break
                time.sleep(5)

            if status == "FAILED":
                logger.error(f'Textract job failed for S3 Key {s3_key}: {result.get("StatusMessage")}')
                raise Exception("Textract processing failed")
            
            # Concatenate all detected text
            text_blocks = [block['Text'] for block in result['Blocks'] if block['BlockType'] == 'LINE']
            return "\n".join(text_blocks)

        except ClientError as e:
            logger.error("Error getting raw text from S3: %s", str(e), exc_info=True)
            raise

    def _invoke_bedrock(self, prompt: str, max_tokens: int = 2048) -> str:
        """
        Invokes the Bedrock model with given prompt

        Args:
            prompt (str): The prompt to send to the Bedrock model.
            max_tokens (int, optional): The maximum number of tokens to generate. Defaults to 2048.

        Returns:
            str: The response from the Bedrock model.
        """
        try:
            body = json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
                "temperature": 0.1,
            })
            response = self.bedrock_client.invoke_model(
                modelId=settings.claude_model_id,
                body=body,
                contentType="application/json",
                accept="application/json"
            )
            result = json.loads(response["body"].read())
            return result.get("content", [{}])[0].get("text", "").strip()
        except ClientError as e:
            logger.error("Error invoking Bedrock model: %s", str(e), exc_info=True)
            raise

    def _classify_document(self, raw_text: str) -> str:
        """
        Uses Bedrock to classify the document based on its text content.
        """
        document_types = [
            "ssn_card", "ein_document", "passport", "driving_license", 
            "tlc_license", "invoice", "receipt", "bank_statement", "void_check", "unknown"
        ]
        
        prompt = f"""
        Analyze the following text extracted from a document and classify it into one of the following categories:
        {', '.join(document_types)}.
        
        Return only the single, most appropriate category name and nothing else.
        
        Text content:
        ---
        {raw_text[:4000]} 
        ---
        """
        
        classification = self._invoke_bedrock(prompt, max_tokens=50)

        # Clean up the response to get a valid type
        for doc_type in document_types:
            if doc_type in classification.lower():
                logger.info(f"Document classified as: {doc_type}")
                return doc_type
        
        logger.warning("Could not classify document. Defaulting to 'unknown'.")
        return "unknown"
    
    def _extract_structured_data(self, raw_text: str, doc_type: str) -> Dict[str, Any]:
        """
        Uses Bedrock to extract structured data from the text based on document type.
        """
        extraction_prompts = {
            "driving_license": "Extract the following fields: first_name, last_name, license_number, date_of_birth, issue_date, expiration_date, address.",
            "passport": "Extract the following fields: full_name, passport_number, nationality, date_of_birth, issue_date, expiration_date.",
            "ssn_card": "Extract the social_security_number and full_name.",
            "ein_document": "Extract the employer_identification_number (EIN) and company_name.",
            "tlc_license": "Extract the tlc_license_number, full_name, and expiration_date.",
            "invoice": "Extract invoice_number, vendor_name, customer_name, invoice_date, due_date, total_amount, and line_items (as a list of objects with description and amount).",
            "receipt": "Extract merchant_name, transaction_date, total_amount, and items (as a list of strings).",
            "bank_statement": "Extract bank_name, account_holder_name, account_number, statement_period_start_date, statement_period_end_date, and ending_balance.",
            "void_check": "Extract account_holder_name, bank_name, routing_number, and account_number.",
            "unknown": "Extract any names, dates, and amounts you can find."
        }

        instruction = extraction_prompts.get(doc_type, extraction_prompts["unknown"])
        
        prompt = f"""
        You are an expert data extraction assistant. Based on the document type '{doc_type}', analyze the following text and extract the required information.
        
        Instruction: {instruction}
        
        Return the result as a single, well-formed JSON object. If a field is not found, its value should be null.
        
        Text content:
        ---
        {raw_text}
        ---
        """
        
        response_text = self._invoke_bedrock(prompt)
        
        try:
            # Find the JSON block in the response
            json_match = response_text[response_text.find('{'):response_text.rfind('}')+1]
            return json.loads(json_match)
        except json.JSONDecodeError:
            logger.error(f"Failed to parse JSON from Bedrock response: {response_text}")
            return {"error": "Failed to extract structured data", "raw_response": response_text}
        
    def process_document(self, s3_key: str) -> Dict[str, Any]:
        """
        Main method to process a document from S3.

        Args:
            s3_key: The key of the file in the S3 bucket.

        Returns:
            A dictionary with the document type and the extracted structured data.
        """
        try:
            # 1. Get raw text using AWS Textract
            raw_text = self._get_raw_text_from_s3(s3_key)
            if not raw_text:
                raise ValueError("No text could be extracted from the document.")

            # 2. Classify document type using AWS Bedrock
            doc_type = self._classify_document(raw_text)

            # 3. Extract structured data using AWS Bedrock
            extracted_data = self._extract_structured_data(raw_text, doc_type)

            return {
                "document_type": doc_type,
                "extracted_data": extracted_data
            }
        except Exception as e:
            logger.error(f"Full document processing failed for S3 key {s3_key}: {e}")
            return {"error": str(e)}
        

document_processor = DocumentProcessor()

