### app/utils/bedrock.py

# Standard library imports
import os
import json

# Third party imports
import boto3
from botocore.exceptions import ClientError

# Local imports
from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class BedrockClient:
    """Bedrock client for interacting with Amazon Bedrock"""

    def __init__(self):
        """Initialize the Bedrock client"""
        self.client = boto3.client(
            "bedrock-runtime",
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region,
        )

    def _format_prompt(self, template_path: str, replacements: dict) -> str:
        """Format a prompt with replacements"""
        with open(template_path, encoding="utf-8") as f:
            template = f.read()
        for key, value in replacements.items():
            template = template.replace(f"{{{{{key}}}}}", value)
        return template
    
    def generate_sql(self, user_prompt: str, schema_desc: str) -> str:
        """Generate SQL using the Bedrock client"""
        try:
            template_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "templates", "prompt_sql_gen.txt"
            )
            prompt = self._format_prompt(template_path, {
                "prompt": user_prompt,
                "schema_description": schema_desc,
            })

            response = self.client.invoke_model(
                modelId=settings.claude_model_id,
                body=json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "messages": [
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    "max_tokens": 500,
                    "temperature": 0.2
                }),
                contentType="application/json",
                accept="application/json",
            )
            result = json.loads(response["body"].read())
            return result.get("content", [{}])[0].get("text", "").strip()
        except (ClientError, Exception) as e:
            logger.error("Error generating SQL: %s", e)
            raise e
        
    def apply_query_filters(self, sql_query: str, filters: dict) -> str:
        """Apply filters to a SQL query"""
        try:
            template_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "templates", "prompt_sql_filter.txt"
            )
            prompt = self._format_prompt(template_path, {
                "original_query": sql_query,
                "filters": str(filters),
            })

            response = self.client.invoke_model(
                modelId=settings.claude_model_id,
                body=json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "messages": [
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    "max_tokens": 500,
                    "temperature": 0.2
                }),
                contentType="application/json",
                accept="application/json",
            )
            result = json.loads(response["body"].read())
            return result.get("content", [{}])[0].get("text", "").strip()
        except Exception as e:
            logger.error("Error applying query filters: %s", e)
            raise e
    
    
    def validate_sql(self, sql_query: str) -> bool:
        """Validate SQL using the Bedrock client"""
        try:
            validation_prompt = f"""
            You are a SQL security expert. Validate this SQL query for:
            1. Must only contain SELECT statement
            2. Must not include INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE
            3. Must not contain dangerous expressions

            Query: {sql_query}

            Return only YES or NO.
            """

            response = self.client.invoke_model(
                modelId=settings.claude_model_id,
                body=json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "messages": [
                        {
                            "role": "user",
                            "content": validation_prompt
                        }
                    ],
                    "max_tokens": 10,
                    "temperature": 0.1
                }),
                contentType="application/json",
                accept="application/json",
            )
            result = json.loads(response["body"].read())
            return "yes" in result.get("content", [{}])[0].get("text", "").lower()
        except (ClientError, Exception) as e:
            logger.error("Error validating SQL: %s", e)
            raise e
    

bedrock_client = BedrockClient()
