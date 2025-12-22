### api/app/esign/esign_client.py

# Standard library imports
import base64
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
import logging

# Third party imports
import jwt
import requests
from docusign_esign import (
    ApiClient, EnvelopesApi, EnvelopeDefinition, Document, Signer, SignHere
)
# from docusign_esign.client.auth.oauth import OAuthToken

# Local imports
from .config import Config
from .storage import EnvelopeStorage

logger = logging.getLogger("ESign client -*-*-*-")


class ESignClient:
    """
    This class is used to send documents for e-signature and manage the envelope data.
    """
    def __init__(self):
        self.config = Config()
        self.storage = EnvelopeStorage()
        self.api_client = ApiClient()
        self.api_client.host = self.config.get_value("base_path")
        self._configure_auth()

    def _configure_auth(self) -> None:
        """Configure JWT authentication."""
        try: 
            private_key = self.config.load_private_key()
            if not private_key:
                raise ValueError("Private key not found")
            
            expires_in = 3600
            auth_server = self.config.get_value("auth_server")
            client_id = self.config.get_value("client_id")
            user_id = self.config.get_value("user_id")

            # Prepare JWT token
            jwt_token = jwt.encode({
                "iss": client_id,
                "sub": user_id,
                "aud": auth_server,
                "iat": datetime.now(timezone.utc),
                "exp": datetime.now(timezone.utc) + timedelta(seconds=expires_in),
                "scope": "signature impersonation"
            }, private_key, algorithm="RS256")

            # Get access token
            response = requests.post(
                f"https://{auth_server}/oauth/token",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={
                    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                    "assertion": jwt_token
                },
                timeout=60
            )

            if response.status_code != 200:
                raise Exception(f"Authentication failed: {response.text}")
            
            access_token = response.json()["access_token"]
            self.api_client.set_default_header("Authorization", f"Bearer {access_token}")
        except Exception as e:
            logger.error("Error configuring auth: %s", e, exc_info=True)
            raise e

    def send_document(self, document_path: str, signers: List[Dict], **kwargs) -> str:
        """Send document for e-signature"""
        with open(document_path, "rb") as file:
            document_base64 = base64.b64encode(file.read()).decode("utf-8")

        # Create envelope definition
        envelope_definition = EnvelopeDefinition(
            email_subject=kwargs.get("email_subject", "Please sign this document"),
            documents=[
                Document(
                    document_base64=document_base64,
                    name=kwargs.get("document_name", "Document"),
                    file_extension=document_path.split(".")[-1],
                    document_id="1"
                )
            ],
            recipients={
                "signers": [
                    Signer(
                        email=signer["email"],
                        name=signer["name"],
                        recipient_id=str(i+1),
                        routing_order=str(i+1),
                        tabs={
                            'sign_here_tabs': [
                                SignHere(
                                    anchor_string=signer.get("anchor_text", "/sig/"),
                                    anchor_units="pixels",
                                    anchor_y_offset="0",
                                    anchor_x_offset="0"
                                )
                            ]
                        }
                    ) for i, signer in enumerate(signers)
                ]
            },
            status=kwargs.get("status", "sent")
        )

        # Create envelope
        envelopes_api = EnvelopesApi(self.api_client)
        response = envelopes_api.create_envelope(
            account_id=self.config.get_value("account_id"),
            envelope_definition=envelope_definition
        )

        for signer in signers:
            signer["signer_url"] = (f"{self.config.get_value('base_path')}/v2.1/accounts/"
           f"{self.config.get_value('account_id')}/envelopes/{response.envelope_id}/views/recipient")

        # Store the envelope data
        self.storage.add_envelope(response.envelope_id, {
            "status": "sent",
            "document_name": kwargs.get("document_name", "Document"),
            "signers": signers,
            "created_at": datetime.now(timezone.utc).isoformat()
        })

        return response.envelope_id
    
    def get_envelope_status(self, envelope_id: str) -> Dict:
        """Get current status of an envelope."""
        envelopes_api = EnvelopesApi(self.api_client)
        response = envelopes_api.get_envelope(
            account_id=self.config.get_value("account_id"),
            envelope_id=envelope_id
        )

        status = {
            "status": response.status,
            "email_subject": response.email_subject,
            "created": response.created,
            "sent": response.sent,
            "delivered": response.delivered,
            "signed": response.signed,
            "completed": response.completed
        }

        # Update storage
        self.storage.update_envelope_status(envelope_id, response.status)
        return status
    
    def get_envelope_info(self, envelope_id: str) -> Dict:
        """Get information about an envelope."""
        response = self.storage.get_envelope(envelope_id)
        return response
    
    def download_signed_document(self, envelope_id: str, output_path: str) -> bool:
        """Download the signed document"""
        envelopes_api = EnvelopesApi(self.api_client)

        # Get documents from envelope
        documents = envelopes_api.list_documents(
            account_id=self.config.get_value("account_id"),
            envelope_id=envelope_id
        )

        # Download combined document
        document_id = "combined"
        response = envelopes_api.get_document(
            account_id=self.config.get_value("account_id"),
            envelope_id=envelope_id,
            document_id=document_id
        )

        with open(output_path, "wb") as f:
            f.write(response)

        return True
    
    def list_envelopes(self, status: Optional[str] = None) -> List[Dict]:
        """List envelopes with optional status filter"""
        return self.storage.list_envelopes(status)
