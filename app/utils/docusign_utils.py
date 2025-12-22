# app/utils/docusign_utils.py

import asyncio
import base64
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import aiohttp
import jwt
from fastapi import HTTPException, status

from app.core.config import get_docusign_private_key_s3_key, settings
from app.esign.schemas import Signer
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)


class DocusignClient:
    """
    A robust client for interacting with the DocuSign eSignature API.
    """

    def __init__(self):
        self.base_path = settings.docusign_base_path
        self.account_id = settings.docusign_account_id

    def _rest_base(self) -> str:
        """Ensure we have exactly one '/restapi' in the base path."""
        base = (self.base_path or "").rstrip("/")
        if not base.endswith("/restapi"):
            base = f"{base}/restapi"
        return base

    def _load_pem(self, path: str) -> str:
        if not os.path.exists(path):
            logger.error(f"PEM not found at {path}. Set DOCUSIGN_PRIVATE_KEY_PATH.")
        with open(path, "rb") as f:
            return f.read()

    def normalize_pem_continuous(self, raw: str) -> str:
        """
        Convert a continuous PEM string into a properly formatted PEM block.
        """
        if not raw:
            raise ValueError("PEM string is empty")

        raw = raw.strip()

        # Extract parts using markers
        begin = "-----BEGIN RSA PRIVATE KEY-----"
        end = "-----END RSA PRIVATE KEY-----"

        if begin in raw and end in raw:
            # strip markers and rebuild properly
            body = raw.replace(begin, "").replace(end, "").strip()

            # Re-split into 64-character lines per PEM spec
            lines = [body[i : i + 64] for i in range(0, len(body), 64)]

            normalized = f"{begin}\n" + "\n".join(lines) + f"\n{end}\n"
            return normalized

        raise ValueError("Invalid PEM format: markers not found")

    async def _get_access_token(self) -> str:
        """
        Generates a JWT and exchanges it for a DocuSign access token.
        It securely fetches the private key from S3.
        """
        try:
            # If the Client Secret is configured, the key would be picked from the vault
            if settings.aws_credentials_secret_id:
                logger.info("Loading private key from secrets vault")
                private_key = get_docusign_private_key_s3_key()
                private_key = self.normalize_pem_continuous(private_key)
            else:
                # Requirement #5: Get private key from S3 or localhost
                if settings.docusign_pem_path:
                    logger.info("Loading private key from local path")
                    private_key_bytes = self._load_pem(settings.docusign_pem_path)
                elif settings.docusign_private_key_s3_key:
                    logger.info("Loading private key from S3 path")
                    private_key_bytes = s3_utils.download_file(
                        settings.docusign_private_key_s3_key
                    )
                if not private_key_bytes:
                    raise ValueError("Could not retrieve DocuSign private key from S3.")
                private_key = private_key_bytes.decode("utf-8")

            jwt_payload = {
                "iss": settings.docusign_client_id,
                "sub": settings.docusign_user_id,
                "iat": datetime.now(timezone.utc),
                "exp": datetime.now(timezone.utc) + timedelta(hours=1),
                "aud": settings.docusign_auth_server,
                "scope": "signature impersonation",
            }
            jwt_token = jwt.encode(jwt_payload, private_key, algorithm="RS256")

            async with aiohttp.ClientSession() as session:
                url = f"https://{settings.docusign_auth_server}/oauth/token"
                payload = {
                    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                    "assertion": jwt_token,
                }
                async with session.post(url, data=payload) as response:
                    try:
                        response.raise_for_status()
                    except Exception:
                        body = await response.text()
                        logger.error(
                            "DocuSign token error %s: %s", response.status, body
                        )
                        raise
                    token_data = await response.json()
                    return token_data["access_token"]
        except Exception as e:
            logger.error(f"Failed to get DocuSign access token: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Docusign authentication failed.",
            ) from e

    def _prepare_in_person_signers(self, signers, signing_position_info):
        # Construct signer objects for DocuSign API
        host_email = settings.docusign_host_email
        if not host_email:
            raise ValueError("Docusign host email config is missing")
        host_name = settings.docusign_host_name
        if not host_name:
            raise ValueError("Docusign host name config is missing")

        in_person_signers_list = []
        for idx, signer in enumerate(signers, start=1):
            signer_id = signer.signer_id if signer.signer_id else f"signer_{idx}"
            # Get signing positions for this specific signer using their signer_id
            signer_positions = signing_position_info.get(signer_id, {})

            in_person_signers_list.append(
                {
                    "recipientId": str(idx),
                    "routingOrder": str(idx),
                    "hostEmail": host_email,
                    "hostName": host_name,
                    "signerName": signer.name,
                    "tabs": signer_positions,
                }
            )

        docusign_signers = {"inPersonSigners": in_person_signers_list}

        return docusign_signers

    def _prepare_email_signers(self, signers, signing_position_info):
        # Construct signer objects for DocuSign API
        docusign_signers_list = []
        for idx, signer in enumerate(signers, start=1):
            signer_id = signer.signer_id if signer.signer_id else f"signer_{idx}"
            # Get signing positions for this specific signer using their signer_id
            signer_positions = signing_position_info.get(signer_id, {})

            signer_dict = {
                "recipientId": str(idx),
                "routingOrder": str(idx),
                "name": signer.name,
                "email": signer.email,
                "tabs": signer_positions,
            }

            # Add clientUserId if this is an embedded signer
            if signer.signing_type == "embedded":
                signer_dict["clientUserId"] = signer.client_user_id or str(uuid.uuid4())

            docusign_signers_list.append(signer_dict)

        return {"signers": docusign_signers_list}

    # ------------------------
    # Public sync wrapper
    # ------------------------
    def send_envelope(
        self,
        source_s3_key: str,
        document_name: str,
        signers: List[Signer],
        return_url: str = "https://www.google.com",
    ) -> Dict[str, Any]:
        """
        Synchronous wrapper. Use this from normal (non-async) code.
        In FastAPI sync paths (threadpool), prefer anyio.from_thread.run.
        Fallback to asyncio.run for scripts/CLIs.
        """
        try:
            import anyio  # type: ignore

            return anyio.from_thread.run(
                self.send_envelope_async,
                source_s3_key,
                document_name,
                signers,
                return_url,
            )
        except ImportError:
            return asyncio.run(
                self.send_envelope_async(
                    source_s3_key,
                    document_name,
                    signers,
                    return_url,
                )
            )

    # ------------------------
    # Actual async worker
    # ------------------------
    async def send_envelope_async(
        self,
        source_s3_key: str,
        document_name: str,
        signers: List[Signer],
        return_url: str = "https://www.google.com",
        signature_mode: str = "",
        project_name: str = "",
        case_no: str = "",
        signing_position_info={},
    ) -> Dict[str, Any]:
        """
        Requirement #1 & #4: Sends a PDF from S3 for signature to multiple signers
        and generates embedded signing URLs (one-time, short-lived).
        """
        rest = self._rest_base()

        access_token = await self._get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # Download document from S3 and encode
        doc_bytes = s3_utils.download_file(source_s3_key)
        if not doc_bytes:
            raise HTTPException(
                status_code=404,
                detail=f"Source document not found in S3: {source_s3_key}",
            )
        doc_base64 = base64.b64encode(doc_bytes).decode("utf-8")

        recipients = {}
        if signature_mode == "email":
            recipients = self._prepare_email_signers(signers, signing_position_info)

        if signature_mode == "in-person":
            recipients = self._prepare_in_person_signers(signers, signing_position_info)

        # Create envelope definition
        envelope_definition = {
            "emailSubject": f"Please Sign: {document_name}",
            "documents": [
                {
                    "documentBase64": doc_base64,
                    "name": document_name,
                    "fileExtension": "pdf",
                    "documentId": "1",
                }
            ],
            "recipients": recipients,
            "status": "sent",
        }

        # If envelope requires to have a project name that webhook callbacks map back
        custom_fields = []
        if project_name:
            custom_fields.append(
                {"name": "project", "value": project_name, "show": "false"}
            )
        if case_no:
            custom_fields.append({"name": "case_no", "value": case_no, "show": "false"})

        custom_fields.append(
            {"name": "env", "value": settings.environment, "show": "false"}
        )

        if custom_fields:
            envelope_definition["customFields"] = {"textCustomFields": custom_fields}

        logger.info("********** Envelope Details **********")
        logger.info(envelope_definition)
        logger.info("********** Envelope Details **********")

        timeout = aiohttp.ClientTimeout(total=60)
        envelope_id: str
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Create envelope
            create_url = f"{rest}/v2.1/accounts/{self.account_id}/envelopes"
            async with session.post(
                create_url, headers=headers, json=envelope_definition
            ) as response:
                body = await response.text()
                if response.status != 201:
                    logger.error(
                        "DocuSign envelope creation failed (%s): %s",
                        response.status,
                        body,
                    )
                    raise HTTPException(
                        status_code=response.status,
                        detail=f"DocuSign envelope creation failed: {body}",
                    )
                data = await response.json()
                envelope_id = data["envelopeId"]

            # Generate signing URLs for embedded signers
            signing_urls: Dict[str, str] = {}

            for signer in signers:
                if "clientUserId" not in signer:
                    continue  # email-delivered signer; no embedded URL

                rv_url = (
                    f"{rest}/v2.1/accounts/{self.account_id}/envelopes/"
                    f"{envelope_id}/views/recipient"
                )
                view_request = {
                    "returnUrl": return_url,
                    "authenticationMethod": "none",
                    "email": signer["email"],  # must match envelope
                    "userName": signer["name"],  # must match envelope
                    "clientUserId": signer["clientUserId"],  # must match envelope
                    # Optional: keep-alive pings to reduce timeouts
                    # "pingUrl": "https://yourapp.example.com/ping",
                    # "pingFrequency": "600",  # 300–1200 seconds
                }
                async with session.post(
                    rv_url, headers=headers, json=view_request
                ) as view_response:
                    v_body = await view_response.text()
                    if view_response.status != 201:
                        logger.error(
                            "Recipient view failed for %s (%s): %s",
                            signer.get("email"),
                            view_response.status,
                            v_body,
                        )
                        continue
                    view_data = await view_response.json()
                    signing_urls[signer["clientUserId"]] = view_data.get("url", "")

        return {
            "envelope_id": envelope_id,
            "status": "sent",
            "signing_urls": signing_urls,
        }

    async def download_completed_document(self, envelope_id: str) -> bytes:
        """Downloads the final, signed PDF from a completed envelope."""
        access_token = await self._get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            # Let DocuSign know we expect binary/pdf back
            "Accept": "application/pdf",
        }

        rest = self._rest_base()
        async with aiohttp.ClientSession() as session:
            url = f"{rest}/v2.1/accounts/{self.account_id}/envelopes/{envelope_id}/documents/combined"
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    body = await response.text()
                    logger.error(
                        "Failed to download combined document (%s): %s",
                        response.status,
                        body,
                    )
                    response.raise_for_status()
                return await response.read()

    async def download_host_signature_url(self, envelope_id: str) -> bytes:
        """Downloads the final, signed PDF from a completed envelope."""
        access_token = await self._get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            # Let DocuSign know we expect binary/pdf back
            "Accept": "application/pdf",
        }

        rest = self._rest_base()
        async with aiohttp.ClientSession() as session:
            url = f"{rest}/v2.1/accounts/{self.account_id}/envelopes/{envelope_id}/views/recipient"
            data = {
                "returnUrl": "https://yourapp.example.com/done",
                "authenticationMethod": "none",
                "userName": "Chandrashekar J",
                "email": "chandrashekarj@conceptvines.com",
                "recipientId": "1",
            }
            async with session.post(url, headers=headers, json=data) as response:
                if response.status != 200:
                    body = await response.text()
                    logger.error(
                        "Failed to download combined document (%s): %s",
                        response.status,
                        body,
                    )
                    response.raise_for_status()
                return await response.read()

    async def create_signing_url_async(
        self,
        envelope_id: str,
        email: str,
        user_name: str,
        return_url: str = "https://www.google.com",
        client_user_id: str | None = None,
    ) -> str:
        """
        Generate a one-time embedded signing URL for the recipient identified by `email`
        on `envelope_id`. If `clientUserId` is missing, this sets one first.

        Returns: the signing URL (single-use, short-lived).
        """
        rest = self._rest_base()
        access_token = await self._get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # 1) Fetch recipients for the envelope
            rec_url = f"{rest}/v2.1/accounts/{self.account_id}/envelopes/{envelope_id}/recipients"
            async with session.get(rec_url, headers=headers) as r:
                body = await r.text()
                if r.status != 200:
                    logger.error("Fetch recipients failed (%s): %s", r.status, body)
                    raise HTTPException(
                        status_code=r.status,
                        detail=f"Failed to fetch recipients: {body}",
                    )
                recipients = await r.json()

            # 2) Find the signer by email (case-insensitive)
            target = None
            target_email = (email or "").lower()
            for s in recipients.get("signers", []) or []:
                if (s.get("email") or "").lower() == target_email:
                    target = s
                    break
            if not target:
                raise HTTPException(
                    status_code=404,
                    detail=f"No matching recipient in envelope for {email}",
                )

            # 3) Ensure clientUserId (required for embedded signing)
            cue = client_user_id or target.get("clientUserId")
            if not cue:
                cue = f"email-{target_email}"  # deterministic, simple
                update_payload = {
                    "signers": [
                        {
                            "recipientId": target.get("recipientId"),
                            "recipientIdGuid": target.get("recipientIdGuid"),
                            "email": target.get("email") or email,
                            "name": target.get("name") or user_name,
                            "clientUserId": cue,
                        }
                    ]
                }
                upd_url = f"{rest}/v2.1/accounts/{self.account_id}/envelopes/{envelope_id}/recipients"
                async with session.put(
                    upd_url, headers=headers, json=update_payload
                ) as u:
                    u_body = await u.text()
                    if u.status not in (200, 201):
                        logger.error(
                            "Set clientUserId failed (%s): %s", u.status, u_body
                        )
                        raise HTTPException(
                            status_code=u.status,
                            detail=f"Failed to set clientUserId: {u_body}",
                        )

            # 4) Create the Recipient View (signing URL)
            view_payload = {
                "returnUrl": return_url,
                "authenticationMethod": "none",
                # MUST match the envelope's recipient values:
                "email": target.get("email") or email,
                "userName": target.get("name") or user_name,
                "clientUserId": cue,
            }
            view_url = f"{rest}/v2.1/accounts/{self.account_id}/envelopes/{envelope_id}/views/recipient"
            async with session.post(view_url, headers=headers, json=view_payload) as v:
                v_body = await v.text()
                if v.status != 201:
                    logger.error("Recipient view failed (%s): %s", v.status, v_body)
                    raise HTTPException(
                        status_code=v.status,
                        detail=f"createRecipientView failed: {v_body}",
                    )
                v_json = await v.json()
                return v_json["url"]

    def create_signing_url(
        self,
        envelope_id: str,
        email: str,
        user_name: str,
        return_url: str = "https://www.google.com",
        client_user_id: str | None = None,
    ) -> str:
        """
        Synchronous wrapper for create_signing_url_async.
        Works from both non-async and async contexts.
        """
        import threading

        try:
            asyncio.get_running_loop()  # if this doesn't raise, we're in async context
        except RuntimeError:
            # No running loop: safe to run directly
            return asyncio.run(
                self.create_signing_url_async(
                    envelope_id, email, user_name, return_url, client_user_id
                )
            )
        else:
            # We're already on an event loop thread: use a fresh thread + loop
            result = {}
            err = []

            def runner():
                try:
                    result["url"] = asyncio.run(
                        self.create_signing_url_async(
                            envelope_id, email, user_name, return_url, client_user_id
                        )
                    )
                except BaseException as e:
                    err.append(e)

            t = threading.Thread(target=runner, daemon=True)
            t.start()
            t.join()
            if err:
                raise err[0]
            return result["url"]


# Singleton instance
docusign_client = DocusignClient()
