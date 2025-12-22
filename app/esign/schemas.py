# app/esign/schemas.py

from typing import Any, Dict, List, Literal, Optional

from pydantic import AnyHttpUrl, BaseModel, EmailStr


class Signer(BaseModel):
    """
    Defines a signer for a document.
    """

    name: str
    email: EmailStr
    # 'embedded' for signing in-app, 'remote' for sending an email
    signing_type: Literal["embedded", "remote"] = "remote"
    # A unique ID for the embedded signer within your application
    client_user_id: Optional[str] = None
    # Signer ID used to reference this signer in signing_position_info
    signer_id: Optional[str] = None


class EnvelopeCreateRequest(BaseModel):
    """Request to create and send a new DocuSign envelope."""

    source_s3_key: str
    document_name: str
    signers: List[Signer]
    object_type: str
    object_id: int
    # The URL the user is redirected to after embedded signing
    return_url: Optional[str] = None


class EnvelopeCreateResponse(BaseModel):
    """Response after creating an envelope."""

    envelope_id: str
    status: str
    # Contains signing URLs for embedded signers
    signing_urls: Dict[str, str] = {}


class DocuSignWebhookPayload(BaseModel):
    """Structure of the incoming webhook from DocuSign Connect."""

    event: str
    data: Dict[str, Any]


class RecipientViewRequest(BaseModel):
    envelope_id: str
    user_name: str
    email: EmailStr
    client_user_id: str
    return_url: AnyHttpUrl | None = None  # optional override


class RecipientViewResponse(BaseModel):
    url: AnyHttpUrl
    envelope_id: str
    email: EmailStr
    user_name: str
    client_user_id: str
