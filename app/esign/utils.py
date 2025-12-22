import base64
import hashlib
import hmac
import inspect
import json
import logging
import os
from typing import Optional

from sqlalchemy.orm import Session

from app.core.config import settings

from .models import ESignEnvelope


def verify_hmac(headers: dict, raw: bytes):
    secret = settings.docusign_webhook_secret
    if not secret:
        return True  # HMAC not enabled
    calc = base64.b64encode(
        hmac.new(secret.encode(), raw, hashlib.sha256).digest()
    ).decode()
    sigs = [
        headers.get("x-docusign-signature-1"),
        headers.get("x-docusign-signature-2"),
        headers.get("x-docusign-signature-3"),
    ]
    return any(s and hmac.compare_digest(s, calc) for s in sigs if s)


def extract_ctx(headers: dict, payload: dict) -> dict:
    event = (payload.get("event") or "").lower()
    data = payload.get("data") or {}

    # Extract case_no from custom fields
    case_no = None
    try:
        fields = data.get("envelopeSummary", {}).get("customFields", {}).get("textCustomFields", [])
        for f in fields:
            if (f.get("name") or "").lower() == "case_no":
                val = f.get("value")
                case_no = val.strip() if isinstance(val, str) else val
                break
    except (KeyError, TypeError):
        pass

    return {
        "event": event,
        "account_id": data.get("accountId"),
        "envelope_id": data.get("envelopeId"),
        "recipient_id": data.get("recipientId"),
        "user_id": data.get("userId"),
        "delivery_id": headers.get("x-docusign-delivery-id"),
        "configuration_id": payload.get("configurationId"),
        "generated_at": payload.get("generatedDateTime"),
        "case_no": case_no,
    }


def extract_module(payload: dict) -> str | None:
    try:
        fields = payload["data"]["envelopeSummary"]["customFields"]["textCustomFields"]
        for f in fields:
            if (f.get("name") or "").lower() == "project":
                val = f.get("value")
                return val.strip() if isinstance(val, str) else val
    except (KeyError, TypeError):
        return None
    return None


def update_envelope_status(
    db: Session,
    *,
    ctx: dict,
    status: str,
) -> int:
    """
    Update an existing ESignEnvelope's status by ctx['envelope_id'].
    - Update only (no insert)
    - No commit/rollback/flush here
    - Optionally constrain by object_type/object_id
    - Returns number of rows updated (0 if not found / mismatch)
    """
    envelope_id = (ctx or {}).get("envelope_id")
    if not envelope_id:
        raise ValueError("update_envelope_status: missing ctx['envelope_id']")

    q = db.query(ESignEnvelope).filter(ESignEnvelope.envelope_id == envelope_id)

    updated = q.update(
        {ESignEnvelope.status: status},
        synchronize_session=False,
    )
    return updated
