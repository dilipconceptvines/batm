# app/esign/router.py

import inspect
import io
import json
from typing import Any, Dict

from aiohttp import ClientResponseError
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy.orm import Session

from app.bpm_flows.additionaldriver import additionaldriver_esign_utils
from app.bpm_flows.driverlease import driverlease_esign_utils
from app.core.config import settings
from app.core.db import get_db
from app.esign import utils
from app.esign.models import ESignEnvelope
from app.esign.schemas import (
    EnvelopeCreateRequest,
    EnvelopeCreateResponse,
    RecipientViewRequest,
    RecipientViewResponse,
)
from app.users.models import User
from app.users.utils import get_current_user
from app.utils.docusign_utils import docusign_client
from app.utils.logger import get_logger

router = APIRouter(tags=["Esign"], prefix="/esign")
logger = get_logger(__name__)

SUPPORTED_EVENTS = {
    "recipient-completed",
    "recipient-declined",
    "recipient-delivered",
    "envelope-completed",
    "envelope-sent",
}


# Default no-op handlers
async def default_handler(db: Session, ctx: dict, payload: dict):
    logger.info("[default] %s: %s", ctx.get("event"), ctx)


# module -> event -> handler
HANDLERS = {
    "driverlease": {
        "recipient-completed": driverlease_esign_utils.driverlease_recipient_completed,
        "recipient-declined": driverlease_esign_utils.driverlease_recipient_declined,
        "recipient-delivered": driverlease_esign_utils.driverlease_recipient_delivered,
        "envelope-sent": driverlease_esign_utils.driverlease_envelope_sent,
        "envelope-completed": driverlease_esign_utils.driverlease_envelope_completed,
    },
    "additionaldriver": {
        "recipient-completed": additionaldriver_esign_utils.additionaldriver_recipient_completed,
        "recipient-declined": additionaldriver_esign_utils.additionaldriver_recipient_declined,
        "recipient-delivered": additionaldriver_esign_utils.additionaldriver_recipient_delivered,
        "envelope-sent": additionaldriver_esign_utils.additionaldriver_envelope_sent,
        "envelope-completed": additionaldriver_esign_utils.additionaldriver_envelope_completed,
    },
    "default": {
        "recipient-completed": default_handler,
        "recipient-delivered": default_handler,
        "envelope-completed": default_handler,
    },
}


@router.post(
    "/send", response_model=EnvelopeCreateResponse, status_code=status.HTTP_202_ACCEPTED
)
async def send_for_signature(
    request_data: EnvelopeCreateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Sends a document from S3 for e-signature and returns an envelope ID and signing URLs for embedded signers.
    """
    try:
        response = await docusign_client.send_envelope(
            source_s3_key=request_data.source_s3_key,
            document_name=request_data.document_name,
            signers=request_data.signers,
            return_url=request_data.return_url,
        )

        # Create a record in the database to track this envelope
        new_envelope = ESignEnvelope(
            envelope_id=response["envelope_id"],
            status=response["status"],
            object_type=request_data.object_type,
            object_id=request_data.object_id,
            created_by=current_user.id,
        )
        db.add(new_envelope)
        db.commit()

        return EnvelopeCreateResponse(**response)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error sending envelope: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An internal error occurred while sending the document for signature.",
        )


# @router.post("/webhook", status_code=status.HTTP_204_NO_CONTENT)
# async def docusign_webhook(
#     payload: Dict[str, Any],  # Raw dictionary to handle various Docusign structures
#     db: Session = Depends(get_db),
# ):
#     """
#     Receives status updates from DocuSign Connect.
#     This endpoint should be configured in your DocuSign Connect settings.
#     """
#     logger.info(f"Received DocuSign webhook: {payload}")
#     # The actual processing is handled by the service
#     await esign_service.handle_webhook_event(db, payload)
#     return


@router.post("/webhooks/docusign")
async def docusign_webhooks(
    request: Request,
    db: Session = Depends(get_db),
):
    raw = await request.body()
    headers = {k.lower(): v for k, v in request.headers.items()}

    # HMAC (optional)
    if not utils.verify_hmac(headers, raw):
        raise HTTPException(status_code=401, detail="Bad signature")

    # Parse JSON
    try:
        payload = json.loads(raw.decode("utf-8"))
        logger.info(payload)
        logger.info("\n\n")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    event = (payload.get("event") or "").lower()
    if event not in SUPPORTED_EVENTS:
        # ignore other events cleanly
        return JSONResponse({"status": "ignored", "event": event}, status_code=202)

    module = utils.extract_module(payload)

    if not module:
        # 2xx so DocuSign doesn't retry, but we ignore since 'project' is missing
        return JSONResponse(
            {"status": "ignored", "reason": "missing project custom field"},
            status_code=202,
        )

    fields = (
        payload.get("data")
        .get("envelopeSummary")
        .get("customFields")
        .get("textCustomFields")
    )
    for f in fields:
        if (f.get("name") or "").lower() == "env":
            val = f.get("value")
            if val != settings.environment:
                logger.info("Not the env that should pick the task")
                return JSONResponse(
                    {
                        "status": "ignored",
                        "reason": "Not the env that should pick the task",
                    },
                    status_code=202,
                )
    ctx = utils.extract_ctx(headers, payload)

    # Look up handler
    handler = HANDLERS.get(module, {}).get(event) or HANDLERS["default"].get(event)
    if not handler:
        logger.warning("No handler for module=%s event=%s", module, event)
        return JSONResponse(
            {"status": "no-handler", "module": module, "event": event}, status_code=202
        )

    # Call handler (supports async or sync)
    if inspect.iscoroutinefunction(handler):
        await handler(db, ctx, payload)
    else:
        handler(db, ctx, payload)

    return {"status": "ok"}


@router.get("/envelope/{envelope_id}/status")
async def get_envelope_status(envelope_id: str, db: Session = Depends(get_db)):
    """Manually check the status of an envelope being tracked by the system."""
    envelope = (
        db.query(ESignEnvelope).filter(ESignEnvelope.envelope_id == envelope_id).first()
    )
    if not envelope:
        raise HTTPException(
            status_code=404, detail="Envelope not found in tracking database."
        )
    return {
        "envelope_id": envelope.envelope_id,
        "status": envelope.status,
        "last_updated": envelope.updated_on,
    }


@router.post("/recipient-view", response_model=RecipientViewResponse)
async def create_recipient_view(payload: RecipientViewRequest):
    """
    Generate a one-time embedded signing URL for the given envelope + recipient.
    NOTE: The returned URL is single-use and short-lived (generate just-in-time).
    """
    try:
        # Prefer your helper method in docusign_utils (async version)
        # Ensure you have implemented:
        #   DocusignClient.create_signing_url_async(envelope_id, email, user_name, return_url, client_user_id)
        url = await docusign_client.create_signing_url_async(
            envelope_id=payload.envelope_id,
            email=str(payload.email),
            user_name=payload.user_name,
            return_url=str(payload.return_url)
            if payload.return_url
            else "https://www.google.com",
            client_user_id=payload.client_user_id,
        )
        return {
            "url": url,
            "envelope_id": payload.envelope_id,
            "email": payload.email,
            "user_name": payload.user_name,
            "client_user_id": payload.client_user_id,
        }
    except HTTPException:
        # bubble up DocuSign/validation errors as-is
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to create signing URL: {e}"
        )


@router.get("/envelope/{envelope_id}/signed")
async def download_signed(envelope_id: str, download: bool = Query(True)):
    """
    Streams the combined, signed PDF for a COMPLETED envelope.
    - If the envelope isn't completed yet, returns 409.
    - Set ?download=false to view inline instead of attachment.
    """
    try:
        pdf_bytes = await docusign_client.download_completed_document(envelope_id)
    except ClientResponseError as e:
        # Common when the envelope isn't completed or doc not ready
        if e.status in (400, 404, 409):
            raise HTTPException(
                status_code=409,
                detail="Signed document not available yet (envelope likely not completed).",
            )
        raise HTTPException(status_code=502, detail=f"DocuSign error: {e.status}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=502, detail=f"Failed to fetch signed document: {e}"
        )

    if not pdf_bytes:
        raise HTTPException(status_code=404, detail="No document returned.")

    disp = "attachment" if download else "inline"
    headers = {"Content-Disposition": f'{disp}; filename="envelope_{envelope_id}.pdf"'}
    return StreamingResponse(
        io.BytesIO(pdf_bytes), media_type="application/pdf", headers=headers
    )


@router.get("/envelope/{envelope_id}/host")
async def download_host_signature_url_api(
    envelope_id: str, download: bool = Query(True)
):
    """
    Streams the combined, signed PDF for a COMPLETED envelope.
    - If the envelope isn't completed yet, returns 409.
    - Set ?download=false to view inline instead of attachment.
    """
    try:
        pdf_bytes = await docusign_client.download_host_signature_url(envelope_id)

    except ClientResponseError as e:
        # Common when the envelope isn't completed or doc not ready
        if e.status in (400, 404, 409):
            raise HTTPException(
                status_code=409,
                detail="Signed document not available yet (envelope likely not completed).",
            )
        raise HTTPException(status_code=502, detail=f"DocuSign error: {e.status}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=502, detail=f"Failed to fetch signed document: {e}"
        )

    if not pdf_bytes:
        raise HTTPException(status_code=404, detail="No document returned.")

    disp = "attachment" if download else "inline"
    headers = {"Content-Disposition": f'{disp}; filename="envelope_{envelope_id}.pdf"'}
    return StreamingResponse(
        io.BytesIO(pdf_bytes), media_type="application/pdf", headers=headers
    )
