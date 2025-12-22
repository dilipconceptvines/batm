# app/esign/services.py

from io import BytesIO
import uuid

from sqlalchemy.orm import Session

from app.utils.logger import get_logger
from app.esign.models import ESignEnvelope
from app.utils.docusign_utils import docusign_client
from app.utils.s3_utils import s3_utils
from app.uploads.services import upload_service

logger = get_logger(__name__)

class ESignService:
    """Service for managing electronic signatures via DocuSign."""
    async def handle_webhook_event(self, db: Session, payload: dict):
        """
        Requirement #2 & #3: Process webhook events from DocuSign.
        Updates envelope status and downloads the document upon completion.
        """
        event = payload.get('event')
        data = payload.get('data', {})
        envelope_id = data.get('envelopeId')
        
        if not event or not envelope_id:
            logger.warning("Webhook received with missing event or envelopeId.")
            return

        # Find the envelope in our database
        envelope_record = db.query(ESignEnvelope).filter(ESignEnvelope.envelope_id == envelope_id).first()
        if not envelope_record:
            logger.warning(f"Received webhook for untracked envelopeId: {envelope_id}")
            return

        # Update status based on event
        new_status = event.replace("envelope-", "") # e.g., 'completed', 'sent', 'delivered'
        envelope_record.status = new_status
        db.commit()
        logger.info(f"Updated envelope {envelope_id} status to '{new_status}'")

        # If the document signing is complete, download and save it
        if new_status == 'completed':
            try:
                # Download the signed PDF
                pdf_bytes = await docusign_client.download_completed_document(envelope_id)
                
                # Upload to our S3 bucket
                final_s3_key = f"signed_documents/{envelope_record.object_type}/{envelope_record.object_id}/{uuid.uuid4()}.pdf"
                s3_utils.upload_file(BytesIO(pdf_bytes), final_s3_key, "application/pdf")
                
                # Create a record in our `document` table
                document_data = {
                    "document_name": f"Signed Document for {envelope_record.object_type} {envelope_record.object_id}",
                    "document_type": "signed_agreement",
                    "document_format": "PDF",
                    "document_path": final_s3_key,
                    "object_type": envelope_record.object_type,
                    "object_lookup_id": str(envelope_record.object_id),
                    "document_note": f"Completed via DocuSign envelope {envelope_id}"
                }
                upload_service.upsert_document(db, document_data=document_data)
                logger.info(f"Successfully downloaded and saved completed document for envelope {envelope_id} to {final_s3_key}")

            except Exception as e:
                logger.error(f"Failed to process completed envelope {envelope_id}: {e}", exc_info=True)
                envelope_record.status = 'completed_download_failed'
                db.commit()

esign_service = ESignService()