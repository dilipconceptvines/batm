# app/uploads/services.py

from datetime import datetime, timezone
from typing import List, Optional, Union

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.uploads.models import Document
from app.utils.logger import get_logger

logger = get_logger(__name__)


class UploadService:
    """Service for uploading documents to the database"""

    def get_documents(
        self,
        db: Session,
        document_id: Optional[int] = None,
        object_type: Optional[str] = None,
        object_id: Optional[int] = None,
        document_type: Optional[str] = None,
        like_document_type: Optional[str] = None,
        sort_by: Optional[str] = "created_on",
        sort_order: Optional[str] = "desc",
        multiple: bool = False,
    ) -> Union[List[Document], Document, None]:
        """Get documents from the database using modern SQLAlchemy 2.x style."""
        try:
            stmt = select(Document)
            if document_id:
                stmt = stmt.where(Document.id == document_id)
            if object_type:
                stmt = stmt.where(Document.object_type.ilike(f"%{object_type}%"))
            if object_id:
                stmt = stmt.where(Document.object_lookup_id == object_id)
            if document_type:
                stmt = stmt.where(Document.document_type == document_type)
            if like_document_type:
                stmt = stmt.where(
                    Document.document_type.ilike(f"%{like_document_type}%")
                )

            if sort_by and hasattr(Document, sort_by):
                column = getattr(Document, sort_by)
                stmt = stmt.order_by(
                    column.desc() if sort_order == "desc" else column.asc()
                )

            if multiple:
                result = db.execute(stmt)
                documents = result.scalars().all()
                return [doc.to_dict() for doc in documents]

            result = db.execute(stmt)
            document = result.scalar_one_or_none()

            if document:
                return document.to_dict()

            # Return a default structure if no document is found
            return {
                "document_id": "",
                "document_name": "",
                "document_note": "",
                "document_path": "",
                "document_type": document_type or "",
                "document_date": "",
                "document_object_type": object_type or "",
                "document_object_id": object_id or "",
                "document_size": "",
                "document_uploaded_date": "",
                "presigned_url": "",
            }
        except Exception as e:
            logger.error("Error getting documents: %s", str(e))
            raise e

    def create_document(
        self,
        db: Session,
        new_filename: str,
        original_extension: str,
        file_size_kb: int,
        document_path: str,
        notes: str,
        document_type: str,
        object_type: str,
        object_id: int,
        document_date: str,
    ) -> Document:
        """Create a new document in the database"""
        try:
            new_document = Document(
                document_name=new_filename,
                document_type=document_type,
                document_format=original_extension.upper().lstrip("."),
                document_actual_size=int(file_size_kb),
                document_saved_size=int(file_size_kb),
                document_path=document_path,
                document_note=notes,
                object_type=object_type,
                object_lookup_id=str(object_id),
                document_upload_date=datetime.now(timezone.utc),
                document_date=datetime.strptime(document_date, "%Y-%m-%d"),
            )
            db.add(new_document)
            db.flush()
            db.refresh(new_document)
            return new_document
        except Exception as e:
            logger.error("Error creating document: %s", str(e))
            raise e

    def update_document(
        self,
        db: Session,
        document_dict: dict,
        new_filename: str,
        original_extension: str,
        file_size_kb: int,
        document_path: str,
        notes: str,
        document_type: str,
        object_type: str,
        object_id: int,
        document_date: str,
    ) -> Document:
        """Update a document in the database"""
        try:
            stmt = select(Document).where(Document.id == document_dict["document_id"])
            result = db.execute(stmt)
            document = result.scalar_one_or_none()

            if not document:
                raise ValueError("Document to update not found")

            document.document_date = datetime.strptime(document_date, "%Y-%m-%d")
            document.document_name = new_filename
            document.document_type = document_type
            document.document_format = original_extension.upper().lstrip(".")
            document.document_actual_size = int(file_size_kb)
            document.document_saved_size = int(file_size_kb)
            document.document_path = document_path
            document.document_note = notes
            document.object_type = object_type
            document.object_lookup_id = str(object_id)
            document.document_upload_date = datetime.now(timezone.utc)

            db.commit()
            db.refresh(document)
            return document
        except Exception as e:
            logger.error("Error updating document: %s", str(e))
            raise e

    def delete_document(self, db: Session, document_id: int) -> bool:
        """Delete a document from the database"""
        try:
            stmt = select(Document).where(Document.id == document_id)
            result = db.execute(stmt)
            document = result.scalar_one_or_none()

            if document:
                db.delete(document)
                db.commit()
            return True
        except Exception as e:
            logger.error("Error deleting document: %s", str(e))
            raise e

    def upsert_document(self, db: Session, document_data: dict) -> Document:
        """
        Creates a new document or updates an existing one from a dictionary.
        This is the primary method for modifying document records.
        """
        try:
            document_id = document_data.get("id") or document_data.get("document_id")

            document = None
            if document_id:
                stmt = select(Document).where(Document.id == document_id)
                document = db.execute(stmt).scalar_one_or_none()

            if document:
                # Update existing document
                for key, value in document_data.items():
                    if hasattr(document, key):
                        setattr(document, key, value)
            else:
                # Create a new document if it doesn't exist
                # Remove 'id' if present, as it's for a non-existent record
                document_data.pop("id", None)
                document_data.pop("document_id", None)
                document = Document(**document_data)
                db.add(document)

            db.commit()
            db.refresh(document)
            return document

        except Exception as e:
            db.rollback()
            logger.error(f"Error upserting document: {e}", exc_info=True)
            raise e


upload_service = UploadService()
