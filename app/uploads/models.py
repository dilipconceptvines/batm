### app/uploads/models.py

from datetime import datetime
from typing import Optional

from sqlalchemy import Integer, DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

# Local application imports
from app.core.db import Base
from app.users.models import AuditMixin
from app.utils.s3_utils import s3_utils

class Document(Base, AuditMixin):
    """
    Document model using modern SQLAlchemy 2.x syntax.
    """
    __tablename__ = "document"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False, comment="Primary Key for Documents")
    document_date: Mapped[Optional[datetime]] = mapped_column(DateTime, comment="Date of the document.")
    document_name: Mapped[Optional[str]] = mapped_column(String(255), comment="Name of the document")
    document_format: Mapped[Optional[str]] = mapped_column(String(4), comment="Format of the document such as PDF, PNG, DOC, etc.")
    document_actual_size: Mapped[Optional[int]] = mapped_column(Integer, comment="Size of the document in KB")
    document_path: Mapped[Optional[str]] = mapped_column(String(255), comment="Path where the document is stored")
    document_saved_size: Mapped[Optional[int]] = mapped_column(Integer, comment="Size of the document saved in KB.")
    document_upload_date: Mapped[Optional[datetime]] = mapped_column(DateTime, comment="Date and time when the document was scanned or uploaded")
    document_note: Mapped[Optional[str]] = mapped_column(String(255), comment="Notes about the document")
    document_type: Mapped[Optional[str]] = mapped_column(String(255), comment="Type of the document for eg ssn, license")
    object_type: Mapped[Optional[str]] = mapped_column(String(20), comment="The type of the object, for eg medallion, vehicle, driver, lease etc")
    object_lookup_field: Mapped[Optional[str]] = mapped_column(String(10), comment="A lookup to indicate the type of object medallion id, medallion_number")
    object_lookup_id: Mapped[Optional[str]] = mapped_column(String(128), comment="The ID on which the lookup needs to happen")
    
    @property
    def presigned_url(self):
        """
        Get the presigned URL for the document
        """
        if self.document_path:
            return s3_utils.generate_presigned_url(self.document_path)
        return None
    
    def to_dict(self):
        """
        Convert the Document object to a dictionary
        """
        return {
            "document_id": self.id,
            "document_date": self.document_date.isoformat() if self.document_date else None,
            "document_name": self.document_name,
            "document_format": self.document_format,
            "document_actual_size": self.document_actual_size,
            "document_path": self.document_path,
            "document_saved_size": self.document_saved_size,
            "document_size": self.document_actual_size,
            "document_upload_date": self.document_upload_date.isoformat() if self.document_upload_date else None,
            "document_note": self.document_note,
            "document_type": self.document_type,
            "object_type": self.object_type,
            "object_lookup_field": self.object_lookup_field,
            "object_lookup_id": self.object_lookup_id,
            "presigned_url": self.presigned_url
        }