# app/esign/models.py

from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String

from app.core.db import Base
from app.users.models import AuditMixin


class ESignEnvelope(Base, AuditMixin):
    """
    Model to track the status and metadata of a docusign envelope.
    """
    __tablename__ = "esign_envelopes"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)

    # The unique ID provided by DocuSign for the envelope
    envelope_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    
    # Current status of the envelope (e.g., sent, delivered, signed, completed)
    status: Mapped[str] = mapped_column(String(64))
    
    # The application object this envelope is related to (e.g., 'lease', 'medallion_owner')
    object_type: Mapped[str] = mapped_column(String(64))

    # The ID of the application object it's related to
    object_id: Mapped[int]