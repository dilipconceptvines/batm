# app/uploads/schemas.py

# Standard library imports
from enum import Enum as PyEnum


class UploadStatus(str, PyEnum):
    """Status of the document upload"""
    NOT_UPLOADED = "not_uploaded"
    UPLOADED = "uploaded"