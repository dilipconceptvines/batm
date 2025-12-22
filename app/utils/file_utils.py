# app/utils/file_utils.py

import os
from typing import Tuple, Optional

from fastapi import UploadFile

from app.core.config import settings

def validate_file(file: UploadFile) -> Tuple[bool, Optional[str]]:
    """
    Validates a file's type and size based on application settings.

    Args:
        file: The UploadFile object from FastAPI.

    Returns:
        A tuple containing a boolean (True if valid) and an optional error message string.
    """
    # 1. Validate file type based on extension
    # Reads the comma-separated list of allowed extensions from settings.
    allowed_types = {ext.strip().lower() for ext in settings.allowed_file_types.split(',')}
    file_ext = os.path.splitext(file.filename)[1].lower().lstrip('.')

    if not file_ext:
        return False, "File must have an extension."

    if file_ext not in allowed_types:
        return False, f"File type '.{file_ext}' is not allowed. The allowed types are: {', '.join(allowed_types)}."

    # 2. Validate file size (in bytes)
    # The setting allowed_file_size is assumed to be in Kilobytes (KB).
    max_size_in_bytes = settings.allowed_file_size * 1024
    if file.size > max_size_in_bytes:
        return False, f"File size of {file.size / 1024:.2f} KB exceeds the maximum allowed size of {settings.allowed_file_size} KB."

    return True, None