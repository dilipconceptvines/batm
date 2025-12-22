# app/uploads/router.py

import uuid
from pathlib import Path

from fastapi import (
    APIRouter, Depends, HTTPException, UploadFile, Form, File, status
)
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.utils.logger import get_logger
from app.core.config import settings
from app.uploads.services import upload_service
from app.utils.file_utils import validate_file
from app.utils.s3_utils import s3_utils
from app.users.models import User
from app.users.utils import get_current_user

logger = get_logger(__name__)
router = APIRouter(tags=["Documents"])

@router.post("/upload-document")
def upload_document(
    file: UploadFile = File(default=None),
    notes: str = Form(...),
    object_type: str = Form(...),
    object_id: str = Form(...),
    document_type: str = Form(...),
    document_id: int = Form(None),
    document_date: str = Form(...),
    document_name :str = Form(None),
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user)
) -> JSONResponse:
    """Upload a document to the database"""
    try:
        # Validate the file
        is_valid, error = validate_file(file)
        if not is_valid:
            logger.error("Invalid file", error_message=error)
            raise HTTPException(status_code=400, detail=error)
        
        document = None
        if document_id:
            document = upload_service.get_documents(db, document_id=document_id)
            if not document or not document.get("document_id"):
                 document = None # Ensure we create a new one if not found
        
        # Build S3 Path
        subdirectory_path = f"{settings.document_storage_dir}/{object_type}/{object_id}"
        original_extension = Path(file.filename).suffix
        sanitize_file_name = document_type.replace(" ", "_").lower()
        new_filename = f"{sanitize_file_name}{original_extension}" if not document_name else f"{document_name}{original_extension}"
        document_path = f"{subdirectory_path}/{new_filename}"

        # Upload to s3
        s3_utils.upload_file(file_obj=file.file, key=document_path)

        file_size_kb = file.size / 1024

        if document:
            updated_document = upload_service.update_document(
                db, document, new_filename, original_extension, file_size_kb,
                document_path, notes, document_type, object_type, object_id,
                document_date
            )
        else:
            updated_document = upload_service.create_document(
                db, new_filename, original_extension, file_size_kb,
                document_path, notes, document_type, object_type, object_id,
                document_date
            )

        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={
                "status": "success",
                "message": "Document uploaded successfully",
                "document": updated_document.to_dict()
            }
        )
    except Exception as e:
        logger.error("Error uploading document", error_message=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error") from e
    
@router.delete("/delete-document/{document_id}")
def delete_s3_document(
    document_id: int,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user)
) -> JSONResponse:
    """Delete a document from S3"""
    try:
        document = upload_service.get_documents(db, document_id=document_id)
        if not document or not document.get("document_path"):
            logger.error("Document not found: %s", document_id)
            raise HTTPException(status_code=404, detail="Document not found")
        
        s3_utils.delete_file(document.get("document_path"))
        upload_service.delete_document(db, document_id)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "status": "success",
                "message": "Document deleted successfully"
            }
        )
    except Exception as e:
        logger.error("Error deleting document: %s", str(e))
        raise HTTPException(status_code=500, detail="Internal server error") from e
    
@router.post("/upload-static-document")
def upload_static_document(
    file: UploadFile = File(default=None),
    document_type: str = Form(..., description="seeder, json, or other"),
    logged_in_user: User = Depends(get_current_user)
) -> JSONResponse:
    """Upload a static document to the database"""
    try:
        document_path = f"{document_type}/{file.filename}"
        s3_utils.upload_file(file_obj=file.file, key=document_path)

        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={
                "status": "success",
                "message": "Document uploaded successfully"
            }
        )
    except Exception as e:
        logger.error("Error uploading static document: %s", str(e))
        raise HTTPException(status_code=500, detail="Internal server error") from e
    
@router.get("/get-static/{document_type}/{filename}")
def get_static_document(
    document_type: str,
    filename: str,
    logged_in_user: User = Depends(get_current_user)
) -> StreamingResponse:
    """Download a static document from S3"""
    try:
        file_obj = s3_utils.download_file(key=f"{document_type}/{filename}")
        return StreamingResponse(file_obj, media_type="application/octet-stream")
    except Exception as e:
        logger.error("Error getting static document: %s", str(e))
        raise HTTPException(status_code=500, detail="Internal server error") from e