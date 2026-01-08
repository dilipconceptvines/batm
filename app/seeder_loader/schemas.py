### app/data_loader/schemas.py

"""
Pydantic schemas for Data Loader API responses
"""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel


class FileUploadResponse(BaseModel):
    """Response model for file upload"""

    status: str
    message: str
    job_folder: str
    file_name: str
    s3_key: str
    uploaded_at: str


class FileInfo(BaseModel):
    """Model for file information with optional presigned URL"""

    file_name: str
    s3_key: str
    file_size: int
    last_modified: str
    presigned_url: Optional[str] = None


class FileListResponse(BaseModel):
    """Response model for file listing with pagination"""

    status: str
    job_folder: str
    file_count: int
    files: List[FileInfo]
    has_more: bool = False
    next_continuation_token: Optional[str] = None


class ParseRequest(BaseModel):
    """Request model for parse API"""
    all_parses: Optional[bool] = False
    parser_names: Optional[List[str]] = []
    s3_key: str
    job_folder: Optional[str] = None
    dry_run: bool = False


class ParserResult(BaseModel):
    """Result for a single parser execution"""

    parser_name: str
    status: str
    message: str
    sheets_processed: List[str]
    error: Optional[str] = None


class ParseResponse(BaseModel):
    """Response model for parse API"""

    status: str
    message: str
    job_folder: Optional[str]
    s3_key: str
    report_presigned_url: Optional[str] = None
    results: List[ParserResult]
    total_parsers: int
    successful: int
    failed: int
    parse_results: Optional[Dict[str, Any]]= None



class ParserInfo(BaseModel):
    """Model for parser information"""

    name: str
    sheet_names: List[str]
    version: str
    deprecated: bool
    description: Optional[str] = None


class ListParsersResponse(BaseModel):
    """Response model for list parsers API"""

    status: str
    total_parsers: int
    parsers: List[ParserInfo]


class FolderInfo(BaseModel):
    """Model for job folder information"""

    folder_name: str
    last_modified: str
    file_count: int


class FolderListResponse(BaseModel):
    """Response model for folder listing"""

    status: str
    folder_count: int
    folders: List[FolderInfo]
