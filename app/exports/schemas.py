### app/exports/schemas.py

"""
Pydantic schemas for export API requests and responses.
"""

from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any

from pydantic import BaseModel, Field, field_validator

from app.exports.models import ExportStatus, ExportType, ExportFormat


class ExportRequest(BaseModel):
    """Request schema for creating an export job"""
    
    export_type: ExportType = Field(
        ...,
        description="Type of export (EZPASS, PVB, CURB, LEDGER_POSTINGS, LEDGER_BALANCES)"
    )
    
    format: ExportFormat = Field(
        ...,
        description="Export format (excel, csv, pdf, json)"
    )
    
    filters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Filter parameters to apply to export query"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "export_type": "EZPASS",
                "format": "excel",
                "filters": {
                    "from_posting_date": "2024-01-01",
                    "to_posting_date": "2024-01-31",
                    "status": "POSTED_TO_LEDGER"
                }
            }
        }


class ExportResponse(BaseModel):
    """Response schema for export job creation"""
    
    export_id: int = Field(..., description="Unique export job ID")
    
    status: ExportStatus = Field(..., description="Current status of export job")
    
    message: str = Field(..., description="User-friendly status message")
    
    status_url: str = Field(..., description="URL to check export status")
    
    file_url: Optional[str] = Field(None, description="Download URL (when completed)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "export_id": 123,
                "status": "PENDING",
                "message": "Export job created successfully. Check status at /exports/123/status",
                "status_url": "/api/exports/123/status",
                "file_url": None
            }
        }


class ExportStatusResponse(BaseModel):
    """Response schema for export status check"""
    
    export_id: int = Field(..., description="Export job ID")
    
    export_type: ExportType = Field(..., description="Type of export")
    
    format: ExportFormat = Field(..., description="Export format")
    
    status: ExportStatus = Field(..., description="Current status")
    
    progress: Optional[int] = Field(
        None,
        description="Progress percentage (0-100), if available",
        ge=0,
        le=100
    )
    
    total_records: Optional[int] = Field(
        None,
        description="Total number of records in export"
    )
    
    file_url: Optional[str] = Field(
        None,
        description="Download URL (available when status is COMPLETED)"
    )
    
    file_name: Optional[str] = Field(
        None,
        description="Generated filename"
    )
    
    error_message: Optional[str] = Field(
        None,
        description="Error details (if status is FAILED)"
    )
    
    created_at: datetime = Field(..., description="When export was requested")
    
    completed_at: Optional[datetime] = Field(
        None,
        description="When export finished"
    )
    
    created_by: int = Field(..., description="User ID who created export")
    
    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "export_id": 123,
                "export_type": "EZPASS",
                "format": "excel",
                "status": "COMPLETED",
                "progress": 100,
                "total_records": 15432,
                "file_url": "/api/exports/123/download",
                "file_name": "ezpass_export_20240115_143022.xlsx",
                "error_message": None,
                "created_at": "2024-01-15T14:30:22",
                "completed_at": "2024-01-15T14:32:45",
                "created_by": 1
            }
        }


class ExportListItem(BaseModel):
    """Schema for a single export in list view"""
    
    export_id: int
    export_type: ExportType
    format: ExportFormat
    status: ExportStatus
    total_records: Optional[int]
    file_name: Optional[str]
    created_at: datetime
    completed_at: Optional[datetime]
    
    class Config:
        from_attributes = True


class PaginatedExportListResponse(BaseModel):
    """Response schema for paginated export list"""
    
    items: list[ExportListItem] = Field(..., description="List of exports")
    
    total_items: int = Field(..., description="Total number of exports")
    
    page: int = Field(..., description="Current page number")
    
    per_page: int = Field(..., description="Items per page")
    
    total_pages: int = Field(..., description="Total number of pages")
    
    class Config:
        json_schema_extra = {
            "example": {
                "items": [
                    {
                        "export_id": 123,
                        "export_type": "EZPASS",
                        "format": "excel",
                        "status": "COMPLETED",
                        "total_records": 15432,
                        "file_name": "ezpass_export_20240115_143022.xlsx",
                        "created_at": "2024-01-15T14:30:22",
                        "completed_at": "2024-01-15T14:32:45"
                    }
                ],
                "total_items": 42,
                "page": 1,
                "per_page": 10,
                "total_pages": 5
            }
        }
