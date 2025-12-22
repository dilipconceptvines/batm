### app/curb/schemas.py

from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field, ValidationInfo, field_serializer, field_validator

from app.core.config import settings
from app.curb.models import PaymentType


class CurbTripResponse(BaseModel):
    """
    Detailed response schema for a single CURB trip, matching the main table view.
    """

    trip_id: str = Field(..., alias="curb_trip_id")
    driver_id: Optional[str] = Field(None, alias="curb_driver_id")
    tlc_license_no: Optional[str] = None
    vehicle_plate: Optional[str] = Field(None, alias="plate")
    medallion_no: Optional[str] = Field(None, alias="curb_cab_number")
    payment_mode: PaymentType = Field(..., alias="payment_type")
    
    total_amount: Decimal
    fare: Decimal
    tips: Decimal
    tolls: Decimal
    extras: Decimal

    surcharge: Decimal
    improvement_surcharge: Decimal
    congestion_fee: Decimal
    airport_fee: Decimal
    cbdt_fee: Decimal

    # Raw datetime fields from database (used to populate formatted fields)
    start_datetime: Optional[datetime] = Field(None, alias="start_time")
    end_datetime: Optional[datetime] = Field(None, alias="end_time")

    # Formatted date and time fields (will be populated by serializers)
    trip_start_date: Optional[str] = None
    trip_end_date: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    status: Optional[str] = None

    transaction_date: Optional[datetime] = None

    # GPS fields
    start_long: Optional[Decimal] = None
    start_lat: Optional[Decimal] = None
    end_long: Optional[Decimal] = None
    end_lat: Optional[Decimal] = None
    num_service: Optional[int] = None

    @field_serializer("start_datetime", when_used="json")
    def serialize_start_datetime(self, value: Optional[datetime]) -> Optional[str]:
        """Format start date using common_date_format"""
        if value and settings.common_date_format:
            return value.strftime(settings.common_date_format)
        return value.strftime("%Y-%m-%d") if value else None

    @field_serializer("end_datetime", when_used="json")
    def serialize_end_datetime(self, value: Optional[datetime]) -> Optional[str]:
        """Format end date using common_date_format"""
        if value and settings.common_date_format:
            return value.strftime(settings.common_date_format)
        return value.strftime("%Y-%m-%d") if value else None

    @field_serializer("trip_start_date", when_used="json")
    def serialize_trip_start_date(self, value: Optional[str]) -> Optional[str]:
        """Format trip start date and time from start_datetime"""
        if self.start_datetime and settings.common_date_format and settings.common_time_format:
            return f"{self.start_datetime.strftime(settings.common_date_format)} {self.start_datetime.strftime(settings.common_time_format)}"
        return None

    @field_serializer("trip_end_date", when_used="json")
    def serialize_trip_end_date(self, value: Optional[str]) -> Optional[str]:
        """Format trip end date and time from end_datetime"""
        if self.end_datetime and settings.common_date_format and settings.common_time_format:
            return f"{self.end_datetime.strftime(settings.common_date_format)} {self.end_datetime.strftime(settings.common_time_format)}"
        return None

    @field_serializer("start_time", when_used="json")
    def serialize_start_time(self, value: Optional[str]) -> Optional[str]:
        """Format start time from start_datetime"""
        if self.start_datetime:
            if settings.common_time_format:
                return self.start_datetime.strftime(settings.common_time_format)
            return self.start_datetime.strftime("%H:%M:%S")
        return None

    @field_serializer("end_time", when_used="json")
    def serialize_end_time(self, value: Optional[str]) -> Optional[str]:
        """Format end time from end_datetime"""
        if self.end_datetime:
            if settings.common_time_format:
                return self.end_datetime.strftime(settings.common_time_format)
            return self.end_datetime.strftime("%H:%M:%S")
        return None

    @field_serializer("transaction_date", when_used="json")
    def serialize_transaction_date(self, value: Optional[datetime]) -> Optional[str]:
        """Format transaction date and time using common_date_format and common_time_format"""
        if value and settings.common_date_format and settings.common_time_format:
            return f"{value.strftime(settings.common_date_format)} {value.strftime(settings.common_time_format)}"
        return None

    class Config:
        """Pydantic configuration for aliasing and population by name."""
        from_attributes = True
        populate_by_name = True


class PaginatedCurbTripResponse(BaseModel):
    """
    Paginated response schema for a list of CURB trips.
    """

    items: List[CurbTripResponse]
    total_items: int
    page: int
    per_page: int
    total_pages: int


# --- Granular Import Request Schemas ---


class CurbDriverImportRequest(BaseModel):
    """
    Request schema for importing CURB data for a specific driver.
    """

    driver_id: Optional[str] = Field(None, description="Driver ID (internal system ID)")
    tlc_license_no: Optional[str] = Field(None, description="TLC License Number")
    start_date: date = Field(description="Start date for import (YYYY-MM-DD)")
    end_date: date = Field(description="End date for import (YYYY-MM-DD)")

    @field_validator("end_date")
    @classmethod
    def validate_date_range(cls, v, info: ValidationInfo):
        data = info.data if hasattr(info, "data") else {}
        if "start_date" in data and v < data["start_date"]:
            raise ValueError("end_date must be greater than or equal to start_date")
        return v

    @field_validator("tlc_license_no")
    @classmethod
    def validate_driver_identifier(cls, v, info: ValidationInfo):
        # At least one driver identifier must be provided
        data = info.data if hasattr(info, "data") else {}
        driver_id = data.get("driver_id")
        if not v and not driver_id:
            raise ValueError("Either driver_id or tlc_license_no must be provided")
        return v


class CurbMedallionImportRequest(BaseModel):
    """
    Request schema for importing CURB data for a specific medallion.
    """

    medallion_number: str = Field(description="Medallion number to import data for")
    start_date: date = Field(description="Start date for import (YYYY-MM-DD)")
    end_date: date = Field(description="End date for import (YYYY-MM-DD)")

    @field_validator("end_date")
    @classmethod
    def validate_date_range(cls, v, info: ValidationInfo):
        data = info.data if hasattr(info, "data") else {}
        if "start_date" in data and v < data["start_date"]:
            raise ValueError("end_date must be greater than or equal to start_date")
        return v


class CurbDateRangeImportRequest(BaseModel):
    """
    Request schema for importing CURB data for a specific date range.
    """

    start_date: date = Field(description="Start date for import (YYYY-MM-DD)")
    end_date: date = Field(description="End date for import (YYYY-MM-DD)")
    driver_ids: Optional[List[str]] = Field(
        None, description="Optional list of specific driver IDs to filter"
    )
    medallion_numbers: Optional[List[str]] = Field(
        None, description="Optional list of specific medallion numbers to filter"
    )

    @field_validator("end_date")
    @classmethod
    def validate_date_range(cls, v, info: ValidationInfo):
        data = info.data if hasattr(info, "data") else {}
        if "start_date" in data and v < data["start_date"]:
            raise ValueError("end_date must be greater than or equal to start_date")
        return v


class CurbImportResponse(BaseModel):
    """
    Response schema for CURB import operations.
    """

    task_id: str = Field(description="Celery task ID for tracking")
    message: str = Field(description="Import initiation message")
    import_type: str = Field(
        description="Type of import (driver, medallion, date_range)"
    )
    parameters: dict = Field(description="Parameters used for the import")


class CurbImportStatusResponse(BaseModel):
    """
    Response schema for CURB import status and results.
    """

    task_id: str
    status: str  # PENDING, SUCCESS, FAILURE, etc.
    result: Optional[dict] = None
    error: Optional[str] = None
