### app/medallions/schemas.py

# Standard library imports
from enum import Enum as PyEnum
from typing import Optional, Dict , List

from datetime import date

# Third party imports
from pydantic import BaseModel


class MedallionStatus(str, PyEnum):
    """Medallion status"""
    IN_PROGRESS = "I"
    AVAILABLE = "A"
    ASSIGNED_TO_VEHICLE = "V"
    ACTIVE = "Y"
    ARCHIVED = "N"


class MedallionStatusCheck(str,PyEnum):
    I = "IN PROGRESS"
    A = "AVAILABLE"
    V = "ASSIGNED TO VEHICLE"
    Y = "ACTIVE"
    N = "ARCHIVED"


class MedallionOwnerType(str, PyEnum):
    """Medallion owner Type"""
    INDIVIDUAL = "I"
    CORPORATION = "C"
class MedallionType(str, PyEnum):
    """Medallion Type"""
    REGULAR = "Regular"
    WAV = "WAV"

class VehicleAllocation(BaseModel):
    Regular: List[str] = ["Non-WAV Hybrid", "Non-WAV Gas"]
    Wav: List[str] = ["WAV Hybrid", "WAV Gas"]
class MedallionSearchParams(BaseModel):
    """Medallion search parameters"""
    page: Optional[int] = 1
    per_page: Optional[int] = 1000
    medallion_number: Optional[str] = None
    medallion_status: Optional[str] = None
    medallion_type: Optional[str] = None
    medallion_owner: Optional[str] = None
    renewal_date_from: Optional[date] = None
    renewal_date_to: Optional[date] = None
    lease_expiry_from: Optional[date] = None
    lease_expiry_to: Optional[date] = None
    sort_fields: Optional[Dict[str, str]] = {"created_on": "desc"}


class NEWMED(str, PyEnum):
    """New medallion document types"""
    FS6_DOCUEMNT="fs6"
    SINGED_LIST="signed_lease"
    RENEWAL_RECEIPT="renewal_receipt"