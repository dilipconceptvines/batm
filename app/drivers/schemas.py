## app/drivers/schemas.py

# Standard library imports
from enum import Enum as PyEnum
from typing import Optional

# Third party imports
from pydantic import BaseModel


class DriverStatus(str, PyEnum):
    """
    All the driver statuses in the system
    """

    IN_PROGRESS = "In Progress"
    REGISTERED = "Registered"
    ACTIVE = "Active"
    INACTIVE = "Inactive"


class NEWDR(str, PyEnum):
    """All the required documents for a new driver"""

    TLC_LICENSE = "tlc_license"
    DMV_LICENSE = "dmv_license"
    DRIVER_SSN = "driver_ssn"
    DRIVER_PHOTO = "driver_photo"


class DOVFinancialInfo(BaseModel):
    """
    Financial information for a DOV lease
    """

    tlc_vehicle_lifetime_cap: Optional[float] = 0
    amount_collected: Optional[float] = 0
    lease_amount: Optional[float] = 0
    med_lease: Optional[float] = 0
    med_tlc_maximum_amount: Optional[float] = 0
    veh_lease: Optional[float] = 0
    veh_sales_tax: Optional[float] = 0
    tlc_inspection_fees: Optional[float] = 0
    tax_stamps: Optional[float] = 0
    registration: Optional[float] = 0
    veh_tlc_maximum_amount: Optional[float] = 0
    total_vehicle_lease: Optional[float] = 0
    total_medallion_lease_payment: Optional[float] = 0        

class DOVLease(BaseModel):
    """
    DOV lease schema
    """

    lease_type: str
    financial_information: DOVFinancialInfo
