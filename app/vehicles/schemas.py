### app/vehicles/schemas.py

# Standard library imports
from datetime import datetime , date
from enum import Enum as PyEnum
from typing import Optional

from pydantic import BaseModel, field_validator, model_validator


class VehicleStatus(str, PyEnum):
    """All the vehicle statuses in the system"""

    IN_PROGRESS = "In Progress"
    PENDING_DELIVERY = "Pending Delivery"
    AVAILABLE = "Delivered"
    AVAILABLE_FOR_HACK_UP = "Available for Hackup"
    HACK_UP_IN_PROGRESS = "Hack-up In Progress"
    HACKED_UP = "Available"
    OUT_OF_SERVICE = "Out of Service"
    ACTIVE = "Active"
    ARCHIVED = "Archived"
    DE_HACK_UP_IN_PROGRESS = "De Hack-up In Progress"

class VehicleType(str, PyEnum):
    """All the vehicle types in the system"""
    NonWavHybrid = "Non-WAV Hybrid"
    NonWavGas = "Non-WAV Gas"
    WavHybrid = "WAV Hybrid"
    WavGas = "WAV Gas"


class HackupStatus(str, PyEnum):
    """All the vehicle hackup statuses in the system"""

    ACTIVE = "Active"
    INACTIVE = "Inactive"
    INPROGRESS = "In Progress"

class VehicleEntityStatus(str , PyEnum):
    ACTIVE = "Active"
    INACTIVE = "Inactive"

class RegistrationStatus(str, PyEnum):
    """All the vehicle registration statuses in the system"""

    ACTIVE = "active"
    INACTIVE = "inactive"


class NEWVR(str, PyEnum):
    """New vehicle document types"""

    VEHICLE_TYPE = "vehicle_type"
    DOCUMENT_2 = "document2"
    DOCUMENT_3 = "document3"


class HackUpData(BaseModel):
    tpep_provider: Optional[str] = None
    configuration_type: Optional[str] = None

    is_paint_completed: Optional[bool] = None
    paint_completed_date: Optional[datetime] = None
    paint_completed_charges: Optional[float] = None
    paint_status: Optional[str] = None
    paint_from_location: Optional[str] = None
    paint_to_location: Optional[str] = None

    is_camera_installed: Optional[bool] = None
    camera_type: Optional[str] = None
    camera_installed_date: Optional[datetime] = None
    camera_installed_charges: Optional[float] = None
    camera_status: Optional[str] = None
    camera_from_location: Optional[str] = None
    camera_to_location: Optional[str] = None

    is_partition_installed: Optional[bool] = None
    partition_type: Optional[str] = None
    partition_installed_date: Optional[datetime] = None
    partition_installed_charges: Optional[float] = None
    partition_status: Optional[str] = None
    partition_from_location: Optional[str] = None
    partition_to_location: Optional[str] = None

    is_meter_installed: Optional[bool] = None
    meter_type: Optional[str] = None
    meter_serial_number: Optional[str] = None
    meter_installed_charges: Optional[float] = None
    meter_installed_date: Optional[datetime] = None
    meter_status: Optional[str] = None
    meter_from_location: Optional[str] = None
    meter_to_location: Optional[str] = None

    is_rooftop_installed: Optional[bool] = None
    rooftop_type: Optional[str] = None
    rooftop_installed_date: Optional[datetime] = None
    rooftop_installation_charges: Optional[float] = None
    rooftop_status: Optional[str] = None
    rooftop_from_location: Optional[str] = None
    rooftop_to_location: Optional[str] = None

    @field_validator('*', mode='before')
    @classmethod
    def convert_empty_to_none(cls, values):
        for field, value in values.items():
            if value in [0, "", []]:
                values[field] = None
        return values


class NewDealer(BaseModel):
    dealer_name: Optional[str] = None
    dealer_bank_name: Optional[str] = None
    dealer_bank_account_number: Optional[str] = None


class ProcessTypeEnum(str, PyEnum):
    paint = "Paint"
    meter = "Meter"
    rooftop = "Rooftop"
    camera = "Camera"
    partition = "Partition"


class ProcessStatusEnum(str, PyEnum):
    pending = "pending"
    in_progress = "in_progress"
    completed = "completed"


class HackupProcessStatus(BaseModel):
    process_type: ProcessTypeEnum
    location: Optional[str] = None
    status: ProcessStatusEnum

class DeliveryData(BaseModel):
    is_delivered: Optional[bool] = None
    expected_delivery_date: Optional[date] = None
    delivery_location: Optional[str] = None
    delivery_note: Optional[str] = None
    vehicle_status: VehicleStatus = VehicleStatus.AVAILABLE

class ExpensesAndComplianceCategory(str, PyEnum):
    VEHICLE_PURCHASE = "vehicle_purchase"
    VEHICLE_HACKUP = "vehicle_hackup"
    REPAIRS_AND_MAINTENANCE = "repairs_and_maintenance"
    INSPECTIONS_AND_COMPLIANCE = "inspections_and_compliance"
    OTHER_VHICLE_DOCUMENTS = "other_vehicle_documents"

class ExpensesAndComplianceSubType(str, PyEnum):
    OTHERS = "others"
    PAINT = "paint"
    METER = "meter"
    ROOFTOP = "rooftop"
    CAMERA = "camera"
    PARTITION = "partition"
    INVOICE = "invoice"
    SERVICE_AND_MAINTENANCE = "service_and_maintenance"
    REPAIRS = "repairs"
    TLC_INSPECTION = "tlc_inspection"
    MILE_RUN_INSPECTION = "mile_run_inspection"
    DMV_INSPECTION = "dmv_inspection"
    INSURANCE = "insurance"
    WARRANTY_DOCUMENT = "warranty_document"
    LOAN_FINANCE_DOCUMENT = "loan/finance_agreement"
    ACCIDENT_INCIDENT_REPORT = "accident/incident_report"
    VEHICLE_PHOTO_DAMAGE_DOCUMENTION = "vehicle_photo/damage_documentation"
    INSURANCE_RIDER_ENDORSEMENT = "insurance_rider/endorsement"
    Liability_Insurance = "liability_insurance"
    Worker_Compensation_Insurance = "worker_compensation_insurance"

class VehiclePurchaseExpenseSchema(BaseModel):
    id: Optional[int] = None
    vehicle_id: int
    category: Optional[ExpensesAndComplianceCategory] = None
    sub_type: Optional[ExpensesAndComplianceSubType] = None
    invoice_number: Optional[str] = None
    amount: Optional[float] = 0.0
    base_price: Optional[float] = 0.0
    sales_tax: Optional[float] = 0.0
    vendor_name: Optional[str] = None
    issue_date: Optional[date] = None
    expiry_date: Optional[date] = None
    note : Optional[str] = None
    document_id: Optional[int] = None
    @model_validator(mode="after")
    def validate_category_and_subtype(self):
        allowed = {}
        if self.category == ExpensesAndComplianceCategory.VEHICLE_PURCHASE:
            allowed = {
                ExpensesAndComplianceSubType.INVOICE,
            }
        if self.sub_type not in allowed:
            raise ValueError(f"Invalid combination of category and sub_type for category '{self.category}'")
        
        return self  
    class Config:
        from_attributes = True

class VehicleHackupExpenseSchema(BaseModel):
    id: Optional[int] = None
    vehicle_id: int
    category: Optional[ExpensesAndComplianceCategory] = None
    sub_type: Optional[ExpensesAndComplianceSubType] = None
    invoice_number: Optional[str] = None
    amount: Optional[float] = 0.0
    meter_serial_no: Optional[str] = None
    vendor_name: Optional[str] = None
    issue_date: Optional[date] = None
    expiry_date: Optional[date] = None
    note : Optional[str] = None
    document_id: Optional[int] = None

    @model_validator(mode="after")
    def validate_category_and_subtype(self):
        allowed = {}
        if self.category == ExpensesAndComplianceCategory.VEHICLE_HACKUP:
            allowed = {
                ExpensesAndComplianceSubType.PAINT,
                ExpensesAndComplianceSubType.METER,
                ExpensesAndComplianceSubType.ROOFTOP,
                ExpensesAndComplianceSubType.CAMERA,
                ExpensesAndComplianceSubType.PARTITION,
                ExpensesAndComplianceSubType.OTHERS,
            }
        if self.sub_type not in allowed:
            raise ValueError(f"Invalid combination of category and sub_type for category '{self.category}'")
        
        return self  

    class Config:
        from_attributes = True

class VehicleRepairsMaintenanceExpenseSchema(BaseModel):
    id: Optional[int] = None
    vehicle_id: int
    category: Optional[ExpensesAndComplianceCategory] = None
    sub_type: Optional[ExpensesAndComplianceSubType] = None
    invoice_number: Optional[str] = None
    amount: Optional[float] = 0.0
    vendor_name: Optional[str] = None
    issue_date: Optional[date] = None
    expiry_date: Optional[date] = None
    note : Optional[str] = None
    document_id: Optional[int] = None

    @model_validator(mode="after")
    def validate_category_and_subtype(self):
        allowed = {}
        if self.category == ExpensesAndComplianceCategory.REPAIRS_AND_MAINTENANCE:
            allowed = {
                ExpensesAndComplianceSubType.SERVICE_AND_MAINTENANCE,
                ExpensesAndComplianceSubType.REPAIRS,
            }
        if self.sub_type not in allowed:
            raise ValueError(f"Invalid combination of category and sub_type for category '{self.category}'")
        
        return self  

    class Config:
        from_attributes = True

class VehicleInspectionsComplianceExpenseSchema(BaseModel):
    id: Optional[int] = None
    vehicle_id: int
    category: Optional[ExpensesAndComplianceCategory] = None
    sub_type: Optional[ExpensesAndComplianceSubType] = None
    invoice_number: Optional[str] = None
    specific_info : Optional[str] = None
    amount: Optional[float] = 0.0
    vendor_name: Optional[str] = None
    issue_date: Optional[date] = None
    expiry_date: Optional[date] = None
    note : Optional[str] = None
    document_id: Optional[int] = None

    @model_validator(mode="after")
    def validate_category_and_subtype(self):
        allowed = {}
        if self.category == ExpensesAndComplianceCategory.INSPECTIONS_AND_COMPLIANCE:
            allowed = {
                ExpensesAndComplianceSubType.TLC_INSPECTION,
                ExpensesAndComplianceSubType.MILE_RUN_INSPECTION,
                ExpensesAndComplianceSubType.DMV_INSPECTION,
                ExpensesAndComplianceSubType.Worker_Compensation_Insurance,
                ExpensesAndComplianceSubType.Liability_Insurance,
                ExpensesAndComplianceSubType.OTHERS,
            }
        if self.sub_type not in allowed:
            raise ValueError(f"Invalid combination of category and sub_type for category '{self.category}'")
        
        return self  

    class Config:
        """Pydantic configuration"""
        from_attributes = True

class VehicleOtherDocumentsExpenseSchema(BaseModel):
    id: Optional[int] = None
    vehicle_id: int
    category: Optional[ExpensesAndComplianceCategory] = None
    sub_type: Optional[ExpensesAndComplianceSubType] = None
    issue_date: Optional[date] = None
    expiry_date: Optional[date] = None
    note : Optional[str] = None

class VehicleLocation(str, PyEnum):
    BAT_OFFICE = "BAT Office"
    BAT_GARAGE = "DSW Auto Repair (BAT Garage)"
    PARKING_LOT_1 = "Parking Lot 1"
    PARKING_LOT_2 = "Parking Lot 2"
    MAACO = "Maaco"
    METRO_SHOP = "Metro Shop"
    TAXICAB_PRODUCTS = "Taxicab Products"
    HUDSON_TOYOTA ="Hudson Toyota"
    HILLSIDE_TOYOTA ="Hillside Toyota"
    OTHER = "Other"


class UpdateVehicleLocation(BaseModel):
    vehicle_ids: list = []
    new_location: Optional[VehicleLocation] = None
    specific_location: Optional[str] = None
    comment_for_location_change: Optional[str] = None
