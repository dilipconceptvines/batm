from typing import List, Optional, Dict, Any
from pydantic import BaseModel , EmailStr , Field
from datetime import date , datetime , time
from enum import Enum as PyEnum

from app.drivers.schemas import DriverStatus
from app.medallions.schemas import MedallionStatus , MedallionType
from app.vehicles.schemas import VehicleStatus , VehicleType , ExpensesAndComplianceCategory , ExpensesAndComplianceSubType , HackupStatus
from app.leases.schemas import LeaseStatus

class PaymentMode(str ,PyEnum):
    Check = "Check"
    ACH = "ACH"

class DriverSchema(BaseModel):
    # Identifiers
    driver_id: Optional[str] = Field(
        None, description="External or legacy driver identifier"
    )

    # Personal info
    first_name: str
    middle_name: Optional[str]
    last_name: str
    dob: Optional[date]

    ssn: Optional[str]

    # DMV License
    dmv_license_number: str
    dmv_license_issued_state: Optional[str] = Field(
        None, description="State code (e.g., NY, NJ)"
    )
    is_dmv_license_active: bool
    dmv_license_expiry_date: Optional[date]

    # TLC License
    tlc_license_number: str
    tlc_issued_state: Optional[str]
    is_tlc_license_active: bool
    tlc_license_expiry_date: Optional[date]

    # Contact
    phone_number_1: Optional[str]
    phone_number_2: Optional[str]
    email_address: Optional[EmailStr]

    # Address & payment
    primary_address_line_1: Optional[str]
    pay_to_mode: PaymentMode

    bank_account_number: Optional[str]
    bank_routing_number: Optional[str]

    # Status
    driver_status: DriverStatus
    driver_locked: Optional[bool] = False


class AddressSchmea(BaseModel):
    address_line_1: str
    address_line_2: Optional[str]
    city: Optional[str]
    state: Optional[str]
    zip_code: Optional[str]

class BankAccountSchema(BaseModel):
    bank_name: Optional[str]
    bank_account_number: str
    bank_account_status: Optional[str]
    bank_routing_number: str
    bank_account_type: Optional[str]
    bank_address: Optional[str]

class IndividualSchema(BaseModel):
    first_name: str
    middle_name: Optional[str]
    last_name: str
    primary_address: Optional[str]
    secondary_address: Optional[str]
    ssn: Optional[str]
    dob: Optional[date]
    passport: Optional[str]
    passport_expiry_date: Optional[date]
    full_name: Optional[str]
    primary_contact_number: Optional[str]
    additional_phone_number_1: Optional[str]
    additional_phone_number_2: Optional[str]
    primary_email_address: Optional[EmailStr]
    bank_account_number: Optional[str]
    pay_to_mode: PaymentMode


class CorporationSchema(BaseModel):
    corporation_name:str
    registered_date: Optional[date]
    ein: Optional[str]
    primary_address: Optional[str]
    primary_contact: Optional[str]
    primary_contact_number: Optional[str]
    primary_email_address: Optional[EmailStr]
    bank_account_number: Optional[str]
    is_llc: bool
    is_active: bool
    is_holding_co: bool
    parent_company: Optional[str]

class CorporationOwnerSchema(BaseModel):
    coporation_name: str
    name: str
    owner_role: str
    is_payee: bool
    is_primary_contact: bool
    is_authorized_signatory: bool
    ssn: Optional[str] 
    address_line_1: Optional[str]
    contact_number: Optional[str]
    email: Optional[EmailStr]

class CorporationPayeesSchema(BaseModel):
    coporation_name	: str
    pay_to_mode : PaymentMode
    payee: Optional[str]
    bank_account_number: Optional[str]	
    payee_sequence: int
    payee_type: str
    owner_name : Optional[str]
    allocation_percentage: float

class MedallionSchema(BaseModel):
    medallion_number: str
    medallion_type: Optional[MedallionType]
    owner_type: str
    medallion_status: MedallionStatus
    ein: Optional[str]
    ssn: Optional[str]
    medallion_renewal_date: Optional[date]

class MedallionLeaseSchema(BaseModel):
    contract_start_date: date
    contract_end_date: date
    contract_signed_mode: Optional[str]
    mail_sent_date: Optional[date]
    mail_received_date: Optional[date]
    lease_signed_flag: bool
    lease_signed_date: Optional[date]
    in_house_lease: Optional[str]
    med_active_exemption: Optional[str]
    medallion_number: str

class DealerSchema(BaseModel):
    dealer_name: str
    dealer_bank_name: Optional[str]
    dealer_bank_account_number: Optional[str]

class VehicleEntity(BaseModel):
    entity_name: str
    entity_address_line_1: Optional[str]
    ein: str

class VehicleSchema(BaseModel):
    vin: str
    make: Optional[str]
    model: Optional[str]
    year: Optional[str]
    cylinders: Optional[int]
    color: Optional[str]
    vehicle_type: VehicleType
    is_hybrid: Optional[bool]
    base_price: Optional[float]
    sales_tax: Optional[float]
    vehicle_office: Optional[str]
    is_delivered: Optional[bool]
    expected_delivery_date: Optional[date]
    delivery_location: Optional[str]
    is_insurance_procured: Optional[bool]
    tlc_hackup_inspection_date: Optional[date]
    is_medallion_assigned: Optional[bool]
    vehicle_status: VehicleStatus
    entity_name: Optional[str]
    dealer_name: Optional[str]
    medallion_number: Optional[str]
    purchase_date: Optional[date]
    vehicle_total_price: Optional[float]
    vehicle_true_cost: Optional[float]
    vehicle_hack_up_cost: Optional[float]
    vehicle_lifetime_cap: Optional[float]

class VehicleExpensesSchema(BaseModel):
    vin: str
    category: ExpensesAndComplianceCategory
    sub_type: ExpensesAndComplianceSubType
    amount: float
    issue_date: Optional[date]
    expiry_date: Optional[date]
    specific_info: Optional[str]
    note: Optional[str]

class VehicleHackupSchema(BaseModel):
    vin: str
    tpep_provider: str
    configuration_type: str
    is_paint_completed: bool
    paint_completed_date: Optional[date]
    paint_completed_charges: Optional[float]
    is_camera_installed: bool
    camera_installed_date: Optional[date]
    camera_installed_charges: Optional[float]
    camera_type: Optional[str]
    is_partition_installed: bool
    partition_installed_date: Optional[date]
    partition_installed_charges: Optional[float]
    partition_type: Optional[str]
    is_meter_installed: bool
    meter_installed_date: Optional[date]
    meter_type: Optional[str]
    meter_serial_number: Optional[str]
    meter_installed_charges: Optional[float]
    is_rooftop_installed: bool
    rooftop_type: Optional[str]
    rooftop_installed_date: Optional[date]
    rooftop_installation_charges: Optional[float]
    status: Optional[HackupStatus]

class VehicleInspectionSchema(BaseModel):
    vin: str
    mile_run: Optional[float]
    inspection_date: Optional[date]
    inspection_time: Optional[time]
    odometer_reading_date: Optional[date]
    odometer_reading_time: Optional[time]
    odometer_reading: Optional[float]
    logged_date: Optional[date]
    logged_time: Optional[time]
    inspection_fee: Optional[float]
    result: Optional[str]
    next_inspection_due_date: Optional[date]
    status: Optional[str]
    inspection_type: Optional[str]

class VehicleRegistrationSchema(BaseModel):
    vin: str
    registration_date: Optional[date]
    registration_expiry_date: Optional[date]
    registration_fee: Optional[float]
    plate_number: Optional[str]
    status: Optional[str]
    registration_state: Optional[str]
    registration_class: Optional[str]

class LeaseSchema(BaseModel):
    lease_id: str
    lease_type: str
    medallion_number: str
    vin: str
    lease_start_date: date
    lease_end_date: date
    duration_in_weeks: int
    is_auto_renewed: bool
    lease_date: date
    lease_status: LeaseStatus
    lease_pay_day: str
    cancellation_fee: Optional[float]
    security_deposit: Optional[float]
    is_day_shift: bool
    is_night_shift: bool
    total_lease_payment_amount: Optional[float]

class DriverLease(BaseModel):
    tlc_license: str
    lease_id: str
    driver_role: str
    date_added: Optional[date]
    is_additional_driver: bool
    is_active_lease: bool

class UserSchema(BaseModel):
    first_name: str
    middle_name: Optional[str]
    last_name: str
    email_address: EmailStr
    password: str
    roles: str

class RoleSchema(BaseModel):
    name: str
    description: str

class CaseStepConfigSchema(BaseModel):
    step_id: int
    case_step_name: str
    next_assignee_name: Optional[str]
    next_step_id: Optional[int]
    step_name: str
    user_roles: str
    case_type_prefix: str

class StepConfigSchema(BaseModel):
    step_name: str
    schema_name: str

class FirstStepConfigSchema(BaseModel):
    prefix: str
    first_step: str

class CaseStepSchema(BaseModel):
    name: str
    case_type_prefix: str
    weight: int

class CaseTypeSchema(BaseModel):
    name: str
    prefix: str

class CaseStatusSchema(BaseModel):
    name: str