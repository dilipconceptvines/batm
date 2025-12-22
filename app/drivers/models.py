## app/drivers/models.py

# Third party imports
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from app.core.config import settings

# Local imports
from app.core.db import Base
from app.notes.models import DriverNote
from app.users.models import AuditMixin
from app.utils.general import format_us_phone_number


class DMVLicense(Base, AuditMixin):
    """
    DMV License model
    """

    __tablename__ = "driver_dmv_license"

    id = Column(
        Integer, primary_key=True, nullable=False, comment="Primary Key for Driver"
    )
    is_dmv_license_active = Column(Boolean, nullable=True)
    dmv_license_number = Column(
        String(255), nullable=True, comment="TLC License Number"
    )
    dmv_license_issued_state = Column(
        String(255), nullable=True, comment="TLC License Number"
    )
    dmv_class = Column(String(255), nullable=True)
    dmv_license_status = Column(String(255), nullable=True)
    dmv_class_change_date = Column(
        DateTime, nullable=True, comment="TLC Class Change Date"
    )
    dmv_license_expiry_date = Column(
        DateTime, nullable=True, comment="TLC License Expiry Date"
    )
    dmv_renewal_fee = Column(Integer, nullable=True)

    driver = relationship(
        "Driver",
        back_populates="dmv_license",
        foreign_keys="Driver.dmv_license_number_id",
    )

    def to_dict(self):
        """Convert the DMV License model to a dictionary"""
        return {
            "id": self.id,
            "is_dmv_license_active": self.is_dmv_license_active,
            "dmv_license_number": self.dmv_license_number,
            "dmv_license_issued_state": self.dmv_license_issued_state,
            "dmv_class": self.dmv_class,
            "dmv_license_status": self.dmv_license_status,
            "dmv_class_change_date": self.dmv_class_change_date,
            "dmv_license_expiry_date": self.dmv_license_expiry_date,
            "dmv_renewal_fee": self.dmv_renewal_fee,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by,
        }


class TLCLicense(Base, AuditMixin):
    """
    TLC License model
    """

    __tablename__ = "driver_tlc_license"

    id = Column(
        Integer, primary_key=True, nullable=False, comment="Primary Key for Driver"
    )
    is_tlc_license_active = Column(Boolean, nullable=True)
    tlc_license_number = Column(
        String(255), nullable=True, comment="TLC License Number"
    )
    tlc_issued_state = Column(String(255), nullable=True, comment="TLC License Number")
    tlc_license_expiry_date = Column(
        DateTime, nullable=True, comment="TLC License Expiry Date"
    )
    tlc_ddc_date = Column(DateTime, nullable=True, comment="TLC DDC Date")
    tlc_drug_test_date = Column(DateTime, nullable=True, comment="TLC Drug Test Date")
    previous_tlc_license_number = Column(
        String(255), nullable=True, comment="TLC License Number"
    )
    tlc_hack_date = Column(DateTime, nullable=True, comment="TLC Hack Date")
    tlc_lease_card_date = Column(DateTime, nullable=True, comment="TLC Hack Date")
    tlc_renewal_fee = Column(Integer, nullable=True)

    driver = relationship(
        "Driver",
        back_populates="tlc_license",
        foreign_keys="Driver.tlc_license_number_id",
    )

    def to_dict(self):
        """Convert the TLC License model to a dictionary"""
        return {
            "id": self.id,
            "is_tlc_license_active": self.is_tlc_license_active,
            "tlc_license_number": self.tlc_license_number,
            "tlc_issued_state": self.tlc_issued_state,
            "tlc_license_expiry_date": self.tlc_license_expiry_date,
            "tlc_ddc_date": self.tlc_ddc_date,
            "tlc_drug_test_date": self.tlc_drug_test_date,
            "previous_tlc_license_number": self.previous_tlc_license_number,
            "tlc_hack_date": self.tlc_hack_date,
            "tlc_lease_card_date": self.tlc_lease_card_date,
            "tlc_renewal_fee": self.tlc_renewal_fee,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by,
        }


class Driver(Base, AuditMixin):
    """
    Driver model
    """

    __tablename__ = "drivers"

    id = Column(
        Integer, primary_key=True, nullable=False, comment="Primary Key for Driver"
    )
    first_name = Column(String(128), nullable=True, comment="First Name of the Driver")
    middle_name = Column(
        String(128), nullable=True, comment="Middle Name of the Driver"
    )
    last_name = Column(String(128), nullable=True, comment="Last Name of the Driver")
    full_name = Column(String(128), nullable=True, comment="Full Name of the Driver")
    ssn = Column(
        String(128), nullable=True, comment="Social Security Number of the Driver"
    )
    dob = Column(String(64), nullable=True)
    phone_number_1 = Column(String(18), nullable=True)
    phone_number_2 = Column(String(18), nullable=True)
    email_address = Column(String(128), nullable=True)
    driver_id = Column(String(128), nullable=True, unique=True, index=True)
    # TODO: Should this be moved to driver accounts ? - Does not make sense for new driver
    outstanding_receivables = Column(
        String(255), nullable=True, comment="Outstanding Receivables for the Driver"
    )
    outstanding_payments = Column(
        String(255), nullable=True, comment="Outstanding Payments for the Driver"
    )
    violation_due_at_registration = Column(
        String(255), nullable=True, comment="Violation due at registration"
    )
    # TODO: Violation due at registration to be added, while registering a new driver
    # the amount pending from the dmv site will be entered here
    driver_type = Column(
        String(64), nullable=True, comment="Type of the Driver, WAV Or Regular"
    )
    drive_locked = Column(
        Boolean, nullable=True, comment="Indicates if the Driver is Locked"
    )
    driver_status = Column(
        String(50),
        nullable=True,
        comment="Driver status can be Registration In Progress, Registered, Approved, Active or Inactive",
    )
    is_additional_driver = Column(
        Boolean,
        nullable=True,
        comment="Flag indicating if this is an Additional Driver",
    )
    driver_manager_id = Column(
        Integer,
        ForeignKey("drivers.id"),
        nullable=True,
        index=True,
        comment="Foreign Key to Drivers table for Manager",
    )
    contract_end_date = Column(String(255), nullable=True, comment="Contract End Date")
    contract_start_date = Column(
        String(255), nullable=True, comment="Contract Start Date"
    )
    pay_to = Column(
        String(128),
        nullable=True,
        comment="Name of the person to whom payment has to be made",
    )
    pay_to_mode = Column(
        String(128), nullable=True, comment="Would be with Check or ACH"
    )
    # There is no fixed list so it does not make sense to create one column per item.
    # Will keep this field open.
    other_payment_options = Column(
        String(128),
        nullable=True,
        comment="Other Payment options Pay By Cash, Pay by Checks or Pay by credit card",
    )
    other_payment_details = Column(
        String(128),
        nullable=True,
        comment="Payment details when Pay By Cash, Pay by Checks or Pay by credit card is selected",
    )

    primary_emergency_contact_person = Column(
        String(128), nullable=True, comment="Primary emergency contact detail"
    )
    primary_emergency_contact_relationship = Column(
        String(128), nullable=True, comment="Relation with primary emergency contact"
    )
    primary_emergency_contact_number = Column(
        String(128),
        nullable=True,
        comment="Relation with primary emergency contact number",
    )
    additional_emergency_contact_person = Column(
        String(128), nullable=True, comment="Additional emergency contact detail"
    )
    additional_emergency_contact_relationship = Column(
        String(128), nullable=True, comment="Relation with additional emergency contact"
    )
    additional_emergency_contact_number = Column(
        String(128),
        nullable=True,
        comment="Relation with additional emergency contact number",
    )

    bank_account_id = Column(
        Integer,
        ForeignKey("bank_account.id"),
        nullable=True,
        index=True,
        comment="Foreign Key to Bank Account",
    )

    primary_address_id = Column(
        Integer,
        ForeignKey("address.id"),
        nullable=True,
        index=True,
        comment="Foreign Key to Address (Primary) table for Driver Address",
    )

    secondary_address_id = Column(
        Integer,
        ForeignKey("address.id"),
        nullable=True,
        index=True,
        comment="Foreign Key to Address (Secondary) table for Driver Address",
    )

    tlc_license_number_id = Column(
        Integer,
        ForeignKey("driver_tlc_license.id"),
        nullable=True,
        index=True,
        comment="Foreign Key to TCL License Table",
    )

    dmv_license_number_id = Column(
        Integer,
        ForeignKey("driver_dmv_license.id"),
        nullable=True,
        index=True,
        comment="Foreign Key to DMV License Table",
    )

    primary_driver_address = relationship(
        "Address", back_populates="primary_driver", foreign_keys=[primary_address_id]
    )
    secondary_driver_address = relationship(
        "Address",
        back_populates="secondary_driver",
        foreign_keys=[secondary_address_id],
    )

    driver_bank_account = relationship(
        "BankAccount", back_populates="driver", foreign_keys=[bank_account_id]
    )

    tlc_license = relationship(
        "TLCLicense", back_populates="driver", foreign_keys=[tlc_license_number_id]
    )

    dmv_license = relationship(
        "DMVLicense", back_populates="driver", foreign_keys=[dmv_license_number_id]
    )

    lease_drivers = relationship("LeaseDriver", back_populates="driver")

    driver_notes = relationship("DriverNote", back_populates="driver")
    
    transaction_receipts = relationship(
        "DTR",
        back_populates="primary_driver",
        viewonly=True,
        foreign_keys="DTR.primary_driver_id"
    )

    dtrs = relationship(
        "DTR",
        back_populates="primary_driver",
        viewonly=True,
        foreign_keys="DTR.primary_driver_id"
    )

    def to_dict(self):
        """Convert the Driver model to a dictionary"""
        return {
            "id": self.id,
            "first_name": self.first_name,
            "middle_name": self.middle_name,
            "last_name": self.last_name,
            "full_name": self.full_name,
            "ssn": self.ssn,
            "dob": self.dob,
            "phone_number_1": format_us_phone_number(self.phone_number_1),
            "phone_number_2": format_us_phone_number(self.phone_number_2),
            "email_address": self.email_address,
            "driver_id": self.driver_id,
            "outstanding_receivables": self.outstanding_receivables,
            "outstanding_payments": self.outstanding_payments,
            "violation_due_at_registration": self.violation_due_at_registration,
            "driver_type": self.driver_type,
            "drive_locked": self.drive_locked,
            "driver_status": self.driver_status,
            "is_additional_driver": self.is_additional_driver,
            "driver_manager_id": self.driver_manager_id,
            "contract_end_date": self.contract_end_date,
            "contract_start_date": self.contract_start_date,
            "pay_to": self.pay_to,
            "pay_to_mode": self.pay_to_mode,
            "other_payment_options": self.other_payment_options,
            "other_payment_details": self.other_payment_details,
            "primary_emergency_contact_person": self.primary_emergency_contact_person,
            "primary_emergency_contact_relationship": self.primary_emergency_contact_relationship,
            "primary_emergency_contact_number": self.primary_emergency_contact_number,
            "additional_emergency_contact_person": self.additional_emergency_contact_person,
            "additional_emergency_contact_relationship": self.additional_emergency_contact_relationship,
            "additional_emergency_contact_number": self.additional_emergency_contact_number,
            "bank_account_id": self.bank_account_id,
            "primary_address_id": self.primary_address_id,
            "secondary_address_id": self.secondary_address_id,
            "tlc_license_number_id": self.tlc_license_number_id,
            "dmv_license_number_id": self.dmv_license_number_id,
            "primary_driver_address": self.primary_driver_address.to_dict()
            if self.primary_driver_address
            else None,
            "secondary_driver_address": self.secondary_driver_address.to_dict()
            if self.secondary_driver_address
            else None,
            "tlc_license": self.tlc_license.to_dict() if self.tlc_license else None,
            "dmv_license": self.dmv_license.to_dict() if self.dmv_license else None,
            "lease_drivers": [
                lease_driver.to_dict() for lease_driver in self.lease_drivers
            ],
            # "daily_receipts": [
            #     daily_receipt.to_dict() for daily_receipt in self.daily_receipts
            # ],
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by,
        }
