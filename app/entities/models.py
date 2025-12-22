## app/entities/models.py

# Third party imports
from sqlalchemy import (
    Column, Integer, String, ForeignKey, Boolean, DateTime,
    Date, CHAR , BigInteger , Numeric
)
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Mapped, mapped_column

# Local imports
from app.core.db import Base
from app.users.models import User, AuditMixin
from app.bpm.models import SLA


class Entity(Base, AuditMixin):
    """
    Entity model
    """
    __tablename__ = "entity"

    # Columns
    id = Column(Integer, primary_key=True, nullable=False,
                comment="Primary Key for the Entity")
    dos_id = Column(String(255), nullable=True,
                    comment="DOS Identifier for the Entity")
    entity_name = Column(String(255), nullable=True,
                         comment="Name of the Entity also known as Management Name")
    original_title = Column(String(
        255), nullable=True, comment="Vehicle-related field retained for data migration")
    registered_date = Column(DateTime, nullable=True,
                             comment="Date when the Entity was registered")
    is_corporation = Column(Boolean, nullable=True,
                            comment="Indicates if the Entity is a Corporation")
    contact_person_id = Column(Integer, ForeignKey(
        "individual.id"), nullable=True, comment="Foreign Key to Individual table for contact person")
    bank_id = Column(Integer,ForeignKey(
        "bank_account.id"),nullable=True,comment="Bank Identifier for the Entity")
    entity_address_id = Column(Integer, ForeignKey('address.id'), nullable=True,
                               index=True, comment="Foreign Key to Address table for the Entity's address")
    owner = Column(Integer, nullable=True,
                   comment="Owner of the Entity, referenced from Individual table")
    joint_owner = Column(Integer, nullable=True,
                         comment="Joint Owner of the Entity, referenced from Individual table")
    # TODO: num corporation is actually a query, we can remove
    num_corporations = Column(Integer, nullable=True,
                              comment="Number of corporations under the Entity")
    ein_ssn = Column(String(255), nullable=True)
    president = Column(String(128), nullable=True,
                       comment="ID of the President")
    secretary = Column(String(128), nullable=True,
                       comment="ID of the Secretary")
    corporate_officer = Column(
        String(128), nullable=True, comment="ID of the Corporate Officer")
    # TODO: Add EIN number - Ensure the logic in medallion search is updated for the same
    # TODO: Add bank account info
    # corporation = relationship(
    #     "Corporation", back_populates="entity", foreign_keys="Corporation.entity_id"
    # )

    entity_address = relationship(
        "Address", back_populates="entity", foreign_keys=[entity_address_id])
    bank_account = relationship(
        "BankAccount", back_populates="entity", foreign_keys=[bank_id])
    
    contact_person = relationship(
        "Individual", back_populates="entity", foreign_keys=[contact_person_id]
    )

    def to_dict(self):
        """Convert the Entity model to a dictionary"""
        return {
            "id": self.id,
            "entity_name": self.entity_name,
            "entity_address": self.entity_address.to_dict() if self.entity_address else None,
            "owner": self.owner,
            "joint_owner": self.joint_owner,
            "num_corporations": self.num_corporations,
            "contact_person": self.contact_person.to_dict() if self.contact_person else None,
            "bank": self.bank_account.to_dict() if self.bank_account else None,
            "ein": self.ein,
            "president": self.president,
            "secretary": self.secretary,
            "corporate_officer": self.corporate_officer,
            "vehicles": [vehicle.to_dict() for vehicle in self.vehicle],
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by
        }


class Corporation(Base, AuditMixin):
    """
    Corporation model
    """
    __tablename__ = "corporation"

    id = Column(Integer, primary_key=True,
                nullable=False, comment='Primary Key for Corporation')
    name = Column(String(255), nullable=True, comment="Name of the Corporation")
    is_holding_entity = Column(Boolean, default=False, comment="Indicates if the Corporation is a Holding Entity")
    ein = Column(String(255), nullable=True, comment="EIN of the Corporation")
    is_llc = Column(Boolean, default=False, comment="Indicates if the Corporation is an LLC")
    primary_contact_number = Column(String(255), nullable=True, comment="Primary phone number for the Corporation")
    primary_email_address = Column(String(255), nullable=True, comment="Primary email address for the Corporation")
    primary_address_id = Column(Integer, ForeignKey("address.id"), nullable=True, index=True, comment="Foreign Key to Address table for primary address")
    secondary_address_id = Column(Integer, ForeignKey("address.id"), nullable=True, index=True, comment="Foreign Key to Address table for secondary address")
    registered_date = Column(DateTime, nullable=True, comment="Date when the Corporation was registered")
    linked_pad_owner_id = Column(Integer, ForeignKey("corporation.id"), nullable=True)
    contract_signed_mode = Column(CHAR(
        1), nullable=True, comment="Contract Signed Mode: In Person (I) or by Mail (M) or Print (P)")
    is_mailing_address_same = Column(Boolean , nullable =True , comment = "Indicates whether the mailing address is the same as the primary address")
    medallion_owners = relationship(
        "MedallionOwner", back_populates="corporation")
    
    primary_address = relationship(
    "Address", back_populates="primary_corporations", foreign_keys=[primary_address_id]
    )

    secondary_address = relationship(
    "Address", back_populates="secondary_corporations", foreign_keys=[secondary_address_id]
    )

    corporation_payees = relationship(
        "CorporationPayee", back_populates="corporation", foreign_keys="CorporationPayee.corporation_id"
    )
    
    linked_pad_owner = relationship(
        "Corporation",
        remote_side=[id],
        backref="linked_corporations"
    )
    corporation_owners = relationship(
        "CorporationOwners", back_populates="corporation", foreign_keys="CorporationOwners.corporation_id"
    )
    corporation_owned_payees = relationship(
        "CorporationPayee", back_populates="corporation_owner", foreign_keys="CorporationPayee.corporation_owner_id"
    )



class CorporationPayee(Base, AuditMixin):
    """
    CorporationPayee model
    """
    __tablename__ = "corporation_payees"
    id :Mapped[int] = mapped_column(Integer , primary_key=True , nullable=False , comment="Primary Key for Corporation Payee")
    corporation_id :Mapped[int] = mapped_column(Integer , ForeignKey("corporation.id") , nullable=False , comment="Foreign Key to Corporation table")
    payee_type: Mapped[str] = mapped_column(String(25) , nullable=True , comment="Payee Type: Individual or Corporation")
    individual_id : Mapped[int] = mapped_column(Integer , ForeignKey("individual.id") ,nullable=True , comment="Payee ID: Foreign Key to Individual table based on the Payee Type")
    corporation_owner_id : Mapped[int] = mapped_column(Integer , ForeignKey("corporation.id") ,nullable=True , comment="Payee ID: Foreign Key to Corporation table based on the Payee Type")
    pay_to_mode: Mapped[str] = mapped_column(String(25) , nullable=True , comment="Pay to Mode: Check and ACH")
    payee: Mapped[str] = mapped_column(String(255) , nullable=True , comment="Payee for the Corporation")
    bank_account_id: Mapped[int] = mapped_column(Integer , ForeignKey("bank_account.id") , nullable=True , comment="Foreign Key to id in the Bank Account table")
    sequence : Mapped[int] = mapped_column(Integer , nullable=True, comment="Sequence of the Payee")
    allocation_percentage : Mapped[float] = mapped_column(Numeric(5, 2) , nullable=True , comment="Allocation Percentage for the Payee")
    bank_account = relationship(
        "BankAccount", back_populates="corporation_payee", foreign_keys=[bank_account_id]
    ) 
    corporation = relationship(
        "Corporation", back_populates="corporation_payees", foreign_keys=[corporation_id]
    )
    individual_owner = relationship(
        "Individual", back_populates="corporation_payees", foreign_keys=[individual_id]
    )
    corporation_owner = relationship(
        "Corporation", back_populates="corporation_owned_payees",foreign_keys=[corporation_owner_id]
    )

    def to_dict(self):
        """Convert the CorporationPayee model to a dictionary"""
        return {
            "id": self.id,
            "corporation_id": self.corporation_id,
            "pay_to_mode": self.pay_to_mode,
            "payee": self.payee,
            "bank_account": self.bank_account.to_dict() if self.bank_account else None,
            "created_on": self.created_on,
            "updated_on": self.updated_on
        }
    

class CorporationOwners(Base, AuditMixin):
    """
    CorporationOwners model
    """
    __tablename__ = "corporation_owners"
    id = Column(Integer, primary_key=True,
                nullable=False, comment='Primary Key for Corporation Owners')
    corporation_id = Column(Integer, ForeignKey("corporation.id"), nullable=False,
                            index=True, comment="Foreign Key to Corporation table")
    name = Column(String(255), nullable=True, comment="Name of the Corporation Owner")
    owner_type = Column(String(25), nullable=True,
                        comment="Owner Type: manager or member")
    owner_id = Column(Integer, ForeignKey("individual.id") ,nullable=True, comment="Owner Id from Individual table")
    is_payee = Column(Boolean, default=False, comment="Indicates if the Corporation Owner is a Payee")
    is_primary_contact = Column(Boolean, default=False,
                                comment="Indicates if the Corporation Owner is the Primary Contact")
    is_authorized_signatory = Column(Boolean, default=False,
                                    comment="Indicates if the Corporation Owner is an Authorized Signatory")
    individual_owner = relationship(
        "Individual", back_populates="corporation_owners", foreign_keys=[owner_id]
    )
    corporation = relationship(
        "Corporation", back_populates="corporation_owners", foreign_keys=[corporation_id]
    )
    

class Individual(Base, AuditMixin):
    """
    Individual model
    """
    __tablename__ = "individual"

    id = Column(Integer, primary_key=True,
                nullable=False, comment='Primary Key')
    first_name = Column(String(255), nullable=True)
    middle_name = Column(String(64), nullable=True)
    last_name = Column(String(255), nullable=True)
    primary_address_id = Column(Integer, ForeignKey('address.id'), nullable=True,
                                index=True, comment='Foreign Key to Primary Address in Address table')
    secondary_address_id = Column(Integer, ForeignKey('address.id'), nullable=True,
                                  index=True, comment='Foreign Key to Secondary Address in Address table')
    bank_account_id = Column(Integer, ForeignKey(
        'bank_account.id'), nullable=True, comment='Foreign Key to id in the Bank Account table')
    masked_ssn = Column(String(255), nullable=True)
    dob = Column(String(255), nullable=True)
    passport = Column(String(255), nullable=True)
    passport_expiry_date = Column(DateTime, nullable=True)
    driving_license = Column(String(255), nullable=True)
    driving_license_expiry_date = Column(DateTime, nullable=True)
    full_name = Column(String(255), nullable=True)
    primary_contact_number = Column(String(255), nullable=True)
    additional_phone_number_1 = Column(String(255), nullable=True)
    additional_phone_number_2 = Column(String(255), nullable=True)
    primary_email_address = Column(String(255), nullable=True)
    payee = Column(String(255), nullable=True)
    pay_to_mode = Column(String(25), nullable=True, comment="Pay to Mode: Check and ACH")
    correspondence_method = Column(String(255), nullable=True)

    bank_account = relationship(
        "BankAccount", back_populates="individual_bank_account", foreign_keys=[bank_account_id])
    primary_address = relationship(
        "Address", back_populates="primary_individuals", foreign_keys=[primary_address_id])
    secondary_address = relationship(
        "Address", foreign_keys=[secondary_address_id], back_populates="secondary_individuals")
    medallion_owners = relationship(
        "MedallionOwner", back_populates="individual")
    
    entity = relationship(
        "Entity", back_populates="contact_person", foreign_keys=[Entity.contact_person_id]
    )
    corporation_owners = relationship(
        "CorporationOwners", back_populates="individual_owner", foreign_keys="CorporationOwners.owner_id"
    )
    corporation_payees = relationship(
        "CorporationPayee", back_populates="individual_owner", foreign_keys="CorporationPayee.individual_id"
    )

    def to_dict(self):
        """Convert the Individual model to a dictionary"""
        return {
            "id": self.id,
            "first_name": self.first_name,
            "middle_name": self.middle_name,
            "last_name": self.last_name,
            "primary_address": self.primary_address.to_dict() if self.primary_address else None,
            "secondary_address": self.secondary_address.to_dict() if self.secondary_address else None,
            "bank_account": self.bank_account.to_dict() if self.bank_account else None,
            "masked_ssn": self.masked_ssn,
            "dob": self.dob,
            "passport": self.passport,
            "passport_expiry_date": self.passport_expiry_date,
            "full_name": self.full_name,
            "primary_contact_number": self.primary_contact_number,
            "additional_phone_number_1": self.additional_phone_number_1,
            "additional_phone_number_2": self.additional_phone_number_2,
            "primary_email_address": self.primary_email_address,
            "payee": self.payee,
            "pay_to_mode": self.pay_to_mode,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by
        }
    

class Address(Base, AuditMixin):
    """
    Address model
    """
    __tablename__ = "address"

    id = Column(Integer, primary_key=True,
                nullable=False, comment='Primary Key for Address')
    address_line_1 = Column(String(255), nullable=True,
                            comment='Line 1 of the Address')
    address_line_2 = Column(String(255), nullable=True,
                            comment='Line 2 of the Address')
    city = Column(String(255), nullable=True, comment='City of the Address')
    state = Column(String(255), nullable=True,
                   comment='State Name in the Address')
    zip = Column(String(255), nullable=True,
                 comment='Zip Code for the Address')
    po_box = Column(String(255), nullable=True,
                    comment='PO Box for the Address')
    latitude = Column(String(255), nullable=True,
                      comment='Latitude of the Address')
    longitude = Column(String(255), nullable=True,
                       comment='Longitude of the Address')
    from_date = Column(DateTime, nullable=True,
                       comment='Address is effective from this date')
    to_date = Column(DateTime, nullable=True,
                     comment='Address is effective till this date')
    primary_contact_number = Column(String(255), nullable=True)

    primary_email_address = Column(String(255), nullable=True)

    additional_phone_1 = Column(String(255), nullable=True)
    
    additional_phone_2 = Column(String(255), nullable=True)

    medallion_owners = relationship(
        "MedallionOwner", back_populates="primary_address")
    primary_individuals = relationship(
        "Individual", back_populates="primary_address", foreign_keys="Individual.primary_address_id")
    secondary_individuals = relationship(
        "Individual", back_populates="secondary_address", foreign_keys="Individual.secondary_address_id")
    primary_corporations = relationship(
    "Corporation", back_populates="primary_address", foreign_keys="Corporation.primary_address_id")

    secondary_corporations = relationship(
    "Corporation", back_populates="secondary_address", foreign_keys="Corporation.secondary_address_id")
    entity = relationship(
        "Entity", back_populates="entity_address", foreign_keys="Entity.entity_address_id"
    )

    vehicle_entity = relationship(
        "VehicleEntity", back_populates="owner_address", foreign_keys="VehicleEntity.entity_address_id"
    )
    bank_account = relationship(
        "BankAccount", back_populates="bank_address", foreign_keys="BankAccount.bank_address_id"
    )

    primary_driver = relationship(
        "Driver", back_populates="primary_driver_address", foreign_keys="Driver.primary_address_id")
    secondary_driver = relationship(
        "Driver", back_populates="secondary_driver_address", foreign_keys="Driver.secondary_address_id")
    
    def to_dict(self):
        """Convert the Address model to a dictionary"""
        return {
            "id": self.id,
            "address_line_1": self.address_line_1,
            "address_line_2": self.address_line_2,
            "city": self.city,
            "state": self.state,
            "zip": self.zip,
            "po_box": self.po_box,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "from_date": self.from_date,
            "to_date": self.to_date,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by
        }


class BankAccount(Base, AuditMixin):
    """
    BankAccount model
    """
    __tablename__ = "bank_account"

    id = Column(Integer, primary_key=True,
                nullable=False, comment='Primary Key for the Bank Account')
    bank_name = Column(String(255), nullable=True, comment='Name of the Bank')
    bank_account_number = Column(
        BigInteger, nullable=True, comment='Bank Account Number')
    bank_account_status = Column(
        String(255), nullable=True, comment='Account Status (Not used as a code)')
    bank_account_name = Column(
        String(255), nullable=True, comment='Name of the Bank Account Holder')
    effective_from = Column(
        Date, nullable=True, comment='Date when the bank account if effective from')
    bank_routing_number = Column(
        String(45), nullable=True, comment='Bank Routing Number to be used for ACH')
    bank_account_type = Column(CHAR(
        1), nullable=True, comment='Bank Account Type to indicate Savings (S) or Checking (C)')
    bank_address_id = Column(Integer, ForeignKey(
        'address.id'), nullable=True, comment='Bank Address Id used from the Address table')

    individual_bank_account = relationship(
        "Individual", back_populates="bank_account", foreign_keys="Individual.bank_account_id")
    
    bank_address = relationship(
        "Address", back_populates="bank_account", foreign_keys=[bank_address_id]
    )

    driver = relationship(
        "Driver", back_populates="driver_bank_account", foreign_keys="Driver.bank_account_id"
    )

    mo_leases = relationship(
    "MOLease",
    back_populates="bank_account",
    foreign_keys="MOLease.bank_account_id"
    )
    corporation_payee = relationship(
        "CorporationPayee", back_populates="bank_account", foreign_keys="CorporationPayee.bank_account_id"
    )

    entity = relationship(
        "Entity", back_populates="bank_account", foreign_keys="Entity.bank_id"
    )

    def to_dict(self):
        """Convert the BankAccount model to a dictionary"""
        return {
            "id": self.id,
            "bank_name": self.bank_name,
            "bank_account_number": self.bank_account_number,
            "bank_account_status": self.bank_account_status,
            "effective_from": self.effective_from,
            "bank_routing_number": self.bank_routing_number,
            "bank_account_type": self.bank_account_type,
            "bank_address": self.bank_address.to_dict() if self.bank_address else None,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by
        }
