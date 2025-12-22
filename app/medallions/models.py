### app/medallions/models.py

# Third party imports
from sqlalchemy import (
    Column, Integer, String, Date, CHAR, Boolean, ForeignKey,
    DECIMAL, DateTime
)
from sqlalchemy.orm import relationship

# Local imports
from app.core.db import Base
from app.users.models import AuditMixin


class MedallionStorage(Base, AuditMixin):
    """
    MedallionStorage model
    """
    __tablename__ = "medallion_storage"

    # Columns
    id = Column(Integer, primary_key=True, nullable=False,
                comment="Primary Key for Medallion Storage")
    medallion_number = Column(String(
        64), nullable=True, comment="Medallion number associated with the storage")
    storage_initiated_date = Column(
        Date, nullable=True, comment="Date when the storage was initiated")
    storage_date = Column(
        Date, nullable=True, comment="Date when the storage was completed")
    storage_mode = Column(CHAR(1), nullable=True,
                          comment="Storage Mode: Physical (P) or Virtual (V)")
    print_name = Column(String(255), nullable=True , comment = "Persion Who Approve Storage")
    storage_letter_signed_by = Column(
        Integer, nullable=True, comment="User ID of the person who signed the storage letter")
    storage_rate_card = Column(
        Date, nullable=True, comment="Rate card date associated with the storage")
    storage_reason = Column(String(
        45), nullable=True, comment="Reason for storage (no lookup planned currently)")
    retrieval_date = Column(
        Date, nullable=True, comment="Date when the Medallion was retrieved from storage")
    retrieved_by = Column(String(
        255), nullable=True, comment="Document retrieved by user")
    contract_signed_mode = Column(CHAR(1), nullable=True,
                                   comment="Contract signed mode: Print (P) or Mail (M) or In Person (I)")
    
    def to_dict(self):
        """Convert the MedallionStorage model to a dictionary"""
        return {
            "id": self.id,
            "medallion_number": self.medallion_number,
            "storage_initiated_date": self.storage_initiated_date,
            "storage_date": self.storage_date,
            "storage_mode": self.storage_mode,
            "storage_letter_signed_by": self.storage_letter_signed_by,
            "storage_rate_card": self.storage_rate_card,
            "storage_reason": self.storage_reason,
            "retrieval_date": self.retrieval_date,
            "retrieved_by": self.retrieved_by,
            "contract_signed_mode": self.contract_signed_mode,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by
        }
    
    

class MedallionRenewal(Base, AuditMixin):
    """
    Medallioan renewal model
    """
    __tablename__ = "medallion_renewals"

    # Columns
    id = Column(
        Integer, primary_key=True, nullable=False, comment="Primary Key for Medallion Renewal")
    medallion_number = Column(String(
        4), nullable=True, comment="Medallion number associated with the renewal")
    renewal_date = Column(DateTime, nullable=True,
                          comment="Date when the renewal occurred")
    renewal_from = Column(Date, nullable=True,
                          comment="Start date of the renewal period")
    renewal_to = Column(Date, nullable=True,
                        comment="End date of the renewal period")
    renewal_fee = Column(DECIMAL(15, 2), nullable=True,
                         comment="Fee for the renewal")
    
    def to_dict(self):
        """Convert the MedallionRenewal model to a dictionary"""
        return {
            "id": self.id,
            "medallion_number": self.medallion_number,
            "renewal_data": self.renewal_data,
            "renewal_from": self.renewal_from,
            "renewal_to": self.renewal_to,
            "renewal_fee": self.renewal_fee,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by
        }
    


class MOLease(Base, AuditMixin):
    """
    MO Lease model
    """
    __tablename__ = "mo_leases"

    # Columns
    id = Column(Integer, primary_key=True, nullable=False)
    payee = Column(String(
        255), nullable=True, comment="Name of the Payee for the MO Lease")
    bank_account_id = Column(Integer, ForeignKey(
        'bank_account.id'), nullable=True, comment="Foreign Key to Bank Account table")
    contract_start_date = Column(
        Date, nullable=True, comment="Start Date of the Contract")
    contract_end_date = Column(
        Date, nullable=True, comment="End Date of the Contract")
    royalty_amount = Column(DECIMAL(15, 2), nullable=True,
                            comment="Royalty Amount for the MO Lease")
    contract_term = Column(String(
        64), nullable=True, comment="Contract Term for the MO Lease")
    contract_signed_mode = Column(CHAR(
        1), nullable=True, comment="Contract Signed Mode: In Person (I) or by Mail (M)")
    mail_sent_date = Column(Date, nullable=True,
                            comment="Date when the contract mail was sent")
    mail_received_date = Column(
        Date, nullable=True, comment="Date when the contract mail was received after signature")
    lease_signed_flag = Column(
        Boolean, nullable=True, comment="Indicator if the lease is signed")
    lease_signed_date = Column(
        Date, nullable=True, comment="Date when the lease was signed")
    in_house_lease = Column(CHAR(1), nullable=True,
                            comment="Indicator if the lease is of In House type")
    med_active_exemption = Column(CHAR(
        1), nullable=True, comment="Indicator to pay (Y) the Management Fee even if the Medallion is not active")

    medallion = relationship(
        "Medallion",
        back_populates="mo_lease",
        foreign_keys="Medallion.mo_leases_id"
    )
    bank_account = relationship(
        "BankAccount",
        back_populates="mo_leases",
        foreign_keys=[bank_account_id]
    )

    def to_dict(self):
        """Convert the MOLease model to a dictionary"""
        return {
            "id": self.id,
            "payee": self.payee,
            "bank_account_id": self.bank_account_id,
            "contract_start_date": self.contract_start_date,
            "contract_end_date": self.contract_end_date,
            "royalty_amount": self.royalty_amount,
            "contract_term": self.contract_term,
            "contract_signed_mode": self.contract_signed_mode,
            "mail_sent_date": self.mail_sent_date,
            "mail_received_date": self.mail_received_date,
            "lease_signed_flag": self.lease_signed_flag,
            "lease_signed_date": self.lease_signed_date,
            "in_house_lease": self.in_house_lease,
            "med_active_exemption": self.med_active_exemption,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by
        }
    

class MedallionOwner(Base, AuditMixin):
    """
    MedallionOwner model
    """
    __tablename__ = "medallion_owners"

    id = Column(
        Integer, primary_key=True, nullable=False, comment='Primary Key')
    medallion_owner_type = Column(
        CHAR(1), nullable=True, comment='Individual (I) or Corporation (C)')

    primary_phone = Column(CHAR(24), nullable=True)
    primary_email_address = Column(String(100), nullable=True)
    medallion_owner_status = Column(CHAR(
        1), nullable=True, comment='Status: Active (Y), Archived (N), or In Progress (I)')
    active_till = Column(Date, nullable=True)

    # Foreign key relationships
    individual_id = Column(ForeignKey(
        'individual.id'), nullable=True, comment='Primary Contact Person. Referred from Individual Table.')
    corporation_id = Column(ForeignKey(
        'corporation.id'), nullable=True, comment='Foreign Key to Corporation table')
    primary_address_id = Column(Integer, ForeignKey(
        'address.id'), nullable=True, comment='Address Id used from the Address table')
    update_address_sign_mode = Column(CHAR(1), nullable=True,
                                      comment="Update Address Signed Mode: In Person (I) or by Mail (M) or Print (P)")
    is_mailing_address_same = Column(Boolean, nullable=True, default=True,
                                     comment="Indicator if the mailing address is same as primary address")

    # Reverse Lookups for code
    primary_address = relationship(
        "Address", foreign_keys=[primary_address_id], back_populates="medallion_owners")
    individual = relationship(
        "Individual", foreign_keys=[individual_id], back_populates="medallion_owners"
    )
    corporation = relationship(
        "Corporation", foreign_keys=[corporation_id], back_populates="medallion_owners"
    )

    medallions = relationship(
        "Medallion", back_populates="owner")
    
    def to_dict(self):
        """Convert the MedallionOwner model to a dictionary"""
        return {
            "id": self.id,
            "medallion_owner_type": self.medallion_owner_type,
            "primary_phone": self.primary_phone,
            "primary_email_address": self.primary_email_address,
            "medallion_owner_status": self.medallion_owner_status,
            "active_till": self.active_till,
            "individual_id": self.individual_id,
            "corporation_id": self.corporation_id,
            "primary_address_id": self.primary_address_id,
            "primary_address": self.primary_address.to_dict() if self.primary_address else None,
            "individual": self.individual.to_dict() if self.individual else None,
            "corporation": self.corporation.to_dict() if self.corporation else None,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by
        }
    
    

class Medallion(Base, AuditMixin):
    """
    Medallion model
    """
    __tablename__ = "medallions"

    id = Column(Integer, primary_key=True, nullable=False)
    medallion_number = Column(CHAR(
        64), nullable=True,unique=True, comment='Medallion Number by which the Medallion is identified')
    medallion_type = Column(CHAR(
        24), nullable=True, comment='Medallion Type which could be Wav or Regular')
    owner_type = Column(CHAR(1), nullable=True,
                        comment='Owner Type is Corporation (C) or Individual (I)')
    hack_indicator = Column(String(255), nullable=True)
    medallion_status = Column(CHAR(
        1), nullable=True, comment='The status of the Medallion. Possible statuses: I, A, V, Y, N')
    medallion_renewal_date = Column(
        Date, nullable=True, comment='Date when the Medallion renewal is due')
    validity_start_date = Column(String(
        255), nullable=True, comment='Validity Start Date as per the recent renewal')
    validity_end_date = Column(String(
        255), nullable=True, comment='Validity End Date as per the recent renewal')
    first_signed = Column(String(255), nullable=True)
    merchant_name = Column(String(255), nullable=True,
                           comment='Retained for Data Migration if required.')
    default_amount = Column(String(255), nullable=True)
    agent_number = Column(Integer, nullable=True, default=358)
    agent_name = Column(String(255), nullable=True, default="Big Apple Taxi Management")
    manger_name = Column(String(255), nullable=True,
                         comment='Retained for Data Migration from older system.')
    pay_to = Column(String(255), nullable=True,
                    comment='Name of the person to whom payment has to be made')

    last_renewal_date = Column(
        Date, nullable=True, comment='Date when the Medallion was renewed last time')
    renewal_path=Column(String(64), nullable=True)
    fs6_status = Column(CHAR(1), nullable=True)
    fs6_date = Column(DateTime, nullable=True)
    mo_leases_id = Column(Integer, ForeignKey('mo_leases.id'), nullable=True,
                          comment='The id of the MO Leases.')
    owner_id = Column(Integer, ForeignKey('medallion_owners.id'), nullable=True,
                      comment='The id of the Medallion Owner. It is referenced from the Medallion Owner table.')
    merchant_owner_id = Column(Integer, ForeignKey('bank_account.id'),
                               nullable=True, comment='Retained for Data Migration if required.')
    
    mo_lease = relationship("MOLease", back_populates="medallion")
    owner = relationship("MedallionOwner", back_populates="medallions")
    vehicle = relationship(
        "Vehicle",
        back_populates="medallions"
    )

    lease = relationship(
        "Lease",
        back_populates="medallion",
    )

    medallion_notes = relationship("MedallionNote", back_populates="medallion")
    
    dtrs = relationship(
        "DTR", 
        back_populates="medallion",
        viewonly=True
    )

    def to_dict(self):
        """Convert the Medallion model to a dictionary"""
        return {
            "id": self.id,
            "medallion_number": self.medallion_number,
            "medallion_type": self.medallion_type,
            "owner_type": self.owner_type,
            "hack_indicator": self.hack_indicator,
            "medallion_status": self.medallion_status,
            "medallion_renewal_date": self.medallion_renewal_date,
            "validity_start_date": self.validity_start_date,
            "validity_end_date": self.validity_end_date,
            "first_signed": self.first_signed,
            "merchant_name": self.merchant_name,
            "default_amount": self.default_amount,
            "agent_number": self.agent_number,
            "agent_name": self.agent_name,
            "manger_name": self.manger_name,
            "pay_to": self.pay_to,
            "last_renewal_date": self.last_renewal_date,
            "renewal_path": self.renewal_path,
            "fs6_status": self.fs6_status,
            "fs6_date": self.fs6_date,
            "mo_leases_id": self.mo_leases_id,
            "owner_id": self.owner_id,
            "owner_name": (
                self.owner.corporation.name
                if getattr(self.owner, "medallion_owner_type", None) == "C" and getattr(self.owner, "corporation", None)
                else self.owner.individual.full_name
                if getattr(self.owner, "medallion_owner_type", None) == "I" and getattr(self.owner, "individual", None)
                else None
            ),
            "ssn": (
                self.owner.individual.masked_ssn
                if getattr(self.owner, "medallion_owner_type", None) == "I" and getattr(self.owner, "individual", None)
                else None
            ),
            "ein": (
                self.owner.corporation.ein
                if getattr(self.owner, "medallion_owner_type", None) == "C" and getattr(self.owner, "corporation", None)
                else None
            ),
            "merchant_owner_id": self.merchant_owner_id,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by
        }
    