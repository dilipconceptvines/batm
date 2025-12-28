# app/leases/models.py

from typing import Optional

from sqlalchemy import (
    CHAR, Boolean, Column, Date,
    Float, ForeignKey, Integer,
    String, DateTime
)
from sqlalchemy.orm import Mapped, foreign, mapped_column, relationship

from app.core.config import settings
from app.core.db import Base
from app.esign.models import ESignEnvelope
from app.users.models import AuditMixin


class LeaseSchedule(Base, AuditMixin):
    """
    Lease Schedule model
    """

    __tablename__ = "lease_schedule"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    lease_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("leases.id"), nullable=True
    )
    installment_number: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    installment_due_date: Mapped[Optional[Date]] = mapped_column(Date, nullable=True)
    installment_amount: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    period_start_date: Mapped[Optional[Date]] = mapped_column(
        Date, nullable=True, comment="Start date of the period covered by this installment"
    )
    period_end_date: Mapped[Optional[Date]] = mapped_column(
        Date, nullable=True, comment="End date of the period covered by this installment"
    )
    medallion_installment_amount: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="Medallion portion of installment"
    )
    vehicle_installment_amount: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="Vehicle portion of installment"
    )
    installment_status: Mapped[Optional[str]] = mapped_column(
        CHAR(2), nullable=True, comment="D, P, OD, C"
    )
    installment_paid_date: Mapped[Optional[Date]] = mapped_column(
        Date, nullable=True, comment="Date when the installment is paid"
    )
    dtr_number: Mapped[Optional[str]] = mapped_column(
        CHAR(24), nullable=True, comment="DTR Number"
    )
    remarks: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, comment="Comment against the installment"
    )

    # Relationships
    lease: Mapped["Lease"] = relationship(
        back_populates="lease_schedule", foreign_keys=[lease_id]
    )

    posted_to_ledger = Column(Integer, default=0, comment='0 = not posted, 1 = posted to ledger')
    posted_on = Column(DateTime(timezone=True), nullable=True, comment='Timestamp when posted to ledger')
    ledger_posting_id = Column(String(50), nullable=True, comment='Reference to ledger posting')
    ledger_balance_id = Column(String(50), nullable=True, comment='Reference to ledger balance')

    def to_dict(self):
        """Convert the LeaseSchedule model to a dictionary"""
        return {
            "id": self.id,
            "lease_id": self.lease_id,
            "installment_number": self.installment_number,
            "installment_due_date": self.installment_due_date,
            "installment_amount": self.installment_amount,
            "period_start_date": self.period_start_date,
            "period_end_date": self.period_end_date,
            "medallion_installment_amount": self.medallion_installment_amount,
            "vehicle_installment_amount": self.vehicle_installment_amount,
            "installment_status": self.installment_status,
            "installment_paid_date": self.installment_paid_date,
            "dtr_number": self.dtr_number,
            "remarks": self.remarks,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by,
        }


class LeaseConfiguration(Base, AuditMixin):
    """
    Lease Configuration model
    """

    __tablename__ = "lease_configuration"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    lease_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("leases.id"), nullable=True
    )
    lease_breakup_type: Mapped[Optional[str]] = mapped_column(
        String(64),
        nullable=True,
        comment="Lease breakup type can one of MED, VEH, DAY, NGT, LT, SUN-D, SUN-N, MON-D, MON-N, TUE-D",
    )
    lease_limit: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    lease: Mapped["Lease"] = relationship(
        back_populates="lease_configuration", foreign_keys=[lease_id]
    )

    def to_dict(self):
        """Convert the LeaseConfiguration model to a dictionary"""
        return {
            "id": self.id,
            "lease_id": self.lease_id,
            "lease_breakup_type": self.lease_breakup_type,
            "lease_limit": self.lease_limit,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by,
        }


class LeaseDriverDocument(Base, AuditMixin):
    """
    Lease Driver Document model
    """

    __tablename__ = "lease_driver_documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    lease_driver_id = mapped_column(
        Integer,
        ForeignKey("lease_drivers.id"),
        nullable=False,
        comment="Foreign Key to Lease Driver Table",
    )
    document_envelope_id = mapped_column(
        String(120),
        nullable=True,
        comment="Document envelope id that is present in DocuSign",
    )
    has_frontend_signed = mapped_column(Boolean, nullable=True, default=False)
    frontend_signed_date = mapped_column(Date, nullable=True)
    has_driver_signed = mapped_column(Boolean, nullable=True, default=False)
    driver_signed_date = mapped_column(Date, nullable=True)

    document_id: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, index=True, comment="Associated document ID"
    )

    signing_type: Mapped[Optional[str]] = mapped_column(
        String(32), nullable=True, comment="Signature mode (free text)"
    )

    # Relationships
    lease_driver: Mapped["LeaseDriver"] = relationship(back_populates="documents")

    envelope: Mapped["ESignEnvelope"] = relationship(
        "ESignEnvelope",
        primaryjoin=lambda: LeaseDriverDocument.document_envelope_id
        == foreign(ESignEnvelope.envelope_id),
        viewonly=True,  # no FK; read-only relationship
        uselist=False,  # one envelope per LDD row
    )

    def to_dict(self):
        """Convert the LeaseDriverDocument model to a dictionary"""
        return {
            "id": self.id,
            "lease_driver_id": self.lease_driver_id,
            "document_envelope_id": self.document_envelope_id,
            "has_frontend_signed": self.has_frontend_signed,
            "frontend_signed_date": self.frontend_signed_date,
            "has_driver_signed": self.has_driver_signed,
            "driver_signed_date": self.driver_signed_date,
            "signing_type": self.signing_type,
            "document_id": self.document_id,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by,
        }


class LeasePaymentConfiguration(Base, AuditMixin):
    """
    Lease Payment Configuration model
    """

    __tablename__ = "lease_payment_configuration"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    config_type = mapped_column(String(128), nullable=True, comment="Config Type")
    day_shift_amount = mapped_column(
        Integer, nullable=True, comment="Day shift payment"
    )
    night_shift_amount = mapped_column(
        Integer, nullable=True, comment="Night shift payment"
    )
    total_amount = mapped_column(Integer, nullable=True, comment="Total payment amount")

    def to_dict(self):
        """Convert the LeaseConfiguration model to a dictionary"""
        return {
            "id": self.id,
            "lease_config_type": self.config_type,
            "day_shift": self.day_shift_amount,
            "night_shift": self.night_shift_amount,
            "total_amount": self.total_amount,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by,
        }


class LeaseDriver(Base, AuditMixin):
    """
    Lease Driver model
    """

    __tablename__ = "lease_drivers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    driver_id: Mapped[Optional[str]] = mapped_column(
        String(128),
        ForeignKey("drivers.driver_id", onupdate="CASCADE"),
        nullable=True,
        index=True,
        comment="Foreign Key to driver.driver_id",
    )
    lease_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("leases.id"),
        nullable=True,
        comment="Foreign Key to lease Table",
    )
    driver_role: Mapped[Optional[str]] = mapped_column(
        String(30), nullable=True, comment="Driver role will include DL,NL, X,DX, NX"
    )
    is_day_night_shift: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True, comment="Is driver doing night shifts"
    )
    co_lease_seq: Mapped[Optional[str]] = mapped_column(
        CHAR(1), nullable=True, comment="Co Lease number in sequence"
    )
    date_added: Mapped[Optional[Date]] = mapped_column(
        Date, nullable=True, comment="Date lesee has been added"
    )
    date_terminated: Mapped[Optional[Date]] = mapped_column(
        Date, nullable=True, comment="Date lesee has been terminated"
    )
    is_additional_driver: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True, default=False, comment="Flag indicating if this is an additional driver"
    )

    # Relationships
    lease: Mapped["Lease"] = relationship(
        back_populates="lease_driver", foreign_keys=[lease_id]
    )
    driver: Mapped["Driver"] = relationship(
        back_populates="lease_drivers", foreign_keys=[driver_id]
    )
    documents: Mapped[list["LeaseDriverDocument"]] = relationship(
        back_populates="lease_driver",
        primaryjoin="and_(LeaseDriver.id == LeaseDriverDocument.lease_driver_id, LeaseDriverDocument.is_active == True)",
        order_by=lambda: LeaseDriverDocument.created_on.desc(),
    )

    def to_dict(self):
        """Convert the LeaseDriver model to a dictionary"""
        return {
            "id": self.id,
            "driver_id": self.driver.id,
            "driver_lookup_id": self.driver_id,
            "lease_id": self.lease_id,
            "driver_role": self.driver_role,
            "is_day_night_shift": self.is_day_night_shift,
            "co_lease_seq": self.co_lease_seq,
            "date_added": self.date_added,
            "date_terminated": self.date_terminated,
            "is_additional_driver": self.is_additional_driver,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by,
        }


class Lease(Base, AuditMixin):
    """
    Lease model
    """

    __tablename__ = "leases"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    lease_id: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, comment="Lease ID", unique=True
    )
    lease_type: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True, comment="Type of lease"
    )
    medallion_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("medallions.id"),
        nullable=True,
        comment="Foreign Key to medallion Table",
    )
    vehicle_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("vehicles.id"),
        nullable=True,
        comment="Foreign Key to Vehicles Table",
    )
    lease_start_date: Mapped[Optional[Date]] = mapped_column(
        Date, nullable=True, comment="Date of lease start date"
    )
    lease_end_date: Mapped[Optional[Date]] = mapped_column(
        Date, nullable=True, comment="Date of lease end date"
    )
    duration_in_weeks: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, comment="Date of lease end date"
    )
    is_auto_renewed: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True, comment="Has the lease been auto renewed"
    )
    is_day_shift: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True, comment="day shift"
    )
    is_night_shift: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True, comment="night shift"
    )
    repairs_responsibility: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True, comment="Repairs responsibility"
    )
    lease_date: Mapped[Optional[Date]] = mapped_column(
        Date, nullable=True, comment="Date of lease"
    )
    lease_status: Mapped[Optional[str]] = mapped_column(
        String(48), nullable=True, comment="Status of lease (A,C or R)"
    )
    lease_pay_day: Mapped[Optional[str]] = mapped_column(
        String(3), nullable=True, comment="Day for lease payment"
    )
    lease_payments_type: Mapped[Optional[str]] = mapped_column(
        String(15), nullable=True, comment="Behind or Advanced"
    )
    deposit_amount_paid: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, comment="DEPRECATED: Amount for lease payments type - migrated to deposits table"
    )
    management_recommendation_amount: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, comment="Amount recommended for management"
    )
    lease_remark: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, comment="Remarks for lease"
    )
    cancellation_fee: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, comment="Calculated vehicle lease"
    )
    additional_balance_due: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, comment="Additional Balance Due"
    )

    # Segment configuration
    total_segments: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, comment="Total segments in the lease"
    )

    current_segment: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, comment="Current segment of the lease"
    )

    last_renewed_date: Mapped[Optional[Date]] = mapped_column(
        Date, nullable=True, comment="Last date when the lease was renewed"
    )

    # New columns to support price overrides and acceptance
    preset_weekly_rate: Mapped[Optional[float]] = mapped_column(
        Float, comment="The default rate from the LeasePreset table at time of creation"
    )
    overridden_weekly_rate: Mapped[Optional[float]] = mapped_column(
        Float, comment="The manually overridden rate, if any"
    )
    override_reason: Mapped[Optional[str]] = mapped_column(String(255))

    # Termination information
    termination_date: Mapped[Optional[Date]] = mapped_column(
        Date, nullable=True, comment="Date when the lease was terminated"
    )
    termination_reason: Mapped[Optional[str]] = mapped_column(
        String(500), nullable=True, comment="Reason for lease termination"
    )

    # Relationships
    medallion: Mapped["Medallion"] = relationship(
        back_populates="lease", foreign_keys=[medallion_id]
    )
    vehicle: Mapped["Vehicle"] = relationship(
        back_populates="lease", foreign_keys=[vehicle_id]
    )
    lease_driver: Mapped[list["LeaseDriver"]] = relationship(
        back_populates="lease", foreign_keys=[LeaseDriver.lease_id]
    )
    lease_configuration: Mapped[list["LeaseConfiguration"]] = relationship(
        back_populates="lease"
    )
    lease_schedule: Mapped[list["LeaseSchedule"]] = relationship(
        back_populates="lease",
        primaryjoin="and_(Lease.id == LeaseSchedule.lease_id, LeaseSchedule.is_active == True)",
    )
    lease_notes: Mapped[list["LeaseNote"]] = relationship(
            "LeaseNote", back_populates="lease"
        )
    
    transaction_receipts: Mapped[list["DTR"]] = relationship(
        back_populates="lease",
        viewonly=True
    )
    
    dtrs: Mapped[list["DTR"]] = relationship(
        back_populates="lease",
        viewonly=True
    )

    deposit: Mapped["Deposit"] = relationship(
        "Deposit", back_populates="lease", uselist=False
    )

    def to_dict(self):
        """Convert the Lease model to a dictionary"""
        # Check if lease has any active drivers
        is_driver_associated = False
        if self.lease_driver:
            is_driver_associated = any(ld.is_active for ld in self.lease_driver)

        return {
            "id": self.id,
            "lease_id": self.lease_id,
            "lease_type": self.lease_type.upper() if self.lease_type else None,
            "medallion_id": self.medallion_id,
            "vehicle_id": self.vehicle_id,
            "lease_start_date": self.lease_start_date.strftime(
                settings.common_date_format
            ) if self.lease_start_date else None,
            "lease_end_date": self.lease_end_date.strftime(settings.common_date_format) if self.lease_end_date else None,
            "duration_in_weeks": self.duration_in_weeks,
            "is_auto_renewed": self.is_auto_renewed,
            "is_day_shift": self.is_day_shift,
            "is_night_shift": self.is_night_shift,
            "deposit_amount_paid": self.deposit_amount_paid,
            "repairs_responsibility": self.repairs_responsibility,
            "lease_date": self.lease_date,
            "lease_status": self.lease_status,
            "lease_pay_day": self.lease_pay_day,
            "lease_payments_type": self.lease_payments_type,
            "management_recommendation_amount": self.management_recommendation_amount,
            "lease_remark": self.lease_remark,
            "cancellation_fee": self.cancellation_fee,
            "termination_date": self.termination_date.strftime(settings.common_date_format) if self.termination_date else None,
            "termination_reason": self.termination_reason,
            "is_driver_associated": is_driver_associated,
            "medallion": self.medallion.to_dict() if self.medallion else None,
            "vehicle": self.vehicle.to_dict() if self.vehicle else None,
            "lease_driver": [
                lease_driver.to_dict() for lease_driver in self.lease_driver
            ]
            if self.lease_driver
            else None,
            "lease_configuration": [
                lease_configuration.to_dict()
                for lease_configuration in self.lease_configuration
            ]
            if self.lease_configuration
            else None,
            "lease_schedule": [
                lease_schedule.to_dict() for lease_schedule in self.lease_schedule
            ]
            if self.lease_schedule
            else None,
            "deposit": self.deposit.to_dict() if self.deposit else None,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by,
        }


class LeasePreset(Base, AuditMixin):
    """Lease Preset Model."""

    __tablename__ = "lease_presets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    lease_type: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    vehicle_year: Mapped[Optional[int]] = mapped_column(Integer)
    vehicle_make: Mapped[Optional[str]] = mapped_column(String(50))
    vehicle_model: Mapped[Optional[str]] = mapped_column(String(50))

    # The default weekly rate configuration
    weekly_rate: Mapped[float] = mapped_column(Float)

    def to_dict(self):
        """Convert the LeasePreset model to a dictionary"""
        return {
            "id": self.id,
            "lease_type": self.lease_type,
            "vehicle_year": self.vehicle_year,
            "vehicle_make": self.vehicle_make,
            "vehicle_model": self.vehicle_model,
            "weekly_rate": self.weekly_rate,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
        }
