## app/notes/models.py

# Third party imports
from sqlalchemy import Column, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship

# Local imports
from app.core.db import Base
from app.users.models import AuditMixin


class Note(Base, AuditMixin):
    """
    Note model
    """

    __tablename__ = "notes"

    id = Column(
        Integer, primary_key=True, nullable=False, comment="Primary Key for Note"
    )
    note_type = Column(String(128), nullable=True, comment="Type of the note")
    note = Column(Text, nullable=True, comment="Note content")

    # Relationships
    driver_notes = relationship("DriverNote", back_populates="note")
    medallion_notes = relationship("MedallionNote", back_populates="note")
    vehicle_notes = relationship("VehicleNote", back_populates="note")
    lease_notes = relationship("LeaseNote", back_populates="note")

    def to_dict(self):
        """Convert the Note model to a dictionary"""
        from app.core.config import settings

        return {
            "id": self.id,
            "note_type": self.note_type,
            "note": self.note,
            "created_on": self.created_on.strftime(settings.common_date_format) if self.created_on and settings.common_date_format else "",
            "updated_on": self.updated_on.strftime(settings.common_date_format) if self.updated_on and settings.common_date_format else "",
            "created_by": self.created_by if self.created_by else "",
        }


class DriverNote(Base, AuditMixin):
    """
    Driver Note association model (many-to-many between Driver and Note)
    """

    __tablename__ = "driver_notes"

    id = Column(
        Integer, primary_key=True, nullable=False, comment="Primary Key for DriverNote"
    )
    driver_id = Column(
        Integer,
        ForeignKey("drivers.id"),
        nullable=False,
        index=True,
        comment="Foreign Key to Driver Table",
    )
    note_id = Column(
        Integer,
        ForeignKey("notes.id"),
        nullable=False,
        index=True,
        comment="Foreign Key to Note Table",
    )

    # Relationships
    driver = relationship("Driver", back_populates="driver_notes")
    note = relationship("Note", back_populates="driver_notes")

    def to_dict(self):
        """Convert the DriverNote model to a dictionary"""
        return {
            "id": self.id,
            "driver_id": self.driver_id,
            "note_id": self.note_id,
            "note": self.note.to_dict() if self.note else None,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by,
        }
    
class MedallionNote(Base, AuditMixin):
    """
    Medallion Note association model (many-to-many between Medallion and Note)
    """

    __tablename__ = "medallion_notes"

    id = Column(Integer, primary_key=True, nullable=False, comment="Primary Key for MedallionNote")
    medallion_id = Column(
        Integer,
        ForeignKey("medallions.id"),
        nullable=False,
        index=True,
        comment="Foreign Key to Medallion Table",
    )
    note_id = Column(
        Integer,
        ForeignKey("notes.id"),
        nullable=False,
        index=True,
        comment="Foreign Key to Note Table",
    )

    # Relationships
    medallion = relationship("Medallion", back_populates="medallion_notes")
    note = relationship("Note", back_populates="medallion_notes")

    def to_dict(self):
        """Convert the MedallionNote model to a dictionary"""
        return {
            "id": self.id,
            "medallion_id": self.medallion_id,
            "note_id": self.note_id,
            "note": self.note.to_dict() if self.note else None,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by,
        }


class VehicleNote(Base, AuditMixin):
    """
    Vehicle Note association model (many-to-many between Vehicle and Note)
    """

    __tablename__ = "vehicle_notes"

    id = Column(Integer, primary_key=True, nullable=False, comment="Primary Key for VehicleNote")
    vehicle_id = Column(
        Integer,
        ForeignKey("vehicles.id"),
        nullable=False,
        index=True,
        comment="Foreign Key to Vehicle Table",
    )
    note_id = Column(
        Integer,
        ForeignKey("notes.id"),
        nullable=False,
        index=True,
        comment="Foreign Key to Note Table",
    )

    # Relationships
    vehicle = relationship("Vehicle", back_populates="vehicle_notes")
    note = relationship("Note", back_populates="vehicle_notes")

    def to_dict(self):
        """Convert the VehicleNote model to a dictionary"""
        return {
            "id": self.id,
            "vehicle_id": self.vehicle_id,
            "note_id": self.note_id,
            "note": self.note.to_dict() if self.note else None,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by,
        }


class LeaseNote(Base, AuditMixin):
    """
    Lease Note association model (many-to-many between Lease and Note)
    """

    __tablename__ = "lease_notes"

    id = Column(Integer, primary_key=True, nullable=False, comment="Primary Key for LeaseNote")
    lease_id = Column(
        Integer,
        ForeignKey("leases.id"),
        nullable=False,
        index=True,
        comment="Foreign Key to Lease Table",
    )
    note_id = Column(
        Integer,
        ForeignKey("notes.id"),
        nullable=False,
        index=True,
        comment="Foreign Key to Note Table",
    )

    # Relationships
    lease = relationship("Lease", back_populates="lease_notes")
    note = relationship("Note", back_populates="lease_notes")

    def to_dict(self):
        """Convert the LeaseNote model to a dictionary"""
        return {
            "id": self.id,
            "lease_id": self.lease_id,
            "note_id": self.note_id,
            "note": self.note.to_dict() if self.note else None,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "created_by": self.created_by,
        }