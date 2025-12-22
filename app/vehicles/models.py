### app/vehicles/models.py

# Third party imports
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    DateTime
)
from sqlalchemy.orm import relationship

# Local imports
from app.core.db import Base
from app.users.models import AuditMixin
from app.utils.general import generate_random_6_digit
from app.vehicles.schemas import VehicleEntityStatus


class VehicleExpensesAndCompliance(Base , AuditMixin):

    __tablename__ = "vehicle_expenses_and_compliance"

    id = Column(Integer, primary_key=True, nullable=False)
    vehicle_id = Column(Integer, ForeignKey("vehicles.id"), nullable=False)
    category = Column(String(255), nullable=True)
    sub_type = Column(String(255), nullable=True)
    invoice_number = Column(String(255), nullable=True)
    base_price = Column(Float, nullable=True , default=0.0)
    sales_tax = Column(Float, nullable=True , default=0.0)
    amount = Column(Float, nullable=True , default=0.0)
    vendor_name = Column(String(255), nullable=True)
    issue_date = Column(Date, nullable=True)
    expiry_date = Column(Date, nullable=True)
    meter_serial_no = Column(String(255), nullable=True)
    mile_run_due_date = Column(Date, nullable=True)
    specific_info = Column(Text, nullable=True)
    note = Column(Text, nullable=True)
    document_id = Column(Integer, ForeignKey("document.id"), nullable=True)
    deleted_at = Column(DateTime, nullable=True)
    deleted_by = Column(Integer, ForeignKey("users.id"), nullable=True)

    vehicle = relationship("Vehicle", back_populates="expenses_and_compliance", foreign_keys=[vehicle_id])
    
class Dealer(Base, AuditMixin):
    """
    Dealer model
    """

    __tablename__ = "dealers"
    id = Column(
        Integer, primary_key=True, nullable=False, comment="Primary Key for Vehicles"
    )
    dealer_name = Column(String(255), nullable=True, comment="Name of the dealer")
    dealer_bank_name = Column(
        String(255), nullable=True, comment="Name of the dealer bank name"
    )
    dealer_bank_account_number = Column(
        String(50), nullable=True, comment="Name of the dealer bank name"
    )
    vehicle = relationship(
        "Vehicle", back_populates="dealer", foreign_keys="Vehicle.dealer_id"
    )


class VehicleEntity(Base, AuditMixin):
    """
    Vehicle Entity model
    """

    __tablename__ = "vehicle_entity"

    id = Column(Integer, primary_key=True, nullable=False)
    entity_name = Column(String(255), nullable=True)
    owner_id = Column(
        Integer, default=generate_random_6_digit, unique=True, nullable=True
    )
    entity_status = Column(
        String(255), default=VehicleEntityStatus.INACTIVE, nullable=True
    )
    ein = Column(String(255), nullable=True)
    entity_address_id = Column(Integer, ForeignKey("address.id"), nullable=True)
    contact_number = Column(String(255), nullable=True)
    contact_email = Column(String(255), nullable=True)

    owner_address = relationship(
        "Address", back_populates="vehicle_entity", foreign_keys=[entity_address_id]
    )
    vehicles = relationship(
        "Vehicle", back_populates="vehicle_entity", foreign_keys="Vehicle.entity_id"
    )


class VehicleInspection(Base, AuditMixin):
    """
    Vehicle Inspection model
    """

    __tablename__ = "vehicle_inspections"

    id = Column(
        Integer,
        primary_key=True,
        nullable=False,
        comment="Primary Key for Vehicle Inspections",
    )
    vehicle_id = Column(
        Integer,
        ForeignKey("vehicles.id"),
        nullable=False,
        comment="Foreign Key to Vehicle table",
    )

    inspection_type = Column(
        String(255), nullable=True, comment="Type of inspection conducted"
    )

    mile_run = Column(
        Boolean, nullable=True, comment="Indicates if a mile run was completed"
    )
    inspection_date = Column(Date, nullable=True, comment="Date of inspection")
    inspection_time = Column(String(255), nullable=True, comment="Time of inspection")
    odometer_reading_date = Column(
        Date, nullable=True, comment="Date of odometer reading"
    )
    odometer_reading_time = Column(
        String(255), nullable=True, comment="Time of odometer reading"
    )
    odometer_reading = Column(
        Integer, nullable=True, comment="Odometer reading at the time of inspection"
    )
    logged_date = Column(
        Date, nullable=True, comment="Date when the inspection details were logged"
    )
    logged_time = Column(
        String(255),
        nullable=True,
        comment="Time when the inspection details were logged",
    )
    inspection_fee = Column(
        Float, nullable=True, comment="Fee charged for the inspection"
    )
    result = Column(
        Enum("Pass", "Fail", name="inspection_result"),
        nullable=True,
        comment="Result of the inspection",
    )
    next_inspection_due_date = Column(
        Date, nullable=True, comment="Date when the next inspection is due"
    )
    status = Column(String(50), nullable=False, comment="Registration Status")
    # Relationship with the Vehicle
    vehicle = relationship("Vehicle", back_populates="inspections")

    def to_dict(self):
        """Convert VehicleInspection object to a dictionary"""
        return {
            "id": self.id,
            "vehicle_id": self.vehicle_id,
            "inspection_type": self.inspection_type,
            "mile_run": self.mile_run,
            "inspection_date": self.inspection_date,
            "inspection_time": self.inspection_time,
            "odometer_reading_date": self.odometer_reading_date,
            "odometer_reading_time": self.odometer_reading_time,
            "odometer_reading": self.odometer_reading,
            "logged_date": self.logged_date,
            "logged_time": self.logged_time,
            "inspection_fee": self.inspection_fee,
            "result": self.result,
            "next_inspection_due_date": self.next_inspection_due_date,
            "status": self.status,
        }


class VehicleRegistration(Base, AuditMixin):
    """
    Vehicle Registration model
    """

    __tablename__ = "vehicle_registration"

    id = Column(
        Integer,
        primary_key=True,
        nullable=False,
        comment="Primary Key for Vehicle Registration",
    )
    vehicle_id = Column(
        Integer,
        ForeignKey("vehicles.id"),
        nullable=False,
        comment="Foreign Key to Vehicle table",
    )
    registration_date = Column(Date, nullable=False, comment="Date of registration")
    registration_expiry_date = Column(
        Date, nullable=False, comment="Expiry date of the registration"
    )
    registration_fee = Column(
        Float, nullable=True, comment="Fee paid for the registration"
    )
    registration_state = Column(String(2), nullable=True, comment="Registration State")
    registration_class = Column(String(2), nullable=True, comment="Registration Class")
    plate_number = Column(String(255), nullable=True, comment="Vehicle plate number")
    status = Column(String(50), nullable=False, comment="Registration Status")
    # Back-populates the relationship with Vehicle
    vehicle = relationship("Vehicle", back_populates="registrations")

    def to_dict(self):
        """Convert VehicleRegistration object to a dictionary"""
        return {
            "id": self.id,
            "vehicle_id": self.vehicle_id,
            "registration_date": self.registration_date,
            "registration_expiry_date": self.registration_expiry_date,
            "registration_fee": self.registration_fee,
            "registration_state": self.registration_state,
            "registration_class": self.registration_class,
            "plate_number": self.plate_number,
            "status": self.status,
        }


class VehicleHackUp(Base, AuditMixin):
    """
    Vehicle HackUp model
    """

    __tablename__ = "vehicle_hackups"

    id = Column(
        Integer,
        primary_key=True,
        nullable=False,
        comment="Primary Key for Vehicle HackUp",
    )
    vehicle_id = Column(
        Integer,
        ForeignKey("vehicles.id"),
        nullable=False,
        comment="Foreign Key to Vehicles table",
    )
    tpep_provider = Column(
        String(255), nullable=True, comment="TPEP Type selected by the user"
    )
    configuration_type = Column(
        String(255), nullable=True, comment="Configuration type Camera or Partition"
    )

    paint_task_id = Column(
        Integer,
        ForeignKey("hackup_tasks.id"),
        nullable=True,
        comment="Foreign Key to HackUp Tasks table",
    )

    camera_task_id = Column(
        Integer,
        ForeignKey("hackup_tasks.id"),
        nullable=True,
        comment="Foreign Key to HackUp Tasks table",
    )

    partition_task_id = Column(
        Integer,
        ForeignKey("hackup_tasks.id"),
        nullable=True,
        comment="Foreign Key to HackUp Tasks table",
    )

    meter_task_id = Column(
        Integer,
        ForeignKey("hackup_tasks.id"),
        nullable=True,
        comment="Foreign Key to HackUp Tasks table",
    )

    meter_serial_number = Column(
        String(255), nullable=True, comment="Serial number of the installed meter"
    )

    rooftop_task_id = Column(
        Integer,
        ForeignKey("hackup_tasks.id"),
        nullable=True,
        comment="Foreign Key to HackUp Tasks table",
    )

    dmv_registration_task_id = Column(
        Integer,
        ForeignKey("hackup_tasks.id"),
        nullable=True,
        comment="Foreign Key to HackUp Tasks table",
    )

    tlc_inspection_task_id = Column(
        Integer,
        ForeignKey("hackup_tasks.id"),
        nullable=True,
        comment="Foreign Key to HackUp Tasks table",
    )

    dealership_task_id = Column(
        Integer,
        ForeignKey("hackup_tasks.id"),
        nullable=True,
        comment="Foreign Key to HackUp Tasks table",
    )

    bat_garage_task_id = Column(
        Integer,
        ForeignKey("hackup_tasks.id"),
        nullable=True,
        comment="Foreign Key to HackUp Tasks table",
    )

    is_insurance_procured = Column(
        Boolean, nullable=True, comment="The insurance procured for the vehicle"
    )

    insurance_number = Column(
        String(255), nullable=True, comment="Insurance number of the vehicle"
    )

    insurance_start_date = Column(
        Date, nullable=True, comment="Insurance start date of the vehicle"
    )

    insurance_end_date = Column(
        Date, nullable=True, comment="Insurance end date of the vehicle"
    )

    status = Column(String(50), nullable=False, comment="HackUp Status")

    # Relationships
    vehicle = relationship("Vehicle", back_populates="hackups")
    paint_task = relationship(
        "HackUpTasks", foreign_keys=[paint_task_id], back_populates="paint_hackups"
    )
    camera_task = relationship(
        "HackUpTasks", foreign_keys=[camera_task_id], back_populates="camera_hackups"
    )
    partition_task = relationship(
        "HackUpTasks",
        foreign_keys=[partition_task_id],
        back_populates="partition_hackups",
    )
    meter_task = relationship(
        "HackUpTasks", foreign_keys=[meter_task_id], back_populates="meter_hackups"
    )
    rooftop_task = relationship(
        "HackUpTasks", foreign_keys=[rooftop_task_id], back_populates="rooftop_hackups"
    )
    dmv_registration_task = relationship(
        "HackUpTasks",
        foreign_keys=[dmv_registration_task_id],
        back_populates="dmv_registration_hackups",
    )
    tlc_inspection_task = relationship(
        "HackUpTasks",
        foreign_keys=[tlc_inspection_task_id],
        back_populates="tlc_inspection_hackups",
    )
    dealership_task = relationship(
        "HackUpTasks",
        foreign_keys=[dealership_task_id],
        back_populates="dealership_hackups",
    )
    bat_garage_task = relationship(
        "HackUpTasks",
        foreign_keys=[bat_garage_task_id],
        back_populates="bat_garage_hackups",
    )

    def to_dict(self):
        """Convert VehicleHackUp object to a dictionary"""
        return {
            "id": self.id,
            "vehicle_id": self.vehicle_id,
            "tpep_type": self.tpep_provider,
            "configuration_type": self.configuration_type,
            "paint": self.paint_task.to_dict() if self.paint_task else {},
            "camera": self.camera_task.to_dict() if self.camera_task else {},
            "partition": self.partition_task.to_dict() if self.partition_task else {},
            "meter_type": self.meter_type,
            "meter_serial_number": self.meter_serial_number,
            "meter": self.meter_task.to_dict() if self.meter_task else {},
            "rooftop": self.rooftop_task.to_dict() if self.rooftop_task else {},
            "dmv_registration": self.dmv_registration_task.to_dict()
            if self.dmv_registration_task
            else {},
            "tlc_inspection": self.tlc_inspection_task.to_dict()
            if self.tlc_inspection_task
            else {},
            "dealership": self.dealership_task.to_dict()
            if self.dealership_task
            else {},
            "bat_garage": self.bat_garage_task.to_dict()
            if self.bat_garage_task
            else {},
            "status": self.status,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
        }


class HackUpTasks(Base, AuditMixin):
    __tablename__ = "hackup_tasks"

    id = Column(Integer, primary_key=True, nullable=False)
    task_name = Column(String(255), nullable=True)
    drop_location = Column(String(255), nullable=True)
    drop_by = Column(String(255), nullable=True)
    drop_date = Column(Date, nullable=True)
    completed_by = Column(String(255), nullable=True)
    completed_date = Column(Date, nullable=True)
    status = Column(String(255), nullable=True)
    amount = Column(Float, nullable=True , default=0.0)
    note = Column(Text, nullable=True)
    is_task_done = Column(Boolean, nullable=True)
    is_required = Column(Boolean, nullable=True)

    # relationship
    paint_hackups = relationship(
        "VehicleHackUp",
        back_populates="paint_task",
        foreign_keys="VehicleHackUp.paint_task_id",
    )
    camera_hackups = relationship(
        "VehicleHackUp",
        back_populates="camera_task",
        foreign_keys="VehicleHackUp.camera_task_id",
    )
    partition_hackups = relationship(
        "VehicleHackUp",
        back_populates="partition_task",
        foreign_keys="VehicleHackUp.partition_task_id",
    )
    meter_hackups = relationship(
        "VehicleHackUp",
        back_populates="meter_task",
        foreign_keys="VehicleHackUp.meter_task_id",
    )
    rooftop_hackups = relationship(
        "VehicleHackUp",
        back_populates="rooftop_task",
        foreign_keys="VehicleHackUp.rooftop_task_id",
    )
    dmv_registration_hackups = relationship(
        "VehicleHackUp",
        back_populates="dmv_registration_task",
        foreign_keys="VehicleHackUp.dmv_registration_task_id",
    )
    tlc_inspection_hackups = relationship(
        "VehicleHackUp",
        back_populates="tlc_inspection_task",
        foreign_keys="VehicleHackUp.tlc_inspection_task_id",
    )
    dealership_hackups = relationship(
        "VehicleHackUp",
        back_populates="dealership_task",
        foreign_keys="VehicleHackUp.dealership_task_id",
    )
    bat_garage_hackups = relationship(
        "VehicleHackUp",
        back_populates="bat_garage_task",
        foreign_keys="VehicleHackUp.bat_garage_task_id",
    )

    def to_dict(self):
        return {
            "id": self.id,
            "task_name": self.task_name,
            "drop_location": self.drop_location,
            "drop_by": self.drop_by,
            "drop_date": self.drop_date,
            "completed_by": self.completed_by,
            "completed_date": self.completed_date,
            "status": self.status,
            "amount": self.amount,
            "note": self.note,
            "is_task_done": self.is_task_done,
            "is_required": self.is_required,
        }


class Vehicle(Base, AuditMixin):
    """
    Vehicle model
    """

    __tablename__ = "vehicles"

    id = Column(
        Integer, primary_key=True, nullable=False, comment="Primary Key for Vehicles"
    )
    vin = Column(String(64), nullable=True, comment="Vehicle Identification Number")
    make = Column(String(45), nullable=True, comment="Make of the vehicle")
    model = Column(String(45), nullable=True, comment="Model of the vehicle")

    year = Column(String(4), nullable=True)
    cylinders = Column(Integer, nullable=True)
    color = Column(String(50), nullable=True)
    vehicle_type = Column(
        String(255), nullable=True, comment="Would be either Regular or Wav"
    )
    is_hybrid = Column(Boolean, nullable=True, comment="Is the vehicle hybrid or not")
    base_price = Column(
        Float, nullable=True, default=0.0, comment="Base price of the vehicle"
    )
    sales_tax = Column(
        Float, nullable=True, default=0.0, comment="Sales tax applied to the vehicle"
    )
    vehicle_total_price = Column(
        Integer, nullable=True, comment="Total cost of the vehicle"
    )
    vehicle_true_cost = Column(
        Integer,
        nullable=True,
        comment="True cost typically could mean that hack up costs and other miscellaneous costs are accounted",
    )
    vehicle_hack_up_cost = Column(
        Integer, nullable=True, comment="Total hack up cost of the vehicle"
    )
    vehicle_lifetime_cap = Column(
        Integer,
        nullable=True,
        comment="Cap calculated off between true cost or tlc cap",
    )

    invoice_number = Column(
        String(255), nullable=True, comment="Invoice number of the vehicle"
    )
    invoice_date = Column(Date, nullable=True, comment="Invoice date of the vehicle")

    # TODO: This may be removed, keeping it for now
    vehicle_office = Column(String(255), nullable=True, comment="Vehicle Office")
    is_delivered = Column(
        Boolean, nullable=True, comment="Is the vehicle hybrid or not"
    )
    expected_delivery_date = Column(
        Date, nullable=True, comment="The expected delivery date of the vehicle"
    )
    delivery_location = Column(
        String(255), nullable=True, comment="Delivery location of the vehicle"
    )
    delivery_note = Column(Text, nullable=True, comment="Delivery note for the vehicle")
    is_insurance_procured = Column(
        Boolean, nullable=True, comment="The insurance procured for the vehicle"
    )
    is_medallion_assigned = Column(
        Boolean, nullable=True, comment="Is a medallion allocated to the vehicle"
    )

    current_location = Column(
        String(255), nullable=True, comment="Current location of the vehicle"
    )

    specific_location = Column(
        String(255), nullable=True, comment="Specific location of the vehicle"
    )
    
    location_changed_date = Column(
        Date, nullable=True, comment="Date when the location was last changed"
    )

    comment_for_location_change = Column(
        Text, nullable=True, comment="Comment for the location change"
    )

    vehicle_status = Column(
        String(50),
        nullable=True,
        comment="Status of the vehicle - one among Registration In Progress, Registered, Delivered, Hacked, Active & Inactive",
    )
    entity_id = Column(
        Integer,
        ForeignKey("vehicle_entity.id"),
        nullable=True,
        comment="Foreign Key to Entity Table",
    )
    dealer_id = Column(
        Integer,
        ForeignKey("dealers.id"),
        nullable=True,
        comment="Foreign Key to dealer Table",
    )
    medallion_id = Column(
        Integer,
        ForeignKey("medallions.id"),
        nullable=True,
        comment="Foreign Key to medallion Table",
    )
    tsp = Column(
        String(255), nullable=True, comment="Taxi and Limousine Commission (TLC) Permit"
    )
    security_type = Column(String(255), nullable=True, comment="Security Type")
    vehicle_entity = relationship(
        "VehicleEntity", back_populates="vehicles", foreign_keys=[entity_id]
    )
    dealer = relationship("Dealer", back_populates="vehicle", foreign_keys=[dealer_id])

    hackups = relationship(
        "VehicleHackUp",
        back_populates="vehicle",
    )

    registrations = relationship("VehicleRegistration", back_populates="vehicle")

    inspections = relationship("VehicleInspection", back_populates="vehicle")

    lease = relationship(
        "Lease",
        back_populates="vehicle",
    )

    medallions = relationship("Medallion", back_populates="vehicle")

    vehicle_notes = relationship("VehicleNote", back_populates="vehicle")

    insurances = relationship(
        "VehicleInsurance",
        back_populates="vehicle",
        cascade="all, delete-orphan"
    )
    
    dtrs = relationship(
        "DTR", 
        back_populates="vehicle",
        viewonly=True
    )

    expenses_and_compliance = relationship("VehicleExpensesAndCompliance", back_populates="vehicle")

    def get_active_plate_number(self):
        """
        Get the plate number from the active vehicle registration.
        Falls back to the first registration if no active one exists.
        Returns None if no registrations exist.
        """
        if not self.registrations:
            return None

        # Try to find an active registration
        active_reg = next(
            (reg for reg in self.registrations if reg.status == "Active"),
            None
        )

        # If no active registration, use the first one
        if not active_reg and self.registrations:
            active_reg = self.registrations[0]

        return active_reg.plate_number if active_reg else None

    def to_dict(self):
        """Convert Vehicle object to a dictionary"""
        from app.core.config import settings

        # Get latest active registration
        active_registration = None
        if self.registrations:
            active_registration = next(
                (reg for reg in self.registrations if reg.status == "Active"), None
            )

        return {
            "vehicle_id": self.id,
            "vin": self.vin,
            "make": self.make,
            "model": self.model,
            "year": self.year,
            "cylinders": self.cylinders,
            "color": self.color,
            "vehicle_type": self.vehicle_type,
            "is_hybrid": self.is_hybrid,
            "base_price": self.base_price,
            "sales_tax": self.sales_tax,
            "vehicle_office": self.vehicle_office,
            "is_delivered": self.is_delivered,
            "expected_delivery_date": self.expected_delivery_date,
            "delivery_location": self.delivery_location,
            "delivery_note": self.delivery_note,
            "current_location": self.current_location,
            "specific_location": self.specific_location,
            "location_changed_date": self.location_changed_date,
            "comment_for_location_change": self.comment_for_location_change,
            "is_insurance_procured": self.is_insurance_procured,
            "is_medallion_assigned": self.is_medallion_assigned,
            "vehicle_status": self.vehicle_status,
            "entity_id": self.entity_id,
            "entity_name": self.vehicle_entity.entity_name
            if self.vehicle_entity
            else None,
            "dealer_id": self.dealer_id,
            "medallion_id": self.medallion_id,
            "plate_number": active_registration.plate_number
            if active_registration
            else "",
            "registration_state": active_registration.registration_state
            if active_registration
            else "",
            "registration_date": active_registration.registration_date.strftime(
                settings.common_date_format
            )
            if active_registration
            and active_registration.registration_date
            and settings.common_date_format
            else "",
            "created_on": self.created_on,
            "updated_on": self.updated_on,
        }


class VehicleInsurance(Base, AuditMixin):
    """
    Vehicle Insurance model
    """

    __tablename__ = "vehicle_insurance"

    id = Column(
        Integer,
        primary_key=True,
        nullable=False,
        comment="Primary Key for Vehicle Insurance",
    )
    vehicle_id = Column(
        Integer,
        ForeignKey("vehicles.id"),
        nullable=False,
        comment="Foreign Key to Vehicle table",
    )
    insurance_type = Column(
        String(255), nullable=True, comment="Type of insurance"
    )

    insurance_number = Column(
        String(255), nullable=True, comment="Insurance number of the vehicle"
    )
    
    insurance_start_date = Column(
        Date, nullable=True, comment="Insurance start date of the vehicle"
    )

    insurance_end_date = Column(
        Date, nullable=True, comment="Insurance end date of the vehicle"
    )

    amount = Column(
        Float, nullable=True, comment="Insurance amount"
    )

    status = Column(String(50), nullable=False, comment="Insurance Status")

    # Relationship with the Vehicle
    vehicle = relationship(
        "Vehicle",
        back_populates="insurances"
    )

    def to_dict(self):
        """Convert VehicleInsurance object to a dictionary"""
        return {
            "id": self.id,
            "vehicle_id": self.vehicle_id,
            "insurance_type": self.insurance_type,
            "insurance_number": self.insurance_number,
            "insurance_start_date": self.insurance_start_date,
            "insurance_end_date": self.insurance_end_date,
            "amount": self.amount,
            "status": self.status,
        }