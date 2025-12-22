## app/bpm_flows/new_vehicle/utils.py

# Standard library imports
from datetime import datetime

# Third-party imports
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import and_

# Local application imports
import app.bat.utils as bat_utils
from app.vehicles.models import Vehicle, VehicleRegistration, VehicleHackUp, Dealer
from app.medallions.models import Medallion
from app.leases.models import Lease
from app.entities.models import Entity
from app.vehicles.schemas import VehicleStatus
from app.leases.schemas import LeaseStatus
from app.medallions.schemas import MedallionStatus
from app.uploads.models import Document
from app.utils.logger import get_logger

logger = get_logger(__name__)


def set_vehicle_de_hack_status(db: Session, vehicle: Vehicle):

    medallion = bat_utils.fetch_medallion_by_medallion_id(db, vehicle.medallion_id)

    if not medallion:
        raise HTTPException(status_code=404 , detail="Medallion not found for the vehicle")
    
    if medallion.medallion_status != "Y" :
        raise HTTPException(status_code=404 , detail="Medallion is not active could not de-hack vehicle")
    
    if vehicle.vehicle_status != VehicleStatus.HACKED_UP :
        raise HTTPException(status_code=404 , detail=f"Vehicle is not in {VehicleStatus.HACKED_UP.value} status, cannot be Dehacked")
    
    medallion.medallion_status = MedallionStatus.AVAILABLE
    vehicle.vehicle_status = VehicleStatus.AVAILABLE
    vehicle.is_medallion_assigned = False
    vehicle.medallion_id = None
    #TODO: replace delete is_active = false
    hackup=db.query(VehicleHackUp).filter(VehicleHackUp.vehicle_id==vehicle.id).first()
    hackup.is_active=False
    hackup.status= LeaseStatus.INACTIVE
    
    db.flush()


def terminate_vehicle(db: Session, vehicle: Vehicle):
    """
    Terminates the vehicle for a given vehicle.

    Args:
        vehicle (Vehicle): The vehicle object to terminate the lease for.
        db (Session): The SQLAlchemy database session.

    """

    medallion = db.query(Medallion).filter(
        Medallion.id == vehicle.medallion_id).first()
    
    hackup = db.query(VehicleHackUp).filter(VehicleHackUp.vehicle_id==vehicle.id,
                                                       VehicleHackUp.is_active==True).first()
    
    if hackup :
        raise HTTPException(status_code=404 , detail="vehicle is hackup , cannot terminate")
    
    if vehicle.vehicle_status == VehicleStatus.ARCHIVED :
        raise HTTPException(status_code=404, detail="Vehicle already terminated")
    
    
    
    Active_lease= db.query(Lease).filter(and_(
        Lease.vehicle_id == vehicle.id ,
        Lease.lease_status == "Active"
        )).first()
    
    if Active_lease :
        raise HTTPException(status_code=404 , detail="vehicle lease is active cannot terminate")
    
    if medallion :
        if medallion.medallion_status in ["V"]:
            medallion.medallion_status = MedallionStatus.AVAILABLE
    
    vehicle.vehicle_status = VehicleStatus.ARCHIVED
    vehicle.is_medallion_assigned = False
    vehicle.medallion_id = None
    db.flush()


def get_documents_for_vehicle(db: Session, vehicle_id: str):
    """
    Fetch the documents for a given vehicle
    """
    documents = db.query(Document).filter(
        Document.object_type == "vehicle",
        Document.object_lookup_id == vehicle_id
    ).all()

    for document in documents:
        document.presigned_url = get_presigned_url(document)

    return documents


def create_or_update_empty_vehicle(db: Session, entity_id: int = None) -> Vehicle:
    """
    Creates or updates an empty Driver object in the database.

    Args:
        db (Session): The database session.
        driver_id (int, optional): ID of the driver to update. If None, a new driver will be created.

    Returns:
        Driver: The created or updated Driver object.
    """

    # Create a new vehicle
    new_vehicle = Vehicle()

    new_vehicle.entity_id = entity_id
    new_vehicle.vehicle_status = VehicleStatus.IN_PROGRESS
    db.add(new_vehicle)
    db.flush()
    db.refresh(new_vehicle)
    return new_vehicle


def get_vehicle_details(db: Session, vehicle: Vehicle):
    """
    Returns basic details of a vehicle.

    Args:
        vehicle (Vehicle): The vehicle object.
        db (Session): The SQLAlchemy database session.

    Returns:
        dict: A dictionary containing vehicle details.
    """
    # Fetch the associated entity name
    entity = db.query(Entity).filter(
        Entity.id == vehicle.entity_id).first()
    entity_name = entity.entity_name if entity else ""

    # Return the vehicle details
    return {
        "vehicle_id": vehicle.id,
        "entity_name": entity_name,
        "vin": vehicle.vin if vehicle.vin else "",
        "make": vehicle.make if vehicle.make else "",
        "model": vehicle.model if vehicle.model else "",
        "year": vehicle.year if vehicle.year else "",
        "vehicle_type": vehicle.vehicle_type if vehicle.vehicle_type else "",
        "cylinders": vehicle.cylinders if vehicle.cylinders else 0,
    }


def fetch_vehicle_by_vehicle_id(db: Session, vehicle_id: str) -> Vehicle:
    """
    Fetch the vehicle by vehicle id
    """
    vehicle = db.query(Vehicle).filter(
        Vehicle.id == vehicle_id).first()
    
    if not vehicle :
        raise ValueError("vehicle not found")

    return vehicle


def fetch_vehicle_by_vin(db: Session, vehicle_vin: str) -> Vehicle:
    """
    Fetch the vehicle by vin
    """
    vehicle = db.query(Vehicle).filter(
        Vehicle.vin == vehicle_vin).first()

    return vehicle


def fetch_vehicle_details(db: Session, vehicle: Vehicle):
    """
    Fetches detailed information for a given vehicle.

    Args:
        vehicle (Vehicle): The vehicle object.
        db (Session): The SQLAlchemy database session.

    Returns:
        dict: A dictionary containing vehicle and dealer information.
    """
    if not vehicle:
        return {"error": "Vehicle not found"}

    # Fetch the associated dealer
    dealer = db.query(Dealer).filter(
        Dealer.id == vehicle.dealer_id).first()

    # Prepare the result
    return {
        "vehicle": {
            "vin": vehicle.vin,
            "make": vehicle.make,
            "model": vehicle.model,
            "year": vehicle.year,
            "cylinders": str(vehicle.cylinders) if vehicle.cylinders else 0,
            "color": vehicle.color,
            "vehicle_type": vehicle.vehicle_type,
            "is_hybrid": vehicle.is_hybrid,
            "base_price": vehicle.base_price,
            "sales_tax": vehicle.sales_tax,
            "vehicle_office": vehicle.vehicle_office,
            "entity_name": vehicle.vehicle_entity.entity_name if vehicle.vehicle_entity else ""
        },
        "dealer": {
            "dealer_name": dealer.dealer_name if dealer else "",
            "dealer_bank_name": dealer.dealer_bank_name if dealer else "",
            "dealer_bank_account_number": dealer.dealer_bank_account_number if dealer else "",
        }
    }


def process_vehicle_details(db: Session, vehicle: Vehicle, vehicle_data: dict):
    """
    Dynamically creates or updates the Vehicle and Dealer based on the provided data.

    Args:
        data (dict): A dictionary containing the fields to update or create.
        db (Session): The SQLAlchemy database session.

    Returns:
        dict: A dictionary containing the result of the operation.
    """
    # Dealer-specific fields
    dealer_fields = ["dealer_name", "dealer_bank_name",
                     "dealer_bank_account_number"]

    # Find or create the dealer
    dealer = db.query(Dealer).filter(
        Dealer.dealer_bank_account_number == vehicle_data.get("dealer_bank_account_number")).first()

    if not dealer:
        logger.info("Creating a new dealer")
        dealer = Dealer()
        db.add(dealer)

    # Update dealer fields dynamically
    for field in dealer_fields:
        if field in vehicle_data:
            setattr(dealer, field, vehicle_data[field])

    # Vehicle-specific fields
    vehicle_fields = [
        "vin", "make", "model", "year", "cylinders", "color",
        "vehicle_type", "is_hybrid", "base_price", "sales_tax", "vehicle_office"
    ]

    if not vehicle:
        logger.info("Vehicle does not exist, creating one")
        vehicle = vehicle_data.Vehicle()
        db.add(vehicle)

    # Update vehicle fields dynamically
    for field in vehicle_fields:
        if field in vehicle_data:
            setattr(vehicle, field, vehicle_data[field])

    # Link the dealer to the vehicle
    vehicle.dealer = dealer
    return (vehicle, dealer)


def update_vehicle_delivery_info(
    db: Session,
    vehicle: Vehicle,
    expected_delivery_date: str,
    delivery_location: str
):
    """
    Updates the expected delivery date and delivery location for a given vehicle.

    Args:
        vehicle (Vehicle): The vehicle object to update.
        db (Session): The SQLAlchemy database session.
        expected_delivery_date (date): The new expected delivery date.
        delivery_location (str): The new delivery location.

    Returns:
        vehicle: An updated vehicle object.
    """
    try:
        delivery_date = datetime.strptime(
            expected_delivery_date, "%Y-%m-%d"
        ).date()
    except ValueError:
        return {"error": "Invalid date format. Use YYYY-MM-DD."}

    vehicle.expected_delivery_date = delivery_date
    vehicle.delivery_location = delivery_location
    return vehicle


def get_vehicle_delivery_info(
    db: Session,
    vehicle: Vehicle,
):
    """
    Fetch the vehicle delivery information
    """
    return {
        "expected_delivery_date": vehicle.expected_delivery_date,
        "delivery_location": vehicle.delivery_location
    }


def update_vehicle_delivery_complete_info(
    db: Session,
    vehicle: Vehicle,
    is_delivered: bool,
    is_insurance_procured: bool,
    tlc_hackup_inspection_date: str
):
    """
    Updates the expected delivery date and delivery location for a given vehicle.

    Args:
        vehicle (Vehicle): The vehicle object to update.
        db (Session): The SQLAlchemy database session.
        expected_delivery_date (date): The new expected delivery date.
        delivery_location (str): The new delivery location.

    Returns:
        vehicle: An updated vehicle object.
    """
    try:
        tlc_hackup_inspection_date = datetime.strptime(
            tlc_hackup_inspection_date, "%Y-%m-%d").date()
    except ValueError:
        return {"error": "Invalid date format. Use YYYY-MM-DD."}
    vehicle.vehicle_status= VehicleStatus.AVAILABLE
    vehicle.is_delivered = is_delivered
    vehicle.is_insurance_procured = is_insurance_procured
    vehicle.tlc_hackup_inspection_date = tlc_hackup_inspection_date
    
    db.flush()
    return vehicle


def get_vehicle_delivery_complete_info(
    db: Session,
    vehicle: Vehicle,
):
    """
    Fetch the vehicle delivery complete information
    """
    return {"is_insurance_procured": vehicle.is_insurance_procured if vehicle.is_insurance_procured else "", "is_delivered": vehicle.is_delivered if vehicle.is_delivered else "", "tlc_hackup_inspection_date": vehicle.tlc_hackup_inspection_date if vehicle.tlc_hackup_inspection_date else ""}


def get_active_vehicle_registration(db: Session, vehicle: Vehicle):
    """
    Fetch the active VehicleRegistration for a given vehicle.
    """
    return db.query(VehicleRegistration).filter_by(vehicle_id=vehicle.id, status="active").first()