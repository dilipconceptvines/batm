# Standard library imports
from datetime import datetime

# Third party imports
import pandas as pd
from sqlalchemy.orm import Session

# Local imports
from app.core.db import SessionLocal
from app.core.config import settings
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.vehicles.models import VehicleEntity
from app.medallions.models import Medallion
from app.vehicles.models import Vehicle
from app.vehicles.schemas import VehicleStatus
from app.medallions.schemas import MedallionStatus
from app.core.config import settings
from app.utils.general import get_safe_value

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def parse_date(date_str):
    """
    Parses a date string into a datetime object.
    If the string is invalid or empty, returns None.
    """
    try:
        return date_str.to_pydatetime() if pd.notnull(date_str) else None
    except ValueError:
        return None

def parse_vehicles(db: Session, df: pd.DataFrame):
    """Parse and load vehicles from dataframe into database."""
    try:
        for _, row in df.iterrows():
            # Use get_safe_value() to safely fetch values from DataFrame rows
            vin = get_safe_value(row, "vin")
            entity_name = get_safe_value(row, "entity_name")
            
            # Skip rows missing mandatory fields
            if not vin:
                logger.warning("Skipping row with missing VIN")
                continue

            # Find the associated entity based on the entity name
            entity = db.query(VehicleEntity).filter(
                VehicleEntity.entity_name == entity_name).first()
            

            # Find associated medallion based in the medallion number
            medallion_id = None
            medallion_number = get_safe_value(row, "medallion_number")
            if medallion_number:
                medallion = db.query(Medallion).filter(Medallion.medallion_number == medallion_number).first()
                if medallion:
                    medallion_id = medallion.id
                    medallion.medallion_status = MedallionStatus.ASSIGNED_TO_VEHICLE
                    medallion.is_active = True
                    db.add(medallion) 

            # Check for existing records
            vehicle = db.query(Vehicle).filter(Vehicle.vin == vin).first()

            vehicle_total_price = (get_safe_value(row, "base_price") or 0) + (get_safe_value(row, "sales_tax") or 0)
            vehicle_true_cost = vehicle_total_price + (get_safe_value(row, "vehicle_hack_up_cost") or 0)


            if vehicle:
                # Update existing records
                logger.info("Updating existing vehicle with VIN: %s", vin)
                vehicle.make = get_safe_value(row, "make")
                vehicle.model = get_safe_value(row, "model")
                vehicle.year = get_safe_value(row, "year")
                vehicle.cylinders = get_safe_value(row, "cylinders")
                vehicle.color = get_safe_value(row, "color")
                vehicle.vehicle_type = get_safe_value(row, "vehicle_type")
                vehicle.is_hybrid = get_safe_value(row, "is_hybrid")
                vehicle.base_price = get_safe_value(row, "base_price") or 0
                vehicle.sales_tax = get_safe_value(row, "sales_tax") or 0
                vehicle.vehicle_office = get_safe_value(row, "vehicle_office")
                vehicle.is_delivered = get_safe_value(row, "is_delivered")
                vehicle.expected_delivery_date = parse_date(get_safe_value(row, "expected_delivery_date"))
                vehicle.delivery_location = get_safe_value(row, "delivery_location")
                vehicle.is_insurance_procured = get_safe_value(row, "is_insurance_procured")
                vehicle.is_medallion_assigned = get_safe_value(row, "is_medallion_assigned")
                vehicle.entity_id = entity.id if entity else None
                vehicle.medallion_id = medallion_id
                vehicle.vehicle_total_price = vehicle_total_price
                vehicle.vehicle_true_cost = vehicle_true_cost
                vehicle.vehicle_hack_up_cost = get_safe_value(row, "vehicle_hack_up_cost") or 0
                vehicle.vehicle_lifetime_cap = vehicle_true_cost if vehicle_true_cost < settings.tlc_vehicle_cap_total else settings.tlc_vehicle_cap_total
            else:
                # Insert new ones
                logger.info("Adding new vehicle with VIN: %s", vin)
                vehicle = Vehicle(
                    vin=vin,
                    make=get_safe_value(row, "make"),
                    model=get_safe_value(row, "model"),
                    year=get_safe_value(row, "year"),
                    cylinders=get_safe_value(row, "cylinders"),
                    color=get_safe_value(row, "color"),
                    vehicle_type=get_safe_value(row, "vehicle_type"),
                    is_hybrid=get_safe_value(row, "is_hybrid"),
                    base_price=get_safe_value(row, "base_price") or 0,
                    sales_tax=get_safe_value(row, "sales_tax") or 0,
                    vehicle_office=get_safe_value(row, "vehicle_office"),
                    is_delivered=get_safe_value(row, "is_delivered"),
                    expected_delivery_date=parse_date(get_safe_value(row, "expected_delivery_date")),
                    delivery_location=get_safe_value(row, "delivery_location"),
                    is_insurance_procured=get_safe_value(row, "is_insurance_procured"),
                    is_medallion_assigned=True if medallion_id else False,
                    vehicle_status=VehicleStatus.AVAILABLE,
                    entity_id=entity.id if entity else None,
                    medallion_id=medallion_id if medallion_id else None,
                    vehicle_total_price= vehicle_total_price,
                    vehicle_true_cost= vehicle_true_cost,
                    vehicle_hack_up_cost=get_safe_value(row, "vehicle_hack_up_cost") or 0,
                    vehicle_lifetime_cap= vehicle_true_cost if vehicle_true_cost < settings.tlc_vehicle_cap_total else settings.tlc_vehicle_cap_total,
                    is_active=True,
                    created_by=SUPERADMIN_USER_ID,
                    created_on=datetime.now()
                )
                db.add(vehicle)
                db.flush()

                logger.info("Vehicle '%s' added to the database.", vin)

        db.commit()
        logger.info("✅ Data successfully processed.")
    except Exception as e:
        db.rollback()
        logger.error("Error parsing data: %s", e)
        raise
    finally:
        db.close()

if __name__ == "__main__":
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bat_file_key))
    vehicles_df = pd.read_excel(excel_file, 'vehicles')
    parse_vehicles(db_session, vehicles_df)
    db_session.close()



