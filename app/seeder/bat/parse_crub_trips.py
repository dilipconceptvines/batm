# Third party imports
import pandas as pd
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.curb.models import CURBTrip
from app.ledger.models import LedgerEntry
from app.ledger.schemas import LedgerSourceType
from app.leases.services import lease_service
from app.drivers.services import driver_service
from app.utils.general import get_random_date

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def parse_crub_trips(db: Session, df: pd.DataFrame):
    """Parse CRUB trips"""
    try:
        for _, row in df.iterrows():
            date = get_random_date(days=7)
            # Convert date strings to datetime objects
            start_dt = pd.to_datetime(row["start_time"], errors='coerce')
            end_dt = pd.to_datetime(row["end_time"], errors='coerce')

            record_id = row["record_id"]
            period = row["period"]
            cab_number = row["cab_number"]
            driver_id = row["driver_id"]
            start_date = date
            start_time = start_dt.time() if pd.notnull(start_dt) else None
            end_date = date
            end_time = end_dt.time() if pd.notnull(end_dt) else None
            trip_amount = row["trip_amount"]
            tips = row["tips"]
            extras = row["extras"]
            tolls = row["tolls"]
            tax = row["tax"]
            imp_tax = row["imp_tax"]
            total_amount = row["total_amount"]
            gps_start_lat = row["gps_start_lat"]
            gps_start_lon = row["gps_start_lon"]
            gps_end_lat = row["gps_end_lat"]
            gps_end_lon = row["gps_end_lon"]
            from_address = row["from_address"]
            to_address = row["to_address"]
            payment_type = row["payment_type"]
            auth_code = row["auth_code"]
            auth_amount = row["auth_amt"]
            ehail_fee = row["ehail_fee"]
            health_fee = row["health_fee"]
            passengers = row["passenger_count"]
            distance_service = row["distance_service"]
            distance_bs = row["distance_bs"]
            reservation_number = row["reservation_number"]
            congestion_fee = row["congestion_fee"]
            airport_fee = row["airport_fee"]
            cbdt_fee = row["cbdt"]

            # Create or update the CURBTrip record

            trip = db.query(CURBTrip).filter(CURBTrip.record_id == record_id).first()
            if trip:
                # Update existing record
                trip.period = period
                trip.cab_number = cab_number
                trip.driver_id = driver_id
                trip.start_date = start_date
                trip.start_time = start_time
                trip.end_date = end_date
                trip.end_time = end_time
                trip.trip_amount = trip_amount
                trip.tips = tips
                trip.extras = extras
                trip.tolls = tolls
                trip.tax = tax
                trip.imp_tax = imp_tax
                trip.total_amount = total_amount
                trip.gps_start_lat = gps_start_lat
                trip.gps_start_lon = gps_start_lon
                trip.gps_end_lat = gps_end_lat
                trip.gps_end_lon = gps_end_lon
                trip.from_address = from_address
                trip.to_address = to_address
                trip.payment_type = payment_type
                trip.auth_code = auth_code
                trip.auth_amount = auth_amount
                trip.ehail_fee = ehail_fee
                trip.health_fee = health_fee
                trip.passengers = passengers
                trip.distance_service = distance_service
                trip.distance_bs = distance_bs
                trip.reservation_number = reservation_number
                trip.congestion_fee = congestion_fee
                trip.airport_fee = airport_fee
                trip.cbdt_fee = cbdt_fee
                trip.is_reconciled = True

            else:
                # Create a new record
                trip = CURBTrip(
                    record_id=record_id,
                    period=period,
                    cab_number=cab_number,
                    driver_id=driver_id,
                    start_date=start_date,
                    start_time=start_time,
                    end_date=end_date,
                    end_time=end_time,
                    trip_amount=trip_amount,
                    tips=tips,
                    extras=extras,
                    tolls=tolls,
                    tax=tax,
                    imp_tax=imp_tax,
                    total_amount=total_amount,
                    gps_start_lat=gps_start_lat,
                    gps_start_lon=gps_start_lon,
                    gps_end_lat=gps_end_lat,
                    gps_end_lon=gps_end_lon,
                    from_address=from_address,
                    to_address=to_address,
                    payment_type=payment_type,
                    auth_code=auth_code,
                    auth_amount=auth_amount,
                    ehail_fee=ehail_fee,
                    health_fee=health_fee,
                    passengers=passengers,
                    distance_service=distance_service,
                    distance_bs=distance_bs,
                    reservation_number=reservation_number,
                    congestion_fee=congestion_fee,
                    airport_fee=airport_fee,
                    cbdt_fee=cbdt_fee,
                    is_reconciled = True
                )
                db.add(trip)

            logger.info("CRUB trips parsed successfully , Record: %s" , record_id)

            driver = driver_service.get_drivers(db=db , driver_id=driver_id)
            lease = lease_service.get_lease(db=db , driver_id= driver_id)
            if not driver or not lease:
                logger.warning("Skipping record %s due to missing driver or lease.", record_id)
                continue
            ledger_entry=LedgerEntry(
                driver_id = driver.id,
                medallion_id = lease.medallion_id,
                vehicle_id = lease.vehicle_id,
                amount = total_amount,
                debit = True,
                description = f"CRUB Trip start_date {start_date} to end_date {end_date}",
                source_type = LedgerSourceType.CURB,
                source_id = trip.id
            )

            db.add(ledger_entry)
            db.flush()
        # Commit the changes to the database

        db.commit()
    except Exception as e:
        logger.error("Error parsing CRUB trips: %s", e)
        db.rollback()
        raise

if __name__ == "__main__":
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bat_file_key))
    curb_df = pd.read_excel(excel_file, 'curb_trip')
    parse_crub_trips(db_session, curb_df)
    db_session.close()