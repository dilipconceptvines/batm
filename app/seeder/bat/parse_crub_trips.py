# Third party imports
import pandas as pd
from datetime import datetime, timedelta

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.curb.models import CURBTrip
from app.ledger.models import LedgerEntry
from app.ledger.schemas import LedgerSourceType
from app.leases.services import lease_service
from app.drivers.services import driver_service
from app.utils.general import get_random_date
from app.seeder_loader.parser_registry import parser
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df
import tempfile
import os

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="curb_trips",
    sheet_names=[data_loader_settings.parser_curb_trips_sheet],
    version="1.0",
    deprecated=False,
    description="Process curb trips from Excel sheet"
)
def parse_crub_trips(db: Session, df: pd.DataFrame) -> ParseResult:
    """Parse CRUB trips"""
    result = ParseResult(sheet_name=data_loader_settings.parser_curb_trips_sheet)
    created_count = 0
    updated_count = 0

    try:
        for idx, row in df.iterrows():
            try:
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
                    updated_count += 1
                    result.record_updated(idx)

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
                    created_count += 1
                    result.record_inserted(idx)

                # logger.info("CRUB trips parsed successfully , Record: %s" , record_id)

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
            
            except Exception as row_error:
                logger.exception("Error parsing CRUB trips row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))
                
        logger.info("✅ Data successfully processed.")
        return result
    except Exception as e:
        logger.exception("Critical failure in parser curb_trips: %s", e)
        raise RuntimeError(f"Parser curb_trips failed: {e}") from e

if __name__ == "__main__":
    logger.info("Loading curb trips configuration")
    db_session = SessionLocal()

    tmp_file_path = None
    try:
        # Download file to temp
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
            tmp_file_path = tmp_file.name
            
        file_bytes = s3_utils.download_file(settings.bat_file_key)
        if not file_bytes:
             raise Exception("Failed to download file from S3")
        
        with open(tmp_file_path, 'wb') as f:
            f.write(file_bytes)
            
        excel_file = pd.ExcelFile(tmp_file_path)
        data_df = pd.read_excel(excel_file, "curb_trip")

        # Unlike other parsers, this one didn't return a result in the original code, 
        # but we should probably update it to do so later or just handle the fact it doesn't.
        # However, checking the parser function (line 32), it DOES NOT return ParseResult currently.
        # It's void. 
        # I should probably update the parser function first to return ParseResult to stay consistent with USER REQUEST "implement this in all parse files".
        # But for now I'll just wrap the main logic and assume I need to fix the parser too.
        # Wait, the user requirement is "Modify all parser files... result, failed_reason...".
        # If parse_crub_trips doesn't return ParseResult, I can't apply it.
        # I need to check if I can quickly update the parser to return ParseResult.
        # Yes, I should update the parser signature first in a separate call if needed, but I can do it in parallel or sequential.
        # Let's verify line 32 in previous `view_file`.
        # def parse_crub_trips(db: Session, df: pd.DataFrame):
        # It does NOT return ParseResult. I MUST FIX THIS.
        
        # For this 'replace_file_content' call, I will assume I will fix the function in the next call.
        # Or better, I'll update the function signature AND return value in this same turn if possible? 
        # No, better to do signature update separately.
        # For now, I'll put the standard logic here, but it will fail if `result` is None.
        # I will update the function in the next tool call.
        
        # Actually, let's just stick to updating __main__ here, and I will fix the function immediately after.
        
        result = parse_crub_trips(db_session, data_df)
        
        # Since currently it returns None, this would fail. 
        # I'll comment out the apply/write-back part until I fix the function? 
        # No, I should fix the function FIRST. 
        # I'll abort this specific tool call and do the function update first?
        # No, I can't abort mid-stream easily. I will proceed with this update but I MUST update the function in the next tool call.
        # Actually, I'll just write the code assuming it returns result, and then fix the function. Python won't check types at runtime until execution.
        
        if result:
             updated_df = apply_parse_result_to_df(data_df, result)

             # Write back to temp file
             with pd.ExcelWriter(
                tmp_file_path,
                engine="openpyxl",
                mode="a",
                if_sheet_exists="replace"
             ) as writer:
                updated_df.to_excel(writer, sheet_name="curb_trip", index=False)
                
             # Upload back to S3
             with open(tmp_file_path, 'rb') as f:
                s3_utils.upload_file(f, settings.bat_file_key)

        db_session.commit()
        logger.info("Curb trips committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing curb trips: %s", e)
        raise
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        db_session.close()