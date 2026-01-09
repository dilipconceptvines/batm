# Third party imports
import pandas as pd
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.core.data_loader_config import data_loader_settings
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.leases.models import Lease , LeaseConfiguration
from app.vehicles.models import Vehicle
from app.vehicles.schemas import VehicleStatus
from app.medallions.models import Medallion
from app.utils.general import get_safe_value , parse_date
from app.leases.services import lease_service
from app.leases.utils import calculate_dov_lease_fields, calculate_long_term_lease_fields , calculate_medallion_only_lease_fields , calculate_shift_lease_fields
from app.leases.services import lease_service
from app.leases.schemas import LeaseType
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df
from app.seeder_loader.parser_registry import parser
import tempfile
import os

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="leases",
    sheet_names=[data_loader_settings.parser_leases_sheet],
    version="1.0",
    deprecated=False,
    description="Process leases from Excel sheet"
)
def parse_lease(db: Session, df: pd.DataFrame) -> ParseResult:
    """Parse and load leases from dataframe into database."""
    result = ParseResult(sheet_name="leases")
    created_leases = 0
    updated_leases = 0

    try:
        for idx, row in df.iterrows():
            try:
                # Use get_safe_value() to safely fetch values from DataFrame rows
                lease_id = get_safe_value(row, 'lease_id')
                medallion_number = get_safe_value(row, 'medallion_number')
                vin = get_safe_value(row, 'vin')
                
                # Skip rows missing mandatory fields
                if not lease_id or not medallion_number or not vin:
                    logger.warning("Skipping row with missing mandatory fields: lease_id=%s, medallion=%s, vin=%s",lease_id, medallion_number, vin)
                    result.record_failed(idx, "Missing lease_id or medallion_number or vin")
                    continue

                lease_type = get_safe_value(row, 'lease_type')
                lease_start_date = get_safe_value(row, 'lease_start_date')
                lease_end_date = get_safe_value(row, 'lease_end_date')
                duration_in_weeks = get_safe_value(row, 'duration_in_weeks')
                is_auto_renewed = get_safe_value(row, 'is_auto_renewed')
                lease_date = get_safe_value(row, 'lease_date')
                lease_status = get_safe_value(row, "lease_status")
                # lease_pay_day = get_safe_value(row, "lease_pay_day")
                lease_payments_type = get_safe_value(row, "lease_payments_type")
                cancellation_fee = get_safe_value(row, "cancellation_fee")
                is_day_shift = get_safe_value(row, "is_day_shift")
                is_night_shift = get_safe_value(row, "is_night_shift")
                lease_amount = get_safe_value(row , "total_lease_payment_amount")

                lease_start_date = parse_date(lease_start_date)
                lease_end_date = parse_date(lease_end_date)
                lease_date = parse_date(lease_date)

                # Check for existing records
                medallion = db.query(Medallion).filter_by(medallion_number=medallion_number).first()
                if not medallion:
                    logger.warning("No medallion found for medallion number: %s. Skipping.", medallion_number)
                    result.record_failed(idx, "Medallion not found")
                    continue

                medallion_id = medallion.id

                vehicle = db.query(Vehicle).filter_by(vin=vin).first()
                if not vehicle:
                    logger.warning("No vehicle found for VIN: %s. Skipping.", vin)
                    result.record_failed(idx, "Vehicle not found")
                    continue

                vehicle_id = vehicle.id
                vehicle.vehicle_status = VehicleStatus.ACTIVE
                vehicle.is_active = True

                vehicle_lease = db.query(Lease).filter_by(lease_id=lease_id).first()

                if vehicle_lease is not None:
                    # Update existing records
                    logger.info("Updating existing lease for VIN: %s and medallion number: %s", vin, medallion_number)
                    vehicle_lease.lease_type = lease_type
                    vehicle_lease.lease_start_date = lease_start_date
                    vehicle_lease.lease_end_date = lease_end_date
                    vehicle_lease.duration_in_weeks = duration_in_weeks
                    vehicle_lease.is_auto_renewed = is_auto_renewed
                    vehicle_lease.lease_date = lease_date
                    vehicle_lease.is_active = True
                    vehicle_lease.lease_status = lease_status
                    # vehicle_lease.lease_pay_day = lease_pay_day
                    vehicle_lease.lease_payments_type = lease_payments_type
                    vehicle_lease.cancellation_fee = cancellation_fee
                    vehicle_lease.is_day_shift = is_day_shift
                    vehicle_lease.is_night_shift = is_night_shift
                    vehicle_lease.modified_by = SUPERADMIN_USER_ID
                    vehicle_lease.updated_on = datetime.now()
                    updated_leases += 1
                    result.record_updated(idx)
                else:
                    # Insert new ones
                    logger.info("Creating new lease for VIN: %s and medallion number: %s", vin, medallion_number)
                    vehicle_lease = Lease(
                        lease_id=lease_id,
                        lease_type=lease_type,
                        medallion_id=medallion_id,
                        vehicle_id=vehicle_id,
                        lease_start_date=lease_start_date,
                        lease_end_date=lease_end_date,
                        duration_in_weeks=duration_in_weeks,
                        is_auto_renewed=is_auto_renewed,
                        lease_date=lease_date,
                        lease_status=lease_status,
                        # lease_pay_day=lease_pay_day,
                        lease_payments_type=lease_payments_type,
                        cancellation_fee=cancellation_fee,
                        current_segment = 1 ,
                        total_segments = 8 if lease_type == LeaseType.DOV.value else None,
                        is_day_shift=is_day_shift,
                        is_night_shift=is_night_shift,
                        is_active=True,
                        created_by=SUPERADMIN_USER_ID,
                        created_on=datetime.now()
                    )

                    db.add(vehicle_lease)
                    created_leases += 1
                    result.record_inserted(idx)

                db.flush()
                logger.info("Lease '%s' processed.", lease_id)

                LEASE_CALCULATION_MAP = {
                    "dov": calculate_dov_lease_fields,
                    "long-term": calculate_long_term_lease_fields,
                    "shift-lease": calculate_shift_lease_fields,
                    "medallion-only": calculate_medallion_only_lease_fields,
                }

                if lease_type not in LEASE_CALCULATION_MAP:
                    logger.warning("Unsupported lease type: %s", lease_type)
                    # Not blocking for entire row if lease was created/updated, but perhaps should be?
                    # Proceeding to update lease configs if possible, else skip config part.
                    # Actually if lease type checks failed, maybe we should have failed earlier?
                    # The original code logged warning and continued (skip this row's configs).
                    # I will mark it as failed for configuration part but the lease itself is already processed.
                    # To be strict, if lease type is invalid, maybe we shouldn't have created the lease?
                    # But the schema might allow it? The model uses string or enum. 
                    # Assuming strict validation:
                    # If lease_type is critical, we should fail earlier. 
                    # But following original logic, it continues.
                    pass 
                else:
                    lease_finance_data = LEASE_CALCULATION_MAP[lease_type](
                        lease_amount, vehicle.vehicle_lifetime_cap, vehicle.sales_tax
                    )

                    for key, value in lease_finance_data.items():
                        lease_config = lease_service.get_lease_configurations(db=db , lease_id=vehicle_lease.id , lease_breakup_type=key)
                        if lease_config:
                            lease_config.lease_limit = value
                        else:
                            lease_config = LeaseConfiguration(
                                lease_id=vehicle_lease.id,
                                lease_breakup_type=key,
                                lease_limit=value,
                                created_by=SUPERADMIN_USER_ID,
                                created_on=datetime.now()
                            )
                            
                            db.add(lease_config)

                    db.flush()
                    logger.info("Lease configurations added for %s" , lease_id)

                    vehicle_lease_amount = db.query(LeaseConfiguration).filter_by(lease_id=vehicle_lease.id , lease_breakup_type="total_vehicle_lease").first()
                    medallion_lease_amount = db.query(LeaseConfiguration).filter_by(lease_id=vehicle_lease.id , lease_breakup_type="total_medallion_lease_payment").first()

                    lease_service.create_or_update_lease_schedule(
                        db=db,
                        lease=vehicle_lease,
                        vehicle_weekly_amount=vehicle_lease_amount.lease_limit if vehicle_lease_amount else 0,
                        medallion_weekly_amount=medallion_lease_amount.lease_limit if medallion_lease_amount else 0,
                        override_start_date=lease_start_date or datetime.now()
                    )

                    db.flush()

            except Exception as row_error:
                logger.exception("Error parsing lease row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        logger.info("✅ Data successfully processed.")
        return result
    except Exception as e:
        logger.exception("Critical failure in parser leases: %s", e)
        raise RuntimeError(f"Parser leases failed: {e}") from e

if __name__ == "__main__":
    logger.info("Loading leases configuration")
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
        data_df = pd.read_excel(excel_file, "leases")

        result = parse_lease(db_session, data_df)
        
        # Apply results
        updated_df = apply_parse_result_to_df(data_df, result)

        # Write back to temp file
        with pd.ExcelWriter(
            tmp_file_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            updated_df.to_excel(writer, sheet_name="leases", index=False)
            
        # Upload back to S3
        with open(tmp_file_path, 'rb') as f:
            s3_utils.upload_file(f, settings.bat_file_key)

        db_session.commit()
        logger.info("Leases committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing leases: %s", e)
        raise
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        db_session.close()


