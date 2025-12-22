# Standard library imports
from datetime import datetime
from decimal import Decimal

import numpy as np

# Third party imports
import pandas as pd
from sqlalchemy.orm import Session

from app.audit_trail.models import (
    AuditTrail,  # Import AuditTrail to resolve User.audit_trail relationship
)

# Import models to ensure they're available for SQLAlchemy relationship resolution
from app.bpm.models import SLA  # Import SLA to resolve User.sla relationship

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.driver_payments.models import (
    DriverTransactionReceipt,  # Import for Driver relationships
)
from app.drivers.models import Driver, TLCLicense
from app.dtr.models import DTR, DTRStatus
from app.entities.models import (  # Import Address and BankAccount for relationships
    Address,
    BankAccount,
)
from app.leases.models import Lease
from app.medallions.models import Medallion
from app.utils.general import get_safe_value
from app.utils.logger import get_logger
from app.vehicles.models import Vehicle

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1


def parse_date(date_val):
    """
    Parses a date value into a datetime object.
    If the value is invalid or empty, returns None.
    """
    try:
        if (
            date_val is None
            or date_val == ""
            or (isinstance(date_val, float) and pd.isna(date_val))
        ):
            return None

        if pd.notnull(date_val):
            if isinstance(date_val, str):
                date_val = date_val.strip()
                if not date_val:
                    return None
                return datetime.strptime(date_val, "%Y-%m-%d").date()
            elif hasattr(date_val, "to_pydatetime"):
                return date_val.to_pydatetime().date()
            elif hasattr(date_val, "date"):
                return date_val.date()
            else:
                return date_val
        return None
    except Exception as e:
        logger.warning(f"Error parsing date '{date_val}' (type: {type(date_val)}): {e}")
        return None


def parse_datetime(datetime_val):
    """
    Parses a datetime value into a datetime object.
    If the value is invalid or empty, returns None.
    """
    try:
        if pd.notnull(datetime_val):
            if isinstance(datetime_val, str):
                return datetime.strptime(datetime_val, "%Y-%m-%d %H:%M:%S")
            return (
                datetime_val.to_pydatetime()
                if hasattr(datetime_val, "to_pydatetime")
                else datetime_val
            )
        return None
    except Exception as e:
        logger.warning(f"Error parsing datetime {datetime_val}: {e}")
        return None


def parse_decimal(value):
    """
    Parses a numeric value into a Decimal.
    If the value is invalid or empty, returns Decimal("0.00").
    """
    try:
        if pd.notnull(value):
            return Decimal(str(value))
        return Decimal("0.00")
    except Exception as e:
        logger.warning(f"Error parsing decimal {value}: {e}")
        return Decimal("0.00")


def parse_boolean(value):
    """
    Parses a boolean value.
    Returns False if the value is invalid or empty.
    """
    if pd.isnull(value):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ["true", "1", "yes"]
    return bool(value)


def parse_dtrs(db: Session, df: pd.DataFrame):
    """Parse and load DTRs from dataframe into database."""
    try:
        # Clean column names and replace NaNs
        df.columns = df.columns.str.strip().str.lower()
        df = df.replace({np.nan: None})

        # Operation counters
        created_dtrs = 0
        updated_dtrs = 0
        skipped_dtrs = 0

        for _, row in df.iterrows():
            # Use get_safe_value() to safely fetch values from DataFrame rows
            dtr_number = get_safe_value(row, "dtr_number")
            receipt_number = get_safe_value(row, "receipt_number")

            # Skip rows missing mandatory fields
            if not dtr_number or not receipt_number:
                logger.warning("Skipping row with missing dtr_number or receipt_number")
                skipped_dtrs += 1
                continue

            # Get foreign key references
            medallion_no = get_safe_value(row, "medallion_no")
            driver_license_no = get_safe_value(row, "driver_license_no")
            lease_number = get_safe_value(row, "lease_number")

            # Look up foreign key IDs
            medallion = (
                db.query(Medallion)
                .filter(Medallion.medallion_number == medallion_no)
                .first()
            )

            # Look up driver by TLC license number
            # Join Driver with TLCLicense to find driver by license number
            driver = (
                db.query(Driver)
                .join(TLCLicense, Driver.tlc_license_number_id == TLCLicense.id)
                .filter(TLCLicense.tlc_license_number == driver_license_no)
                .first()
            )

            # Look up lease by lease_id field (not lease_number)
            lease = db.query(Lease).filter(Lease.lease_id == lease_number).first()

            if not driver:
                logger.warning(
                    f"Driver with license {driver_license_no} not found. Skipping DTR {dtr_number}"
                )
                skipped_dtrs += 1
                continue

            if not lease:
                logger.warning(
                    f"Lease with number {lease_number} not found. Skipping DTR {dtr_number}"
                )
                skipped_dtrs += 1
                continue

            # Get vehicle ID from medallion if available
            vehicle_id = None
            medallion_id = None
            if medallion:
                medallion_id = medallion.id
                vehicle = (
                    db.query(Vehicle)
                    .filter(Vehicle.medallion_id == medallion.id)
                    .first()
                )
                if vehicle:
                    vehicle_id = vehicle.id

            # Parse dates BEFORE creating DTR object
            period_start_raw = get_safe_value(row, "period_start_date")
            period_end_raw = get_safe_value(row, "period_end_date")
            generation_date_raw = get_safe_value(row, "generation_date")

            logger.debug(
                f"Raw values for {dtr_number}: start={period_start_raw}, end={period_end_raw}, gen={generation_date_raw}"
            )

            period_start_date = parse_date(period_start_raw)
            period_end_date = parse_date(period_end_raw)
            generation_date = parse_datetime(generation_date_raw) or datetime.now()

            if not period_start_date:
                logger.warning(
                    f"DTR {dtr_number}: period_start_date is None after parsing '{period_start_raw}'"
                )
            if not period_end_date:
                logger.warning(
                    f"DTR {dtr_number}: period_end_date is None after parsing '{period_end_raw}'"
                )

            # Check for existing DTR
            dtr = db.query(DTR).filter(DTR.dtr_number == dtr_number).first()

            if not dtr:
                # Create new DTR with required fields
                logger.info(f"Adding new DTR: {dtr_number}")
                created_dtrs += 1
            else:
                # Update existing DTR
                logger.info(f"Updating existing DTR: {dtr_number}")
                dtr.period_start_date = period_start_date
                dtr.period_end_date = period_end_date
                dtr.generation_date = generation_date
                updated_dtrs += 1

            dtr = DTR(
                dtr_number=dtr_number,
                receipt_number=receipt_number,
                period_start_date=period_start_date,
                period_end_date=period_end_date,
                generation_date=generation_date,
                created_by=SUPERADMIN_USER_ID,
                created_on=datetime.now(),
                lease_id=lease.id,
                driver_id=driver.id,
                vehicle_id=vehicle_id,
                medallion_id=medallion_id,
            )
            db.add(dtr)
            db.flush()

            # # Foreign keys
            # dtr.lease_id = lease.id
            # dtr.driver_id = driver.id
            # dtr.vehicle_id = vehicle_id
            # dtr.medallion_id = medallion_id

            # Status
            status_str = get_safe_value(row, "status")
            if status_str:
                try:
                    dtr.status = DTRStatus[status_str.upper()]
                except KeyError:
                    logger.warning(f"Invalid status {status_str}, defaulting to DRAFT")
                    dtr.status = DTRStatus.DRAFT
            else:
                dtr.status = DTRStatus.DRAFT

            # Earnings
            dtr.gross_cc_earnings = parse_decimal(
                get_safe_value(row, "gross_cc_earnings")
            )
            dtr.gross_cash_earnings = parse_decimal(
                get_safe_value(row, "gross_cash_earnings")
            )
            dtr.total_gross_earnings = parse_decimal(
                get_safe_value(row, "total_gross_earnings")
            )

            # Charges
            dtr.lease_amount = parse_decimal(get_safe_value(row, "lease_amount"))
            dtr.mta_tif_fees = parse_decimal(get_safe_value(row, "mta_tif_fees"))
            dtr.ezpass_tolls = parse_decimal(get_safe_value(row, "ezpass_tolls"))
            dtr.violation_tickets = parse_decimal(
                get_safe_value(row, "violation_tickets")
            )
            dtr.tlc_tickets = parse_decimal(get_safe_value(row, "tlc_tickets"))
            dtr.repairs = parse_decimal(get_safe_value(row, "repairs"))
            dtr.driver_loans = parse_decimal(get_safe_value(row, "driver_loans"))
            dtr.misc_charges = parse_decimal(get_safe_value(row, "misc_charges"))

            # Totals
            dtr.subtotal_charges = parse_decimal(
                get_safe_value(row, "subtotal_charges")
            )
            dtr.prior_balance = parse_decimal(get_safe_value(row, "prior_balance"))
            dtr.net_earnings = parse_decimal(get_safe_value(row, "net_earnings"))
            dtr.total_due_to_driver = parse_decimal(
                get_safe_value(row, "total_due_to_driver")
            )

            # Additional driver flag
            dtr.is_additional_driver_dtr = parse_boolean(
                get_safe_value(row, "is_additional_driver_dtr")
            )

            # Update timestamp
            dtr.updated_by = SUPERADMIN_USER_ID
            dtr.updated_on = datetime.now()

        db.commit()
        logger.info("✅ DTR data successfully processed.")
        logger.info(
            f"📊 Summary: {created_dtrs} created, {updated_dtrs} updated, {skipped_dtrs} skipped"
        )

        return {
            "dtrs_created": created_dtrs,
            "dtrs_updated": updated_dtrs,
            "dtrs_skipped": skipped_dtrs,
        }
    except Exception as e:
        db.rollback()
        logger.error(f"Error parsing DTR data: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    import os

    db_session = SessionLocal()

    # Path to the CSV file
    csv_path = os.path.join(os.path.dirname(__file__), "dtr_seed_data.csv")

    logger.info(f"Loading DTR data from: {csv_path}")
    dtrs_df = pd.read_csv(csv_path)

    parse_dtrs(db_session, dtrs_df)
    db_session.close()
