### app/leases/utils.py

# Standard imports
import json
import re
from datetime import date, datetime, timedelta, timezone

from dateutil.rrule import DAILY, WEEKLY, rrule
from sqlalchemy.orm import Session

# Local imports
from app.core.config import settings
from app.drivers.utils import extract_driver_info
from app.leases.models import Lease, LeaseConfiguration, LeaseDriver
from app.medallions.models import Medallion
from app.medallions.utils import extract_medallion_info
from app.utils.general import format_us_phone_number
from app.utils.lambda_utils import LambdaInvocationError, invoke_lambda_function
from app.utils.logger import get_logger
from app.vehicles.models import Vehicle
from app.vehicles.utils import extract_vehicle_info

logger = get_logger(__name__)


def extract_lease_info(lease):
    """
    Extract lease information with null safety for document generation.

    Args:
        lease: Lease object

    Returns:
        Dictionary with lease information
    """
    if not lease:
        return {
            "lease_id": "N/A",
            "lease_start_date": "N/A",
            "lease_end_date": "N/A",
            "lease_pay_day": "N/A",
            "duration_in_weeks": "N/A",
        }

    return {
        "lease_id": lease.lease_id if lease.lease_id else "N/A",
        "lease_start_date": lease.lease_start_date.strftime("%Y-%m-%d")
        if lease.lease_start_date
        else "N/A",
        "lease_end_date": lease.lease_end_date.strftime("%Y-%m-%d")
        if lease.lease_end_date
        else "N/A",
        "lease_pay_day": lease.lease_pay_day if lease.lease_pay_day else "N/A",
        "duration_in_weeks": str(lease.duration_in_weeks)
        if lease.duration_in_weeks
        else "N/A",
    }


def generate_medallion_lease_document(db: Session, lease: Lease, authorized_agent: str):
    """Generate medallion lease document"""
    try:
        for lease_driver in lease.lease_driver:
            if not lease_driver.is_active:
                continue
            # Prepare payload for Lambda function
            payload = {
                "data": prepare_medallion_lease_document(
                    db, lease_driver, authorized_agent
                ),
                "bucket": settings.s3_bucket_name,
                "identifier": f"form_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                "template_id": settings.medallion_lease_template_id,
            }

            logger.info("Calling Lambda function with payload: %s", payload)

            response = invoke_lambda_function(
                function_name="pdf_filler", payload=payload
            )

            # Extract s3_key from response
            logger.info("Response from Lambda: %s", response)
            response_body = json.loads(response["body"])
            s3_key = response_body.get("s3_key")  # Use the output key we specified

            return {
                "document_name": f"Medallion Lease Document for {lease_driver.driver.full_name} (Driver ID {lease_driver.driver_id}) for Lease ID {lease.lease_id}",
                "document_format": "PDF",
                "document_path": s3_key,
                "document_type": "driver_medallion_lease",
                "object_type": f"co-leasee-{lease_driver.co_lease_seq}",
                "object_lookup_id": str(lease_driver.id),
                "document_note": f"Medallion lease document created for {lease_driver.driver.full_name}",
                "document_date": datetime.now(timezone.utc).isoformat().split("T")[0],
            }
    except LambdaInvocationError as e:
        logger.error(
            "Lambda error generating medallion lease document: %s (Status: %s)",
            e.message,
            e.status_code,
        )
        raise
    except Exception as e:
        logger.error(
            "Error generating medallion lease document: %s", str(e), exc_info=True
        )
        raise


def generate_dov_vehicle_lease_document(db, lease, authorized_agent):
    """Generate vehicle lease document"""
    try:
        for lease_driver in lease.lease_driver:
            if not lease_driver.is_active:
                continue

            # Prepare payload for Lambda function
            payload = {
                "data": prepare_vehicle_lease_document(
                    db, lease_driver, authorized_agent
                ),
                "bucket": settings.s3_bucket_name,
                "identifier": f"form_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                "template_id": settings.dov_vehicle_lease_template_id,
            }

            logger.info("Calling Lambda function with payload: %s", payload)

            response = invoke_lambda_function(
                function_name="pdf_filler", payload=payload
            )

            # Extract s3_key from response
            logger.info("Response from Lambda: %s", response)
            response_body = json.loads(response["body"])
            s3_key = response_body.get("s3_key")  # Use the output key we specified
            return {
                "document_name": f"Vehicle  Lease Document for {lease_driver.driver.full_name} (Driver ID {lease_driver.driver_id}) for Lease ID {lease.lease_id}",
                "document_format": "PDF",
                "document_path": s3_key,
                "document_type": "driver_vehicle_lease",
                "object_type": f"co-leasee-{lease_driver.co_lease_seq}",
                "object_lookup_id": str(lease_driver.id),
                "document_date": datetime.now(timezone.utc).isoformat().split("T")[0],
                "document_note": "Vehicle lease document created.",
            }
    except LambdaInvocationError as e:
        logger.error(
            "Lambda error generating vehicle lease document: %s (Status: %s)",
            e.message,
            e.status_code,
        )
        raise
    except Exception as e:
        logger.error(
            "Error generating vehicle lease document: %s", str(e), exc_info=True
        )
        raise


def generate_long_term_lease_document(db, lease, authorized_agent):
    """Generate vehicle lease document"""
    try:
        for lease_driver in lease.lease_driver:
            if not lease_driver.is_active:
                continue

            # Prepare payload for Lambda function
            payload = {
                "data": prepare_long_term_lease_document(db, lease, authorized_agent),
                "bucket": settings.s3_bucket_name,
                "identifier": f"form_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                "template_id": settings.long_term_lease_template_id,
            }

            logger.info("Calling Lambda function with payload: %s", payload)

            response = invoke_lambda_function(
                function_name="pdf_filler", payload=payload
            )

            # Extract s3_key from response
            logger.info("Response from Lambda: %s", response)
            response_body = json.loads(response["body"])
            s3_key = response_body.get("s3_key")  # Use the output key we specified
            return {
                "document_name": f"Long Term Lease Document  Lease Document for {lease_driver.driver.full_name} (Driver ID {lease_driver.driver_id}) for Lease ID {lease.lease_id}",
                "document_format": "PDF",
                "document_path": s3_key,
                "document_type": "driver_long_term_lease",
                "object_type": f"co-leasee-{lease_driver.co_lease_seq}",
                "object_lookup_id": str(lease_driver.id),
                "document_date": datetime.now(timezone.utc).isoformat().split("T")[0],
                "document_note": "Long Term lease document created.",
            }
    except LambdaInvocationError as e:
        logger.error(
            "Lambda error generating long term lease document: %s (Status: %s)",
            e.message,
            e.status_code,
        )
        raise
    except Exception as e:
        logger.error(
            "Error generating long term lease document: %s", str(e), exc_info=True
        )
        raise


def prepare_medallion_lease_document(
    db: Session, lease_driver: LeaseDriver = None, authorized_agent: str = ""
):
    lease = lease_driver.lease
    driver = lease_driver.driver
    vehicle = lease.vehicle
    medallion = lease.medallion

    # Use centralized extract functions with null safety
    driver_info = extract_driver_info(driver)
    vehicle_info = extract_vehicle_info(vehicle)
    medallion_info = extract_medallion_info(medallion)

    # --- 2. Retrieve all financial components from LeaseConfiguration ---
    configs = (
        db.query(LeaseConfiguration)
        .filter(LeaseConfiguration.lease_id == lease.id)
        .all()
    )

    def get_config_value(key: str) -> float:
        """Helper to safely get a numeric value from the configurations list."""
        config = next((c for c in configs if c.lease_breakup_type == key), None)
        return float(config.lease_limit) if config and config.lease_limit else 0.0

    med_lease = get_config_value("med_lease")
    tlc_inspection_fee = get_config_value("tlc_inspection_fees")
    time_stamps_amount = get_config_value("tax_stamps")
    vehicle_registration_amount = get_config_value("registration")
    total_weekly_lease_amount = get_config_value("total_medallion_lease_payment")

    medallion_lease_document_info = {
        "date_of_agreement": (lease.lease_date or lease.created_on).strftime(
            settings.common_date_format
        ),
        "manager_name": settings.bat_manager_name,
        "driver_name": driver_info["name"],
        "driver_address": driver_info["address"],
        "driver_primary_phone": driver_info["primary_phone"],
        "driver_email": driver_info["email"],
        "driver_ssn": driver_info["ssn"],
        "driver_dmv_license": driver_info["dmv_license"],
        "driver_dmv_expiry": driver_info["dmv_license_expiration"],
        "driver_tlc_license": driver_info["tlc_license"],
        "driver_tlc_expiry": driver_info["tlc_license_expiration"],
        "medallion_number": medallion_info["number"],
        "plate_number": vehicle_info["plate_number"],
        "vehicle_make": vehicle_info["make"],
        "vehicle_vin": vehicle_info["vin"],
        "vehicle_year": vehicle_info["year"],
        "vehicle_meter_make": "N/A",
        # "vehicle_meter_make": vehicle_info["vehicle_meter_type"],
        "vehicle_meter_serial_number": vehicle_info["vehicle_meter_serial_number"],
        "lease_start_date": lease.lease_start_date.strftime(settings.common_date_format)
        if lease.lease_start_date
        else "N/A",
        "lease_end_date": lease.lease_end_date.strftime(settings.common_date_format)
        if lease.lease_end_date
        else "N/A",
        "medallion_lease_payment": str(f"$ {med_lease:.2f}" or "$ 0.00"),
        "total_weeks": str(lease.duration_in_weeks or 0),
        "total_payment_for_lease_term": str(
            f"$ {total_weekly_lease_amount * lease.duration_in_weeks:,.2f}"
        ),
        "tlc_inspection_fee": f"$ {tlc_inspection_fee:.2f}",
        "time_stamps_amount": f"$ {time_stamps_amount:.2f}",
        "vehicle_registration_amount": f"$ {vehicle_registration_amount:.2f}",
        "total_weekly_lease_amount": f"$ {total_weekly_lease_amount:,.2f}",
        "payment_due_day": settings.payment_date,
        "security_deposit": str(f"$ {lease.deposit_amount_paid:,.2f}" or "$ 0.00"),
        "cancellation_charges": str(f"$ {lease.cancellation_fee:,.2f}" or "$ 0.00"),
        "additional_balance_due": "$ 0.00",
        "security_deposit_holding_account_number": settings.security_deposit_holding_number,
        "authorized_agent": settings.bat_authorized_agent,
        "bat_manager": settings.bat_manager_name,
        "agent_sign_date": date.today().strftime(settings.common_date_format),
        "images": [
            {
                "path": settings.common_signature_file,
                "page": 13,
                "x": 280,
                "y": 165,
                "width": 180,
                "height": 15,
                "opacity": 0.8,
            },
        ],
    }
    return medallion_lease_document_info


def prepare_long_term_lease_document(
    db: Session, lease: Lease, authorized_agent: str
) -> dict:
    """
    Prepare vehicle lease document with dynamic data from the lease object,
    populating the specific dov_vehicle_lease_template structure.
    """

    # --- 1. Get all related objects from the lease, with safety checks ---
    driver_lease_assoc = lease.lease_driver[0] if lease.lease_driver else None
    driver = driver_lease_assoc.driver if driver_lease_assoc else None
    vehicle = lease.vehicle
    medallion = lease.medallion

    # Use centralized extract functions with null safety
    driver_info = extract_driver_info(driver)
    vehicle_info = extract_vehicle_info(vehicle)
    medallion_info = extract_medallion_info(medallion)

    # --- 2. Retrieve all financial components from LeaseConfiguration ---
    configs = (
        db.query(LeaseConfiguration)
        .filter(LeaseConfiguration.lease_id == lease.id)
        .all()
    )

    def get_config_value(key: str) -> float:
        """Helper to safely get a numeric value from the configurations list."""
        config = next((c for c in configs if c.lease_breakup_type == key), None)
        return float(config.lease_limit) if config and config.lease_limit else 0.0

    lease_weekly_payment = get_config_value("med_lease")
    tlc_inspection_fee = get_config_value("tlc_inspection_fees")
    time_stamps_amount = get_config_value("tax_stamps")
    vehicle_registration_amount = get_config_value("registration")

    total_weekly_lease_amount = get_config_value("total_medallion_lease_payment")
    term_lease_payment = total_weekly_lease_amount * (lease.duration_in_weeks or 0)
    shift = (
        settings.full_time_drivers
        if (
            driver_lease_assoc.lease.is_day_shift
            and driver_lease_assoc.lease.is_night_shift
        )
        else (
            settings.day_shift_drivers
            if driver_lease_assoc.lease.is_day_shift
            else settings.night_shift_drivers
        )
    )

    long_term_lease_template = {
        "date_of_agreement": (lease.lease_date or lease.created_on).strftime(
            settings.common_date_format
        ),
        "manager_name": settings.bat_manager_name,
        "driver_name": driver_info["name"],
        "driver_address": driver_info["address"],
        "driver_primary_phone": driver_info["primary_phone"],
        "driver_email": driver_info["email"],
        "driver_ssn": driver_info["ssn"],
        "driver_dmv_license": driver_info["dmv_license"],
        "driver_dmv_expiry": driver_info["dmv_license_expiration"],
        "driver_tlc_license": driver_info["tlc_license"],
        "driver_tlc_expiry": driver_info["tlc_license_expiration"],
        "medallion_number": medallion_info["number"],
        "plate_number": vehicle_info["plate_number"],
        "vehicle_make": vehicle_info["make"],
        "vehicle_vin": vehicle_info["vin"],
        "vehicle_year": vehicle_info["year"],
        "vehicle_meter_make": "N/A",
        # "vehicle_meter_make": vehicle_info["vehicle_meter_type"],
        "vehicle_meter_serial_number": vehicle_info["vehicle_meter_serial_number"],
        "lease_start_date": lease.lease_start_date.strftime(settings.common_date_format)
        if lease.lease_start_date
        else "N/A",
        "lease_end_date": lease.lease_end_date.strftime(settings.common_date_format)
        if lease.lease_end_date
        else "N/A",
        "tlc_inspection_fee": f"$ {tlc_inspection_fee:.2f}",
        "time_stamps_amount": f"$ {time_stamps_amount:.2f}",
        "vehicle_registration_amount": f"$ {vehicle_registration_amount:,.2f}",
        "total_weekly_lease_amount": f"$ {total_weekly_lease_amount:,.2f}",
        "payment_due_day": settings.payment_date,
        "term_lease_payment": f"$ {term_lease_payment:,.2f}",
        "lease_weekly_payment": f"$ {lease_weekly_payment:,.2f}",
        "total_weeks": str(lease.duration_in_weeks or 0),
        "security_deposit": str(f"$ {lease.deposit_amount_paid:,.2f}" or "$ 0.00"),
        "lease_id": lease.lease_id or "",
        "authorized_agent": settings.bat_authorized_agent,
        "agent_sign_date": date.today().strftime(settings.common_date_format),
        "bat_manager": settings.bat_manager_name,
        "cancellation_charges": str(f"$ {lease.cancellation_fee:,.2f}" or "$ 0.00"),
        "shift_times": shift,
        "images": [
            {
                "path": settings.common_signature_file,
                "page": 14,
                "x": 300,
                "y": 380,
                "width": 180,
                "height": 15,
                "opacity": 0.8,
            },
        ],
    }
    return long_term_lease_template


def prepare_vehicle_lease_document(
    db: Session, lease_driver: LeaseDriver, authorized_agent: str
) -> dict:
    """
    Prepare vehicle lease document with dynamic data from the lease object,
    populating the specific dov_vehicle_lease_template structure.
    """

    # --- 1. Get all related objects from the lease, with safety checks ---
    driver = lease_driver.driver
    lease = lease_driver.lease
    vehicle = lease.vehicle
    medallion = lease.medallion

    # Use centralized extract functions with null safety
    driver_info = extract_driver_info(driver)
    vehicle_info = extract_vehicle_info(vehicle)
    medallion_info = extract_medallion_info(medallion)

    # --- 2. Retrieve all financial components from LeaseConfiguration ---
    configs = (
        db.query(LeaseConfiguration)
        .filter(LeaseConfiguration.lease_id == lease.id)
        .all()
    )

    def get_config_value(key: str) -> float:
        """Helper to safely get a numeric value from the configurations list."""
        config = next((c for c in configs if c.lease_breakup_type == key), None)
        return float(config.lease_limit) if config and config.lease_limit else 0.0

    lease_weekly_payment = get_config_value("veh_lease")
    vehicle_sales_tax = get_config_value("veh_sales_tax")
    tlc_inspection_fee = get_config_value("tlc_inspection_fees")
    time_stamps_amount = get_config_value("tax_stamps")
    vehicle_registration_amount = get_config_value("registration")

    total_weekly_lease_amount = get_config_value("total_vehicle_lease")
    term_lease_payment = total_weekly_lease_amount * (lease.duration_in_weeks or 0)

    tlc_vehicle_lifetime_cap = get_config_value("tlc_vehicle_lifetime_cap")
    # --- 4. Populate the template dictionary ---
    dov_vehicle_lease_template = {
        "date_of_agreement": (lease.lease_date or lease.created_on).strftime(
            settings.common_date_format
        ),
        "manager_name": settings.bat_manager_name,
        "driver_name": driver_info["name"],
        "driver_address": driver_info["address"],
        "driver_primary_phone": driver_info["primary_phone"],
        "driver_email": driver_info["email"],
        "driver_ssn": driver_info["ssn"],
        "driver_dmv_license": driver_info["dmv_license"],
        "driver_dmv_expiry": driver_info["dmv_license_expiration"],
        "driver_tlc_license": driver_info["tlc_license"],
        "driver_tlc_expiry": driver_info["tlc_license_expiration"],
        "medallion_number": medallion_info["number"],
        "plate_number": vehicle_info["plate_number"],
        "vehicle_make": vehicle_info["make"],
        "vehicle_vin": vehicle_info["vin"],
        "vehicle_year": vehicle_info["year"],
        "vehicle_meter_make": "N/A",
        # "vehicle_meter_make": vehicle_info["vehicle_meter_type"],
        "vehicle_meter_serial_number": vehicle_info["vehicle_meter_serial_number"],
        "lease_start_date": lease.lease_start_date.strftime(settings.common_date_format)
        if lease.lease_start_date
        else "N/A",
        "lease_end_date": lease.lease_end_date.strftime(settings.common_date_format)
        if lease.lease_end_date
        else "N/A",
        "vehicle_sales_tax": f"$ {vehicle_sales_tax:.2f}",
        "tlc_inspection_fee": f"$ {tlc_inspection_fee:.2f}",
        "time_stamps_amount": f"$ {time_stamps_amount:.2f}",
        "vehicle_registration_amount": f"$ {vehicle_registration_amount:,.2f}",
        "total_weekly_lease_amount": f"$ {total_weekly_lease_amount:,.2f}",
        "payment_due_day": settings.payment_date,
        "term_lease_payment": f"$ {term_lease_payment:,.2f}",
        "vehicle_base_price": str(f"$ {vehicle.base_price:,.2f}" or "$ 0.00"),
        "lease_weekly_payment": f"$ {lease_weekly_payment:,.2f}",
        "total_weeks": str(lease.duration_in_weeks or 0),
        "lease_additional_dues": "$ 0.00",
        "security_deposit": str(f"$ {lease.deposit_amount_paid:,.2f}" or "$ 0.00"),
        "cancellation_charges": str(f"$ {lease.cancellation_fee:,.2f}" or "$ 0.00"),
        "vehicle_sale_price": str(f"${tlc_vehicle_lifetime_cap:,.2f}" or "$ 0.00"),
        "purchase_weeks": "208",  # TODO: Handle 4 year config no of weeks in a different way
        "located_at": settings.security_deposit_located_at,
        "lease_id": lease.lease_id or "",
        "security_deposit_holding_account_number": settings.security_deposit_holding_number,
        "security_deposit_holding_bank": settings.security_deposit_holding_bank,
        "authorized_agent": settings.bat_authorized_agent,
        "agent_sign_date": date.today().strftime(settings.common_date_format),
        "bat_manager": settings.bat_manager_name,
        "images": [
            {
                "path": settings.common_signature_file,
                "page": 16,
                "x": 300,
                "y": 380,
                "width": 180,
                "height": 15,
                "opacity": 0.8,
            },
        ],
    }

    return dov_vehicle_lease_template


def calculate_weekly_lease_schedule(
    lease_start_date: datetime = None,
    duration_weeks: int = settings.lease_6_months,
    payment_due_day: str = settings.payment_date,
    weekly_lease_amount: float = 0.00,
    lease_end_date: datetime = None,
):
    try:
        # Ensure lease_start_date is a datetime object
        if isinstance(lease_start_date, date) and not isinstance(
            lease_start_date, datetime
        ):
            lease_start_date = datetime.combine(lease_start_date, datetime.min.time())

        day_num = settings.day_name_to_num[payment_due_day.strip().lower()]
        daily_rate = weekly_lease_amount / 7

        # Calculate lease end date if not provided
        if lease_end_date is None:
            lease_end_date = lease_start_date + timedelta(weeks=duration_weeks)
        elif isinstance(lease_end_date, date) and not isinstance(
            lease_end_date, datetime
        ):
            lease_end_date = datetime.combine(lease_end_date, datetime.min.time())

        # Week runs from (payment_due_day - 1) to (payment_due_day - 1)
        # E.g., if payment_due_day is Monday (0), week runs Sunday (6) to Saturday (5)
        week_start_day = (day_num - 1) % 7

        # Find the first proper week start (Sunday in our case) on or after lease_start_date
        days_to_week_start = (week_start_day - lease_start_date.weekday() + 7) % 7
        if days_to_week_start == 0:
            first_proper_week_start = lease_start_date
        else:
            first_proper_week_start = lease_start_date + timedelta(
                days=days_to_week_start
            )

        schedule = []
        installment_no = 1

        # PART 1: Prorated first installment (from lease_start_date to day before first_proper_week_start)
        if first_proper_week_start > lease_start_date:
            first_period_end = first_proper_week_start - timedelta(days=1)
            first_period_days = (first_period_end - lease_start_date).days + 1
            first_prorated_amount = daily_rate * first_period_days

            # First payment due date (first occurrence of payment_due_day)
            days_to_payment_day = (day_num - lease_start_date.weekday() + 7) % 7
            if days_to_payment_day == 0:
                days_to_payment_day = 7
            first_due_date = lease_start_date + timedelta(days=days_to_payment_day)

            due_date_obj = (
                first_due_date.date()
                if isinstance(first_due_date, datetime)
                else first_due_date
            )
            rounded_amount = round(first_prorated_amount, 2)
            schedule.append(
                {
                    "installment_no": installment_no,
                    "due_date": due_date_obj.strftime("%a, %B %d, %Y"),
                    "start_date": (
                        lease_start_date.date()
                        if isinstance(lease_start_date, datetime)
                        else lease_start_date
                    ).strftime("%a, %B %d, %Y"),
                    "amount_due": f"$ {rounded_amount:,.2f}",
                    "is_prorated": True,
                    "active_days": first_period_days,
                    "period_start": (
                        lease_start_date.date()
                        if isinstance(lease_start_date, datetime)
                        else lease_start_date
                    ).strftime("%a, %B %d, %Y"),
                    "period_end": (
                        first_period_end.date()
                        if isinstance(first_period_end, datetime)
                        else first_period_end
                    ).strftime("%a, %B %d, %Y"),
                }
            )
            installment_no += 1

        # PART 2: Full 7-day cycles (Sunday to Saturday)
        current_week_start = first_proper_week_start

        while current_week_start + timedelta(days=6) < lease_end_date:
            current_week_end = current_week_start + timedelta(days=6)

            # Payment due date for this week
            days_to_payment = (day_num - current_week_start.weekday() + 7) % 7
            current_due_date = current_week_start + timedelta(days=days_to_payment)

            due_date_obj = (
                current_due_date.date()
                if isinstance(current_due_date, datetime)
                else current_due_date
            )
            rounded_amount = round(weekly_lease_amount, 2)
            schedule.append(
                {
                    "installment_no": installment_no,
                    "due_date": due_date_obj.strftime("%a, %B %d, %Y"),
                    "start_date": (
                        current_week_start.date()
                        if isinstance(current_week_start, datetime)
                        else current_week_start
                    ).strftime("%a, %B %d, %Y"),
                    "amount_due": f"$ {rounded_amount:,.2f}",
                    "is_prorated": False,
                    "active_days": 7,
                    "period_start": (
                        current_week_start.date()
                        if isinstance(current_week_start, datetime)
                        else current_week_start
                    ).strftime("%a, %B %d, %Y"),
                    "period_end": (
                        current_week_end.date()
                        if isinstance(current_week_end, datetime)
                        else current_week_end
                    ).strftime("%a, %B %d, %Y"),
                }
            )
            installment_no += 1
            current_week_start += timedelta(days=7)

        # PART 3: Prorated last installment (from last proper week start through the end of the lease term)
        # When lease_end_date is passed in, it represents the last active day (inclusive)
        # When calculated from duration_weeks, it represents the day after the last active day (exclusive)
        # To handle both cases: if we have an explicit end date passed in, we use it as-is (inclusive)
        # If calculated, we subtract 1 day to make it inclusive
        if current_week_start < lease_end_date:
            last_period_start = current_week_start
            last_period_end = lease_end_date
            # Calculate days: since lease_end_date is now the last active day, we need to include it
            last_period_days = (last_period_end - last_period_start).days + 1
            last_prorated_amount = daily_rate * last_period_days

            # Payment due date for last week
            days_to_payment = (day_num - last_period_start.weekday() + 7) % 7
            last_due_date = last_period_start + timedelta(days=days_to_payment)

            due_date_obj = (
                last_due_date.date()
                if isinstance(last_due_date, datetime)
                else last_due_date
            )
            rounded_amount = round(last_prorated_amount, 2)
            schedule.append(
                {
                    "installment_no": installment_no,
                    "due_date": due_date_obj.strftime("%a, %B %d, %Y"),
                    "start_date": (
                        last_period_start.date()
                        if isinstance(last_period_start, datetime)
                        else last_period_start
                    ).strftime("%a, %B %d, %Y"),
                    "amount_due": f"$ {rounded_amount:,.2f}",
                    "is_prorated": True,
                    "active_days": last_period_days,
                    "period_start": (
                        last_period_start.date()
                        if isinstance(last_period_start, datetime)
                        else last_period_start
                    ).strftime("%a, %B %d, %Y"),
                    "period_end": (
                        last_period_end.date()
                        if isinstance(last_period_end, datetime)
                        else last_period_end
                    ).strftime("%a, %B %d, %Y"),
                }
            )

        return schedule
    except Exception as e:
        logger.error(
            "Error calculating weekly lease schedule: %s", str(e), exc_info=True
        )
        raise e


def calculate_short_term_lease_schedule(
    lease_start_date: datetime, duration_days: int, daily_lease_amount: float
):
    try:
        schedule = []
        for i, dt in enumerate(
            rrule(freq=DAILY, count=duration_days, dtstart=lease_start_date)
        ):
            schedule.append(
                {
                    "installment_no": i + 1,
                    "due_date": dt.date(),
                    "amount_due": daily_lease_amount,
                }
            )

        return schedule
    except Exception as e:
        logger.error("Error calculating short term lease schedule: %s", str(e))
        raise e


def get_driver_documents_with_envelope(db: Session, lease_driver):
    """
    Helper function to get active documents and envelope info for a lease driver.
    Only returns documents that have an active LeaseDriverDocument associated with them.
    Works for both primary drivers and additional drivers.

    Args:
        db: Database session
        lease_driver: LeaseDriver record

    Returns:
        List of document dictionaries with envelope information
    """
    from sqlalchemy import desc

    from app.leases.models import LeaseDriverDocument
    from app.uploads.models import Document

    # Get all active LeaseDriverDocument records for this driver
    lease_driver_documents = (
        db.query(LeaseDriverDocument)
        .filter(
            LeaseDriverDocument.lease_driver_id == lease_driver.id,
            LeaseDriverDocument.is_active,
        )
        .all()
    )

    # If no active LeaseDriverDocument, return empty list
    if not lease_driver_documents:
        return []

    documents_list = []

    # Iterate through all LeaseDriverDocument records
    for lease_driver_doc in lease_driver_documents:
        document = (
            db.query(Document)
            .filter(Document.object_lookup_id == lease_driver.id)
            .first()
        )
        if document:
            documents_list.append(document.to_dict())

    return documents_list


def get_all_lease_documents(db: Session, lease, lease_drivers):
    """
    Get all documents associated with a lease.
    This includes:
    - Lease-level documents (general lease documents)
    - Primary driver documents (from lease drivers)
    - Additional driver documents (from additional drivers)

    Args:
        db: Database session
        lease: Lease record
        lease_drivers: List of LeaseDriver records

    Returns:
        List of all document dictionaries
    """
    from sqlalchemy import desc

    from app.leases.models import LeaseDriverDocument
    from app.uploads.models import Document

    all_documents = []

    # 1. Get general lease documents (object_type="lease")
    lease_docs = (
        db.query(Document)
        .filter(
            Document.object_type == "lease",
            Document.object_id == lease.id,
        )
        .order_by(desc(Document.created_on))
        .all()
    )

    for doc in lease_docs:
        all_documents.append(
            {
                "document_id": doc.id,
                "document_name": doc.document_name,
                "document_type": doc.document_type,
                "document_format": doc.document_format,
                "document_date": doc.document_date,
                "document_path": doc.document_path,
                "presigned_url": doc.presigned_url,
                "file_size": doc.document_actual_size
                if doc.document_actual_size
                else 0,
                "comments": doc.document_note,
                "created_on": doc.created_on,
                "object_type": doc.object_type,
                "is_sent_for_signature": False,
                "has_front_desk_signed": None,
                "has_driver_signed": None,
                "envelope_id": None,
                "signing_type": None,
                "frontend_signed_date": None,
                "driver_signed_date": None,
            }
        )

    # 2. Get documents for all lease drivers (primary and additional)
    if lease_drivers:
        for lease_driver in lease_drivers:
            # Get active LeaseDriverDocument for envelope info
            lease_driver_document = (
                db.query(LeaseDriverDocument)
                .filter(
                    LeaseDriverDocument.lease_driver_id == lease_driver.id,
                    LeaseDriverDocument.is_active == True,
                )
                .first()
            )

            # Determine document type based on whether this is an additional driver
            if lease_driver.is_additional_driver:
                doc_type = "additional-driver"
                object_lookup_id = str(lease_driver.id)
            else:
                doc_type = "lease"
                object_lookup_id = str(lease_driver.lease_id)

            # Get documents for this driver
            driver_docs = (
                db.query(Document)
                .filter(
                    Document.object_lookup_id == object_lookup_id,
                    Document.document_type == doc_type,
                )
                .order_by(desc(Document.created_on))
                .all()
            )

            for doc in driver_docs:
                doc_dict = {
                    "document_id": doc.id,
                    "document_name": doc.document_name,
                    "document_type": doc.document_type,
                    "document_format": doc.document_format,
                    "document_date": doc.document_date,
                    "document_path": doc.document_path,
                    "presigned_url": doc.presigned_url,
                    "file_size": doc.document_actual_size
                    if doc.document_actual_size
                    else 0,
                    "comments": doc.document_note,
                    "created_on": doc.created_on,
                    "object_type": doc.object_type,
                    "driver_id": lease_driver.driver_id,
                    "lease_driver_id": lease_driver.id,
                    "is_additional_driver": lease_driver.is_additional_driver,
                }

                # Add envelope info if LeaseDriverDocument exists
                if lease_driver_document:
                    doc_dict.update(
                        {
                            "is_sent_for_signature": True,
                            "has_front_desk_signed": lease_driver_document.has_frontend_signed,
                            "has_driver_signed": lease_driver_document.has_driver_signed,
                            "envelope_id": lease_driver_document.document_envelope_id,
                            "signing_type": lease_driver_document.signing_type,
                            "frontend_signed_date": lease_driver_document.frontend_signed_date,
                            "driver_signed_date": lease_driver_document.driver_signed_date,
                        }
                    )
                else:
                    doc_dict.update(
                        {
                            "is_sent_for_signature": False,
                            "has_front_desk_signed": None,
                            "has_driver_signed": None,
                            "envelope_id": None,
                            "signing_type": None,
                            "frontend_signed_date": None,
                            "driver_signed_date": None,
                        }
                    )

                all_documents.append(doc_dict)

    return all_documents
