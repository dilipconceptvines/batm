### app/utils/general.py

from sqlalchemy import or_ , func
from sqlalchemy.orm import Session

# Standard library imports
from typing import Optional, Dict
import os
import pandas as pd
import random
import base64
import string
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from app.core.config import settings

from app.entities.models import BankAccount

# Third party imports
from fastapi import HTTPException

def apply_multi_filter(query, column, value):
    items = [v.strip() for v in value.split(",") if v.strip()]
    return query.filter(or_(*[column.ilike(f"%{v}%") for v in items]))


def get_safe_value(row: pd.Series, column_name:str):
    value = row.get(column_name)
    if pd.isna(value):
        return None
    return value

def split_name(full_name: str):
    parts = full_name.strip().split()
    first_name = parts[0] if len(parts) > 0 else None
    middle_name = " ".join(parts[1:-1]) if len(parts) > 2 else None
    last_name = parts[-1] if len(parts) > 1 else None
    return first_name, middle_name, last_name

def fill_if_missing(target: dict, key: str, source: dict, source_key: str):
            if not target.get(key):
                value = source.get(source_key , None)
                if isinstance(value, list) and value:
                    target[key] = value[0]
                elif value is not None:
                    target[key] = value

def parse_custom_time(t: str) -> datetime.time:
    if not t:
        return None
    
    meridiem = t[-1]
    if meridiem not in ['A', 'P']:
        raise ValueError("Invalid time format")
    
    formatted = t[:-1] + (' AM' if meridiem == 'A' else ' PM')
    return datetime.strptime(formatted, "%I%M %p").time()

def get_date_from_string(from_date, duration_str: str) -> datetime:

    if isinstance(from_date, str):
        from_date = datetime.fromisoformat(from_date)

    duration_str = duration_str.lower().strip()
    number = int(duration_str.split()[0])
    unit = duration_str.split()[1]

    if "month" in unit:
        return from_date + relativedelta(months=number)
    elif "week" in unit:
        return from_date + relativedelta(weeks=number)
    elif "day" in unit:
        return from_date + relativedelta(days=number)
    elif "year" in unit:
        return from_date + relativedelta(years=number)
    else:
        raise ValueError("Unsupported time unit in string")
    
def get_random_date(days = None ,  start_date = None , end_date = None):
    if days:
        today = datetime.today().date()
        start_date = today - timedelta(days=days)
        random_date = start_date + timedelta(days=random.randint(0, (today - start_date).days))
        return random_date
    else:
        if not start_date:
            start_date =start_date = datetime(2025, 1, 1).date()
        if not end_date:
            end_date = datetime.today().date()
        random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        return random_date


def get_file_from_local(file_path: str):
    """Retrieve a file from the local media directory and encode it in Base64."""
   
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File in path '{file_path}' not found.")

    with open(file_path, "rb") as file:
        return base64.b64encode(file.read()).decode("utf-8")
    
def generate_random_string(length=6, alphanumeric=True):
    """
    Generate a random string of specified length.
    
    Args:
        length (int): Length of the string to generate (default: 6)
        alphanumeric (bool): If True, uses uppercase letters and digits (A-Z0-9).
                           If False, uses only digits (0-9).
                           
    Returns:
        str: Random string of specified length
        
    Example:
        generate_random_string(6, True)   # Returns something like "A7B2X9"
        generate_random_string(6, False)  # Returns something like "123456"
    """
    if alphanumeric:
        characters = string.ascii_uppercase + string.digits  # A-Z0-9
    else:
        characters = string.digits  # 0-9 only
    
    return ''.join(random.choices(characters, k=length))

def generate_alphanumeric_code(length=6):
    """Generate a random alphanumeric code of a given length"""
    characters = string.ascii_uppercase + string.digits  # A-Z0-9
    return ''.join(random.choices(characters, k=length))

def generate_16_digit_mix():
    """Generate a 14-character alphanumeric string with 2 to 3 uppercase letters."""
    num_letters = random.choice([2, 3])  # Choose 2 or 3 letters
    num_digits = 16 - num_letters  # Remaining characters will be digits
    
    letters = random.choices(string.ascii_uppercase, k=num_letters)
    digits = random.choices(string.digits, k=num_digits)
    
    result = letters + digits
    random.shuffle(result)  # Shuffle to mix letters and digits
    
    return ''.join(result)

def generate_random_6_digit():
    """
    Generates a random 6-digit number.

    Returns:
        int: A random 6-digit number.
    """
    return random.randint(100000, 999999)

def format_us_phone_number(phone_number: str) -> str:
    """
    Format a phone number string as a US phone number.

    Args:
        phone_number: Phone number string (may contain digits, spaces, dashes, etc.)

    Returns:
        Formatted phone number as (XXX) XXX-XXXX or original string if formatting fails
    """
    if not phone_number:
        return phone_number

    try:
        # Remove all non-digit characters
        digits = ''.join(filter(str.isdigit, phone_number))

        # Format as (XXX) XXX-XXXX if we have 10 digits
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11 and digits[0] == '1':
            # Handle numbers with country code
            return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        else:
            # If not standard length, return as-is
            return phone_number
    except (ValueError, TypeError, AttributeError):
        # If formatting fails, return as-is
        return phone_number
    

def format_address(address_obj) -> str:
    """
    Format an address object into a single string with null safety.

    Args:
        address_obj: Address object with address_line_1, address_line_2, city, state, zip

    Returns:
        Formatted address string or "N/A"
    """
    if not address_obj:
        return "N/A"

    parts = [
        getattr(address_obj, "address_line_1", None),
        getattr(address_obj, "address_line_2", None),
        getattr(address_obj, "city", None),
        getattr(address_obj, "state", None),
        getattr(address_obj, "zip", None),
    ]

    # Filter out None and empty strings
    formatted = ", ".join(part for part in parts if part)
    return formatted if formatted else "N/A"


def calculate_long_term_lease_fields(total_lease_amount: float , vehicle_lifetime_cap: float , vehicle_sales_tax: float) -> Dict[str, Optional[float]]:
    """
    Calculate detailed lease breakdown fields based on total weekly lease amount.

    Args:
        total_lease_amount (float): The total weekly lease amount (e.g., 1150.00)
    
    Returns:
        Dict[str, Optional[float]]: A dictionary of all lease-related fields.
    """

    # Fixed fees (from configuration)
    registration = settings.registration
    tlc_inspection_fees = settings.tlc_inspection_fees
    tax_stamps = settings.tax_stamps

    # Derived medallion lease component
    med_lease = round(total_lease_amount - (registration + tlc_inspection_fees + tax_stamps), 2)

    # Build response dictionary
    return {
        "tlc_vehicle_lifetime_cap": vehicle_lifetime_cap,
        "amount_collected": 0.0,
        "lease_amount": round(total_lease_amount, 2),
        "med_lease": med_lease,
        "med_tlc_maximum_amount": 0.0,
        "veh_lease": 0.0,
        "veh_sales_tax": vehicle_sales_tax,
        "tlc_inspection_fees": tlc_inspection_fees,
        "tax_stamps": tax_stamps,
        "registration": registration,
        "veh_tlc_maximum_amount": 0.0,
        "total_vehicle_lease": 0.0,
        "total_medallion_lease_payment": round(total_lease_amount, 2)
    }


def calculate_shift_lease_fields(total_lease_amount: float , vehicle_lifetime_cap: float , vehicle_sales_tax: float) -> Dict[str, Optional[float]]:
    """
    Calculate detailed shift lease breakdown fields based on total weekly lease amount.
    
    Args:
        total_lease_amount (float): The total weekly lease amount (e.g., 630.00)
    
    Returns:
        Dict[str, Optional[float]]: A dictionary containing lease-related computed fields.
    """

    # Fixed fees (from configuration)
    registration = settings.registration
    tlc_inspection_fees = settings.tlc_inspection_fees
    tax_stamps = settings.tax_stamps

    # Derived medallion lease component
    med_lease = round(total_lease_amount - (registration + tlc_inspection_fees + tax_stamps), 2)

    # Build response dictionary
    return {
        "tlc_vehicle_lifetime_cap": vehicle_lifetime_cap,
        "amount_collected": 0.0,
        "lease_amount": round(total_lease_amount, 2),
        "med_lease": med_lease,
        "med_tlc_maximum_amount": 0.0,
        "veh_lease": 0.0,
        "veh_sales_tax": vehicle_sales_tax,
        "tlc_inspection_fees": tlc_inspection_fees,
        "tax_stamps": tax_stamps,
        "registration": registration,
        "veh_tlc_maximum_amount": 0.0,
        "total_vehicle_lease": 0.0,
        "total_medallion_lease_payment": round(total_lease_amount, 2)
    }


def calculate_medallion_only_lease_fields(total_lease_amount: float , vehicle_lifetime_cap: float , vehicle_sales_tax: float) -> Dict[str, Optional[float]]:
    """
    Calculate detailed medallion-only lease breakdown fields based on total weekly lease amount.

    Args:
        total_lease_amount (float): The total weekly lease amount (e.g., 900.00)
    
    Returns:
        Dict[str, Optional[float]]: A dictionary containing computed lease fields.
    """

    # Fixed fees (from configuration)
    registration = settings.registration
    tlc_inspection_fees = settings.tlc_inspection_fees
    tax_stamps = settings.tax_stamps

    # Derived medallion lease component
    med_lease = round(total_lease_amount - (registration + tlc_inspection_fees + tax_stamps), 2)

    # Build response dictionary
    return {
        "tlc_vehicle_lifetime_cap": vehicle_lifetime_cap,
        "amount_collected": 0.0,
        "lease_amount": round(total_lease_amount, 2),
        "med_lease": med_lease,
        "med_tlc_maximum_amount": 0.0,
        "veh_lease": vehicle_lifetime_cap,
        "veh_sales_tax": vehicle_sales_tax,
        "tlc_inspection_fees": tlc_inspection_fees,
        "tax_stamps": tax_stamps,
        "registration": registration,
        "veh_tlc_maximum_amount": 0.0,
        "total_vehicle_lease": 0.0,
        "total_medallion_lease_payment": round(total_lease_amount, 2)
    }

def calculate_dov_lease_fields(
    total_lease_amount: float,
    vehicle_lifetime_cap: float,
    vehicle_sales_tax: float
) -> Dict[str, Optional[float]]:
    """
    Calculate DOV Lease related financial fields based on total weekly lease amount.

    Args:
        total_lease_amount (float): Total weekly lease amount (e.g., 1150.00)
        vehicle_lifetime_cap (float): TLC lifetime vehicle cap (e.g., 42900.00)
        vehicle_sales_tax (float): Weekly vehicle sales tax (e.g., 12.31)

    Returns:
        dict: Calculated DOV lease fields
    """

    # --- Fixed BAT Fees ---
    registration = settings.registration
    tlc_inspection_fees = settings.tlc_inspection_fees
    tax_stamps = settings.tax_stamps

    # --- Split total lease into medallion + vehicle ---
    base_med_lease = 781.51
    base_vehicle_lease = 206.25
    base_total = base_med_lease + base_vehicle_lease

    med_lease = (base_med_lease / base_total) * total_lease_amount
    veh_lease = (base_vehicle_lease / base_total) * total_lease_amount

    # --- TLC caps ---
    tlc_vehicle_lifetime_cap = vehicle_lifetime_cap
    veh_tlc_maximum_amount = vehicle_lifetime_cap / 208  # 4-year DOV lifetime cap
    med_tlc_maximum_amount = settings.tlc_medallion_weekly_cap_hybrid

    # --- Weekly totals ---
    total_vehicle_lease = veh_lease + vehicle_sales_tax + registration + tlc_inspection_fees + tax_stamps
    total_medallion_lease_payment = med_lease + registration + tlc_inspection_fees + tax_stamps


    # --- Return structured data ---
    return {
        "tlc_vehicle_lifetime_cap": round(tlc_vehicle_lifetime_cap, 2),
        "amount_collected": 0.0,
        "lease_amount": round(total_lease_amount, 2),
        "med_lease": round(med_lease, 2),
        "med_tlc_maximum_amount": round(med_tlc_maximum_amount, 2),
        "veh_lease": round(veh_lease, 2),
        "veh_sales_tax": round(vehicle_sales_tax, 2),
        "tlc_inspection_fees": round(tlc_inspection_fees, 2),
        "tax_stamps": round(tax_stamps, 2),
        "registration": round(registration, 2),
        "veh_tlc_maximum_amount": round(veh_tlc_maximum_amount, 2),
        "total_vehicle_lease": round(total_vehicle_lease, 2),
        "total_medallion_lease_payment": round(total_medallion_lease_payment, 2),
    }


def get_random_routing_number(db: Session) -> str:
    """
    Return a random routing number from the BankAccount table.
    Only non-empty, non-null routing numbers are considered.
    """
    try:
        routing_numbers = (
            db.query(BankAccount.bank_routing_number)
            .filter(
                BankAccount.bank_routing_number.isnot(None),
                func.trim(BankAccount.bank_routing_number) != ""
            )
            .distinct()
            .all()
        )

        # Convert rows to plain strings
        routing_numbers = [row[0] for row in routing_numbers]

        if not routing_numbers:
            raise HTTPException(
                status_code=404,
                detail="No routing numbers available"
            )

        return random.choice(routing_numbers)

    except HTTPException:
        raise   # re-raise HTTPException without wrapping

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch routing numbers: {str(e)}"
        )
    
def clean_value(value: str, is_bool=False):
    """
    Cleans CSV values:
    - For booleans: empty -> False
    - For strings: empty -> None
    """
    if value is None:
        return False if is_bool else None

    v = str(value).strip()

    if is_bool:
        # empty/blank -> False
        if v == "":
            return False
        return v.lower() in ("true", "1", "y", "yes")

    # String field
    return v or None