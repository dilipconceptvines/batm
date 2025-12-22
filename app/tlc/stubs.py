### app/tlc/stubs.py

import math
import random
from datetime import date, datetime, time, timedelta

from app.tlc.models import TLCDisposition, TLCViolationStatus, TLCViolationType
from app.tlc.schemas import (
    PaginatedTLCViolationResponse,
    TLCViolationListResponse,
)
from app.leases.schemas import LeaseType

# --- Pre-defined lists to generate realistic fake data ---
FAKE_DRIVERS = ["117439", "135469", "127454", "134466", "147664"]
FAKE_PLATES = ["Y207163C", "Y208142C", "Y204572C", "Y204139C", "Y203591C"]
FAKE_MEDALLIONS = ["1P43", "1P81", "5X23", "5X43", "2X77"]
FAKE_SUMMONS = ["1492882902", "1495204730", "1495204785", "1498274288", "1499096380"]
FAKE_TYPES = [TLCViolationType.FI, TLCViolationType.FN, TLCViolationType.RF]
FAKE_LEASE_ID = ["1234-DOV-20251127", "4521-LTL-20251003", "7789-TWL-20250914", "3098-MOL-20250722", "5643-LTL-20251101"]
FAKE_VIN = ["1HGCM82633A123456", "2T3ZF4DV9BW012345", "3FAHP0HA6AR987654", "5YJ3E1EA7KF123789", "JHMGE8H57DC456321"]
FAKE_EMAIL = ["omshrestha@bat.com", "john@bat.com", "jane@bat.com", "mike@bat.com", "emily@bat.com"]

def _generate_random_date(days_ago_max=45) -> date:
    """Generates a random date within the last `days_ago_max` days."""
    return (datetime.now() - timedelta(days=random.randint(0, days_ago_max))).date()

def _generate_random_time_str() -> str:
    """Generates a random time string in the HHMM(A/P) format."""
    hour = random.randint(1, 12)
    minute = random.randint(0, 59)
    meridiem = random.choice(['AM', 'PM'])
    return f"{hour:02d}{minute:02d}{meridiem}"

def generate_fake_name():
    """Generates a random full name."""
    first_names = ["John", "Jane", "Michael", "Emily", "David"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones"]
    return f"{random.choice(first_names)} {random.choice(last_names)}"

def create_stub_tlc_response(
    page: int, per_page: int
) -> PaginatedTLCViolationResponse:
    """Creates a paginated response with fake TLC violation data."""
    items = []
    total_items = 95  # Sample total

    for i in range(per_page):
        item = TLCViolationListResponse(
            id=i + 1,
            plate=random.choice(FAKE_PLATES),
            state="NY",
            type=random.choice(FAKE_TYPES),
            summons_no=random.choice(FAKE_SUMMONS),
            issue_date=_generate_random_date(),
            issue_time=datetime.strptime(_generate_random_time_str(), "%I%M%p").time(),
            driver_id=random.choice(FAKE_DRIVERS),
            medallion_no=random.choice(FAKE_MEDALLIONS),
            driver_name = generate_fake_name(),
            vin = random.choice(FAKE_VIN),
            lease_id = random.choice(FAKE_LEASE_ID),
            lease_type = random.choice(LeaseType.values()),
            driver_email = random.choice(FAKE_EMAIL),
        )
        items.append(item)

    total_pages = math.ceil(total_items / per_page)

    return PaginatedTLCViolationResponse(
        items=items,
        total_items=total_items,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
    )