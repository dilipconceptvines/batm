### app/pvb/stubs.py

import math
import random
from datetime import datetime, timedelta, date, time

from app.pvb.models import PVBViolationStatus
from app.pvb.schemas import (
    PaginatedPVBViolationResponse,
    PVBViolationResponse,
)

# --- Pre-defined lists to generate realistic fake data ---
FAKE_DRIVERS = ["117439", "135469", "127454", "134466", "147664", None]
FAKE_PLATES = ["Y207163C", "Y208142C", "Y204572C", "Y204139C", "Y203591C"]
FAKE_MEDALLIONS = ["1P43", "1P81", "5X23", "5X43", "2X77", None]
FAKE_SUMMONS = ["1492882902", "1495204730", "1495204785", "1498274288", "1499096380"]
FAKE_TYPES = ["PAS", "OMT"]
FAKE_STATUSES = [
    PVBViolationStatus.IMPORTED,
    PVBViolationStatus.ASSOCIATED,
    PVBViolationStatus.POSTED_TO_LEDGER,
    PVBViolationStatus.ASSOCIATION_FAILED,
]

def _generate_random_date(days_ago_max=30) -> date:
    """Generates a random date within the last `days_ago_max` days."""
    return (datetime.now() - timedelta(days=random.randint(0, days_ago_max))).date()

def _generate_random_time() -> time:
    """Generates a random time."""
    return time(hour=random.randint(0, 23), minute=random.randint(0, 59))

def create_stub_pvb_response(
    page: int, per_page: int
) -> PaginatedPVBViolationResponse:
    """Creates a paginated response with fake PVB violation data."""
    items = []
    total_items = 85  # Sample total

    for i in range(per_page):
        status = random.choices(FAKE_STATUSES, weights=[10, 60, 25, 5])[0]
        is_associated = status not in [PVBViolationStatus.IMPORTED, PVBViolationStatus.ASSOCIATION_FAILED]
        is_posted = status == PVBViolationStatus.POSTED_TO_LEDGER

        item = PVBViolationResponse(
            id=random.randint(1000, 9999),
            plate=random.choice(FAKE_PLATES),
            state="NY",
            type=random.choice(FAKE_TYPES),
            summons=random.choice(FAKE_SUMMONS),
            issue_date=_generate_random_date(),
            issue_time=_generate_random_time(),
            medallion_no=random.choice(FAKE_MEDALLIONS) if is_associated else None,
            driver_id=random.choice(FAKE_DRIVERS) if is_associated else None,
            posting_date=_generate_random_date(days_ago_max=5) if is_posted else None,
            status=status,
            total_amount=random.choice([65.00, 115.00, 50.00, 250.00]),
        )
        items.append(item)

    total_pages = math.ceil(total_items / per_page)

    return PaginatedPVBViolationResponse(
        items=items,
        total_items=total_items,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
    )