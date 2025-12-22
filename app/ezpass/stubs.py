### app/ezpass/stubs.py

import math
import random
from datetime import datetime, timedelta
from decimal import Decimal

from app.ezpass.models import EZPassTransactionStatus
from app.ezpass.schemas import (
    EZPassTransactionResponse,
    PaginatedEZPassTransactionResponse,
)

# --- Pre-defined lists to generate realistic fake data ---
FAKE_DRIVERS = ["117439", "135469", "127454", "134466", "147664"]
FAKE_PLATES = ["Y204711C-NY", "Y208750C-NY", "Y207897C-NY", "Y207263C-NY", "Y204872C-NY"]
FAKE_MEDALLIONS = ["1P43", "1P81", "5X23", "5X43", "2X77"]
FAKE_STATUSES = [
    EZPassTransactionStatus.IMPORTED,
    EZPassTransactionStatus.ASSOCIATED,
    EZPassTransactionStatus.POSTED_TO_LEDGER,
    EZPassTransactionStatus.ASSOCIATION_FAILED,
    EZPassTransactionStatus.POSTING_FAILED,
]

def _generate_random_datetime(days_ago_max=14):
    """Generates a random datetime within the last `days_ago_max` days."""
    return datetime.now() - timedelta(
        days=random.randint(0, days_ago_max),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )

def create_stub_ezpass_response(
    page: int, per_page: int
) -> PaginatedEZPassTransactionResponse:
    """Creates a paginated response with fake EZPass transaction data."""
    items = []
    total_items = 85  # Sample total to match UI mockups

    for i in range(per_page):
        trans_date = _generate_random_datetime()
        status = random.choices(FAKE_STATUSES, weights=[10, 50, 30, 5, 5])[0]
        
        # Determine if fields should be populated based on status
        is_associated = status in [
            EZPassTransactionStatus.ASSOCIATED,
            EZPassTransactionStatus.POSTED_TO_LEDGER,
            EZPassTransactionStatus.POSTING_FAILED
        ]
        is_posted = status == EZPassTransactionStatus.POSTED_TO_LEDGER

        item = EZPassTransactionResponse(
            id=random.randint(1000, 9999),
            transaction_datetime=trans_date,
            medallion_no=random.choice(FAKE_MEDALLIONS) if is_associated else None,
            driver_id=random.choice(FAKE_DRIVERS) if is_associated else None,
            tag_or_plate=f"NY {random.choice(FAKE_PLATES)}",
            posting_date=trans_date + timedelta(days=1) if is_posted else None,
            status=status,
            amount=Decimal(str(round(random.uniform(5.0, 25.0), 2))),
            failure_reason="Could not find an active lease for the driver at the time of the transaction."
            if status == EZPassTransactionStatus.ASSOCIATION_FAILED
            else None,
            agency="MTAB&T",
        )
        items.append(item)

    total_pages = math.ceil(total_items / per_page)

    return PaginatedEZPassTransactionResponse(
        items=items,
        total_items=total_items,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
    )