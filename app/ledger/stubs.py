# app/ledger/stubs.py

import math
import random
from datetime import datetime, timedelta
from decimal import Decimal

from app.ledger.models import (
    BalanceStatus,
    EntryType,
    PostingCategory,
    PostingStatus,
)
from app.ledger.schemas import (
    LedgerBalanceResponse,
    LedgerPostingResponse,
    PaginatedLedgerBalanceResponse,
    PaginatedLedgerPostingResponse,
)

# --- Pre-defined lists to generate realistic fake data ---
FAKE_DRIVERS = [
    "Andrews, JB",
    "Michel Rahman",
    "John Michel",
    "George Fernando",
    "Greg Paul",
    "Susan Boyle",
    "Michael Chen",
    "Linda Rodriguez",
]
FAKE_VINS = [
    "4T1BF3EBXFU123456",
    "1G1FY2D5XF1987654",
    "JN8AS5JE9FW234567",
    "WBAKA2C5XFC876543",
]
FAKE_MEDALLIONS = ["5Y55", "7X22", "9A44", "3Z11"]


def _generate_random_date(days_ago_max=90):
    """Generates a random datetime within the last `days_ago_max` days."""
    return datetime.now() - timedelta(
        days=random.randint(0, days_ago_max),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )


def create_stub_posting_response(
    page: int, per_page: int
) -> PaginatedLedgerPostingResponse:
    """Creates a paginated response with fake ledger posting data."""
    items = []
    total_items = 95  # To match the Figma mock

    for i in range(per_page):
        amount = Decimal(str(round(random.uniform(50.0, 1000.0), 2)))
        entry_type = random.choice([EntryType.DEBIT, EntryType.CREDIT])
        category = random.choice(list(PostingCategory))

        item = LedgerPostingResponse(
            posting_id=f"POST-2025-{random.randint(10000, 99999)}",
            status=random.choices(
                [PostingStatus.POSTED, PostingStatus.VOIDED], weights=[9, 1]
            )[0],
            date=_generate_random_date(),
            category=category,
            type=entry_type,
            amount=amount if entry_type == EntryType.DEBIT else -amount,
            driver_name=random.choice(FAKE_DRIVERS),
            lease_id=random.randint(1000, 2000),
            vehicle_vin=random.choice(FAKE_VINS),
            medallion_no=random.choice(FAKE_MEDALLIONS),
            reference_id=f"{category.value.upper()}-{random.randint(100, 999)}",
        )
        items.append(item)

    total_pages = math.ceil(total_items / per_page)

    return PaginatedLedgerPostingResponse(
        items=items,
        total_items=total_items,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
    )


def create_stub_balance_response(
    page: int, per_page: int
) -> PaginatedLedgerBalanceResponse:
    """Creates a paginated response with fake ledger balance data."""
    items = []
    total_items = 95  # To match the Figma mock

    for i in range(per_page):
        original_amount = Decimal(str(round(random.uniform(200.0, 5000.0), 2)))
        balance = Decimal(str(round(random.uniform(0, float(original_amount)), 2)))
        status = BalanceStatus.OPEN if balance > 0 else BalanceStatus.CLOSED

        item = LedgerBalanceResponse(
            balance_id=f"BAL-2025-{random.randint(10000, 99999)}",
            category=random.choice(list(PostingCategory)),
            status=status,
            reference_id=f"REF-{random.randint(100, 999)}-2025",
            driver_name=random.choice(FAKE_DRIVERS),
            lease_id=random.randint(1000, 2000),
            vehicle_vin=random.choice(FAKE_VINS),
            original_amount=original_amount,
            prior_balance=Decimal("0.00"),
            balance=balance,
        )
        items.append(item)

    total_pages = math.ceil(total_items / per_page)

    return PaginatedLedgerBalanceResponse(
        items=items,
        total_items=total_items,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
    )