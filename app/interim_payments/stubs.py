### app/interim_payments/stubs.py

import math
import random
from datetime import datetime, timedelta
from decimal import Decimal

from app.interim_payments.models import PaymentMethod
from app.interim_payments.schemas import (
    InterimPaymentResponse,
    PaginatedInterimPaymentResponse,
)

# --- Pre-defined lists to generate realistic fake data ---
FAKE_PAYMENT_IDS = ["INTPAY-2025-00001", "INTPAY-2025-00002", "INTPAY-2025-00003", "INTPAY-2025-00004", "INTPAY-2025-00005"]
FAKE_TLC_LICENSES = ["5501234", "5502345", "5503456", "5504567", "5505678"]
FAKE_LEASE_IDS = ["LSE-2025-001", "LSE-2025-002", "LSE-2025-003", "LSE-2025-004", "LSE-2025-005"]
FAKE_CATEGORIES = ["LEASE", "REPAIR", "PVB", "TLC", "MISC"]
FAKE_REFERENCE_IDS = [
    "LSE-2025-001-WK42", "REPAIR-2025-00123", "1492882902", 
    "TLC-VIOL-2025-001", "MISC-2025-00001", "LSE-2025-002-WK43"
]
FAKE_AMOUNTS = [50.00, 75.00, 100.00, 125.00, 150.00, 200.00, 250.00, 300.00, 500.00]
FAKE_PAYMENT_METHODS = [PaymentMethod.CASH, PaymentMethod.CHECK, PaymentMethod.DRIVER_CREDIT]

def _generate_random_payment_date(days_ago_max=30) -> datetime:
    """Generates a random payment date within the last `days_ago_max` days."""
    return datetime.now() - timedelta(
        days=random.randint(0, days_ago_max),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )

def create_stub_interim_payments_response(
    page: int, per_page: int
) -> PaginatedInterimPaymentResponse:
    """Creates a paginated response with fake interim payment data."""
    items = []
    total_items = 73  # Sample total

    for i in range(per_page):
        item = InterimPaymentResponse(
            payment_id_display=random.choice(FAKE_PAYMENT_IDS),
            tlc_license=random.choice(FAKE_TLC_LICENSES),
            lease_id=random.choice(FAKE_LEASE_IDS),
            category=random.choice(FAKE_CATEGORIES),
            reference_id=random.choice(FAKE_REFERENCE_IDS),
            amount=Decimal(str(random.choice(FAKE_AMOUNTS))),
            payment_date=_generate_random_payment_date(),
            payment_method=random.choice(FAKE_PAYMENT_METHODS),
        )
        items.append(item)

    total_pages = math.ceil(total_items / per_page)

    return PaginatedInterimPaymentResponse(
        items=items,
        total_items=total_items,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
    )