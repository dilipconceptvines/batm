### app/loans/stubs.py

import math
import random
from datetime import date, timedelta, datetime
from decimal import Decimal

from app.loans.models import LoanInstallmentStatus, LoanStatus
from app.loans.schemas import (
    DriverLoanListResponse,
    PaginatedDriverLoanResponse,
    LoanInstallmentListResponse,
    PaginatedLoanInstallmentResponse,
)

# --- Pre-defined lists to generate realistic fake data ---
FAKE_DRIVERS = ["Andrews JB", "Michel Rahman", "John Michel", "George Fernando", "Greg Paul"]
FAKE_MEDALLIONS = ["5X23", "2224", "2C14", "2X24", "2T98"]
FAKE_LEASE_TYPES = ["DOV", "DOV", "Long Term", "Weekly", "DOV"]
FAKE_STATUSES = [LoanStatus.HOLD, LoanStatus.OPEN, LoanStatus.CLOSED, LoanStatus.CANCELLED]
FAKE_INSTALLMENT_STATUSES = [LoanInstallmentStatus.SCHEDULED, LoanInstallmentStatus.POSTED, LoanInstallmentStatus.DUE, LoanInstallmentStatus.PAID]

def _generate_random_date(days_ago_max=60) -> date:
    """Generates a random Sunday within the last `days_ago_max` days."""
    today = datetime.now().date()
    random_day = today - timedelta(days=random.randint(0, days_ago_max))
    # Adjust to the nearest previous Sunday
    sunday = random_day - timedelta(days=(random_day.weekday() + 1) % 7)
    return sunday

def create_stub_loan_response(
    page: int, per_page: int
) -> PaginatedDriverLoanResponse:
    """Creates a paginated response with fake driver loan data."""
    items = []
    total_items = 95  # Sample total

    for i in range(per_page):
        driver_index = i % len(FAKE_DRIVERS)
        interest_rate = random.choice([0, 0, 0, 5, 10, 12])
        
        item = DriverLoanListResponse(
            loan_id=f"DLN-2025-{str(45 + i).zfill(5)}",
            status=random.choices(FAKE_STATUSES, weights=[40, 20, 20, 20])[0],
            driver_name=FAKE_DRIVERS[driver_index],
            medallion_no=FAKE_MEDALLIONS[driver_index],
            lease_type=FAKE_LEASE_TYPES[driver_index],
            principal_amount=Decimal(str(round(random.uniform(500.0, 3000.0), 2))),
            interest_rate=Decimal(str(interest_rate)),
            start_week=_generate_random_date(),
        )
        items.append(item)

    total_pages = math.ceil(total_items / per_page)

    return PaginatedDriverLoanResponse(
        items=items,
        total_items=total_items,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
    )

def create_stub_installment_response(page: int, per_page: int) -> PaginatedLoanInstallmentResponse:
    """
    Creates a stubbed paginated response for loan installments for testing purposes.
    """
    from datetime import date, timedelta
    from decimal import Decimal
    
    stub_items = []
    total_items = 50
    start_index = (page - 1) * per_page
    
    statuses = [
        LoanInstallmentStatus.SCHEDULED,
        LoanInstallmentStatus.DUE,
        LoanInstallmentStatus.POSTED,
        LoanInstallmentStatus.PAID,
    ]
    
    for i in range(start_index, min(start_index + per_page, total_items)):
        week_start = date(2025, 1, 5) + timedelta(weeks=i)
        week_end = week_start + timedelta(days=6)
        
        stub_items.append(
            LoanInstallmentListResponse(
                installment_id=f"INST-{2025}-{str(i+1).zfill(4)}",
                loan_id=f"DLN-2025-{str((i % 10) + 1).zfill(3)}",
                driver_name=f"Driver {chr(65 + (i % 26))} {chr(65 + ((i+1) % 26))}",
                medallion_no=f"1A{str(10 + (i % 90)).zfill(2)}",
                lease_id=f"LSE-2024-{str((i % 20) + 1).zfill(3)}",
                vehicle_id=100 + (i % 50),
                week_start_date=week_start,
                week_end_date=week_end,
                principal_amount=Decimal("200.00") + Decimal(str(i % 100)),
                interest_amount=Decimal("5.00") + Decimal(str(i % 10)),
                total_due=Decimal("205.00") + Decimal(str(i % 110)),
                status=statuses[i % len(statuses)],
                posted_on=week_start if i % 2 == 0 else None,
                ledger_posting_ref=f"LDG-{str(i+1).zfill(6)}" if i % 2 == 0 else None,
            )
        )
    
    total_pages = math.ceil(total_items / per_page) if per_page > 0 else 0
    
    return PaginatedLoanInstallmentResponse(
        items=stub_items,
        total_items=total_items,
        page=page,
        per_page=per_page,
        total_pages=total_pages
    )