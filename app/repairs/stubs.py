### app/repairs/stubs.py

import math
import random
from datetime import date, timedelta, datetime
from decimal import Decimal

from app.repairs.models import RepairInvoiceStatus, WorkshopType, RepairInstallmentStatus
from app.repairs.schemas import (
    PaginatedRepairInvoiceResponse,
    RepairInvoiceListResponse,
    RepairInstallmentListResponse,
    PaginatedRepairInstallmentResponse,
)

# --- Pre-defined lists to generate realistic fake data ---
FAKE_DRIVERS = ["Andrews JB", "Michel Rahman", "John Michel", "George Fernando", "Greg Paul"]
FAKE_MEDALLIONS = ["5X23", "2224", "2C14", "2X24", "2T98"]
FAKE_LEASE_TYPES = ["DOV", "DOV", "Long Term", "Weekly", "DOV"]
FAKE_STATUSES = [RepairInvoiceStatus.OPEN, RepairInvoiceStatus.CLOSED, RepairInvoiceStatus.HOLD]

def _generate_random_date(days_ago_max=90) -> date:
    """Generates a random date within the last `days_ago_max` days."""
    return (datetime.now() - timedelta(days=random.randint(0, days_ago_max))).date()

def create_stub_repair_invoice_response(
    page: int, per_page: int
) -> PaginatedRepairInvoiceResponse:
    """Creates a paginated response with fake vehicle repair invoice data."""
    items = []
    total_items = 95  # Sample total to simulate pagination

    for i in range(per_page):
        driver_index = i % len(FAKE_DRIVERS)
        item = RepairInvoiceListResponse(
            repair_id=f"RPR-2025-{str(12 + i).zfill(5)}",
            invoice_number=f"EXT-45{random.randint(80, 99)}",
            invoice_date=_generate_random_date(),
            status=random.choices(FAKE_STATUSES, weights=[70, 25, 5])[0],
            driver_name=FAKE_DRIVERS[driver_index],
            medallion_no=FAKE_MEDALLIONS[driver_index],
            lease_type=FAKE_LEASE_TYPES[driver_index],
            workshop_type=random.choice([WorkshopType.EXTERNAL, WorkshopType.BIG_APPLE]),
            total_amount=Decimal(str(round(random.uniform(500.0, 2500.0), 2))),
        )
        items.append(item)

    total_pages = math.ceil(total_items / per_page)

    return PaginatedRepairInvoiceResponse(
        items=items,
        total_items=total_items,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
    )

def create_stub_repair_installment_response(page: int, per_page: int) -> PaginatedRepairInstallmentResponse:
    """
    Creates a stubbed paginated response for repair installments for testing purposes.
    """
    
    stub_items = []
    total_items = 50
    start_index = (page - 1) * per_page
    
    statuses = [
        RepairInstallmentStatus.SCHEDULED,
        RepairInstallmentStatus.POSTED,
        RepairInstallmentStatus.PAID,
    ]
    
    workshops = [WorkshopType.BIG_APPLE, WorkshopType.EXTERNAL]
    
    for i in range(start_index, min(start_index + per_page, total_items)):
        week_start = date(2025, 1, 5) + timedelta(weeks=i)
        week_end = week_start + timedelta(days=6)
        
        stub_items.append(
            RepairInstallmentListResponse(
                installment_id=f"RPR-2025-{str((i % 10) + 1).zfill(3)}-{str((i % 5) + 1).zfill(2)}",
                repair_id=f"RPR-2025-{str((i % 10) + 1).zfill(3)}",
                invoice_number=f"INV-{str(1000 + i).zfill(6)}",
                driver_name=f"Driver {chr(65 + (i % 26))} {chr(65 + ((i+1) % 26))}",
                medallion_no=f"1A{str(10 + (i % 90)).zfill(2)}",
                lease_id=f"LSE-2024-{str((i % 20) + 1).zfill(3)}",
                vehicle_id=100 + (i % 50),
                week_start_date=week_start,
                week_end_date=week_end,
                principal_amount=Decimal("150.00") + Decimal(str(i % 100)),
                status=statuses[i % len(statuses)],
                posted_on=week_start if i % 2 == 0 else None,
                ledger_posting_ref=f"LDG-{str(i+1).zfill(6)}" if i % 2 == 0 else None,
                workshop_type=workshops[i % len(workshops)],
            )
        )
    
    total_pages = math.ceil(total_items / per_page) if per_page > 0 else 0
    
    return PaginatedRepairInstallmentResponse(
        items=stub_items,
        total_items=total_items,
        page=page,
        per_page=per_page,
        total_pages=total_pages
    )