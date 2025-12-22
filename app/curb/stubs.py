### app/curb/stubs.py

import math
import random
from datetime import datetime, timedelta
from decimal import Decimal

from app.curb.models import PaymentType
from app.curb.schemas import CurbTripResponse, PaginatedCurbTripResponse

# --- Pre-defined lists to generate realistic fake data ---
FAKE_DRIVERS = ["05732414", "00504138", "00506140", "00503126", "00304132"]
FAKE_PLATES = ["Y203812C-NY", "Y204712C-NY", "Y206832C-NY", "Y204862C-NY", "Y207865C-NY"]
FAKE_MEDALLIONS = ["1P43", "1P81", "5X23", "5Z34", "1C56"]

def _generate_random_datetime(days_ago_max=7):
    """Generates a random datetime within the last `days_ago_max` days."""
    return datetime.now() - timedelta(
        days=random.randint(0, days_ago_max),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )

def create_stub_curb_trip_response(
    page: int, per_page: int
) -> PaginatedCurbTripResponse:
    """Creates a paginated response with fake CURB trip data."""
    items = []
    total_items = 95  # A sample total to simulate pagination

    for i in range(per_page):
        start_time = _generate_random_datetime()
        end_time = start_time + timedelta(minutes=random.randint(5, 45))

        fare = Decimal(str(round(random.uniform(8.0, 40.0), 2)))
        tips = Decimal(str(round(random.uniform(0.0, 10.0), 2)))
        tolls = Decimal(str(round(random.uniform(0.0, 15.0), 2)))
        extras = Decimal(str(round(random.uniform(0.0, 5.0), 2)))


        surcharge = Decimal("0.50")
        improvement_surcharge = Decimal("0.30")
        congestion_fee = Decimal("2.75")
        airport_fee = Decimal("0.00")
        cbdt_fee = Decimal("0.00")

        total_amount = (
            fare
            + tips
            + tolls
            + extras
            + surcharge
            + improvement_surcharge
            + congestion_fee
            + airport_fee
            + cbdt_fee
        )
        
        item = CurbTripResponse(
            curb_trip_id=f"TRPN{random.randint(100, 999)}",
            curb_driver_id=random.choice(FAKE_DRIVERS),
            tlc_license_no=random.choice(FAKE_DRIVERS),
            plate=random.choice(FAKE_PLATES),
            curb_cab_number=random.choice(FAKE_MEDALLIONS),
            fare=fare,
            tips=tips,
            tolls=tolls,
            extras=extras,
            surcharge=surcharge,
            improvement_surcharge=improvement_surcharge,
            congestion_fee=congestion_fee,
            airport_fee=airport_fee,
            cbdt_fee=cbdt_fee,
            total_amount=total_amount,
            payment_mode=random.choice(list(PaymentType)),
            start_time=start_time,
            end_time=end_time,
            status=random.choice(["UNRECONCILED", "RECONCILED", "POSTED_TO_LEDGER"]),
            start_location_gps=f"{random.uniform(40.7, 40.8):.4f}° N",
            end_location_gps=f"{random.uniform(73.9, 74.0):.4f}° W",
        )
        items.append(item)

    total_pages = math.ceil(total_items / per_page)

    return PaginatedCurbTripResponse(
        items=items,
        total_items=total_items,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
    )