# Current Balances Feature - Integration Guide

## Overview
This document provides complete integration instructions for the Current Balances feature in the Big Apple Taxi Management System.

## File Structure

```
app/
├── current_balances/
│   ├── __init__.py
│   ├── router.py          # API endpoints
│   ├── services.py        # Business logic
│   └── schemas.py         # Pydantic models
├── main.py               # Update to include router
└── tests/
    └── test_current_balances.py
```

## Step 1: Create Package Directory

```bash
mkdir -p app/current_balances
touch app/current_balances/__init__.py
```

## Step 2: Create __init__.py

```python
# app/current_balances/__init__.py

"""
Current Balances Module

Provides real-time view of weekly financial positions for all leases.
"""

from app.current_balances.router import router
from app.current_balances.services import CurrentBalancesService
from app.current_balances.schemas import (
    CurrentBalancesResponse,
    WeeklyBalanceRow,
    WeekPeriod,
    CurrentBalancesFilter
)

__all__ = [
    'router',
    'CurrentBalancesService',
    'CurrentBalancesResponse',
    'WeeklyBalanceRow',
    'WeekPeriod',
    'CurrentBalancesFilter'
]
```

## Step 3: Register Router in Main App

Update `app/main.py` to include the current balances router:

```python
# app/main.py

from app.current_balances.router import router as current_balances_router

# ... existing code ...

# Add after other routers
bat_app.include_router(current_balances_router)

logger.info("Current Balances router registered")
```

## Step 4: Add Required Dependencies

Ensure these are in your `requirements.txt`:

```txt
fastapi>=0.104.0
sqlalchemy>=2.0.0
pydantic>=2.4.0
pandas>=2.0.0
openpyxl>=3.1.0  # For Excel export
python-dateutil>=2.8.0
```

Install dependencies:

```bash
pip install pandas openpyxl
```

## Step 5: Database Models (Ensure These Exist)

The service relies on these existing models. Verify they are properly set up:

### Required Models:
- `app.leases.models.Lease`
- `app.drivers.models.Driver`
- `app.vehicles.models.Vehicle`
- `app.medallions.models.Medallion`
- `app.dtr.models.DTR`
- `app.curb.models.CurbTransaction`
- `app.ezpass.models.EZPassTransaction`
- `app.pvb.models.PVBViolation`
- `app.tlc.models.TLCTicket`
- `app.repairs.models.Repair`
- `app.loans.models.Loan`
- `app.ledger.models.LedgerBalance`

### Required Model Fields:

#### Lease Model
```python
class Lease(Base):
    id: int
    lease_id: str
    status: LeaseStatus  # ACTIVE, TERMINATED, etc.
    weekly_lease_amount: Decimal
    deposit_amount: Decimal
    primary_driver_id: int
    vehicle_id: int
    medallion_id: int
    # ... relationships ...
    primary_driver = relationship("Driver")
    vehicle = relationship("Vehicle")
    medallion = relationship("Medallion")
```

#### CurbTransaction Model
```python
class CurbTransaction(Base):
    id: int
    lease_id: int
    trip_date: date
    payment_type: str  # 'CC' or 'CASH'
    total_amount: Decimal
    mta_fee: Decimal
    tif_fee: Decimal
    congestion_fee: Decimal
    cbdt_fee: Decimal
    airport_fee: Decimal
```

#### LedgerBalance Model
```python
class LedgerBalance(Base):
    id: int
    lease_id: int
    category: PostingCategory  # EZPASS, PVB, TLC, REPAIR, LOAN, etc.
    balance: Decimal
    status: BalanceStatus  # OPEN, CLOSED
    due_date: date
    created_on: datetime
```

## Step 6: API Endpoints

Once integrated, the following endpoints will be available:

### List Current Balances
```http
GET /current-balances?week_start=2025-11-03&page=1&per_page=25
```

**Query Parameters:**
- `week_start` (optional): Sunday date, defaults to current week
- `page`: Page number (default: 1)
- `per_page`: Items per page (default: 25, max: 100)
- `search`: Search by lease ID, driver name, TLC license, medallion, or plate
- `lease_status`: Filter by ACTIVE, TERMINATED, or TERMINATION_REQUESTED
- `driver_status`: Filter by ACTIVE, SUSPENDED, or BLACKLISTED
- `payment_type`: Filter by CASH or ACH
- `dtr_status`: Filter by GENERATED or NOT_GENERATED

**Response:**
```json
{
  "week_period": {
    "week_start": "2025-11-03",
    "week_end": "2025-11-09",
    "week_label": "Nov 03 - Nov 09, 2025",
    "is_current_week": true
  },
  "items": [
    {
      "lease_id": "L-1045",
      "driver_name": "John Doe",
      "tlc_license": "5123456",
      "medallion_number": "1Y23",
      "plate_number": "Y234",
      "lease_status": "ACTIVE",
      "driver_status": "ACTIVE",
      "dtr_status": "NOT_GENERATED",
      "payment_type": "ACH",
      "cc_earnings": "780.00",
      "weekly_lease_fee": "400.00",
      "mta_tif": "31.50",
      "ezpass_tolls": "25.00",
      "pvb_violations": "0.00",
      "tlc_tickets": "0.00",
      "repairs_wtd": "0.00",
      "loans_wtd": "0.00",
      "misc_charges": "0.00",
      "subtotal_deductions": "456.50",
      "prior_balance": "0.00",
      "deposit_amount": "500.00",
      "net_earnings": "323.50",
      "last_updated": "2025-11-06T14:30:00Z"
    }
  ],
  "total_items": 45,
  "page": 1,
  "per_page": 25,
  "total_pages": 2,
  "last_refresh": "2025-11-06T14:30:22Z"
}
```

### Get Lease Detail with Daily Breakdown
```http
GET /current-balances/lease/L-1045?week_start=2025-11-03
```

**Response:**
```json
{
  "lease_id": "L-1045",
  "driver_name": "John Doe",
  "...": "... (all fields from list endpoint) ...",
  "daily_breakdown": [
    {
      "day_of_week": "Sunday",
      "date": "2025-11-03",
      "cc_earnings": "120.00",
      "mta_tif": "4.50",
      "ezpass": "5.00",
      "pvb_violations": "0.00",
      "tlc_tickets": "0.00",
      "net_daily_earnings": "110.50"
    },
    {
      "day_of_week": "Monday",
      "date": "2025-11-04",
      "cc_earnings": "160.00",
      "mta_tif": "7.00",
      "ezpass": "7.00",
      "pvb_violations": "0.00",
      "tlc_tickets": "0.00",
      "net_daily_earnings": "146.00"
    }
  ],
  "delayed_charges": [
    {
      "category": "EZPass",
      "amount": "12.00",
      "original_date": "2025-10-28",
      "system_entry_date": "2025-11-05",
      "description": "Toll: GWB Plaza"
    }
  ],
  "week_period": {
    "week_start": "2025-11-03",
    "week_end": "2025-11-09",
    "week_label": "Nov 03 - Nov 09, 2025",
    "is_current_week": true
  }
}
```

### Get Available Weeks
```http
GET /current-balances/weeks/available?limit=12
```

**Response:**
```json
[
  {
    "week_start": "2025-11-03",
    "week_end": "2025-11-09",
    "week_label": "Nov 03 - Nov 09, 2025",
    "is_current_week": true
  },
  {
    "week_start": "2025-10-27",
    "week_end": "2025-11-02",
    "week_label": "Oct 27 - Nov 02, 2025",
    "is_current_week": false
  }
]
```

### Get Summary Statistics
```http
GET /current-balances/summary?week_start=2025-11-03
```

**Response:**
```json
{
  "week_period": { "..." },
  "total_leases": 45,
  "total_cc_earnings": 35100.00,
  "total_deductions": 20550.00,
  "total_net_earnings": 14550.00,
  "dtrs_generated": 0,
  "dtrs_not_generated": 45,
  "payment_breakdown": {
    "ach": 38,
    "cash": 7
  },
  "generated_at": "2025-11-06T14:30:22Z"
}
```

### Export to Excel/CSV
```http
POST /current-balances/export?week_start=2025-11-03&format=excel
```

Downloads an Excel or CSV file with all balance data.

## Step 7: Frontend Integration

### Sample Frontend Request (React/JavaScript)

```javascript
// Fetch current balances
const fetchCurrentBalances = async (weekStart, page = 1) => {
  const params = new URLSearchParams({
    page: page,
    per_page: 25,
    ...(weekStart && { week_start: weekStart })
  });
  
  const response = await fetch(
    `/api/current-balances?${params}`,
    {
      headers: {
        'Authorization': `Bearer ${token}`
      }
    }
  );
  
  return await response.json();
};

// Fetch lease detail with daily breakdown
const fetchLeaseDetail = async (leaseId, weekStart) => {
  const params = new URLSearchParams({
    ...(weekStart && { week_start: weekStart })
  });
  
  const response = await fetch(
    `/api/current-balances/lease/${leaseId}?${params}`,
    {
      headers: {
        'Authorization': `Bearer ${token}`
      }
    }
  );
  
  return await response.json();
};

// Export to Excel
const exportToExcel = async (weekStart, filters) => {
  const params = new URLSearchParams({
    format: 'excel',
    week_start: weekStart,
    ...filters
  });
  
  const response = await fetch(
    `/api/current-balances/export?${params}`,
    {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`
      }
    }
  );
  
  const blob = await response.blob();
  const url = window.URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `current_balances_${weekStart}.xlsx`;
  a.click();
};
```

## Step 8: Testing

Run the test suite:

```bash
pytest app/tests/test_current_balances.py -v
```

## Step 9: Deployment Checklist

- [ ] All models have required fields and relationships
- [ ] Database migrations are up to date
- [ ] Dependencies are installed (`pandas`, `openpyxl`)
- [ ] Router is registered in `main.py`
- [ ] Tests pass
- [ ] API documentation is accessible at `/docs`
- [ ] Authentication middleware is configured
- [ ] Logging is properly set up
- [ ] Performance testing completed for large datasets

## Performance Considerations

1. **Pagination**: Always use pagination for list endpoints (default 25 items per page)
2. **Indexing**: Ensure database indexes on:
   - `lease.status`
   - `lease.primary_driver_id`
   - `curb_transaction.lease_id, trip_date`
   - `ledger_balance.lease_id, category, status`
3. **Caching**: Consider caching week periods and summary statistics for 5-10 minutes
4. **Async Processing**: For export operations with large datasets, consider background jobs

## Monitoring

Key metrics to monitor:
- Response time for list endpoint (target: < 500ms)
- Response time for detail endpoint (target: < 1s)
- Export generation time (target: < 10s for 1000 leases)
- Database query count per request
- Error rate

## Support

For issues or questions, contact the development team or check:
- API documentation: `http://your-domain/docs`
- Project documentation: `/app/current_balances/README.md`