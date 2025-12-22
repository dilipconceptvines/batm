# Current Balances - Quick Start Guide

## 5-Minute Setup

### Step 1: Copy Files

```bash
# Create directory
mkdir -p app/current_balances

# Copy the three main files
# - schemas.py (from artifact: current_balances_schemas)
# - services.py (from artifact: current_balances_service)
# - router.py (from artifact: current_balances_router)

# Create __init__.py
cat > app/current_balances/__init__.py << 'EOF'
from app.current_balances.router import router
from app.current_balances.services import CurrentBalancesService

__all__ = ['router', 'CurrentBalancesService']
EOF
```

### Step 2: Update Main Application

```python
# app/main.py

from app.current_balances.router import router as current_balances_router

# Add after other routers (around line 40)
bat_app.include_router(current_balances_router)

logger.info("Current Balances router registered")
```

### Step 3: Install Dependencies

```bash
pip install pandas openpyxl
```

### Step 4: Test

```bash
# Start the server
uvicorn app.main:bat_app --reload

# Visit API docs
open http://localhost:8000/docs

# Look for "Current Balances" section
```

## Usage Examples

### Example 1: Get Current Week Balances

**Request:**
```bash
curl -X GET "http://localhost:8000/current-balances?page=1&per_page=10" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

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
      "daily_breakdown": null,
      "delayed_charges": null,
      "last_updated": "2025-11-06T14:30:22Z"
    }
  ],
  "total_items": 45,
  "page": 1,
  "per_page": 10,
  "total_pages": 5,
  "last_refresh": "2025-11-06T14:30:22Z"
}
```

### Example 2: Search for Specific Lease

**Request:**
```bash
curl -X GET "http://localhost:8000/current-balances?search=L-1045" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

### Example 3: Filter by Payment Type

**Request:**
```bash
curl -X GET "http://localhost:8000/current-balances?payment_type=ACH&dtr_status=NOT_GENERATED" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

### Example 4: Get Historical Week

**Request:**
```bash
curl -X GET "http://localhost:8000/current-balances?week_start=2025-10-27" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

**Response will have:**
- `dtr_status: "GENERATED"` for all items
- Data from finalized DTRs

### Example 5: Get Lease Detail with Daily Breakdown

**Request:**
```bash
curl -X GET "http://localhost:8000/current-balances/lease/L-1045" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

**Response:**
```json
{
  "lease_id": "L-1045",
  "driver_name": "John Doe",
  "cc_earnings": "780.00",
  "net_earnings": "323.50",
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

### Example 6: Get Available Weeks

**Request:**
```bash
curl -X GET "http://localhost:8000/current-balances/weeks/available?limit=4" \
  -H "Authorization: Bearer YOUR_TOKEN"
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
  },
  {
    "week_start": "2025-10-20",
    "week_end": "2025-10-26",
    "week_label": "Oct 20 - Oct 26, 2025",
    "is_current_week": false
  },
  {
    "week_start": "2025-10-13",
    "week_end": "2025-10-19",
    "week_label": "Oct 13 - Oct 19, 2025",
    "is_current_week": false
  }
]
```

### Example 7: Get Summary Statistics

**Request:**
```bash
curl -X GET "http://localhost:8000/current-balances/summary" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

**Response:**
```json
{
  "week_period": {
    "week_start": "2025-11-03",
    "week_end": "2025-11-09",
    "week_label": "Nov 03 - Nov 09, 2025",
    "is_current_week": true
  },
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

### Example 8: Export to Excel

**Request:**
```bash
curl -X POST "http://localhost:8000/current-balances/export?format=excel" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  --output current_balances.xlsx
```

### Example 9: Export with Filters (CSV)

**Request:**
```bash
curl -X POST "http://localhost:8000/current-balances/export?format=csv&payment_type=ACH&lease_status=ACTIVE" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  --output ach_leases.csv
```

## Python Client Example

```python
import requests
from datetime import date, timedelta

class CurrentBalancesClient:
    """Client for Current Balances API"""
    
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url
        self.headers = {"Authorization": f"Bearer {token}"}
    
    def get_current_week_balances(self, page: int = 1, per_page: int = 25):
        """Get current week balances"""
        response = requests.get(
            f"{self.base_url}/current-balances",
            params={"page": page, "per_page": per_page},
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()
    
    def get_lease_detail(self, lease_id: str, week_start: date = None):
        """Get detailed balance for a lease"""
        params = {}
        if week_start:
            params["week_start"] = week_start.isoformat()
        
        response = requests.get(
            f"{self.base_url}/current-balances/lease/{lease_id}",
            params=params,
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()
    
    def search_leases(self, search_term: str):
        """Search for leases"""
        response = requests.get(
            f"{self.base_url}/current-balances",
            params={"search": search_term, "per_page": 100},
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()
    
    def get_summary(self, week_start: date = None):
        """Get summary statistics"""
        params = {}
        if week_start:
            params["week_start"] = week_start.isoformat()
        
        response = requests.get(
            f"{self.base_url}/current-balances/summary",
            params=params,
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()
    
    def export_to_excel(self, filename: str, week_start: date = None):
        """Export to Excel file"""
        params = {"format": "excel"}
        if week_start:
            params["week_start"] = week_start.isoformat()
        
        response = requests.post(
            f"{self.base_url}/current-balances/export",
            params=params,
            headers=self.headers
        )
        response.raise_for_status()
        
        with open(filename, 'wb') as f:
            f.write(response.content)
        
        print(f"Exported to {filename}")


# Usage
client = CurrentBalancesClient(
    base_url="http://localhost:8000",
    token="your_token_here"
)

# Get current week
data = client.get_current_week_balances()
print(f"Found {data['total_items']} leases")
for item in data['items'][:3]:
    print(f"  {item['lease_id']}: {item['driver_name']} - Net: ${item['net_earnings']}")

# Get detail for specific lease
detail = client.get_lease_detail("L-1045")
print(f"\nLease L-1045 Daily Breakdown:")
for day in detail['daily_breakdown']:
    print(f"  {day['day_of_week']}: ${day['net_daily_earnings']}")

# Search
results = client.search_leases("John")
print(f"\nSearch results for 'John': {results['total_items']} found")

# Get summary
summary = client.get_summary()
print(f"\nWeek Summary:")
print(f"  Total Leases: {summary['total_leases']}")
print(f"  Total Earnings: ${summary['total_cc_earnings']}")
print(f"  Total Net: ${summary['total_net_earnings']}")

# Export
client.export_to_excel("current_week.xlsx")
```

## React/JavaScript Frontend Example

```javascript
// CurrentBalancesService.js

class CurrentBalancesService {
  constructor(baseURL, getToken) {
    this.baseURL = baseURL;
    this.getToken = getToken;
  }

  async fetchCurrentWeek(page = 1, perPage = 25, filters = {}) {
    const params = new URLSearchParams({
      page: page,
      per_page: perPage,
      ...filters
    });

    const response = await fetch(
      `${this.baseURL}/current-balances?${params}`,
      {
        headers: {
          'Authorization': `Bearer ${this.getToken()}`
        }
      }
    );

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    return await response.json();
  }

  async fetchLeaseDetail(leaseId, weekStart = null) {
    const params = new URLSearchParams();
    if (weekStart) {
      params.append('week_start', weekStart);
    }

    const response = await fetch(
      `${this.baseURL}/current-balances/lease/${leaseId}?${params}`,
      {
        headers: {
          'Authorization': `Bearer ${this.getToken()}`
        }
      }
    );

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    return await response.json();
  }

  async fetchAvailableWeeks(limit = 12) {
    const response = await fetch(
      `${this.baseURL}/current-balances/weeks/available?limit=${limit}`,
      {
        headers: {
          'Authorization': `Bearer ${this.getToken()}`
        }
      }
    );

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    return await response.json();
  }

  async exportToExcel(weekStart = null, filters = {}) {
    const params = new URLSearchParams({
      format: 'excel',
      ...filters
    });
    if (weekStart) {
      params.append('week_start', weekStart);
    }

    const response = await fetch(
      `${this.baseURL}/current-balances/export?${params}`,
      {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${this.getToken()}`
        }
      }
    );

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const blob = await response.blob();
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `current_balances_${weekStart || 'current'}.xlsx`;
    document.body.appendChild(a);
    a.click();
    window.URL.revokeObjectURL(url);
    document.body.removeChild(a);
  }
}

// Usage in React Component
import React, { useState, useEffect } from 'react';

function CurrentBalancesPage() {
  const [balances, setBalances] = useState(null);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  
  const service = new CurrentBalancesService(
    'http://localhost:8000',
    () => localStorage.getItem('token')
  );

  useEffect(() => {
    loadBalances();
  }, [page]);

  const loadBalances = async () => {
    setLoading(true);
    try {
      const data = await service.fetchCurrentWeek(page, 25);
      setBalances(data);
    } catch (error) {
      console.error('Error loading balances:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleExport = async () => {
    try {
      await service.exportToExcel();
      alert('Export successful!');
    } catch (error) {
      alert('Export failed: ' + error.message);
    }
  };

  if (loading) return <div>Loading...</div>;
  if (!balances) return <div>No data</div>;

  return (
    <div>
      <h1>Current Balances - {balances.week_period.week_label}</h1>
      
      <button onClick={handleExport}>Export to Excel</button>
      
      <table>
        <thead>
          <tr>
            <th>Lease ID</th>
            <th>Driver</th>
            <th>CC Earnings</th>
            <th>Net Earnings</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          {balances.items.map(item => (
            <tr key={item.lease_id}>
              <td>{item.lease_id}</td>
              <td>{item.driver_name}</td>
              <td>${item.cc_earnings}</td>
              <td>${item.net_earnings}</td>
              <td>{item.dtr_status}</td>
            </tr>
          ))}
        </tbody>
      </table>
      
      <div>
        <button 
          disabled={page === 1} 
          onClick={() => setPage(page - 1)}
        >
          Previous
        </button>
        <span>Page {page} of {balances.total_pages}</span>
        <button 
          disabled={page === balances.total_pages} 
          onClick={() => setPage(page + 1)}
        >
          Next
        </button>
      </div>
    </div>
  );
}

export default CurrentBalancesPage;
```

## Common Use Cases

### 1. Dashboard Summary Widget

Display key metrics for the current week:

```python
async def get_dashboard_summary():
    service = CurrentBalancesService(db)
    week_start, week_end = service.get_current_week()
    
    summary = await service.get_summary(week_start)
    
    return {
        "active_leases": summary["total_leases"],
        "total_earnings": summary["total_cc_earnings"],
        "total_net": summary["total_net_earnings"],
        "ach_drivers": summary["payment_breakdown"]["ach"],
        "cash_drivers": summary["payment_breakdown"]["cash"]
    }
```

### 2. Driver Balance Lookup

Find a specific driver's current balance:

```python
async def find_driver_balance(driver_name: str):
    service = CurrentBalancesService(db)
    week_start, week_end = service.get_current_week()
    
    filters = CurrentBalancesFilter(search=driver_name)
    rows, total = await service.get_lease_balances(
        week_start, week_end, 1, 100, filters
    )
    
    if rows:
        return rows[0]  # Return first match
    return None
```

### 3. Weekly Report Generation

Generate automated weekly report:

```python
async def generate_weekly_report(week_start: date):
    service = CurrentBalancesService(db)
    week_end = week_start + timedelta(days=6)
    
    # Get all balances
    rows, _ = await service.get_lease_balances(
        week_start, week_end, 1, 10000, None
    )
    
    # Calculate statistics
    total_earnings = sum(r.cc_earnings for r in rows)
    total_deductions = sum(r.subtotal_deductions for r in rows)
    avg_net = sum(r.net_earnings for r in rows) / len(rows) if rows else 0
    
    return {
        "week": f"{week_start} to {week_end}",
        "total_leases": len(rows),
        "total_earnings": total_earnings,
        "total_deductions": total_deductions,
        "average_net": avg_net,
        "top_earners": sorted(rows, key=lambda x: x.net_earnings, reverse=True)[:10]
    }
```

## Troubleshooting Tips

### Problem: Slow Response Times

**Solution:**
```python
# Add database indexes
CREATE INDEX idx_curb_lease_date ON curb_transactions(lease_id, trip_date);
CREATE INDEX idx_ledger_balance_lease ON ledger_balances(lease_id, status, category);

# Use pagination
response = await service.get_lease_balances(
    week_start, week_end,
    page=1, per_page=25  # Don't fetch all at once
)
```

### Problem: Missing Data in Response

**Debug:**
```python
# Check if lease has driver
assert lease.primary_driver is not None

# Check if CURB data exists
curb_count = db.query(CurbTransaction).filter(
    CurbTransaction.lease_id == lease.id
).count()
print(f"CURB transactions: {curb_count}")

# Check ledger balances
balance_count = db.query(LedgerBalance).filter(
    LedgerBalance.lease_id == lease.id,
    LedgerBalance.status == BalanceStatus.OPEN
).count()
print(f"Open balances: {balance_count}")
```

### Problem: Wrong Net Earnings

**Debug:**
```python
# Manual calculation
lease_id = "L-1045"
week_start = date(2025, 11, 3)
week_end = date(2025, 11, 9)

print("Manual Calculation:")
print(f"CC Earnings: {service._get_curb_earnings(lease.id, week_start, week_end)}")
print(f"Lease Fee: {service._get_weekly_lease_fee(lease)}")
print(f"MTA/TIF: {service._get_mta_tif_charges(lease.id, week_start, week_end)}")
print(f"EZPass: {service._get_ezpass_outstanding(lease.id, medallion.id, week_end)}")
# ... etc
```

## Next Steps

1. ✅ Set up the feature (see 5-Minute Setup above)
2. 📖 Read the full [README.md](README.md) documentation
3. 🧪 Run the test suite
4. 🚀 Deploy to production
5. 📊 Monitor performance and usage

## Need Help?

- **Documentation**: See [README.md](README.md)
- **API Docs**: Visit `/docs` endpoint
- **Issues**: Open a ticket or contact dev team
- **Examples**: More examples in `app/docs/examples.md`

---

**Ready to go! 🚀**