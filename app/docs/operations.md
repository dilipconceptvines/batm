# 🔌 Ledger System - UI to API Endpoints Mapping

## Table of Contents
1. [Create Manual Ledger Entry](#1-create-manual-ledger-entry)
2. [View/Search Ledger Postings](#2-viewsearch-ledger-postings)
3. [View Driver Balance Summary](#3-view-driver-balance-summary)
4. [Apply Payment - Hierarchy Mode](#4-apply-payment---hierarchy-mode)
5. [Apply Payment - Targeted Mode](#5-apply-payment---targeted-mode)
6. [Void Posting](#6-void-posting)
7. [View Balance Details](#7-view-balance-details)
8. [View Posting Details](#8-view-posting-details)
9. [Common API Patterns](#9-common-api-patterns)
10. [Error Handling](#10-error-handling)

---

## 1. Create Manual Ledger Entry

### Screen Actions → API Calls

#### 1.1 Page Load / Initialize Form

**Action**: User navigates to "Create Manual Entry" page

**API Calls**:

```http
# Get current user info (for audit)
GET /users/me
Authorization: Bearer {token}

Response 200:
{
  "id": 1,
  "name": "John Admin",
  "email": "admin@bat.com",
  "role": "FINANCE_MANAGER"
}
```

#### 1.2 Search Driver (Autocomplete)

**Action**: User types in driver search field

**API Call**:

```http
GET /drivers?search={query}&limit=10&status=active
Authorization: Bearer {token}

Example: GET /drivers?search=john&limit=10&status=active

Response 200:
{
  "data": [
    {
      "id": 123,
      "driver_id": "D-12345",
      "first_name": "John",
      "last_name": "Smith",
      "license_number": "D1234567",
      "status": "ACTIVE"
    },
    {
      "id": 124,
      "driver_id": "D-12346",
      "first_name": "Johnny",
      "last_name": "Doe",
      "license_number": "D7654321",
      "status": "ACTIVE"
    }
  ],
  "total": 2
}
```

#### 1.3 Load Driver's Active Leases

**Action**: User selects a driver

**API Call**:

```http
GET /leases?driver_id={driver_id}&status=ACTIVE
Authorization: Bearer {token}

Example: GET /leases?driver_id=123&status=ACTIVE

Response 200:
{
  "data": [
    {
      "id": 456,
      "lease_number": "L-456",
      "driver_id": 123,
      "vehicle_id": 789,
      "medallion_id": 101,
      "status": "ACTIVE",
      "start_date": "2025-01-01",
      "vehicle": {
        "plate_number": "Y234",
        "make": "Toyota",
        "model": "Camry"
      },
      "medallion": {
        "number": "1Y23"
      }
    }
  ],
  "total": 1
}
```

#### 1.4 Get Payment Period (Current/Next Week)

**Action**: User clicks "Use Current Week" or "Use Next Week"

**API Call** (optional helper):

```http
GET /ledger/payment-periods/current
Authorization: Bearer {token}

Response 200:
{
  "period_start": "2025-10-26T00:00:00Z",  # Sunday
  "period_end": "2025-11-01T23:59:59Z",    # Saturday
  "week_number": 43,
  "year": 2025
}

# Or for next week:
GET /ledger/payment-periods/next

Response 200:
{
  "period_start": "2025-11-02T00:00:00Z",
  "period_end": "2025-11-08T23:59:59Z",
  "week_number": 44,
  "year": 2025
}
```

**Note**: This can also be calculated client-side.

#### 1.5 Validate Source ID Uniqueness (Optional - Real-time)

**Action**: User enters source ID, on blur

**API Call**:

```http
GET /ledger/postings/exists?source_type={type}&source_id={id}
Authorization: Bearer {token}

Example: GET /ledger/postings/exists?source_type=MANUAL_ENTRY&source_id=MANUAL-2025-00123

Response 200:
{
  "exists": false
}

# OR if duplicate:
Response 200:
{
  "exists": true,
  "posting_id": "LP-2025-000120",
  "created_at": "2025-10-26T10:15:33Z"
}
```

#### 1.6 Create Posting (Submit Form)

**Action**: User clicks "Create" button

**API Call - For DEBIT (Obligation)**:

```http
POST /ledger/obligations
Authorization: Bearer {token}
Content-Type: application/json

Request Body:
{
  "driver_id": 123,
  "lease_id": 456,
  "category": "EZPASS",
  "original_amount": 25.50,
  "reference_type": "MANUAL_ENTRY",
  "reference_id": "MANUAL-2025-00123",
  "payment_period_start": "2025-10-26T00:00:00Z",
  "payment_period_end": "2025-11-01T23:59:59Z",
  "due_date": "2025-11-01T23:59:59Z",
  "description": "Manual EZPass entry - GWB toll",
  "notes": "Entered by staff - driver reported missing charge"
}

Response 201:
{
  "posting": {
    "id": 1,
    "posting_id": "LP-2025-000123",
    "driver_id": 123,
    "lease_id": 456,
    "posting_type": "DEBIT",
    "category": "EZPASS",
    "amount": "25.50",
    "status": "POSTED",
    "created_at": "2025-10-26T14:35:22Z",
    "created_by": 1
  },
  "balance": {
    "id": 1,
    "balance_id": "LB-2025-000456",
    "driver_id": 123,
    "lease_id": 456,
    "category": "EZPASS",
    "original_amount": "25.50",
    "outstanding_balance": "25.50",
    "status": "OPEN",
    "created_at": "2025-10-26T14:35:22Z"
  }
}
```

**API Call - For CREDIT (Payment)**:

```http
POST /ledger/postings
Authorization: Bearer {token}
Content-Type: application/json

Request Body:
{
  "driver_id": 123,
  "lease_id": 456,
  "posting_type": "CREDIT",
  "category": "EARNINGS",
  "amount": 500.00,
  "source_type": "MANUAL_PAYMENT",
  "source_id": "MANUAL-PAY-2025-00123",
  "payment_period_start": "2025-10-26T00:00:00Z",
  "payment_period_end": "2025-11-01T23:59:59Z",
  "description": "Manual payment entry - driver cash payment"
}

Response 201:
{
  "id": 2,
  "posting_id": "LP-2025-000124",
  "driver_id": 123,
  "lease_id": 456,
  "posting_type": "CREDIT",
  "category": "EARNINGS",
  "amount": "500.00",
  "status": "POSTED",
  "created_at": "2025-10-26T14:36:15Z"
}
```

#### Error Responses:

```http
# Validation Error
Response 400:
{
  "error_code": "VALIDATION_ERROR",
  "message": "Validation failed",
  "details": {
    "amount": ["Amount must be greater than 0"],
    "payment_period_start": ["Payment period must start on Sunday"]
  },
  "timestamp": "2025-10-26T14:35:22Z"
}

# Duplicate Entry
Response 409:
{
  "error_code": "DUPLICATE_POSTING",
  "message": "Duplicate posting: MANUAL_ENTRY:MANUAL-2025-00123 already exists in ledger",
  "details": {
    "existing_posting_id": "LP-2025-000120",
    "created_at": "2025-10-26T10:15:33Z"
  },
  "timestamp": "2025-10-26T14:35:22Z"
}

# Driver Not Found
Response 404:
{
  "error_code": "DRIVER_NOT_FOUND",
  "message": "Driver not found: 123",
  "timestamp": "2025-10-26T14:35:22Z"
}

# Lease Not Active
Response 400:
{
  "error_code": "LEASE_NOT_ACTIVE",
  "message": "Lease is not active: 456. Cannot post transactions to inactive lease.",
  "timestamp": "2025-10-26T14:35:22Z"
}
```

---

## 2. View/Search Ledger Postings

### Screen Actions → API Calls

#### 2.1 Initial Page Load

**Action**: User navigates to ledger postings page

**API Call**:

```http
GET /ledger/postings?limit=50&offset=0
Authorization: Bearer {token}

Response 200:
{
  "data": [
    {
      "id": 1,
      "posting_id": "LP-2025-000001",
      "driver_id": 123,
      "driver_name": "John Smith",
      "lease_id": 456,
      "lease_number": "L-456",
      "posting_type": "DEBIT",
      "category": "EZPASS",
      "amount": "45.00",
      "status": "POSTED",
      "payment_period_start": "2025-10-26T00:00:00Z",
      "created_at": "2025-10-26T10:15:33Z"
    },
    // ... more postings
  ],
  "total": 234,
  "limit": 50,
  "offset": 0,
  "page": 1,
  "total_pages": 5
}
```

#### 2.2 Apply Filters

**Action**: User applies search filters and clicks "Apply Filters"

**API Call**:

```http
GET /ledger/postings?driver_id=123&category=EZPASS&status=POSTED&period_start=2025-10-01T00:00:00Z&period_end=2025-10-31T23:59:59Z&limit=50&offset=0
Authorization: Bearer {token}

Response 200:
{
  "data": [...],
  "total": 45,
  "filters_applied": {
    "driver_id": 123,
    "category": "EZPASS",
    "status": "POSTED",
    "period_start": "2025-10-01T00:00:00Z",
    "period_end": "2025-10-31T23:59:59Z"
  }
}
```

**Available Query Parameters**:
- `driver_id` - Filter by driver
- `lease_id` - Filter by lease
- `category` - Filter by category (TAXES, EZPASS, LEASE, etc.)
- `status` - Filter by status (POSTED, PENDING, VOIDED)
- `posting_type` - Filter by type (DEBIT, CREDIT)
- `period_start` - Payment period start date
- `period_end` - Payment period end date
- `search` - Quick search (posting_id, driver name, amount)
- `limit` - Results per page (default: 50, max: 100)
- `offset` - Pagination offset
- `sort_by` - Sort field (default: created_at)
- `sort_order` - Sort direction (asc/desc, default: desc)

#### 2.3 View Posting Details

**Action**: User clicks on posting row or "View Details" action

**API Call**:

```http
GET /ledger/postings/{posting_id}
Authorization: Bearer {token}

Example: GET /ledger/postings/LP-2025-000001

Response 200:
{
  "id": 1,
  "posting_id": "LP-2025-000001",
  "driver_id": 123,
  "lease_id": 456,
  "vehicle_id": 789,
  "medallion_id": 101,
  "posting_type": "DEBIT",
  "category": "EZPASS",
  "amount": "45.00",
  "source_type": "EZPASS_TRANSACTION",
  "source_id": "EZP-20251026-001",
  "payment_period_start": "2025-10-26T00:00:00Z",
  "payment_period_end": "2025-11-01T23:59:59Z",
  "status": "POSTED",
  "posted_at": "2025-10-26T10:15:33Z",
  "posted_by": 1,
  "voided_by_posting_id": null,
  "voided_at": null,
  "void_reason": null,
  "description": "GWB toll charge",
  "notes": "Imported from EZPass CSV",
  "created_at": "2025-10-26T10:15:33Z",
  "created_by": 1,
  "modified_at": "2025-10-26T10:15:33Z",
  "modified_by": null,
  "driver": {
    "id": 123,
    "driver_id": "D-12345",
    "first_name": "John",
    "last_name": "Smith"
  },
  "lease": {
    "id": 456,
    "lease_number": "L-456"
  }
}
```

#### 2.4 Export Postings

**Action**: User clicks "Export" and selects format

**API Calls**:

```http
# Excel Export
POST /ledger/postings/export
Authorization: Bearer {token}
Content-Type: application/json

Request Body:
{
  "format": "xlsx",
  "filters": {
    "driver_id": 123,
    "category": "EZPASS",
    "period_start": "2025-10-01T00:00:00Z",
    "period_end": "2025-10-31T23:59:59Z"
  }
}

Response 200:
{
  "download_url": "https://s3.amazonaws.com/bat-exports/postings-2025-10-26.xlsx",
  "expires_at": "2025-10-26T15:35:22Z",
  "file_size": 245678,
  "record_count": 234
}

# CSV Export
POST /ledger/postings/export
Content-Type: application/json

Request Body:
{
  "format": "csv",
  "filters": {...}
}

Response 200:
{
  "download_url": "https://s3.amazonaws.com/bat-exports/postings-2025-10-26.csv",
  "expires_at": "2025-10-26T15:35:22Z"
}

# PDF Report Export
POST /ledger/postings/export
Content-Type: application/json

Request Body:
{
  "format": "pdf",
  "filters": {...},
  "include_summary": true
}

Response 200:
{
  "download_url": "https://s3.amazonaws.com/bat-exports/postings-report-2025-10-26.pdf",
  "expires_at": "2025-10-26T15:35:22Z"
}
```

#### 2.5 Pagination

**Action**: User clicks page numbers

**API Call**:

```http
# Page 2
GET /ledger/postings?limit=50&offset=50
Authorization: Bearer {token}

# Page 3
GET /ledger/postings?limit=50&offset=100
Authorization: Bearer {token}

# Or using page parameter (if supported)
GET /ledger/postings?page=2&per_page=50
Authorization: Bearer {token}
```

---

## 3. View Driver Balance Summary

### Screen Actions → API Calls

#### 3.1 Load Driver's Leases (on driver selection)

**Action**: User selects driver from dropdown

**API Call**:

```http
GET /leases?driver_id={driver_id}&status=ACTIVE
Authorization: Bearer {token}

Example: GET /leases?driver_id=123&status=ACTIVE

Response 200:
{
  "data": [
    {
      "id": 456,
      "lease_number": "L-456",
      "status": "ACTIVE",
      "vehicle": {...},
      "medallion": {...}
    }
  ]
}
```

#### 3.2 Get Balance Summary

**Action**: User clicks "Load Balance" or selects lease

**API Call**:

```http
GET /ledger/balances/driver/{driver_id}/lease/{lease_id}
Authorization: Bearer {token}

Example: GET /ledger/balances/driver/123/lease/456

Response 200:
{
  "driver_id": 123,
  "lease_id": 456,
  "total_outstanding": "1245.50",
  "by_category": [
    {
      "category": "TAXES",
      "total_obligations": "150.00",
      "total_paid": "50.00",
      "outstanding_balance": "100.00",
      "open_balance_count": 2
    },
    {
      "category": "EZPASS",
      "total_obligations": "245.00",
      "total_paid": "200.00",
      "outstanding_balance": "45.00",
      "open_balance_count": 3
    },
    {
      "category": "LEASE",
      "total_obligations": "800.00",
      "total_paid": "400.00",
      "outstanding_balance": "400.00",
      "open_balance_count": 1
    },
    {
      "category": "PVB",
      "total_obligations": "230.00",
      "total_paid": "115.00",
      "outstanding_balance": "115.00",
      "open_balance_count": 2
    },
    {
      "category": "TLC",
      "total_obligations": "0.00",
      "total_paid": "0.00",
      "outstanding_balance": "0.00",
      "open_balance_count": 0
    },
    {
      "category": "REPAIRS",
      "total_obligations": "750.00",
      "total_paid": "250.00",
      "outstanding_balance": "500.00",
      "open_balance_count": 3
    },
    {
      "category": "LOANS",
      "total_obligations": "200.00",
      "total_paid": "115.00",
      "outstanding_balance": "85.00",
      "open_balance_count": 1
    },
    {
      "category": "MISC",
      "total_obligations": "0.00",
      "total_paid": "0.00",
      "outstanding_balance": "0.00",
      "open_balance_count": 0
    }
  ],
  "generated_at": "2025-10-26T14:35:22Z"
}
```

#### 3.3 Get Category Details (Drill-down)

**Action**: User clicks 🔍 icon on specific category

**API Call**:

```http
GET /ledger/balances?driver_id={driver_id}&lease_id={lease_id}&category={category}&status=OPEN
Authorization: Bearer {token}

Example: GET /ledger/balances?driver_id=123&lease_id=456&category=EZPASS&status=OPEN

Response 200:
{
  "data": [
    {
      "id": 1,
      "balance_id": "LB-2025-000100",
      "category": "EZPASS",
      "reference_type": "EZPASS_TRANSACTION",
      "reference_id": "EZP-10/20/25",
      "original_amount": "15.00",
      "outstanding_balance": "15.00",
      "due_date": "2025-10-27T23:59:59Z",
      "status": "OPEN"
    },
    {
      "id": 2,
      "balance_id": "LB-2025-000101",
      "category": "EZPASS",
      "reference_type": "EZPASS_TRANSACTION",
      "reference_id": "EZP-10/22/25",
      "original_amount": "12.00",
      "outstanding_balance": "12.00",
      "due_date": "2025-10-29T23:59:59Z",
      "status": "OPEN"
    },
    {
      "id": 3,
      "balance_id": "LB-2025-000102",
      "category": "EZPASS",
      "reference_type": "EZPASS_TRANSACTION",
      "reference_id": "EZP-10/26/25",
      "original_amount": "18.00",
      "outstanding_balance": "18.00",
      "due_date": "2025-11-01T23:59:59Z",
      "status": "OPEN"
    }
  ],
  "total": 3,
  "summary": {
    "total_outstanding": "45.00"
  }
}
```

#### 3.4 Get Recent Activity

**Action**: Page load or refresh

**API Call**:

```http
GET /ledger/postings?driver_id={driver_id}&lease_id={lease_id}&limit=10&sort_by=created_at&sort_order=desc
Authorization: Bearer {token}

Example: GET /ledger/postings?driver_id=123&lease_id=456&limit=10

Response 200:
{
  "data": [
    {
      "posting_id": "LP-2025-000456",
      "posting_type": "DEBIT",
      "category": "EZPASS",
      "amount": "45.00",
      "created_at": "2025-10-26T14:35:22Z",
      "description": "GWB toll"
    },
    {
      "posting_id": "LP-2025-000455",
      "posting_type": "CREDIT",
      "category": "EARNINGS",
      "amount": "500.00",
      "created_at": "2025-10-25T10:15:33Z",
      "description": "Weekly DTR allocation"
    }
    // ... more recent postings
  ]
}
```

#### 3.5 Export Balance Report

**Action**: User clicks "Export Report"

**API Call**:

```http
POST /ledger/balances/export
Authorization: Bearer {token}
Content-Type: application/json

Request Body:
{
  "driver_id": 123,
  "lease_id": 456,
  "format": "pdf",
  "include_details": true
}

Response 200:
{
  "download_url": "https://s3.amazonaws.com/bat-exports/balance-report-D-12345-2025-10-26.pdf",
  "expires_at": "2025-10-26T15:35:22Z"
}
```

#### 3.6 Email Balance Report

**Action**: User clicks email icon

**API Call**:

```http
POST /ledger/balances/email
Authorization: Bearer {token}
Content-Type: application/json

Request Body:
{
  "driver_id": 123,
  "lease_id": 456,
  "recipient_email": "john.smith@email.com",
  "include_details": true
}

Response 200:
{
  "success": true,
  "message": "Balance report sent to john.smith@email.com",
  "email_id": "email-12345"
}
```

---

## 4. Apply Payment - Hierarchy Mode

### Screen Actions → API Calls

#### 4.1 Load Driver/Lease Options

Same as Section 3.1

#### 4.2 Get Current Balance (for display)

Same as Section 3.2

#### 4.3 Calculate Allocation Preview

**Action**: User enters payment amount and clicks "Calculate Allocation Preview"

**API Call**:

```http
POST /ledger/payments/preview-hierarchy
Authorization: Bearer {token}
Content-Type: application/json

Request Body:
{
  "driver_id": 123,
  "lease_id": 456,
  "payment_amount": 500.00,
  "payment_period_start": "2025-10-26T00:00:00Z",
  "payment_period_end": "2025-11-01T23:59:59Z"
}

Response 200:
{
  "total_payment": "500.00",
  "total_allocated": "500.00",
  "remaining_unallocated": "0.00",
  "allocation_by_category": [
    {
      "priority": 1,
      "category": "TAXES",
      "outstanding_before": "100.00",
      "will_be_paid": "100.00",
      "remaining_after": "0.00",
      "status": "FULLY_PAID"
    },
    {
      "priority": 2,
      "category": "EZPASS",
      "outstanding_before": "45.00",
      "will_be_paid": "45.00",
      "remaining_after": "0.00",
      "status": "FULLY_PAID"
    },
    {
      "priority": 3,
      "category": "LEASE",
      "outstanding_before": "400.00",
      "will_be_paid": "355.00",
      "remaining_after": "45.00",
      "status": "PARTIALLY_PAID"
    },
    {
      "priority": 4,
      "category": "PVB",
      "outstanding_before": "115.00",
      "will_be_paid": "0.00",
      "remaining_after": "115.00",
      "status": "NOT_PAID"
    }
    // ... other categories
  ],
  "detailed_allocations": [
    {
      "balance_id": "LB-2025-000090",
      "category": "TAXES",
      "due_date": "2025-10-27T23:59:59Z",
      "amount": "50.00",
      "paying": "50.00",
      "remaining": "0.00",
      "will_close": true
    },
    {
      "balance_id": "LB-2025-000091",
      "category": "TAXES",
      "due_date": "2025-10-29T23:59:59Z",
      "amount": "50.00",
      "paying": "50.00",
      "remaining": "0.00",
      "will_close": true
    }
    // ... more balances
  ],
  "summary": {
    "balances_affected": 6,
    "balances_fully_closed": 5,
    "balances_partially_paid": 1
  }
}
```

#### 4.4 Apply Payment

**Action**: User reviews preview and clicks "Apply Payment"

**API Call**:

```http
POST /ledger/payments/apply-hierarchy
Authorization: Bearer {token}
Content-Type: application/json

Request Body:
{
  "driver_id": 123,
  "lease_id": 456,
  "payment_amount": 500.00,
  "payment_period_start": "2025-10-26T00:00:00Z",
  "payment_period_end": "2025-11-01T23:59:59Z",
  "source_type": "DTR_WEEKLY_ALLOCATION",
  "source_id": "DTR-2025-WEEK43",
  "allocation_type": "DTR_ALLOCATION",
  "notes": "Weekly DTR payment allocation"
}

Response 201:
{
  "payment_posting": {
    "posting_id": "LP-2025-000456",
    "posting_type": "CREDIT",
    "amount": "500.00",
    "status": "POSTED",
    "created_at": "2025-10-26T14:35:22Z"
  },
  "total_payment": "500.00",
  "total_allocated": "500.00",
  "remaining_unallocated": "0.00",
  "allocations": [
    {
      "id": 1,
      "allocation_id": "PA-2025-000001",
      "balance_id": "LB-2025-000090",
      "payment_posting_id": "LP-2025-000456",
      "amount_allocated": "50.00",
      "allocation_type": "DTR_ALLOCATION",
      "allocation_date": "2025-10-26T14:35:22Z"
    }
    // ... more allocations
  ],
  "balances_updated": [
    {
      "balance_id": "LB-2025-000090",
      "previous_outstanding": "50.00",
      "payment_applied": "50.00",
      "new_outstanding": "0.00",
      "status": "CLOSED"
    }
    // ... more updated balances
  ]
}
```

---

## 5. Apply Payment - Targeted Mode

### Screen Actions → API Calls

#### 5.1 Search Balance by ID

**Action**: User enters balance ID and clicks search

**API Call**:

```http
GET /ledger/balances/{balance_id}
Authorization: Bearer {token}

Example: GET /ledger/balances/LB-2025-000123

Response 200:
{
  "id": 1,
  "balance_id": "LB-2025-000123",
  "driver_id": 123,
  "lease_id": 456,
  "category": "PVB",
  "reference_type": "PVB_VIOLATION",
  "reference_id": "PVB-SUMMONS-789456",
  "original_amount": "115.00",
  "prior_balance": "0.00",
  "current_amount": "115.00",
  "payment_applied": "0.00",
  "outstanding_balance": "115.00",
  "due_date": "2025-10-27T23:59:59Z",
  "status": "OPEN",
  "description": "Parking violation - No standing zone",
  "driver": {
    "id": 123,
    "driver_id": "D-12345",
    "first_name": "John",
    "last_name": "Smith"
  },
  "lease": {
    "id": 456,
    "lease_number": "L-456"
  }
}

# If not found:
Response 404:
{
  "error_code": "BALANCE_NOT_FOUND",
  "message": "Balance not found: LB-2025-000123",
  "timestamp": "2025-10-26T14:35:22Z"
}
```

#### 5.2 Search Balances by Driver + Category

**Action**: User selects driver/category and clicks "Search Open Balances"

**API Call**:

```http
GET /ledger/balances?driver_id={driver_id}&category={category}&status=OPEN
Authorization: Bearer {token}

Example: GET /ledger/balances?driver_id=123&category=PVB&status=OPEN

Response 200:
{
  "data": [
    {
      "balance_id": "LB-2025-000123",
      "reference_id": "PVB-SUMMONS-789456",
      "original_amount": "115.00",
      "outstanding_balance": "115.00",
      "due_date": "2025-10-27T23:59:59Z",
      "description": "Parking violation - No standing zone"
    },
    {
      "balance_id": "LB-2025-000456",
      "reference_id": "PVB-SUMMONS-789789",
      "original_amount": "65.00",
      "outstanding_balance": "65.00",
      "due_date": "2025-10-29T23:59:59Z",
      "description": "Parking violation - Fire hydrant"
    }
  ],
  "total": 2
}
```

#### 5.3 Apply Targeted Payment

**Action**: User confirms and clicks "Apply Payment"

**API Call**:

```http
POST /ledger/payments/apply
Authorization: Bearer {token}
Content-Type: application/json

Request Body:
{
  "balance_id": "LB-2025-000123",
  "payment_amount": 115.00,
  "payment_posting": {
    "driver_id": 123,
    "lease_id": 456,
    "source_type": "INTERIM_PAYMENT_CASH",
    "source_id": "CASH-20251026-001",
    "payment_period_start": "2025-10-26T00:00:00Z",
    "payment_period_end": "2025-11-01T23:59:59Z",
    "description": "Driver interim payment - PVB violation"
  },
  "allocation_type": "INTERIM_PAYMENT",
  "notes": "Cash payment received at office. Receipt #RCPT-12345 issued"
}

Response 201:
{
  "payment_posting": {
    "posting_id": "LP-2025-000789",
    "posting_type": "CREDIT",
    "amount": "115.00",
    "status": "POSTED",
    "created_at": "2025-10-26T14:35:22Z"
  },
  "allocation": {
    "allocation_id": "PA-2025-000050",
    "balance_id": "LB-2025-000123",
    "payment_posting_id": "LP-2025-000789",
    "amount_allocated": "115.00",
    "allocation_type": "INTERIM_PAYMENT",
    "allocation_date": "2025-10-26T14:35:22Z"
  },
  "balance": {
    "balance_id": "LB-2025-000123",
    "previous_outstanding": "115.00",
    "payment_applied": "115.00",
    "new_outstanding": "0.00",
    "status": "CLOSED"
  }
}
```

---

## 6. Void Posting

### Screen Actions → API Calls

#### 6.1 Search Posting to Void

**Action**: User enters posting ID and clicks search

**API Call**:

```http
GET /ledger/postings/{posting_id}
Authorization: Bearer {token}

Example: GET /ledger/postings/LP-2025-000456

Response 200:
{
  "id": 1,
  "posting_id": "LP-2025-000456",
  "posting_type": "DEBIT",
  "category": "EZPASS",
  "amount": "25.00",
  "status": "POSTED",
  "driver_id": 123,
  "lease_id": 456,
  "source_type": "EZPASS_TRANSACTION",
  "source_id": "EZP-20251024-001",
  "description": "GWB toll",
  "created_at": "2025-10-24T10:15:33Z",
  "created_by": 1,
  "voided_by_posting_id": null,
  "related_balance": {
    "balance_id": "LB-2025-000789",
    "status": "OPEN",
    "outstanding_balance": "25.00"
  },
  "can_void": true,
  "void_restrictions": []
}

# If already voided:
Response 200:
{
  ...
  "status": "VOIDED",
  "voided_at": "2025-10-24T14:22:10Z",
  "voided_by": 1,
  "void_reason": "Incorrect amount",
  "voided_by_posting_id": "LP-2025-000457",
  "can_void": false,
  "void_restrictions": ["Already voided"]
}
```

#### 6.2 Void Posting

**Action**: User enters reason and clicks "VOID"

**API Call**:

```http
POST /ledger/postings/void
Authorization: Bearer {token}
Content-Type: application/json

Request Body:
{
  "posting_id": "LP-2025-000456",
  "reason": "Incorrect amount - should be $35.00 not $25.00. Staff entered wrong toll charge from EZPass CSV."
}

Response 200:
{
  "success": true,
  "message": "Posting LP-2025-000456 voided successfully",
  "original_posting": {
    "posting_id": "LP-2025-000456",
    "status": "VOIDED",
    "voided_at": "2025-10-26T14:35:22Z",
    "voided_by": 1,
    "void_reason": "Incorrect amount - should be $35.00 not $25.00..."
  },
  "reversal_posting": {
    "posting_id": "LP-2025-000999",
    "posting_type": "CREDIT",
    "amount": "25.00",
    "status": "POSTED",
    "created_at": "2025-10-26T14:35:22Z",
    "description": "Reversal of LP-2025-000456: Incorrect amount..."
  }
}

# Error - Cannot void:
Response 409:
{
  "error_code": "POSTING_ALREADY_VOIDED",
  "message": "Posting already voided: LP-2025-000456",
  "timestamp": "2025-10-26T14:35:22Z"
}
```

---

## 7. View Balance Details

### Screen Actions → API Calls

#### 7.1 Load Balance Details

**Action**: User navigates to balance details page

**API Call**:

```http
GET /ledger/balances/{balance_id}
Authorization: Bearer {token}

Example: GET /ledger/balances/LB-2025-000123

Response 200:
{
  "id": 1,
  "balance_id": "LB-2025-000123",
  "driver_id": 123,
  "lease_id": 456,
  "category": "PVB",
  "reference_type": "PVB_VIOLATION",
  "reference_id": "PVB-SUMMONS-789456",
  "original_amount": "115.00",
  "prior_balance": "0.00",
  "current_amount": "115.00",
  "payment_applied": "0.00",
  "outstanding_balance": "115.00",
  "payment_period_start": "2025-10-26T00:00:00Z",
  "payment_period_end": "2025-11-01T23:59:59Z",
  "due_date": "2025-10-27T23:59:59Z",
  "status": "OPEN",
  "description": "Parking violation - No standing zone",
  "payment_reference": "[]",
  "created_at": "2025-10-25T09:15:22Z",
  "created_by": 1,
  "modified_at": "2025-10-25T09:15:22Z",
  "modified_by": null,
  "driver": {
    "id": 123,
    "driver_id": "D-12345",
    "first_name": "John",
    "last_name": "Smith",
    "email": "john.smith@email.com"
  },
  "lease": {
    "id": 456,
    "lease_number": "L-456",
    "status": "ACTIVE",
    "vehicle": {
      "id": 789,
      "plate_number": "Y234",
      "make": "Toyota",
      "model": "Camry"
    },
    "medallion": {
      "id": 101,
      "number": "1Y23"
    }
  }
}
```

#### 7.2 Get Payment History

**Action**: Page load (included) or separate call if needed

**API Call**:

```http
GET /ledger/allocations?balance_id={balance_id}
Authorization: Bearer {token}

Example: GET /ledger/allocations?balance_id=LB-2025-000123

Response 200:
{
  "data": [
    {
      "allocation_id": "PA-2025-000001",
      "balance_id": "LB-2025-000123",
      "payment_posting_id": "LP-2025-000789",
      "amount_allocated": "50.00",
      "allocation_type": "DTR_ALLOCATION",
      "allocation_date": "2025-10-28T10:15:33Z",
      "notes": "Weekly DTR allocation",
      "balance_after": "65.00",
      "payment_posting": {
        "posting_id": "LP-2025-000789",
        "amount": "500.00",
        "source_type": "DTR_WEEKLY_ALLOCATION"
      }
    },
    {
      "allocation_id": "PA-2025-000002",
      "balance_id": "LB-2025-000123",
      "payment_posting_id": "LP-2025-000890",
      "amount_allocated": "65.00",
      "allocation_type": "INTERIM_PAYMENT",
      "allocation_date": "2025-10-29T14:22:10Z",
      "notes": "Driver cash payment",
      "balance_after": "0.00",
      "payment_posting": {
        "posting_id": "LP-2025-000890",
        "amount": "65.00",
        "source_type": "INTERIM_PAYMENT_CASH"
      }
    }
  ],
  "total": 2,
  "total_allocated": "115.00"
}
```

#### 7.3 Get Related Postings

**Action**: Page load or tab switch

**API Call**:

```http
GET /ledger/postings?balance_id={balance_id}
Authorization: Bearer {token}

# Or get by reference:
GET /ledger/postings?reference_type=PVB_VIOLATION&reference_id=PVB-SUMMONS-789456

Response 200:
{
  "data": [
    {
      "posting_id": "LP-2025-000456",
      "posting_type": "DEBIT",
      "amount": "115.00",
      "status": "POSTED",
      "description": "PVB violation - No standing zone",
      "created_at": "2025-10-25T09:15:22Z"
    }
  ]
}
```

---

## 8. View Posting Details

### Screen Actions → API Calls

#### 8.1 Load Posting Details

Already covered in Section 6.1 - same endpoint

```http
GET /ledger/postings/{posting_id}
```

#### 8.2 View Related Balance

**Action**: User clicks "View Related Balance"

**API Call**:

```http
# Get balance by reference (from posting)
GET /ledger/balances?reference_type={posting.source_type}&reference_id={posting.source_id}
Authorization: Bearer {token}

Example: GET /ledger/balances?reference_type=EZPASS_TRANSACTION&reference_id=EZP-20251024-001

Response 200:
{
  "data": [
    {
      "balance_id": "LB-2025-000789",
      "status": "OPEN",
      "outstanding_balance": "25.00",
      ...
    }
  ]
}
```

#### 8.3 View Audit Trail

**Action**: User clicks "View Audit Trail"

**API Call**:

```http
GET /ledger/postings/{posting_id}/audit
Authorization: Bearer {token}

Example: GET /ledger/postings/LP-2025-000456/audit

Response 200:
{
  "posting_id": "LP-2025-000456",
  "audit_trail": [
    {
      "action": "CREATED",
      "timestamp": "2025-10-24T10:15:33Z",
      "user_id": 1,
      "user_name": "System (EZPass Import)",
      "details": {
        "status": "PENDING"
      }
    },
    {
      "action": "POSTED",
      "timestamp": "2025-10-24T10:15:33Z",
      "user_id": 1,
      "user_name": "System",
      "details": {
        "status": "POSTED"
      }
    },
    {
      "action": "VOIDED",
      "timestamp": "2025-10-24T14:22:10Z",
      "user_id": 5,
      "user_name": "Jane Admin",
      "details": {
        "status": "VOIDED",
        "reason": "Incorrect amount",
        "reversal_posting_id": "LP-2025-000457"
      }
    }
  ]
}
```

---

## 9. Common API Patterns

### 9.1 Authentication

All API calls require authentication via Bearer token:

```http
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

**Get Token** (Login):

```http
POST /auth/login
Content-Type: application/json

Request Body:
{
  "email": "admin@bat.com",
  "password": "password123"
}

Response 200:
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer",
  "expires_in": 3600,
  "user": {
    "id": 1,
    "name": "John Admin",
    "email": "admin@bat.com",
    "role": "FINANCE_MANAGER"
  }
}
```

### 9.2 Pagination Pattern

```http
# Offset-based pagination
GET /ledger/postings?limit=50&offset=0

Response:
{
  "data": [...],
  "total": 234,
  "limit": 50,
  "offset": 0,
  "page": 1,
  "total_pages": 5,
  "has_next": true,
  "has_previous": false,
  "next_url": "/ledger/postings?limit=50&offset=50",
  "previous_url": null
}
```

### 9.3 Error Response Format

All errors follow consistent format:

```http
Response 4xx/5xx:
{
  "error_code": "ERROR_CODE",
  "message": "Human-readable error message",
  "details": {
    "field": ["error message"],
    "additional_info": "..."
  },
  "timestamp": "2025-10-26T14:35:22Z",
  "request_id": "req-12345-67890"
}
```

### 9.4 Rate Limiting

```http
Response Headers:
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 999
X-RateLimit-Reset: 1635264000

# If exceeded:
Response 429:
{
  "error_code": "RATE_LIMIT_EXCEEDED",
  "message": "Rate limit exceeded. Please try again in 60 seconds.",
  "retry_after": 60,
  "timestamp": "2025-10-26T14:35:22Z"
}
```

### 9.5 Health Check

```http
GET /health
Authorization: Bearer {token}

Response 200:
{
  "status": "healthy",
  "services": {
    "database": "up",
    "redis": "up",
    "s3": "up"
  },
  "timestamp": "2025-10-26T14:35:22Z"
}
```

---

## 10. Error Handling

### 10.1 Frontend Error Handling Pattern

```javascript
// Example error handling in frontend
async function createPosting(postingData) {
  try {
    const response = await fetch('/ledger/obligations', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(postingData)
    });

    if (!response.ok) {
      const error = await response.json();
      
      // Handle specific error codes
      switch (error.error_code) {
        case 'DUPLICATE_POSTING':
          showError('This posting already exists', {
            existing_id: error.details.existing_posting_id
          });
          break;
        
        case 'DRIVER_NOT_FOUND':
          showError('Driver not found. Please select a valid driver.');
          break;
        
        case 'VALIDATION_ERROR':
          showValidationErrors(error.details);
          break;
        
        default:
          showError('An error occurred. Please try again.');
      }
      
      return null;
    }

    const result = await response.json();
    return result;
    
  } catch (networkError) {
    showError('Network error. Please check your connection.');
    return null;
  }
}
```

### 10.2 Common Error Codes

| Error Code | HTTP Status | Description | User Action |
|------------|-------------|-------------|-------------|
| `VALIDATION_ERROR` | 400 | Input validation failed | Fix form fields |
| `DUPLICATE_POSTING` | 409 | Posting already exists | Change source ID or view existing |
| `DRIVER_NOT_FOUND` | 404 | Driver doesn't exist | Select valid driver |
| `LEASE_NOT_FOUND` | 404 | Lease doesn't exist | Select valid lease |
| `LEASE_NOT_ACTIVE` | 400 | Lease is inactive | Select active lease |
| `POSTING_NOT_FOUND` | 404 | Posting doesn't exist | Check posting ID |
| `POSTING_ALREADY_VOIDED` | 409 | Already voided | View existing void |
| `BALANCE_NOT_FOUND` | 404 | Balance doesn't exist | Check balance ID |
| `BALANCE_ALREADY_CLOSED` | 409 | Balance is closed | Cannot modify |
| `INSUFFICIENT_BALANCE` | 400 | Payment > outstanding | Reduce payment amount |
| `INVALID_PAYMENT_PERIOD` | 400 | Not Sunday-Saturday | Fix date range |
| `UNAUTHORIZED` | 401 | Not authenticated | Re-login |
| `FORBIDDEN` | 403 | No permission | Contact admin |
| `RATE_LIMIT_EXCEEDED` | 429 | Too many requests | Wait and retry |
| `SERVER_ERROR` | 500 | Server error | Contact support |

### 10.3 Retry Logic

```javascript
// Example retry logic for transient errors
async function apiCallWithRetry(apiCall, maxRetries = 3) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      const response = await apiCall();
      
      // Success
      if (response.ok) {
        return await response.json();
      }
      
      // Don't retry client errors (4xx except 429)
      if (response.status >= 400 && response.status < 500 && response.status !== 429) {
        throw await response.json();
      }
      
      // Retry on 5xx or 429
      if (i < maxRetries - 1) {
        const delay = Math.pow(2, i) * 1000; // Exponential backoff
        await sleep(delay);
        continue;
      }
      
      throw await response.json();
      
    } catch (error) {
      if (i === maxRetries - 1) throw error;
    }
  }
}
```

---

## Summary

This document maps **every UI operation** to its corresponding **API endpoints**, including:

✅ **Request/Response formats** for all operations
✅ **Query parameters** for filtering and pagination
✅ **Error handling** patterns and codes
✅ **Success and error responses** with examples
✅ **Common patterns** (auth, pagination, rate limiting)
✅ **Frontend integration** examples

**Key Points**:
- All endpoints require `Authorization: Bearer {token}`
- Use `GET` for queries, `POST` for creating/actions
- Pagination uses `limit` and `offset` parameters
- Errors follow consistent format with `error_code`
- Preview endpoints available before committing changes
- Export endpoints return download URLs, not direct files

This document provides **complete API integration specification** for frontend developers to build the Ledger UI.