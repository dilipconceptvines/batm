# Interim Payments BPM Flow - Complete Documentation

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Step-by-Step Workflow](#step-by-step-workflow)
4. [JSON Schema Specifications](#json-schema-specifications)
5. [API Endpoint Reference](#api-endpoint-reference)
6. [Implementation Examples](#implementation-examples)
7. [Database Integration](#database-integration)
8. [Error Handling](#error-handling)
9. [Testing Guide](#testing-guide)
10. [Deployment Checklist](#deployment-checklist)

---

## Overview

The Interim Payments workflow allows finance staff to process ad-hoc driver payments and allocate them against outstanding obligations. The workflow is implemented as a two-step BPM process where:

- **Step 210**: Captures driver selection, lease selection, and complete payment details
- **Step 211**: Allocates the payment across outstanding balances

This document provides complete implementation details based on the actual Figma screen designs.

### Key Features

- Per-lease payment processing
- Real-time outstanding balance retrieval
- Flexible allocation across multiple obligation types
- Atomic transaction processing with rollback capability
- Comprehensive audit trail
- Integration with centralized ledger system

---

## Architecture

### System Components

```
┌─────────────────────────────────────────────────────────────┐
│                    COMPONENT ARCHITECTURE                    │
└─────────────────────────────────────────────────────────────┘

Frontend (UI)
    │
    ├── Driver Search Interface
    ├── Lease Selection Interface
    ├── Payment Entry Form
    ├── Balance Allocation Grid
    └── Confirmation Dialogs
    │
    ▼
BPM API Layer (router.py)
    │
    ├── POST /bpm/case (create case)
    ├── GET  /bpm/case/{case_no}/{step_id} (fetch)
    └── POST /bpm/case/{case_no} (process)
    │
    ▼
BPM Flow Layer (flows.py)
    │
    ├── Step 210 - fetch_driver_and_lease_details()
    ├── Step 210 - create_interim_payment_record()
    ├── Step 211 - fetch_outstanding_balances()
    └── Step 211 - process_payment_allocation()
    │
    ▼
Service Layer
    │
    ├── InterimPaymentService
    ├── LedgerService
    ├── DriverService
    ├── LeaseService
    └── AuditTrailService
    │
    ▼
Repository Layer
    │
    ├── InterimPaymentRepository
    ├── LedgerRepository
    ├── DriverRepository
    └── LeaseRepository
    │
    ▼
Database
    │
    ├── interim_payments
    ├── ledger_balances
    ├── ledger_postings
    ├── drivers
    ├── leases
    └── audit_trails
```

### Data Flow

```
User Actions                BPM Flow              Database Operations
─────────────────          ─────────────          ───────────────────

[Create Payment]    ──►    Create Case     ──►    INSERT cases
                                                   (status: In Progress)
        │
        ▼
[Search Driver]     ──►    Fetch Step 210  ──►    SELECT drivers
[View Leases]                                      SELECT leases
        │
        ▼
[Select Lease]      ──►    Process Step    ──►    INSERT interim_payments
[Enter Amount]             210                     (with full payment details)
[Select Method]                                    INSERT case_entities
[Enter Date]                                       INSERT audit_trails
        │
        ▼
[View Balances]     ──►    Fetch Step 211  ──►    SELECT ledger_balances
                                                   (filtered by lease_id)
        │
        ▼
[Allocate Amounts]  ──►    Process Step    ──►    UPDATE interim_payments
[Confirm]                  211                     (add allocations)
                                                   INSERT ledger_postings
                                                   UPDATE ledger_balances
                                                   UPDATE cases (closed)
                                                   INSERT audit_trails
```

---

## Step-by-Step Workflow

### Step 0: Initiation

**Purpose**: Create a new BPM case for interim payment processing.

**User Action**: Click "Create Interim Payment" button from the Interim Payments page.

**API Call**:
```http
POST /bpm/case
Content-Type: application/json
Authorization: Bearer {token}

{
  "case_type": "INTPAY"
}
```

**Response**:
```json
{
  "case_no": "INTPAY000477",
  "created_by": "Meryl",
  "case_created_on": "2024-10-24T10:00:00Z",
  "case_status": "In Progress",
  "steps": [
    {
      "step_name": "Search Driver & Enter Payment Details",
      "sub_steps": [
        {
          "step_name": "Fetch - Search Driver & Enter Payment Details",
          "step_id": "210",
          "operation": "fetch"
        },
        {
          "step_name": "Process - Create Interim Payment Record",
          "step_id": "210",
          "operation": "process"
        }
      ]
    },
    {
      "step_name": "Allocate Payments",
      "sub_steps": [
        {
          "step_name": "Fetch - Allocate Payments",
          "step_id": "211",
          "operation": "fetch"
        },
        {
          "step_name": "Process - Allocate Payments",
          "step_id": "211",
          "operation": "process"
        }
      ]
    }
  ]
}
```

**Database Changes**:
- New record in `cases` table with status "In Progress"
- Case number generated with prefix "INTPAY"

---

### Step 210: Enter Payment Details

This step captures ALL payment information in a single submission.

#### Step 210 - Fetch (Search Driver)

**Purpose**: Search for driver and retrieve active leases.

**User Actions**:
1. Enter TLC License Number in search field
2. Click "Search" button

**API Call**:
```http
GET /bpm/case/INTPAY000477/210?tlc_license_no=00504124
Authorization: Bearer {token}
```

**Response**:
```json
{
  "driver": {
    "id": 101,
    "driver_id": "DRV-101",
    "full_name": "John Doe",
    "status": "Active",
    "tlc_license": "00504124",
    "phone": "(212) 245-8000",
    "email": "joedoe@fsd.com"
  },
  "leases": [
    {
      "id": 2054,
      "lease_id": "LS-2054",
      "medallion_number": "1P43",
      "plate_no": "8SAM401",
      "vin": "4TALWRZV6YW122447",
      "lease_type": "DOV",
      "status": "Active"
    }
  ],
  "selected_interim_payment_id": null
}
```

**UI Display**:
- Driver details shown in top section
- List of active leases displayed with radio button selection
- "Associated Leases" section shows lease details
- Bottom section shows "Enter Payment Details" form

**Database Queries**:
```sql
-- Find driver by TLC license
SELECT * FROM drivers d
JOIN tlc_licenses tl ON d.id = tl.driver_id
WHERE tl.tlc_license_number = '00504124';

-- Find active leases for driver
SELECT l.*, m.medallion_number, v.vin, r.plate_number
FROM leases l
JOIN medallions m ON l.medallion_id = m.id
JOIN vehicles v ON l.vehicle_id = v.id
LEFT JOIN registrations r ON v.id = r.vehicle_id
WHERE l.driver_id = 'DRV-101'
  AND l.lease_status = 'Active'
  AND l.is_additional_driver = 0;
```

#### Step 210 - Process (Submit Payment Details)

**Purpose**: Create interim payment record with complete payment details.

**User Actions**:
1. Select a lease (radio button)
2. Enter Total Payment Amount ($600.00)
3. Select Payment Method from dropdown (Cash/Check/ACH)
4. Select Payment Date from calendar picker
5. Enter optional Notes
6. Click "Proceed to Allocation" button

**API Call**:
```http
POST /bpm/case/INTPAY000477
Content-Type: application/json
Authorization: Bearer {token}

{
  "step_id": "210",
  "data": {
    "driver_id": 101,
    "lease_id": 2054,
    "payment_amount": 600.00,
    "payment_method": "ACH",
    "payment_date": "2025-10-21",
    "notes": "Partial payment for outstanding balances"
  }
}
```

**JSON Schema Validation** (enter_payment_details.json):
- Validates all required fields present
- Validates payment_amount between 0.01 and 999999.99
- Validates payment_method is one of: Cash, Check, ACH
- Validates payment_date format YYYY-MM-DD
- Validates notes length maximum 1000 characters

**Business Logic Validation**:
1. Driver exists in database
2. Driver status is Active
3. Lease exists in database
4. Lease status is Active
5. Driver is primary driver on lease (not additional driver)

**Response**:
```json
{
  "message": "OK",
  "data": {
    "message": "Interim payment entry created successfully.",
    "interim_payment_id": "12345",
    "total_outstanding": 2156.50
  }
}
```

**Database Changes**:
```sql
-- Insert new interim payment with full details
INSERT INTO interim_payments (
  driver_id,
  lease_id,
  payment_date,
  total_amount,
  payment_method,
  notes,
  allocations,
  created_by,
  created_on
) VALUES (
  'DRV-101',
  'LS-2054',
  '2025-10-21',
  600.00,
  'ACH',
  'Partial payment for outstanding balances',
  '[]',
  1,
  NOW()
);

-- Link case to interim payment
INSERT INTO case_entities (
  case_no,
  entity_name,
  identifier,
  identifier_value
) VALUES (
  'INTPAY000477',
  'interim_payment',
  'id',
  '12345'
);

-- Create audit trail
INSERT INTO audit_trails (
  case_no,
  description,
  meta_data,
  created_by,
  created_on
) VALUES (
  'INTPAY000477',
  'Created interim payment of $600.00 (ACH) for driver DRV-101 and lease LS-2054',
  '{...}',
  1,
  NOW()
);
```

**Critical Implementation Details**:
- Payment record created with ALL details (not placeholders)
- `allocations` field initialized as empty array
- `payment_id` NOT generated yet (happens in Step 211)
- Total outstanding calculated for UI display

---

### Step 211: Allocate Payments

This step allocates the payment entered in Step 210 across outstanding balances.

#### Step 211 - Fetch (Get Outstanding Balances)

**Purpose**: Retrieve all outstanding balances for the selected lease.

**User Actions**:
- User automatically navigated to this screen after Step 210
- UI displays payment details from Step 210 at top
- Table shows all outstanding obligations

**API Call**:
```http
GET /bpm/case/INTPAY000477/211
Authorization: Bearer {token}
```

**Response**:
```json
{
  "driver": {
    "driver_id": "DRV-101",
    "driver_name": "John Doe",
    "tlc_license": "00504124"
  },
  "lease": {
    "lease_id": "LS-2054",
    "medallion_no": "1P43"
  },
  "total_outstanding": 2156.50,
  "obligations": [
    {
      "balance_id": 123,
      "category": "LEASE",
      "reference_id": "MED-102-LS-08",
      "description": "LEASE - MED-102-LS-08",
      "outstanding": 265.00,
      "due_date": "2024-10-20"
    },
    {
      "balance_id": 124,
      "category": "REPAIRS",
      "reference_id": "INV-2457",
      "description": "REPAIRS - INV-2457",
      "outstanding": 450.00,
      "due_date": "2024-10-15"
    },
    {
      "balance_id": 125,
      "category": "LOANS",
      "reference_id": "LN-3001",
      "description": "LOANS - LN-3001",
      "outstanding": 92.50,
      "due_date": "2024-10-18"
    },
    {
      "balance_id": 126,
      "category": "EZPASS",
      "reference_id": "EZ-6789",
      "description": "EZPASS - EZ-6789",
      "outstanding": 450.00,
      "due_date": "2024-10-22"
    },
    {
      "balance_id": 127,
      "category": "PVB",
      "reference_id": "PVB-12345",
      "description": "PVB - PVB-12345",
      "outstanding": 92.50,
      "due_date": "2024-10-19"
    },
    {
      "balance_id": 128,
      "category": "MISC",
      "reference_id": "MIS-125890",
      "description": "MISC - MIS-125890",
      "outstanding": 92.50,
      "due_date": "2024-10-17"
    }
  ]
}
```

**UI Display**:
- Top section shows:
  - Total Payment: $600.00 (from Step 210)
  - Allocated: $0.00 (initially)
  - Remaining: $600.00 (initially)
  - Total Outstanding: $2,156.50
- Table with columns:
  - Category
  - Reference ID
  - Description
  - Outstanding
  - Payment Amount (editable input field)
  - Balance (calculated)
  - Due Date

**Database Query**:
```sql
-- CRITICAL: Filter by BOTH driver_id AND lease_id
SELECT 
  lb.id as balance_id,
  lb.category,
  lb.reference_id,
  lb.balance as outstanding,
  lb.created_on as due_date
FROM ledger_balances lb
WHERE lb.driver_id = 'DRV-101'
  AND lb.lease_id = 'LS-2054'
  AND lb.is_closed = 0
  AND lb.balance > 0
ORDER BY lb.created_on ASC;
```

**Critical Implementation Details**:
- Balances filtered by specific lease_id (not all driver balances)
- Only open balances shown (is_closed = 0)
- Only positive balances shown (balance > 0)
- Sorted by creation date (oldest first)

#### Step 211 - Process (Submit Allocation)

**Purpose**: Apply payment allocation to ledger and close case.

**User Actions**:
1. Enter allocation amounts in Payment Amount column
2. UI calculates running totals:
   - Allocated: sum of all payment amounts
   - Remaining: Total Payment - Allocated
   - Balance: Outstanding - Payment Amount (per row)
3. Click "Allocate Payments" button
4. Review confirmation modal showing allocation breakdown
5. Click "Confirm Allocation" button

**API Call**:
```http
POST /bpm/case/INTPAY000477
Content-Type: application/json
Authorization: Bearer {token}

{
  "step_id": "211",
  "data": {
    "allocations": [
      {
        "balance_id": 123,
        "amount": 265.00,
        "category": "LEASE",
        "reference_id": "MED-102-LS-08"
      },
      {
        "balance_id": 124,
        "amount": 335.00,
        "category": "REPAIRS",
        "reference_id": "INV-2457"
      }
    ]
  }
}
```

**JSON Schema Validation** (allocate_payments.json):
- Validates allocations array has at least 1 item
- Validates allocations array has maximum 50 items
- For each allocation:
  - balance_id required (string or integer)
  - amount required (0.01 to 999999.99)
  - category required (enum)
  - reference_id required (1-100 chars)

**Business Logic Validation**:
1. Retrieve interim_payment from case_entity
2. Extract payment_amount from interim_payment (Step 210 data)
3. Calculate total_allocated from allocations array
4. Validate: total_allocated <= payment_amount
5. For each allocation:
   - Verify balance_id exists in ledger_balances
   - Verify balance.driver_id matches selected driver
   - Verify balance.lease_id matches selected lease (CRITICAL)
   - Verify balance is open (is_closed = 0)

**Response**:
```json
{
  "message": "OK",
  "data": {
    "message": "Interim payment successfully created and allocated.",
    "payment_id": "INTPAY-2025-001",
    "driver_name": "John Doe"
  }
}
```

**Database Changes**:
```sql
-- 1. Update interim payment with allocations and generate payment_id
UPDATE interim_payments
SET 
  allocations = '[
    {"category": "LEASE", "reference_id": "MED-102-LS-08", "amount": 265.00},
    {"category": "REPAIRS", "reference_id": "INV-2457", "amount": 335.00}
  ]',
  payment_id = 'INTPAY-2025-001',
  updated_on = NOW()
WHERE id = 12345;

-- 2. Create ledger postings (CREDIT for each allocation)
INSERT INTO ledger_postings (
  driver_id,
  lease_id,
  category,
  reference_id,
  type,
  amount,
  posted_on
) VALUES 
  ('DRV-101', 'LS-2054', 'LEASE', 'MED-102-LS-08', 'CREDIT', 265.00, NOW()),
  ('DRV-101', 'LS-2054', 'REPAIRS', 'INV-2457', 'CREDIT', 335.00, NOW());

-- 3. Update ledger balances
UPDATE ledger_balances
SET 
  balance = balance - 265.00,
  is_closed = CASE WHEN balance - 265.00 <= 0 THEN 1 ELSE 0 END,
  updated_on = NOW()
WHERE id = 123; -- LEASE balance

UPDATE ledger_balances
SET 
  balance = balance - 335.00,
  is_closed = CASE WHEN balance - 335.00 <= 0 THEN 1 ELSE 0 END,
  updated_on = NOW()
WHERE id = 124; -- REPAIRS balance

-- 4. Mark BPM case as closed
UPDATE cases
SET 
  case_status = 'Closed',
  closed_on = NOW()
WHERE case_no = 'INTPAY000477';

-- 5. Create audit trail
INSERT INTO audit_trails (
  case_no,
  description,
  meta_data,
  created_by,
  created_on
) VALUES (
  'INTPAY000477',
  'Completed interim payment INTPAY-2025-001 for $600.00',
  '{
    "payment_id": "INTPAY-2025-001",
    "payment_amount": 600.00,
    "total_allocated": 600.00,
    "allocations_count": 2
  }',
  1,
  NOW()
);
```

**Critical Implementation Details**:
- Payment details retrieved from interim_payment record (Step 210)
- Payment ID generated using service method
- Atomic transaction: all database changes succeed or all rollback
- Ledger postings use CREDIT type (reduces outstanding balance)
- Balances automatically marked as closed when balance reaches zero
- Case marked as closed after successful allocation

---

## JSON Schema Specifications

### Schema 1: enter_payment_details.json (Step 210 - Process)

**File Location**: `app/bpm_flows/interim_payments/schemas/enter_payment_details.json`

**S3 Location**: `s3://{bucket}/json_config/interim_payments/schemas/enter_payment_details.json`

**Complete Schema**:
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Interim Payment - Enter Payment Details",
  "description": "Schema for validating Step 210 (Process) - Driver, Lease, and Payment Details",
  "type": "object",
  "required": [
    "driver_id",
    "lease_id",
    "payment_amount",
    "payment_method",
    "payment_date"
  ],
  "properties": {
    "driver_id": {
      "type": "integer",
      "description": "Primary key of the selected driver from the 'drivers' table",
      "minimum": 1,
      "examples": [101, 205, 387]
    },
    "lease_id": {
      "type": "integer",
      "description": "Primary key of the selected active lease from the 'leases' table",
      "minimum": 1,
      "examples": [2054, 3021, 4156]
    },
    "payment_amount": {
      "type": "number",
      "description": "Total payment amount in dollars",
      "minimum": 0.01,
      "maximum": 999999.99,
      "multipleOf": 0.01,
      "examples": [600.00, 1250.50, 2156.50]
    },
    "payment_method": {
      "type": "string",
      "description": "Method of payment",
      "enum": ["Cash", "Check", "ACH"],
      "examples": ["ACH", "Cash", "Check"]
    },
    "payment_date": {
      "type": "string",
      "description": "Date when payment was received (ISO 8601 format: YYYY-MM-DD)",
      "format": "date",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "examples": ["2025-10-21", "2025-11-04", "2024-12-15"]
    },
    "notes": {
      "type": ["string", "null"],
      "description": "Optional notes about the payment",
      "maxLength": 1000,
      "examples": [
        "Driver paid cash at front desk",
        "Check #12345",
        null
      ]
    }
  },
  "additionalProperties": false
}
```

**Validation Rules**:

| Field | Required | Type | Constraints | Error if Invalid |
|-------|----------|------|-------------|------------------|
| driver_id | Yes | integer | >= 1 | "driver_id is required" |
| lease_id | Yes | integer | >= 1 | "lease_id is required" |
| payment_amount | Yes | number | 0.01 - 999999.99 | "payment_amount must be between 0.01 and 999999.99" |
| payment_method | Yes | string | Must be "Cash", "Check", or "ACH" | "payment_method must be one of: Cash, Check, ACH" |
| payment_date | Yes | string | Format: YYYY-MM-DD | "payment_date must be in YYYY-MM-DD format" |
| notes | No | string or null | Max 1000 characters | "notes cannot exceed 1000 characters" |

### Schema 2: allocate_payments.json (Step 211 - Process)

**File Location**: `app/bpm_flows/interim_payments/schemas/allocate_payments.json`

**S3 Location**: `s3://{bucket}/json_config/interim_payments/schemas/allocate_payments.json`

**Complete Schema**:
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Interim Payment - Allocate Payments",
  "description": "Schema for validating payment allocation submission in Step 211 (Process). Payment details were already captured in Step 210.",
  "type": "object",
  "required": ["allocations"],
  "properties": {
    "allocations": {
      "type": "array",
      "description": "List of balance allocations - how the payment (entered in Step 210) is distributed across outstanding obligations",
      "minItems": 1,
      "maxItems": 50,
      "items": {
        "type": "object",
        "required": ["balance_id", "amount", "category", "reference_id"],
        "properties": {
          "balance_id": {
            "type": ["string", "integer"],
            "description": "The unique ledger balance ID to apply payment to",
            "examples": ["123", 456, "789"]
          },
          "amount": {
            "type": "number",
            "description": "Amount to allocate to this specific balance",
            "minimum": 0.01,
            "maximum": 999999.99,
            "multipleOf": 0.01,
            "examples": [265.00, 335.00, 50.50]
          },
          "category": {
            "type": "string",
            "description": "Category of the obligation being paid",
            "enum": [
              "LEASE",
              "REPAIRS",
              "LOANS",
              "EZPASS",
              "PVB",
              "TLC",
              "TAXES",
              "MISC"
            ],
            "examples": ["LEASE", "REPAIRS", "EZPASS", "PVB"]
          },
          "reference_id": {
            "type": "string",
            "description": "Reference ID of the original obligation (e.g., lease ID, invoice number, loan number)",
            "minLength": 1,
            "maxLength": 100,
            "examples": [
              "MED-102-LS-08",
              "INV-2457",
              "LN-3001",
              "EZ-6789",
              "PVB-12345"
            ]
          }
        },
        "additionalProperties": false
      }
    }
  },
  "additionalProperties": false
}
```

**Validation Rules**:

| Field | Required | Type | Constraints | Error if Invalid |
|-------|----------|------|-------------|------------------|
| allocations | Yes | array | 1-50 items | "allocations must have 1-50 items" |
| allocations[].balance_id | Yes | string or integer | Must exist in database | "balance_id is required" |
| allocations[].amount | Yes | number | 0.01 - 999999.99 | "amount must be between 0.01 and 999999.99" |
| allocations[].category | Yes | string | Must be valid enum value | "category must be one of: LEASE, REPAIRS, etc." |
| allocations[].reference_id | Yes | string | 1-100 characters | "reference_id must be 1-100 characters" |

---

## API Endpoint Reference

### Create BPM Case

**Endpoint**: `POST /bpm/case`

**Purpose**: Initialize a new interim payment workflow.

**Request Headers**:
```
Authorization: Bearer {jwt_token}
Content-Type: application/json
```

**Request Body**:
```json
{
  "case_type": "INTPAY"
}
```

**Success Response** (200 OK):
```json
{
  "case_no": "INTPAY000477",
  "created_by": "Meryl",
  "case_created_on": "2024-10-24T10:00:00Z",
  "case_status": "In Progress",
  "steps": [...]
}
```

**Error Responses**:
- 400 Bad Request: Invalid case_type
- 401 Unauthorized: Missing or invalid token
- 500 Internal Server Error: Database error

### Fetch Driver and Leases (Step 210 - Fetch)

**Endpoint**: `GET /bpm/case/{case_no}/210`

**Purpose**: Search for driver by TLC license and retrieve active leases.

**Request Headers**:
```
Authorization: Bearer {jwt_token}
```

**Query Parameters**:
- `tlc_license_no` (required): TLC License Number (e.g., "00504124")

**Example Request**:
```
GET /bpm/case/INTPAY000477/210?tlc_license_no=00504124
```

**Success Response** (200 OK):
```json
{
  "driver": {
    "id": 101,
    "driver_id": "DRV-101",
    "full_name": "John Doe",
    "status": "Active",
    "tlc_license": "00504124",
    "phone": "(212) 245-8000",
    "email": "joedoe@fsd.com"
  },
  "leases": [
    {
      "id": 2054,
      "lease_id": "LS-2054",
      "medallion_number": "1P43",
      "plate_no": "8SAM401",
      "vin": "4TALWRZV6YW122447",
      "lease_type": "DOV",
      "status": "Active"
    }
  ],
  "selected_interim_payment_id": null
}
```

**Error Responses**:
- 404 Not Found: Driver not found or no active leases
- 401 Unauthorized: Missing or invalid token
- 500 Internal Server Error: Database error

### Create Interim Payment (Step 210 - Process)

**Endpoint**: `POST /bpm/case/{case_no}`

**Purpose**: Create interim payment record with complete payment details.

**Request Headers**:
```
Authorization: Bearer {jwt_token}
Content-Type: application/json
```

**Request Body**:
```json
{
  "step_id": "210",
  "data": {
    "driver_id": 101,
    "lease_id": 2054,
    "payment_amount": 600.00,
    "payment_method": "ACH",
    "payment_date": "2025-10-21",
    "notes": "Partial payment for outstanding balances"
  }
}
```

**Success Response** (200 OK):
```json
{
  "message": "OK",
  "data": {
    "message": "Interim payment entry created successfully.",
    "interim_payment_id": "12345",
    "total_outstanding": 2156.50
  }
}
```

**Error Responses**:
- 400 Bad Request: Validation error (see schema)
- 404 Not Found: Driver or lease not found
- 401 Unauthorized: Missing or invalid token
- 500 Internal Server Error: Database error

### Fetch Outstanding Balances (Step 211 - Fetch)

**Endpoint**: `GET /