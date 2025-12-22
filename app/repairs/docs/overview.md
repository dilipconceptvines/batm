## Comprehensive Documentation: Vehicle Repairs Module

This document provides a complete overview of the new `app/repairs` module, which is designed to manage the full lifecycle of vehicle repair expenses, from invoice creation to automated ledger posting.

### 1. Implementation Documentation

This section details the design, architecture, and functionality of the module.

#### **1.1. Overview**

The Vehicle Repairs module provides a structured workflow for recording repair invoices, creating manageable weekly repayment schedules for drivers, and ensuring these obligations are systematically reconciled through the Centralized Ledger.

The module supports a Business Process Management (BPM) workflow for the manual creation of repair invoices, which then feeds into an automated, schedule-based system for ledger integration.

#### **1.2. The Repair Invoice Lifecycle**

Each repair invoice follows a distinct, status-driven lifecycle, ensuring full auditability and financial accuracy:

1.  **Case Creation:** A user initiates the workflow by clicking "Create Repair Invoice" in the UI. This creates a new BPM case with the prefix "RPRINV".

2.  **Invoice & Schedule Creation (BPM Step):**
    *   The user searches for a driver by their TLC License and selects the associated active lease for the repair.
    *   The user then fills out the invoice details (invoice number, date, total amount, workshop type, etc.) and selects a `Start Week` for the repayment.
    *   Upon submission, the `RepairService` performs several actions in a single transaction:
        a.  Creates a master `RepairInvoice` record with a `DRAFT` status.
        b.  Generates a unique, sequential **Repair ID** (e.g., `RPR-2025-001`).
        c.  Calculates a full weekly installment plan based on the total amount and the predefined `REPAYMENT_MATRIX`, creating multiple `RepairInstallment` records linked to the invoice.
        d.  Updates the `RepairInvoice` status to **`OPEN`**.
        e.  Links the newly created invoice to the BPM case via the `CaseEntity` table.

3.  **Automated Ledger Posting (Celery Task):**
    *   A scheduled Celery task (`repairs.post_due_installments`) runs weekly (e.g., every Sunday at 5 AM).
    *   The task queries for all `RepairInstallment` records that are `SCHEDULED` and whose `week_start_date` has passed.
    *   For each due installment, it calls the `LedgerService.create_obligation` method, creating a `DEBIT` posting with the category `REPAIR`.
    *   The installment's status is then updated to **`POSTED`**.

4.  **Reconciliation:** The driver's weekly earnings are automatically applied to this new `REPAIR` obligation in the ledger according to the established payment hierarchy (after Taxes, Lease, etc.). Once an installment's balance in the ledger is cleared, the `RepairInstallment` status can be updated to `PAID`. The parent `RepairInvoice` is marked `CLOSED` only after all its installments are fully paid.

#### **1.3. File-by-File Architectural Breakdown**

*   **`app/repairs/models.py`**:
    *   `RepairInvoice`: The master record for a single repair, containing total cost, invoice details, and foreign keys to the `Driver`, `Lease`, `Vehicle`, and `Medallion`. Its `status` (`Draft`, `Open`, `Closed`) tracks the overall state.
    *   `RepairInstallment`: A child record representing one weekly payment of a repair invoice. It has its own lifecycle status (`Scheduled`, `Posted`, `Paid`) and is the record that directly triggers a ledger posting.

*   **`app/repairs/exceptions.py`**: Defines specific exceptions like `InvoiceNotFoundError` and `PaymentScheduleGenerationError` for clear and targeted error handling within the service and API layers.

*   **`app/repairs/repository.py`**: The Data Access Layer. It abstracts all database operations, providing clean methods like `create_invoice`, `get_due_installments_to_post` (crucial for the Celery task), and `list_invoices` to support the service and API endpoints.

*   **`app/repairs/services.py`**:
    *   `RepairService`: The central business logic orchestrator.
        *   `_generate_next_repair_id`: Ensures unique, sequential, and human-readable IDs are created for each new invoice.
        *   `_get_weekly_principal`: Implements the business rule for the **Repayment Matrix**, determining the installment amount based on the total invoice cost.
        *   `create_repair_invoice`: The core method for the BPM flow. It manages the entire creation process from generating an ID to creating the invoice, generating the full payment schedule, and linking the final record back to the BPM case.
        *   `post_due_installments_to_ledger`: The logic executed by the weekly Celery task to find and post due installments to the Centralized Ledger.

*   **`app/repairs/tasks.py`**: Defines the `post_due_repair_installments_task` and makes it discoverable by the main Celery application instance.

*   **`app/repairs/schemas.py`**:
    *   `RepairInvoiceListResponse`: Defines the data structure for the main list view.
    *   `PaginatedRepairInvoiceResponse`: The standard paginated response for the list view.
    *   `RepairInvoiceDetailResponse` & `RepairInstallmentResponse`: Defines the comprehensive structure for the detailed invoice view, including the full payment schedule and information about the associated driver, vehicle, and lease.

*   **`app/repairs/stubs.py`**: Provides the `create_stub_repair_invoice_response` function to generate realistic mock data for UI development and testing.

*   **`app/bpm_flows/newrepair/flows.py`**: The implementation of the BPM workflow.
    *   `enter_repair_details_fetch`: The function backing the "Search Driver" part of the UI. It finds active leases for a given TLC license.
    *   `enter_repair_details_process`: The function that processes the submitted form data, calling the `RepairService` to create the invoice and schedule.

*   **`app/bpm_flows/newrepair/schemas/enter_details.json`**: The JSON schema that validates the data submitted during the manual creation workflow, ensuring all required fields are present and correctly formatted.

*   **`app/repairs/router.py`**: The API layer.
    *   `POST /payments/vehicle-repairs/create-case`: The endpoint that initiates the BPM workflow for creating a new repair invoice.
    *   `GET /payments/vehicle-repairs`: The primary endpoint for listing, filtering, and sorting all repair invoices.
    *   `GET /payments/vehicle-repairs/{repair_id}`: Provides the detailed view of a single invoice and its payment schedule.
    *   `GET /payments/vehicle-repairs/export`: The endpoint for exporting filtered data to Excel or PDF.

---

### 2. Integration Guide

Follow these steps to integrate the Vehicle Repairs module into your BAT Connect application.

#### **Step 1: Place New Module Files**

1.  Create a new directory: `app/repairs/`.
2.  Place the 8 corresponding files (`models.py`, `exceptions.py`, etc.) inside `app/repairs/`.
3.  Create a new directory: `app/bpm_flows/newrepair/`.
4.  Place the `flows.py` file inside `app/bpm_flows/newrepair/`.
5.  Create a new directory: `app/bpm_flows/newrepair/schemas/`.
6.  Place the `enter_details.json` file inside this new schemas directory.

#### **Step 2: Update the Database Schema**

The new models require two new tables (`repair_invoices` and `repair_installments`).

Assuming you are using **Alembic**:

1.  **Ensure Model Discovery:** Add the following import to your Alembic `env.py` or a central models file:
    ```python
    from app.repairs.models import RepairInvoice, RepairInstallment
    ```

2.  **Generate Migration Script:**
    ```bash
    alembic revision --autogenerate -m "Add vehicle repairs and installments tables"
    ```

3.  **Apply Migration:**
    ```bash
    alembic upgrade head
    ```

#### **Step 3: Integrate Celery Background Task**

1.  **Make Task Discoverable:** Add `"app.repairs"` to the `autodiscover_tasks` list in `app/core/celery_app.py`.
    ```python
    # FILE: app/core/celery_app.py
    app.autodiscover_tasks([
        # ... other modules
        "app.pvb",
        "app.repairs",  # <<< ADD THIS LINE
    ])
    ```

2.  **Schedule the Task:** Add the new task to your Celery Beat schedule in `app/worker/config.py`. **Crucially, schedule it to run before DTR generation** to ensure repair deductions are included in the weekly report.

    ```python
    # FILE: app/worker/config.py
    beat_schedule = {
        # ... other tasks like curb-post-earnings
        
        "repairs-post-due-installments": {
            "task": "repairs.post_due_installments",
            "schedule": crontab(hour=4, minute=30, day_of_week="sun"),  # Example: Sunday at 4:30 AM
            "options": {"timezone": "America/New_York"},
        },

        "generate-weekly-dtrs": {
            "task": "app.ledger.tasks.generate_weekly_dtrs",
            "schedule": crontab(hour=5, minute=0, day_of_week="sun"), # Runs AFTER repairs are posted
            "options": {"timezone": "America/New_York"}
        },
        # ...
    }
    ```

#### **Step 4: Integrate the API Router**

Add the `repairs` router to your main FastAPI application in `app/main.py`.

```python
# FILE: app/main.py

# ... (other imports) ...
from app.repairs.router import router as repair_routes  # <<< ADD THIS IMPORT

# ... (FastAPI app instance setup and middleware) ...

# Include routers
# ...
bat_app.include_router(pvb_routes)
bat_app.include_router(repair_routes) # <<< ADD THIS LINE
# ... (all other existing routers) ...
```

#### **Step 5: Link BPM Schema to Database**

The BPM engine needs a database record to know where to find the JSON schema for the new step. Insert a new record into your `case_step_config_paths` table.

You will first need the `id` of the `REPAIR_ENTER_DETAILS` step from your `case_step_configs` table. Assuming its ID is `X`, run the following SQL:

```sql
INSERT INTO case_step_config_paths (case_step_config_id, path, is_active, created_on)
VALUES (X, 'newrepair/schemas/enter_details.json', 1, NOW());
```

#### **Step 6: Verification**

1.  **Restart** your FastAPI application and all Celery services.
2.  **Check API Docs:** Navigate to `/docs` and verify the new **"Vehicle Repairs"** tag and its endpoints are present.
3.  **Test Manual Creation:**
    *   Make a `POST` request to `/payments/vehicle-repairs/create-case`.
    *   Use the returned `case_no` to interact with `GET /case/{case_no}` to see the workflow steps.
    *   Submit data to the `POST /case/{case_no}` endpoint with the `REPAIR_ENTER_DETAILS` step_id to create a full repair invoice.
4.  **Verify Database:** Check the `repair_invoices` and `repair_installments` tables to confirm that records are created correctly.
5.  **Test List View:** Make a `GET` request to `/payments/vehicle-repairs` to see the newly created invoice. Test the filtering and sorting parameters.