## Comprehensive Documentation: Driver Loans Module

This document provides a complete overview of the new `app/loans` module, designed to manage the full lifecycle of personal loans provided to drivers.

### 1. Implementation Documentation

This section details the design, architecture, and functionality of the module.

#### **1.1. Overview**

The Driver Loans module provides a structured and automated system for creating, managing, and reconciling personal loans extended to drivers. Its primary purpose is to formalize the loan process and integrate repayments directly into the driver's weekly financial cycle via the Centralized Ledger.

The module supports a Business Process Management (BPM) workflow for creating new loans, which then feeds into an automated, schedule-based system for posting weekly deductions.

#### **1.2. The Driver Loan Lifecycle**

Each loan follows a clear, status-driven lifecycle, ensuring full traceability from creation to final repayment.

1.  **Case Creation:** A user initiates the process from the "Driver Loans" page in the UI, which calls `POST /payments/driver-loans/create-case`. This creates a new BPM case with the prefix "DRLNS".

2.  **Loan Creation & Schedule Generation (BPM Step):**
    *   The workflow consists of a single, comprehensive step: `LOAN_ENTER_DETAILS`.
    *   The user searches for a driver by their TLC License number and selects one of their active leases to associate the loan with.
    *   The user then enters the loan details: `Loan Amount`, `Interest Rate (Annual %)` (defaults to 0), `Start Week` (must be a Sunday), and optional notes.
    *   Upon submission, the `LoanService` orchestrates several actions in a single database transaction:
        a.  It generates a unique, sequential **Loan ID** (e.g., `DLN-2025-001`).
        b.  It creates a master `DriverLoan` record with an initial `DRAFT` status.
        c.  It then immediately calculates the **entire repayment schedule** based on the `Loan Repayment Matrix` specified in the documentation. This includes calculating both the `principal` and daily-accrued `interest` for each weekly installment.
        d.  It creates all `LoanInstallment` records for the full term of the loan and saves them to the database.
        e.  Finally, it updates the master `DriverLoan` status to **`OPEN`** and links the new loan to the BPM case via the `CaseEntity` table.

3.  **Automated Ledger Posting (Celery Task):**
    *   A scheduled Celery task, `loans.post_due_installments`, runs weekly (e.g., every Sunday at 5 AM).
    *   This task queries the database for all `LoanInstallment` records that have a status of `SCHEDULED` and whose `week_start_date` has passed.
    *   For each due installment, it calls the `LedgerService.create_obligation` method. This creates a `DEBIT` posting in the Centralized Ledger with the category `LOAN` for the `total_due` amount (principal + interest).
    *   The installment's status is then updated to **`POSTED`**.

4.  **Reconciliation & Closure:**
    *   The driver's weekly earnings are automatically applied to this new `LOAN` obligation in the ledger according to the established payment hierarchy (after Taxes, Lease, Repairs, etc.).
    *   When the ledger balance for an installment is cleared, its status can be updated to `PAID`.
    *   The parent `DriverLoan` is automatically marked as `CLOSED` only after all of its individual installments have been fully paid.

#### **1.3. File-by-File Architectural Breakdown**

*   **`app/loans/models.py`**: Defines the database schema for the feature.
    *   `DriverLoan`: The master record for a loan, containing the principal, interest rate, and overall status. It's linked to the `Driver`, `Lease`, and `Medallion`.
    *   `LoanInstallment`: Represents a single weekly payment. It stores the calculated `principal_amount`, `interest_amount`, and `total_due`. Its status (`Scheduled`, `Posted`, etc.) tracks its progress through the ledger posting lifecycle.

*   **`app/loans/exceptions.py`**: Contains custom exceptions like `LoanNotFoundError` and `LoanScheduleGenerationError` for specific and clear error handling.

*   **`app/loans/repository.py`**: The Data Access Layer. It abstracts all database queries, providing clean methods like `create_loan`, `get_due_installments_to_post` (used by the Celery task), and a comprehensive `list_loans` method to power the API list view.

*   **`app/loans/services.py`**:
    *   `LoanService`: The central business logic orchestrator.
        *   `_generate_next_loan_id`: Ensures unique, sequential, and human-readable IDs are created.
        *   `_get_weekly_principal`: Implements the **Loan Repayment Matrix** business rule.
        *   `create_loan_and_schedule`: The main method for the BPM flow, handling the creation of the loan and its full repayment schedule in one atomic operation.
        *   `post_due_installments_to_ledger`: Contains the logic executed by the weekly Celery task to post due installments to the Ledger.

*   **`app/loans/tasks.py`**: Defines the `post_due_loan_installments_task` and makes it discoverable by the Celery worker and scheduler.

*   **`app/loans/schemas.py`**:
    *   Defines all Pydantic models for API request/response validation, including `DriverLoanListResponse` for the main grid and `DriverLoanDetailResponse` for the detailed view with its nested `LoanInstallmentResponse` list.

*   **`app/loans/stubs.py`**: Contains the `create_stub_loan_response` function to generate realistic mock data for UI development and testing.

*   **`app/bpm_flows/newloan/flows.py`**: Implements the BPM workflow logic.
    *   `enter_loan_details_fetch`: Backs the "Search Driver" functionality in the UI.
    *   `enter_loan_details_process`: Processes the submitted form data by calling the `LoanService` to create the loan and its schedule.

*   **`app/bpm_flows/newloan/schemas/enter_details.json`**: The JSON schema that validates the data submitted during the manual creation workflow.

*   **`app/loans/router.py`**: The API layer.
    *   `POST /payments/driver-loans/create-case`: Endpoint to initiate the BPM workflow.
    *   `GET /payments/driver-loans`: The primary endpoint for listing, filtering, and sorting all driver loans.
    *   `GET /payments/driver-loans/{loan_id}`: Provides the detailed view of a single loan and its repayment schedule.
    *   `GET /payments/driver-loans/export`: Endpoint for exporting filtered data to Excel or PDF.

---

### 2. Integration Guide

Follow these steps to integrate the Driver Loans module into your BAT Connect application.

#### **Step 1: Place New Module Files**

1.  Create a new directory: `app/loans/`.
2.  Place the 8 corresponding files (`models.py`, `router.py`, etc.) inside `app/loans/`.
3.  Create a new directory: `app/bpm_flows/newloan/`.
4.  Place the `flows.py` file inside `app/bpm_flows/newloan/`.
5.  Create a new directory: `app/bpm_flows/newloan/schemas/`.
6.  Place the `enter_details.json` file inside this new schemas directory.

#### **Step 2: Update the Database Schema**

The new models require two new tables (`driver_loans` and `loan_installments`).

Assuming you are using **Alembic**:

1.  **Ensure Model Discovery:** Add the following import to your Alembic `env.py` or a central models file:
    ```python
    from app.loans.models import DriverLoan, LoanInstallment
    ```

2.  **Generate Migration Script:**
    ```bash
    alembic revision --autogenerate -m "Add driver loans and installments tables"
    ```

3.  **Apply Migration:**
    ```bash
    alembic upgrade head
    ```

#### **Step 3: Link BPM Schema to Database Step**

The BPM engine needs a database record to locate the JSON schema for the new step.

1.  **Find the Step Config ID:** After running the application once, query your `case_step_configs` table to find the `id` for the step with `step_id = 'LOAN_ENTER_DETAILS'`.
    ```sql
    SELECT id FROM case_step_configs WHERE step_id = 'LOAN_ENTER_DETAILS';
    ```

2.  **Insert the Path:** Using the `id` you found (let's assume it's `Y`), insert a new record into the `case_step_config_paths` table:
    ```sql
    INSERT INTO case_step_config_paths (case_step_config_id, path, is_active, created_on)
    VALUES (Y, 'newloan/schemas/enter_details.json', 1, NOW());
    ```

#### **Step 4: Integrate Celery Background Task**

1.  **Make Task Discoverable:** Add `"app.loans"` to the `autodiscover_tasks` list in `app/core/celery_app.py`.
    ```python
    # FILE: app/core/celery_app.py
    app.autodiscover_tasks([
        # ... other modules
        "app.repairs",
        "app.loans",  # <<< ADD THIS LINE
    ])
    ```

2.  **Schedule the Task:** Add the new task to your Celery Beat schedule in `app/worker/config.py`. Schedule it to run *before* DTR generation.

    ```python
    # FILE: app/worker/config.py
    beat_schedule = {
        # ... other tasks like curb and repairs ...
        
        "loans-post-due-installments": {
            "task": "loans.post_due_installments",
            "schedule": crontab(hour=4, minute=45, day_of_week="sun"),  # Example: Sunday at 4:45 AM
            "options": {"timezone": "America/New_York"},
        },

        "generate-weekly-dtrs": {
            "task": "app.ledger.tasks.generate_weekly_dtrs",
            "schedule": crontab(hour=5, minute=0, day_of_week="sun"), # Runs AFTER loans are posted
            # ...
        },
        # ...
    }
    ```

#### **Step 5: Integrate the API Router**

Make the new Driver Loans API endpoints available by adding the router to `app/main.py`.

```python
# FILE: app/main.py

# ... (other imports) ...
from app.loans.router import router as loan_routes  # <<< ADD THIS IMPORT

# ... (FastAPI app instance setup and middleware) ...

# Include routers
# ...
bat_app.include_router(repair_routes)
bat_app.include_router(loan_routes) # <<< ADD THIS LINE
# ... (all other existing routers) ...
```

#### **Step 6: Verification**

1.  **Restart** all your application services (FastAPI, Celery Worker, Celery Beat).
2.  **Check API Docs:** Navigate to `/docs` and verify the new **"Driver Loans"** tag and its endpoints are present.
3.  **Test Manual Creation:**
    *   Call `POST /payments/driver-loans/create-case` to start the BPM workflow.
    *   Use the returned `case_no` to interact with the BPM steps (e.g., `GET /case/{case_no}`).
    *   Submit valid data via `POST /case/{case_no}` with the `LOAN_ENTER_DETAILS` step_id to create a loan.
4.  **Verify Database:** Check the `driver_loans` and `loan_installments` tables to confirm that records are created correctly with a full repayment schedule.
5.  **Test API List View:** Call `GET /payments/driver-loans` to see the newly created loan. Test the various filters.