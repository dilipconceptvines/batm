## Comprehensive Documentation: EZPass Module

This document provides a complete overview of the new `app/ezpass` module. It is divided into two main sections:
1.  **Implementation Documentation:** Details the architecture, data models, business logic, and workflow of the module itself.
2.  **Integration Documentation:** A step-by-step guide for developers to merge and enable this new module within the main BAT Connect application.

---

### 1. Implementation Documentation

This section describes the internal design and functionality of the EZPass module.

#### **1.1. Overview**

The EZPass module automates the entire lifecycle of processing EZPass toll violations from CSV files. It serves as a critical source of financial obligations that are fed into the Centralized Ledger.

The module is designed around a robust, asynchronous workflow to handle potentially large files without blocking the user interface or API.

#### **1.2. The Transaction Lifecycle**

Each toll transaction goes through a distinct, auditable lifecycle managed by a series of services and background tasks:

1.  **File Upload:** A user (typically a Finance or Admin user) uploads a daily EZPass CSV file via the `/trips/ezpass/upload-csv` endpoint.

2.  **Initial Ingestion & Validation:** The `EZPassService` immediately performs initial processing:
    *   It validates the file type and structure.
    *   It creates a parent record in the `ezpass_imports` table to track the batch file.
    *   It parses each row of the CSV, validates basic data types (dates, amounts), and creates a corresponding record in the `ezpass_transactions` table with an initial status of **`IMPORTED`**.
    *   Any rows that fail this initial parsing are logged, and the import summary is updated.
    *   Crucially, this API call returns quickly after saving the raw data.

3.  **Asynchronous Association (Background Task):**
    *   Upon successful import, a Celery task (`associate_ezpass_transactions_task`) is automatically triggered.
    *   This background worker queries the database for all transactions with the `IMPORTED` status.
    *   For each transaction, it executes the core mapping logic:
        a.  It identifies the `Vehicle` by matching the `plate_number` from the toll record.
        b.  It then queries the `curb_trips` table to find a trip that occurred on that `vehicle_id` within a time window (e.g., +/- 30 minutes) of the toll's `transaction_datetime`.
        c.  If a matching CURB trip is found, it successfully links the toll to the `driver_id`, `lease_id`, and `medallion_id` associated with that trip. The transaction status is updated to **`ASSOCIATED`**.
        d.  If no vehicle or no matching trip is found, the status is updated to **`ASSOCIATION_FAILED`**, and the reason is logged in the `failure_reason` field for manual review.

4.  **Asynchronous Ledger Posting (Background Task):**
    *   After the association task finds successfully associated records, it triggers a second Celery task, `post_ezpass_tolls_to_ledger_task`.
    *   This worker queries for all transactions with the `ASSOCIATED` status.
    *   For each transaction, it calls the `LedgerService.create_obligation` method, creating a `DEBIT` posting with the category `EZPASS`.
    *   If the ledger posting is successful, the transaction status is updated to **`POSTED_TO_LEDGER`**.
    *   If the ledger posting fails, the status is updated to **`POSTING_FAILED`** with a corresponding error message.

This asynchronous, status-driven design ensures the system is resilient, auditable, and does not block user interactions during heavy data processing.

#### **1.3. File-by-File Architectural Breakdown**

*   **`app/ezpass/models.py`**: Defines the database schema.
    *   `EZPassImport`: Tracks each uploaded CSV file, acting as a historical log of all import jobs and their summary results (total, success, failed).
    *   `EZPassTransaction`: Stores every individual toll transaction. It holds the raw data from the CSV, the processing `status`, and the foreign keys (`driver_id`, `vehicle_id`, etc.) that are populated during the association phase.

*   **`app/ezpass/exceptions.py`**: Provides custom exceptions for clear error handling. This includes `CSVParseError`, `AssociationError`, and `LedgerPostingError` to pinpoint where in the lifecycle a failure occurred.

*   **`app/ezpass/repository.py`**: The Data Access Layer. This class is the sole touchpoint with the database for this module, providing methods to `create_import_record`, `bulk_insert_transactions`, retrieve transactions by `status`, and perform filtered queries for the API.

*   **`app/ezpass/services.py`**: The core business logic layer.
    *   `EZPassService`: Orchestrates the entire process. The `process_uploaded_csv` method handles the initial intake and triggers the background tasks. The `associate_transactions` and `post_tolls_to_ledger` methods contain the complex logic for mapping data and integrating with the `LedgerService`.
    *   **Celery Tasks**: The decorated functions (`associate_ezpass_transactions_task`, `post_ezpass_tolls_to_ledger_task`) define the background jobs that perform the time-consuming association and ledger posting work.

*   **`app/ezpass/tasks.py`**: This file makes the Celery tasks defined in `services.py` discoverable to the main Celery application instance, ensuring they are registered with the worker.

*   **`app/ezpass/schemas.py`**: Defines the Pydantic models for the API.
    *   `EZPassTransactionResponse`: Specifies the JSON structure for a single transaction record, matching the columns shown in the UI mockups.
    *   `PaginatedEZPassTransactionResponse`: Defines the structure for the paginated API responses, including metadata like `total_items` and `total_pages`.

*   **`app/ezpass/stubs.py`**: Contains functions to generate mock data for the API endpoints. `create_stub_ezpass_response` creates a realistic, paginated list of transactions with various statuses, enabling frontend development and testing via the `use_stubs=true` query parameter.

*   **`app/ezpass/router.py`**: The API layer.
    *   `POST /trips/ezpass/upload-csv`: The endpoint for file uploads.
    *   `GET /trips/ezpass`: The primary endpoint for viewing, filtering, sorting, and paginating the imported EZPass transactions, as shown in the screenshots.
    *   `GET /trips/ezpass/export`: An endpoint for exporting the filtered data to Excel or PDF, following the established pattern in your application.

---

### 2. Integration Guide

Follow these steps to integrate the EZPass module into the main BAT Connect application.

#### **Step 1: Place Module Files**

Create a new directory `app/ezpass` and place all 8 provided files inside it.

#### **Step 2: Apply Database Migration**

The new models require corresponding tables in your database. Use your migration tool (e.g., Alembic) to generate and apply the changes.

1.  **Make the models discoverable** by importing them where your other models are loaded (e.g., in your Alembic `env.py` or a central `app/models.py`).
    ```python
    from app.ezpass.models import EZPassImport, EZPassTransaction
    ```

2.  **Generate the migration script:**
    ```bash
    alembic revision --autogenerate -m "Add EZPass import and transaction tables"
    ```

3.  **Apply the migration:**
    ```bash
    alembic upgrade head
    ```
    This will create the `ezpass_imports` and `ezpass_transactions` tables.

#### **Step 3: Integrate Celery Background Tasks**

Register and schedule the new background tasks for processing the imported files.

**3.1. Make Tasks Discoverable**

Edit your main Celery application file to include `app.ezpass` in the auto-discovery path.

```python
# FILE: app/core/celery_app.py

# ... (other imports)

app.autodiscover_tasks([
    "app.notifications",
    "app.worker",
    "app.curb",
    "app.bpm.sla",
    "app.leases",
    "app.ezpass",  # <<< ADD THIS LINE
])

# ... (rest of the file)
```

**3.2. Add to Celery Beat Schedule (Optional)**

The EZPass tasks are triggered by user actions (file uploads) rather than a fixed schedule. Therefore, **no changes are needed in `app/worker/config.py`** for the `beat_schedule`. The tasks will run on-demand when called.

#### **Step 4: Integrate the API Router**

Add the new `ezpass` router to your main FastAPI application to make the endpoints accessible.

```python
# FILE: app/main.py

# ... (other imports) ...
from app.ezpass.router import router as ezpass_routes # <<< ADD THIS IMPORT

# ... (FastAPI app instance setup) ...

# Include routers
bat_app.include_router(user_routes)
bat_app.include_router(curb_routes)
bat_app.include_router(ezpass_routes) # <<< ADD THIS LINE
# ... (include all other existing routers) ...
```

#### **Step 5: Verification**

1.  **Restart** your FastAPI application, Celery workers, and Celery Beat scheduler.
2.  **API Documentation:** Navigate to your API docs. A new **"EZPass"** tag should be visible with the `/trips/ezpass/upload-csv`, `/trips/ezpass`, and `/trips/ezpass/export` endpoints.
3.  **Test Upload:** Use the API docs to test the `POST /trips/ezpass/upload-csv` endpoint. Upload the provided sample `EZP 1W47 4K19 5J58.csv` file. You should receive a `202 Accepted` response with a task ID.
4.  **Check Logs:** Monitor your Celery worker logs. You should see logs from `associate_ezpass_transactions_task` and subsequently from `post_ezpass_tolls_to_ledger_task` as they process the records.
5.  **Database Check:** Query your database to confirm that records have been created in `ezpass_imports` and `ezpass_transactions`, and that their `status` fields are being updated as the tasks run.
6.  **Test API List View:** Make a `GET` request to the `/trips/ezpass` endpoint to see the imported data reflected in the API response. Test the filtering and sorting parameters.