## Comprehensive Documentation: PVB Violations Module

This document provides a complete overview of the new `app/pvb` module, which handles the import and manual creation of parking violations.

### 1. Implementation Documentation

This section details the design, architecture, and functionality of the module.

#### **1.1. Overview**

The PVB module serves as the authoritative system for managing all parking violations issued by the Department of Finance (DOF). It is designed to capture these violations, associate them with the correct driver and lease, and post them as financial obligations to the Centralized Ledger.

The module supports two distinct workflows for data entry:

1.  **Automated CSV Import:** A primary workflow for bulk-ingesting daily violation reports provided in CSV format. This process is designed to be asynchronous to handle large files efficiently.
2.  **Manual BPM Workflow:** A user-driven, multi-step process for manually entering a single violation, complete with search, data entry, and proof attachment, fully integrated into the existing Business Process Management (BPM) engine.

#### **1.2. The Violation Lifecycle**

Every violation, regardless of its source, follows a clear, status-driven lifecycle, ensuring full traceability from creation to ledger posting.

**A. CSV Import Lifecycle:**

1.  **File Upload:** A user uploads a PVB CSV file via the `/trips/pvb/upload-csv` endpoint.
2.  **Ingestion:** The `PVBService` immediately validates and parses the file. A parent `PVBImport` record is created to track the batch. Each valid row is inserted into the `pvb_violations` table with a status of **`IMPORTED`**. This API call returns quickly.
3.  **Asynchronous Association (Celery Task):** The `associate_pvb_violations_task` is triggered automatically. This background job finds all `IMPORTED` violations and attempts to link them to a driver and lease using the following logic:
    *   It finds the `Vehicle` using the `plate` number from the violation.
    *   It then searches the `curb_trips` table for a trip associated with that vehicle around the `issue_date` and `issue_time` of the violation.
    *   If a match is found, it populates the `driver_id`, `lease_id`, `medallion_id`, and `vehicle_id` on the violation record and updates its status to **`ASSOCIATED`**.
    *   If no match is found, the status is set to **`ASSOCIATION_FAILED`**, and the reason is recorded for manual review.
4.  **Asynchronous Ledger Posting (Celery Task):** After association, the `post_pvb_violations_to_ledger_task` is triggered. It finds all `ASSOCIATED` violations and creates a `DEBIT` obligation in the Centralized Ledger for the `amount_due`. Upon success, the status is updated to **`POSTED_TO_LEDGER`**.

**B. Manual Entry (BPM) Lifecycle:**

1.  **Case Creation:** A user initiates the process from the UI, which calls `POST /trips/pvb/create-case`. This creates a new BPM case with the prefix "CRPVB".
2.  **Step 1: Choose Driver (`PVB_CHOOSE_DRIVER`):** The user searches for a driver/lease. Upon selection, a preliminary `PVBViolation` record is created with a `TEMP-` summons number and linked to the case, driver, and lease.
3.  **Step 2: Enter Details (`PVB_ENTER_DETAILS`):** The user fills in the specific details of the ticket (summons number, fine, etc.). The temporary violation record is updated with this information.
4.  **Step 3: Attach Proof (`PVB_ATTACH_PROOF`):** The user uploads a scan of the ticket. The `Document` record is linked to the violation. The violation's status is then set to **`ASSOCIATED`**, and the ledger posting task is triggered to handle the financial entry asynchronously.

#### **1.3. File-by-File Architectural Breakdown**

*   **`app/pvb/models.py`**:
    *   `PVBImport`: Tracks each uploaded CSV file, providing an audit trail for batch imports.
    *   `PVBViolation`: The core table for every violation. Crucially, the `source` column (`CSV_IMPORT` or `MANUAL_ENTRY`) distinguishes how the record was created, and the `case_no` column links manual entries back to the BPM workflow.

*   **`app/pvb/exceptions.py`**: Defines specific exceptions like `PVBCSVParseError` and `PVBAssociationError` for robust and targeted error handling.

*   **`app/pvb/repository.py`**: The Data Access Layer. Provides methods for creating import records, bulk-inserting violations (while skipping duplicates based on `summons`), and querying the database for the service and API layers.

*   **`app/pvb/services.py`**:
    *   `PVBService`: The central business logic orchestrator.
        *   `process_uploaded_csv`: Manages the entire CSV import workflow.
        *   `associate_violations`: Contains the critical logic for linking violations to drivers via plate and CURB trip data.
        *   `post_violations_to_ledger`: Integrates with the `LedgerService` to create financial obligations.
        *   `create_manual_violation`: A dedicated method used by the BPM flow to create and manage manually entered violations.
    *   **Celery Tasks**: The decorated functions (`associate_pvb_violations_task`, `post_pvb_violations_to_ledger_task`) define the asynchronous jobs that handle the heavy processing.

*   **`app/pvb/tasks.py`**: Ensures the Celery tasks are discoverable by the main Celery application.

*   **`app/pvb/schemas.py`**: Defines the Pydantic models for API contracts.
    *   `PVBViolationResponse`: The data structure for the list view API.
    *   `PaginatedPVBViolationResponse`: The paginated response for the list view.
    *   `PVBManualCreateRequest`: A schema to validate the complete payload from the multi-step manual creation form.

*   **`app/pvb/stubs.py`**: Contains the `create_stub_pvb_response` function for generating realistic mock data for UI development and testing.

*   **`app/bpm_flows/newpvb/flows.py`**: Contains the specific implementation for each step of the "Create PVB" BPM workflow. Each function is registered with the BPM engine using the `@step` decorator and handles the fetching (`fetch`) and processing (`process`) of data for its respective step.

*   **`app/pvb/router.py`**: The API layer.
    *   `POST /trips/pvb/upload-csv`: Endpoint for uploading PVB violation files.
    *   `GET /trips/pvb`: The primary endpoint for viewing, filtering, sorting, and paginating all PVB violations.
    *   `POST /trips/pvb/create-case`: Endpoint to initiate the manual entry BPM workflow.
    *   `GET /trips/pvb/export`: Provides data exporting functionality in Excel or PDF format.

---

### 2. Integration Guide

Follow these steps to integrate the new PVB module into your main application.

#### **Step 1: Place New Module Files**

1.  Create a new directory: `app/pvb/`
2.  Place the 8 corresponding files (`models.py`, `exceptions.py`, etc.) inside `app/pvb/`.
3.  Create a new directory: `app/bpm_flows/newpvb/`
4.  Place the `flows.py` file inside `app/bpm_flows/newpvb/`.

#### **Step 2: Update the Database Schema**

You need to add the `pvb_imports` and `pvb_violations` tables to your database.

Assuming you use **Alembic**:

1.  **Ensure Model Discovery:** Add the following import to your Alembic `env.py` or a central models file:
    ```python
    from app.pvb.models import PVBImport, PVBViolation
    ```

2.  **Generate Migration Script:**
    ```bash
    alembic revision --autogenerate -m "Add PVB import and violation tables"
    ```

3.  **Apply Migration:**
    ```bash
    alembic upgrade head
    ```

#### **Step 3: Integrate BPM and Celery Tasks**

The system is designed for auto-discovery, so integration is straightforward.

1.  **BPM Flow:** The `import_bpm_flows()` function in your `app/main.py` will automatically discover and register the new steps in `app/bpm_flows/newpvb/flows.py` on application startup. **No code changes are needed.**

2.  **Celery Tasks:** Add `"app.pvb"` to the `autodiscover_tasks` list in your main Celery application file to register the new background tasks.

    ```python
    # FILE: app/core/celery_app.py

    app.autodiscover_tasks([
        "app.notifications",
        "app.worker",
        "app.curb",
        "app.bpm.sla",
        "app.leases",
        "app.ezpass",
        "app.pvb",  # <<< ADD THIS LINE
    ])
    ```
    > **Note:** Like the EZPass module, these tasks are triggered by events (file uploads, successful association) and do not need to be added to the Celery Beat schedule in `app/worker/config.py`.

#### **Step 4: Integrate the API Router**

Make the new PVB API endpoints available by including the router in `app/main.py`.

```python
# FILE: app/main.py

# ... (other imports) ...
from app.pvb.router import router as pvb_routes  # <<< ADD THIS IMPORT

# ... (FastAPI app instance setup and middleware) ...

# Include routers
# ...
bat_app.include_router(ezpass_routes)
bat_app.include_router(pvb_routes) # <<< ADD THIS LINE
# ... (all other existing routers) ...
```

#### **Step 5: Verification**

1.  **Restart** your FastAPI application and Celery worker services.
2.  **Check API Documentation:** Navigate to your API docs. A new **"PVB"** tag should be visible with all the expected endpoints.
3.  **Test CSV Upload:** Use the API docs to test `POST /trips/pvb/upload-csv` with the sample CSV file. Confirm you receive a `202 Accepted` response.
4.  **Test Manual Creation:** Make a `POST` request to `/trips/pvb/create-case`. You should receive a `201 Created` response with a new case number (e.g., "CRPVB000001"). Use this case number to interact with the BPM steps (`GET /case/{case_no}`).
5.  **Monitor Logs & Database:** Check your Celery worker logs for the association and ledger posting tasks. Query the `pvb_violations` table to see new records and watch their `status` field change as the tasks complete.