## Integration Guide: PVB Violations Module

This guide outlines the steps to integrate the new `app/pvb` module for managing Parking Violations Bureau tickets into your BAT Connect application.

### **Prerequisites**

Ensure you have created and placed all 9 new files in their correct locations:

*   **In `app/pvb/` (8 files):**
    *   `models.py`
    *   `exceptions.py`
    *   `repository.py`
    *   `services.py`
    *   `tasks.py`
    *   `schemas.py`
    *   `stubs.py`
    *   `router.py`
*   **In `app/bpm_flows/newpvb/` (1 file):**
    *   `flows.py`

### **Step 1: Update the Database Schema**

The new `PVBImport` and `PVBViolation` models require corresponding tables in your database. Use your migration tool to generate and apply these changes.

Assuming you are using **Alembic**:

1.  **Ensure Model Discovery:** Add the following import to your Alembic `env.py` or a central models file that it references:
    ```python
    from app.pvb.models import PVBImport, PVBViolation
    ```

2.  **Generate Migration Script:** Run the following command in your terminal:
    ```bash
    alembic revision --autogenerate -m "Add PVB import and violation tables"
    ```

3.  **Apply Migration:** Update your database schema by running:
    ```bash
    alembic upgrade head
    ```
    This will create the `pvb_imports` and `pvb_violations` tables.

### **Step 2: Integrate Celery Background Tasks**

The new background tasks for associating violations and posting them to the ledger need to be registered with your Celery application.

1.  **Edit `app/core/celery_app.py`:** Add `"app.pvb"` to the `autodiscover_tasks` list.

    ```python
    # FILE: app/core/celery_app.py

    app.autodiscover_tasks([
        "app.notifications",
        "app.worker",
        "app.curb",
        "app.bpm.sla",
        "app.leases",
        "app.ezpass",
        "app.pvb",      # <<< ADD THIS LINE
    ])
    ```
> **Note:** These tasks are event-driven (triggered by file uploads or BPM steps) and do not need to be added to the Celery Beat schedule in `app/worker/config.py`.

### **Step 3: Integrate the API Router**

Make the new PVB API endpoints for file uploads, data viewing, and manual case creation accessible.

1.  **Edit `app/main.py`:** Import the new router and include it in your FastAPI application instance.

    ```python
    # FILE: app/main.py

    # ... (other imports) ...
    from app.pvb.router import router as pvb_routes  # <<< ADD THIS IMPORT

    # ... (FastAPI app instance setup and middleware) ...

    # Include routers
    # ...
    bat_app.include_router(ezpass_routes)
    bat_app.include_router(pvb_routes)  # <<< ADD THIS LINE
    # ... (all other existing routers) ...
    ```

### **Step 4: Verification**

After completing the integration:

1.  **Restart** all application services (FastAPI server, Celery worker, Celery Beat).
2.  **Check API Documentation:** Navigate to your API docs (e.g., `/docs`). A new **"PVB"** tag should be present with the new endpoints.
3.  **Test CSV Upload:** Use the API docs to test the `POST /trips/pvb/upload-csv` endpoint with the sample CSV file.
4.  **Test Manual Workflow:** Make a `POST` request to `POST /trips/pvb/create-case` to initiate the BPM workflow for manual entry.
5.  **Monitor Logs & Database:**
    *   Check your application logs for the `202 Accepted` response after uploading a file.
    *   Monitor the Celery worker logs for messages related to `associate_pvb_violations_task` and `post_pvb_violations_to_ledger_task`.
    *   Query the `pvb_imports` and `pvb_violations` tables in your database to confirm that data is being created and statuses are being updated correctly.
6.  **Test the List API:** Make a `GET` request to `/trips/pvb` to verify that the imported and manually created data is visible and filterable.