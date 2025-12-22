## Integration Guide: EZPass Module

This guide provides the specific steps required to integrate the new `app/ezpass` module into your existing BAT Connect application.

### **Prerequisites**

Ensure you have the 8 new files for the `ezpass` module and have placed them in a new `app/ezpass/` directory within your project:

1.  `models.py`
2.  `exceptions.py`
3.  `repository.py`
4.  `services.py`
5.  `tasks.py`
6.  `schemas.py`
7.  `stubs.py`
8.  `router.py`

### **Step 1: Update the Database Schema**

The new models require two new tables in your database (`ezpass_imports` and `ezpass_transactions`). You must generate and apply a database migration.

Assuming you are using **Alembic**:

1.  **Ensure Model Discovery:** Make sure your Alembic `env.py` (or a central models file it imports) discovers the new models. Add the following import statement:
    ```python
    from app.ezpass.models import EZPassImport, EZPassTransaction
    ```

2.  **Generate Migration Script:** In your terminal, run the `alembic revision` command:
    ```bash
    alembic revision --autogenerate -m "Add EZPass import and transaction tables"
    ```

3.  **Apply Migration:** Apply the newly created migration to your database:
    ```bash
    alembic upgrade head
    ```

### **Step 2: Integrate Celery Background Tasks**

The new background tasks for associating and posting tolls need to be registered with your Celery worker.

1.  **Edit `app/core/celery_app.py`:** Add `"app.ezpass"` to the `autodiscover_tasks` list.

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

> **Note:** The EZPass tasks are triggered on-demand by the file upload API. No changes are required for the Celery Beat schedule in `app/worker/config.py`.

### **Step 3: Integrate the API Router**

Make the new EZPass API endpoints accessible by including the router in your main FastAPI application.

1.  **Edit `app/main.py`:** Add the import for the `ezpass_routes` and include the router in your `bat_app` instance.

    ```python
    # FILE: app/main.py

    # ... (other imports)
    from app.ezpass.router import router as ezpass_routes  # <<< ADD THIS IMPORT

    # ... (FastAPI app instance setup and middleware)

    # Include routers
    bat_app.include_router(user_routes)
    bat_app.include_router(bpm_routes)
    bat_app.include_router(curb_routes)  # Assuming curb is already added
    bat_app.include_router(ezpass_routes) # <<< ADD THIS LINE
    # ... (include all other existing routers)

    # ... (rest of the file)
    ```

### **Step 4: Verification**

After completing the integration steps, restart your application, Celery worker, and Celery Beat services.

1.  **Check API Documentation:** Navigate to your application's API documentation (e.g., `http://localhost:8000/docs`). Verify that a new **"EZPass"** tag is present with the endpoints for uploading, listing, and exporting transactions.
2.  **Perform a Test Upload:** Use the API docs to test the `POST /trips/ezpass/upload-csv` endpoint. Upload the `EZP 1W47 4K19 5J58.csv` file provided. You should receive a `202 Accepted` response.
3.  **Monitor Logs:** Check the logs of your Celery worker. You should see logs indicating the start and completion of `associate_ezpass_transactions_task` and `post_ezpass_tolls_to_ledger_task`.
4.  **Verify Database:**
    *   Confirm a new record was created in the `ezpass_imports` table.
    *   Confirm that records were created in the `ezpass_transactions` table and that their `status` is being updated (from `IMPORTED` to `ASSOCIATED` to `POSTED_TO_LEDGER` or a failure state).
5.  **Test the API:** Make a `GET` request to `/trips/ezpass`. You should see the newly imported data with their processing statuses. Test the various filter parameters to ensure they work as expected.