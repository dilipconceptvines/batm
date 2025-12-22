## Integration Guide: Vehicle Repairs Module

This guide provides the necessary steps to integrate the new `app/repairs` module and the associated "Create Repair Invoice" BPM workflow into your BAT Connect application.

### **Prerequisites**

Ensure you have created and placed all 10 new files in their correct locations:

*   **In `app/repairs/` (8 files):**
    *   `models.py`
    *   `exceptions.py`
    *   `repository.py`
    *   `services.py`
    *   `tasks.py`
    *   `schemas.py`
    *   `stubs.py`
    *   `router.py`
*   **In `app/bpm_flows/newrepair/` (1 file):**
    *   `flows.py`
*   **In `app/bpm_flows/newrepair/schemas/` (1 file):**
    *   `enter_details.json`

### **Step 1: Update the Database Schema**

The new models require two new tables (`repair_invoices` and `repair_installments`). Use your database migration tool to update the schema.

Assuming you are using **Alembic**:

1.  **Ensure Model Discovery:** Add the following import to your Alembic `env.py` or a central models file it loads:
    ```python
    from app.repairs.models import RepairInvoice, RepairInstallment
    ```

2.  **Generate Migration Script:** Run the following command:
    ```bash
    alembic revision --autogenerate -m "Add vehicle repairs and installments tables"
    ```

3.  **Apply Migration:** Apply the new migration to your database:
    ```bash
    alembic upgrade head
    ```

### **Step 2: Link BPM Schema to Database Step**

The BPM engine needs to know where to find the validation schema for the manual creation step.

1.  **Find the Step Config ID:** After your application has run at least once (which registers the BPM steps), query your `case_step_configs` table to find the `id` for the step with `step_id = 'REPAIR_ENTER_DETAILS'`.
    ```sql
    SELECT id FROM case_step_configs WHERE step_id = 'REPAIR_ENTER_DETAILS';
    ```

2.  **Insert the Path:** Using the `id` you found (let's assume it's `X`), insert a new record into the `case_step_config_paths` table:
    ```sql
    INSERT INTO case_step_config_paths (case_step_config_id, path, is_active, created_on)
    VALUES (X, 'newrepair/schemas/enter_details.json', 1, NOW());
    ```

### **Step 3: Integrate Celery Background Tasks**

Register and schedule the new background task for posting repair installments to the ledger.

1.  **Make Task Discoverable:** Edit `app/core/celery_app.py` and add `"app.repairs"` to the `autodiscover_tasks` list.
    ```python
    # FILE: app/core/celery_app.py
    app.autodiscover_tasks([
        # ... other modules
        "app.pvb",
        "app.repairs",  # <<< ADD THIS LINE
    ])
    ```

2.  **Schedule the Task:** Edit `app/worker/config.py` and add the `repairs.post_due_installments` task to the `beat_schedule`. **Schedule it to run before the DTR generation task.**

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
            "schedule": crontab(hour=5, minute=0, day_of_week="sun"), # Runs AFTER repairs
            # ...
        },
        # ...
    }
    ```

### **Step 4: Integrate the API Router**

Make the new Vehicle Repairs API endpoints available.

1.  **Edit `app/main.py`:** Import the new router and include it in your FastAPI application.

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

### **Step 5: Verification**

1.  **Restart** all your application services (FastAPI, Celery Worker, Celery Beat).
2.  **API Documentation:** Check your `/docs` endpoint. A new **"Vehicle Repairs"** tag should be visible with all the expected endpoints.
3.  **Test Manual Creation:**
    *   Call `POST /payments/vehicle-repairs/create-case` to start the BPM workflow.
    *   Use the returned `case_no` with `GET /case/{case_no}` to see the workflow steps.
    *   Submit valid data to `POST /case/{case_no}` using the `REPAIR_ENTER_DETAILS` step_id to create an invoice.
4.  **Verify Database:**
    *   Confirm that a new record is created in `repair_invoices` with a status of `Open`.
    *   Confirm that corresponding records are created in `repair_installments` with a status of `Scheduled`.
5.  **Test List View:** Call `GET /payments/vehicle-repairs` to see the newly created invoice in the list. Test the various filters.