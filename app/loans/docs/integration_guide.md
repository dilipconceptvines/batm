## Integration Guide: Driver Loans Module

This guide outlines the specific steps required to integrate the new `app/loans` module and its associated "Create Driver Loan" BPM workflow into your BAT Connect application.

### **Prerequisites**

Ensure you have created and placed all 10 new files in their correct locations:

*   **In `app/loans/` (8 files):**
    *   `models.py`
    *   `exceptions.py`
    *   `repository.py`
    *   `services.py`
    *   `tasks.py`
    *   `schemas.py`
    *   `stubs.py`
    *   `router.py`
*   **In `app/bpm_flows/newloan/` (1 file):**
    *   `flows.py`
*   **In `app/bpm_flows/newloan/schemas/` (1 file):**
    *   `enter_details.json`

### **Step 1: Update the Database Schema**

The new models require two new tables (`driver_loans` and `loan_installments`). Use your migration tool to update the schema.

Assuming you are using **Alembic**:

1.  **Ensure Model Discovery:** Add the following import to your Alembic `env.py` or a central models file it loads:
    ```python
    from app.loans.models import DriverLoan, LoanInstallment
    ```

2.  **Generate Migration Script:** Run the following command in your terminal:
    ```bash
    alembic revision --autogenerate -m "Add driver loans and installments tables"
    ```

3.  **Apply Migration:** Apply the new migration to your database:
    ```bash
    alembic upgrade head
    ```

### **Step 2: Link BPM Schema to Database Step**

The BPM engine needs to know where to find the validation schema for the manual creation step.

1.  **Find the Step Config ID:** After running the application once, query your `case_step_configs` table to find the `id` for the step with `step_id = 'LOAN_ENTER_DETAILS'`.
    ```sql
    SELECT id FROM case_step_configs WHERE step_id = 'LOAN_ENTER_DETAILS';
    ```

2.  **Insert the Path:** Using the `id` you found (let's assume it's `Z`), insert a new record into the `case_step_config_paths` table:
    ```sql
    INSERT INTO case_step_config_paths (case_step_config_id, path, is_active, created_on)
    VALUES (Z, 'newloan/schemas/enter_details.json', 1, NOW());
    ```

### **Step 3: Integrate Celery Background Task**

Register and schedule the new background task for posting due loan installments to the ledger.

1.  **Make Task Discoverable:** Edit `app/core/celery_app.py` and add `"app.loans"` to the `autodiscover_tasks` list.
    ```python
    # FILE: app/core/celery_app.py
    app.autodiscover_tasks([
        # ... other modules
        "app.repairs",
        "app.loans",  # <<< ADD THIS LINE
    ])
    ```

2.  **Schedule the Task:** Edit `app/worker/config.py` and add the `loans.post_due_installments` task to the `beat_schedule`. **Schedule it to run before the DTR generation task.**

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

### **Step 4: Integrate the API Router**

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

### **Step 5: Verification**

1.  **Restart** all application services (FastAPI, Celery Worker, Celery Beat).
2.  **Check API Documentation:** Navigate to your `/docs` endpoint. A new **"Driver Loans"** tag should be visible with all the expected endpoints.
3.  **Test Manual Creation:**
    *   Call `POST /payments/driver-loans/create-case` to start the BPM workflow.
    *   Use the returned `case_no` to interact with the BPM steps (e.g., `GET /case/{case_no}`).
    *   Submit valid data to `POST /case/{case_no}` using the `LOAN_ENTER_DETAILS` step_id to create a loan.
4.  **Verify Database:** Check the `driver_loans` and `loan_installments` tables to confirm that records are created correctly with a full repayment schedule.
5.  **Test List View:** Call `GET /payments/driver-loans` to see the newly created loan in the list. Test the various filters.