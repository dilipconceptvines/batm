## Integration Guide: CURB Trips & Earnings Module

This guide provides the necessary steps to integrate the newly created `curb` module into your main application. Following these steps will enable automated data import from the CURB API, link it to your system's core entities, and schedule the posting of earnings to the Centralized Ledger.

### Prerequisites

Before you begin, ensure you have all 8 new files for the `app/curb` module:
1.  `models.py`
2.  `exceptions.py`
3.  `repository.py`
4.  `services.py`
5.  `tasks.py`
6.  `schemas.py`
7.  `stubs.py`
8.  `router.py`

### Step 1: Place New Module Files

Place the new files into your project, creating a new `curb` directory inside the `app` directory. The final structure should look like this:

```plaintext
- app/
  - __init__.py
  - core/
  - bpm/
  - curb/            <-- NEW DIRECTORY
    - __init__.py
    - models.py
    - exceptions.py
    - repository.py
    - services.py
    - tasks.py
    - schemas.py
    - stubs.py
    - router.py
  - drivers/
  - leases/
  - ledger/
  - ... (other existing modules)
```

### Step 2: Update the Database Schema

The new `CurbTrip` model requires a corresponding table in your database. You will need to generate and apply a database migration.

Assuming you are using **Alembic** for migrations:

1.  **Ensure the new model is discoverable.** If you have a central models file (like `app/models.py`) or within your Alembic `env.py`, make sure it imports the new model:
    ```python
    from app.curb.models import CurbTrip
    ```

2.  **Generate the migration script:**
    ```bash
    alembic revision --autogenerate -m "Add curb_trips table for CURB integration"
    ```

3.  **Apply the migration to your database:**
    ```bash
    alembic upgrade head
    ```
    This will create the `curb_trips` table with all the necessary columns and relationships.

### Step 3: Update Environment Configuration

The `curb` module requires API credentials. Add the following variables to your **`.env`** file and populate them with the values provided by CURB Mobility.

```env
# FILE: .env

# --- CURB API Integration ---
CURB_URL="https://api.taxitronic.org/vts_service/taxi_service.asmx"
CURB_MERCHANT="YOUR_MERCHANT_ID_HERE"
CURB_USERNAME="YOUR_CURB_API_USERNAME"
CURB_PASSWORD="YOUR_CURB_API_PASSWORD"
```

### Step 4: Integrate Celery Background Tasks

To automate the data import and ledger posting, the new Celery tasks must be registered and scheduled. This requires editing two files.

**4.1. Make Tasks Discoverable**

Edit your main Celery application file to include the new `app.curb` module in the auto-discovery path.

```python
# FILE: app/core/celery_app.py

# ... (other imports)

# Auto discover tasks from different modules
app.autodiscover_tasks([
    "app.notifications",
    "app.worker",
    "app.curb",  # <<< ADD THIS LINE
    "app.bpm.sla",
    "app.leases", # Recommended: Also add this for your lease tasks
])

# ... (rest of the file)
```

**4.2. Schedule the Tasks**

Edit your Celery Beat configuration to define the schedule for the new tasks.

```python
# FILE: app/worker/config.py

from celery.schedules import crontab
# ... (other imports and configurations)

beat_schedule = {
    # --- New CURB Data Import Task (Daily) ---
    "curb-fetch-and-import": {
        "task": "curb.fetch_and_import_curb_trips_task",
        "schedule": crontab(hour=2, minute=0),  # Runs daily at 2:00 AM
        "options": {"timezone": "America/New_York"},
    },

    # --- New CURB Earnings Posting Task (Weekly) ---
    # IMPORTANT: This must run BEFORE the DTR generation task.
    "curb-post-earnings-to-ledger": {
        "task": "curb.post_earnings_to_ledger_task",
        "schedule": crontab(hour=4, minute=0, day_of_week="sun"), # Runs every Sunday at 4:00 AM
        "options": {"timezone": "America/New_York"},
    },
    
    # --- Existing DTR Generation Task ---
    # Ensure this runs AFTER the earnings posting task.
    "generate-weekly-dtrs": {
        "task": "app.ledger.tasks.generate_weekly_dtrs",
        "schedule": crontab(hour=5, minute=0, day_of_week="sun"), # Runs every Sunday at 5:00 AM
        "options": {
            "timezone": "America/New_York"
        }
    },

    # ... (all your other existing scheduled tasks) ...
}

# ... (rest of the file)
```

### Step 5: Integrate the API Router

To make the new "View Trips" pages and administrative endpoints available, include the `curb` router in your main FastAPI application.

```python
# FILE: app/main.py

# ... (other imports) ...
from app.curb.router import router as curb_routes  # <<< ADD THIS IMPORT

# ... (FastAPI app instance setup) ...

# Include routers
bat_app.include_router(user_routes)
bat_app.include_router(curb_routes)  # <<< ADD THIS LINE
bat_app.include_router(bpm_routes)
# ... (include all other existing routers) ...
```

### Step 6: Verification

After completing the steps above, you can verify the integration:

1.  **Restart your application and Celery workers/beat.**
2.  **Check API Documentation:** Navigate to your application's API docs (e.g., `http://localhost:8000/docs`). You should see a new "Trips" section with the endpoints: `/trips/view`, `/trips/view-curb-data`, `/trips/export`, etc.
3.  **Manual Import:** Use the API docs or a tool like Postman to make a `POST` request to the `/trips/curb/import` endpoint. Check your application logs for messages indicating the task has started and is fetching data from CURB.
4.  **Database Check:** After the import task runs, query your database to confirm that records have been added to the `curb_trips` table.
    ```sql
    SELECT * FROM curb_trips LIMIT 10;
    ```
5.  **Check Celery Beat:** Check your Celery Beat logs to ensure the new tasks (`curb-fetch-and-import`, `curb-post-earnings-to-ledger`) are scheduled correctly.

---

With these steps completed, the CURB Trips and Earnings module is now fully integrated into your BAT Connect application, automating a critical part of your financial workflow.