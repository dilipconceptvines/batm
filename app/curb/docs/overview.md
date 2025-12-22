## Comprehensive Documentation

This document is divided into two parts:
1.  **Implementation Documentation:** Explains the architecture and internal workings of the new `curb` module.
2.  **Integration Documentation:** Provides a step-by-step guide on how to integrate this module into your main BAT Connect application.

---

### 1. Implementation Documentation

This section details the design and functionality of the `app/curb` module.

#### **1.1 Overview**

The `curb` module is the primary integration point for fetching, processing, and reconciling driver earnings from the CURB Mobility Taxi Fleet Web Services. Its core purpose is to act as the "Earnings" source for the Centralized Ledger.

The module automates the entire lifecycle of CURB data:
1.  **Data Ingestion:** Periodically fetches trip and transaction data from CURB's SOAP API.
2.  **Normalization & Mapping:** Parses the raw XML data, normalizes it into a standard format, and links it to the corresponding `Driver`, `Medallion`, `Vehicle`, and `Lease` records in the local database.
3.  **Storage:** Persists the enriched data into the new `curb_trips` table.
4.  **Reconciliation:** Communicates back to CURB to mark fetched records as "reconciled," ensuring data is not processed multiple times.
5.  **Ledger Posting:** On a weekly schedule, it calculates net credit card earnings for each driver and posts them as `CREDIT` entries into the Centralized Ledger, where they are automatically applied against outstanding obligations.

#### **1.2 File-by-File Breakdown**

**`app/curb/models.py`**
*   **Purpose:** Defines the database schema for storing CURB data.
*   **`CurbTrip` Model:** This is the central table for the module. It stores a normalized record of every trip or transaction fetched from CURB.
*   **Key Fields:**
    *   `curb_trip_id`: The unique identifier from the CURB system, used to prevent duplicates.
    *   `status`: A critical field (`UNRECONCILED`, `RECONCILED`, `POSTED_TO_LEDGER`) that tracks the record's state in our internal processing pipeline.
    *   `Foreign Keys`: `driver_id`, `lease_id`, `vehicle_id`, `medallion_id` link the trip to the core entities in the BAT system.
    *   `Financial Fields`: A complete breakdown of the trip's financial components (fare, tips, tolls, taxes) is stored using the `Decimal` type for accuracy.
    *   `payment_type`: Tracks how the trip was paid for (Cash, Credit Card, etc.).

**`app/curb/exceptions.py`**
*   **Purpose:** Defines custom exceptions for clear and specific error handling.
*   **Exceptions:**
    *   `CurbApiError`: For issues communicating with the CURB API (e.g., network errors, invalid responses).
    *   `ReconciliationError`: For failures during the reconciliation step.
    *   `DataMappingError`: Thrown when a CURB record cannot be linked to an internal entity (e.g., an unknown Driver TLC License).
    *   `TripProcessingError`: For errors that occur when trying to post a trip's earnings to the ledger.

**`app/curb/repository.py`**
*   **Purpose:** The Data Access Layer (DAL). It abstracts all direct database operations for the `CurbTrip` model.
*   **`CurbRepository` Class:**
    *   `bulk_insert_or_update`: An efficient method that handles a large batch of incoming trips, inserting new ones and updating existing ones to minimize database calls.
    *   Query Methods (`get_unreconciled_trips`, `list_curb_data`, etc.): Provides structured methods for the service layer to retrieve data without writing queries. The `list_curb_data` method is particularly important as it powers the API endpoints for viewing and filtering trip data.

**`app/curb/services.py`**
*   **Purpose:** Contains the core business logic of the module.
*   **`CurbApiService` Class:** A low-level client responsible for constructing the raw XML for SOAP requests and handling the HTTP communication with the CURB API. It knows how to call specific methods like `GET_TRIPS_LOG10` and `Reconciliation_TRIP_LOG`.
*   **`CurbService` Class:** The main orchestrator.
    *   `import_and_map_data()`: Manages the entire import process. It calls `CurbApiService` to get data from multiple endpoints, uses `_parse_and_normalize_trips` to handle the complex XML, and then iterates through records to link them to drivers, medallions, and leases. Finally, it uses the repository's `bulk_insert_or_update` to save the data.
    *   `reconcile_unreconciled_trips()`: Implements the reconciliation logic. It fetches unreconciled trips from the local DB, calls the CURB API to mark them, and updates their local status to `RECONCILED`. It includes logic to bypass the external API call in non-production environments for easier testing.
    *   `post_earnings_to_ledger()`: This is the critical financial integration point. It finds all `RECONCILED` credit card trips for a given period, aggregates the net earnings per driver, and then calls the `LedgerService.apply_weekly_earnings` method. This cleanly separates the concerns of the `curb` and `ledger` modules.
*   **Celery Tasks:**
    *   `fetch_and_import_curb_trips_task`: A scheduled task (intended to run daily) that wraps the `import_and_map_data` and `reconcile_unreconciled_trips` logic.
    *   `post_earnings_to_ledger_task`: A scheduled task (intended to run weekly, before DTR generation) that wraps the `post_earnings_to_ledger` logic.

**`app/curb/tasks.py`**
*   **Purpose:** Makes the Celery tasks defined in `services.py` discoverable by the main Celery application instance. It simply imports them into its namespace.

**`app/curb/schemas.py`**
*   **Purpose:** Defines the Pydantic models for API request and response validation.
*   **`CurbTripResponse`:** Defines the data structure for a single trip record returned by the API, matching the fields required by the UI. It uses aliases to map database column names to the desired JSON field names.
*   **`PaginatedCurbTripResponse`:** Wraps a list of `CurbTripResponse` objects with pagination metadata (total items, page number, etc.).

**`app/curb/stubs.py`**
*   **Purpose:** Provides mock data generation for the API endpoints.
*   **`create_stub_curb_trip_response()`:** Creates a complete, paginated response object filled with realistic-looking fake data. This is activated via the `use_stubs=true` query parameter on the API endpoints, allowing for frontend development without a live backend.

**`app/curb/router.py`**
*   **Purpose:** Exposes the module's functionality through RESTful API endpoints.
*   **Endpoints:**
    *   `GET /trips/view` & `GET /trips/view-curb-data`: These provide the main data grids for viewing trip information, complete with filtering, sorting, and pagination. They directly match the UI mockups.
    *   `GET /trips/export`: Provides functionality to export the filtered trip data into Excel or PDF format, reusing the established `ExcelExporter` and `PDFExporter` utilities.
    *   `POST /trips/curb/import` & `POST /trips/curb/post-earnings`: These are administrative endpoints that allow a user to manually trigger the corresponding Celery tasks. They return a task ID so the status of the background job can be monitored.

---

### 2. Integration Documentation

This section provides the necessary steps to integrate the new `curb` module into the main BAT Connect application.

#### **Step 1: Apply Database Migration**

A new database table, `curb_trips`, is required. You must generate and apply a database migration to create this table. If you are using a tool like Alembic, run the following commands:

```bash
# Ensure your new model in app/curb/models.py is imported in your main models file or alembic's env.py
alembic revision --autogenerate -m "Add curb_trips table for CURB integration"
alembic upgrade head
```

#### **Step 2: Update Environment Configuration**

Add the following new variables to your `.env` file with the appropriate credentials and URLs provided by CURB Mobility.

```env
# .env file

# ... existing variables ...

# --- CURB API Integration ---
CURB_URL="https://api.taxitronic.org/vts_service/taxi_service.asmx"
CURB_MERCHANT="YOUR_MERCHANT_ID"
CURB_USERNAME="YOUR_CURB_USERNAME"
CURB_PASSWORD="YOUR_CURB_PASSWORD"
```
**Note:** For development, you can use the `demo.taxitronic.org` URL as specified in `CurbApiService`.

#### **Step 3: Update Celery Configuration**

To enable the new automated tasks, you must update two Celery configuration files.

**1. Update the main Celery app instance to discover the new tasks.**

```python
# FILE: app/core/celery_app.py

# ... (imports) ...

app = Celery("BAT_scheduler")
app.config_from_object("app.worker.config")

# Add "app.curb" to the autodiscover_tasks list
app.autodiscover_tasks([
    "app.notifications",
    "app.worker",
    "app.curb",  # <<< ADD THIS LINE
    "app.bpm.sla",
    "app.leases", # This was missing from your original celery_app.py but is needed for lease tasks
])

# ... (rest of the file) ...
```

**2. Add the new tasks to the Celery Beat schedule.**

```python
# FILE: app/worker/config.py

# ... (imports) ...

# ... (existing configurations like broker_url, etc.) ...

# Add the new CURB tasks to the beat_schedule dictionary
beat_schedule = {
    # --- Existing CURB Tasks ---
    "curb-fetch-and-import": {
        "task": "curb.fetch_and_import_curb_trips_task",
        "schedule": crontab(hour=2, minute=0),  # Daily at 2 AM
        "options": {"timezone": "America/New_York"},
    },

    # --- New Task for Ledger Posting ---
    # This should run weekly before the DTR generation.
    # Assuming DTR runs on Sunday at 5 AM, this runs at 4 AM.
    "curb-post-earnings-to-ledger": {
        "task": "curb.post_earnings_to_ledger_task",
        "schedule": crontab(hour=4, minute=0, day_of_week="sun"), # Every Sunday at 4 AM
        "options": {"timezone": "America/New_York"},
    },
    
    # --- Existing Ledger Task (Example - ensure it runs AFTER earnings posting) ---
    "generate-weekly-dtrs": {
        "task": "app.ledger.tasks.generate_weekly_dtrs",
        "schedule": crontab(hour=5, minute=0, day_of_week="sun"), # Every Sunday at 5 AM
        "options": {"timezone": "America/New_York"},
    },
    
    # ... (other existing tasks) ...
}

# ... (rest of the file) ...
```
**Important:** The `curb-post-earnings-to-ledger` task **must** be scheduled to run *before* your main DTR generation task (`generate-weekly-dtrs`). This ensures that driver earnings are applied to their balances before the DTR snapshot is taken.

#### **Step 4: Integrate the New API Router**

Finally, include the new `curb` router in your main FastAPI application file.

```python
# FILE: app/main.py

# ... (imports) ...
from app.curb.router import router as curb_routes # <<< ADD THIS IMPORT

# ... (FastAPI app instance) ...

# ... (CORS middleware) ...

# Include routers
bat_app.include_router(user_routes)
bat_app.include_router(curb_routes) # <<< ADD THIS LINE
# ... (include other existing routers) ...
bat_app.include_router(lease_routes)
# ...

# ... (rest of the file) ...
```

---

This completes the implementation and integration of the CURB Trips and Ledger Posting feature. The system is now equipped to automatically handle driver earnings, providing a fully reconciled financial picture in the Centralized Ledger.