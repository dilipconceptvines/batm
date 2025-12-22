## Integration Guide: Miscellaneous Expenses Workflow

### 1. Overview

This guide details the API integration steps required to implement the Miscellaneous Expenses feature. The workflow allows authorized users (like front-desk or finance staff) to create and post one-time charges to a driver's account.

The entire process is managed by the Business Process Management (BPM) engine. This means the client application will interact primarily with the `/bpm/case` endpoints to progress through the single-step workflow.

### 2. Prerequisites

All API requests must be authenticated. The client is required to include a valid JSON Web Token (JWT) in the `Authorization` header of every request.

**Header Format:**
`Authorization: Bearer <your_access_token>`

### 3. Step-by-Step API Workflow

The workflow consists of initiating a case, fetching driver data, and submitting the expense for processing.

#### **Step 0: Initiate the Miscellaneous Expense Case**

This is the entry point for the entire workflow. This endpoint should be called when a user clicks the "Create Misc Expenses" button.

*   **Action:** Create a new BPM case for a miscellaneous expense.
*   **Endpoint:** `POST /bpm/case`
*   **Request Body:**

    ```json
    {
      "case_type": "MISCEXP"
    }
    ```

*   **Successful Response (200 OK):** The API returns the unique `case_no` for this workflow instance. The client **must** store this `case_no` as it is required for all subsequent steps.

    ```json
    {
      "case_no": "MISCEXP000001",
      "created_by": "Merly",
      "case_created_on": "2024-10-24T10:00:00Z",
      "case_status": "Open",
      "steps": [
        {
          "step_name": "Search Driver & Enter Expense Details",
          "sub_steps": [
            {
              "step_name": "Fetch - Search Driver & Enter Expense Details",
              "step_id": "MISCEXP-001",
              // ... other step metadata
            }
          ]
        }
      ]
    }
    ```

#### **Step 1: Search for Driver and Fetch Active Lease(s)**

On the creation screen, the user will search for a driver to apply the charge to. The API supports searching by TLC License, Medallion, or VIN/Plate.

*   **Action:** Fetch driver details and their associated active lease(s).
*   **Endpoint:** `GET /bpm/case/{case_no}/{step_id}`
*   **Path Parameters:**
    *   `case_no`: The case number from Step 0 (e.g., `MISCEXP000001`).
    *   `step_id`: The ID for this step, which is `MISCEXP-001`.
*   **Query Parameters:** (Provide at least one)
    *   `tlc_license_no` (string): The driver's TLC License number.
    *   `medallion_no` (string): A medallion number associated with the driver's lease.
    *   `vin_or_plate` (string): The VIN or Plate Number of the vehicle the driver is leasing.

    **Example URL:** `/bpm/case/MISCEXP000001/MISCEXP-001?tlc_license_no=00504124`

*   **Successful Response (200 OK):** The API returns the matching driver's profile and a list of their currently active leases.

    ```json
    {
      "driver": {
        "id": 101,
        "driver_id": "DRV-101",
        "full_name": "John Doe",
        "status": "Active",
        "tlc_license": "00504124",
        "phone": "(212) 555-8000",
        "email": "joedoe@ifsd.com"
      },
      "leases": [
        {
          "lease_id_pk": 2054,
          "lease_id": "LS-2054",
          "medallion_no": "1P43",
          "plate_no": "8SAM401",
          "vin": "4TALWRZV6YW122447",
          "vehicle_name": "2021 Toyota RAV4",
          "lease_type": "DOV",
          "lease_status": "Active",
          "weekly_lease": 1200.00
        }
      ]
    }
    ```
*   **Error Response (404 Not Found):** If no active driver or no active lease is found for the given search criteria.

#### **Step 2: Submit the Miscellaneous Expense**

After the user selects the driver and their associated lease, they will fill out the expense details form. This final step submits the complete information for creation and immediate posting to the ledger.

*   **Action:** Finalize and process the miscellaneous expense.
*   **Endpoint:** `POST /bpm/case/{case_no}`
*   **Request Body:** The body must contain the `step_id` (`MISCEXP-001`) and a `data` object that conforms to the `MiscellaneousExpenseCreate` schema.

    ```json
    {
      "step_id": "MISCEXP-001",
      "data": {
        "driver_id": 101,
        "lease_id": 2054,
        "vehicle_id": 55,
        "medallion_id": 42,
        "category": "Cleaning",
        "amount": 75.00,
        "expense_date": "2025-10-22",
        "reference_number": "6665F72248Y9",
        "notes": "Vehicle required deep cleaning after shift."
      }
    }
    ```

*   **Successful Response (200 OK):** The API confirms the expense was created and successfully posted to the ledger.

    ```json
    {
      "message": "Miscellaneous expense successfully created and posted to the ledger."
    }
    ```
*   **Error Responses:**
    *   **400 Bad Request:** If validation fails (e.g., amount is zero or negative).
    *   **500 Internal Server Error:** If the ledger posting operation fails.

### 4. Supporting Endpoints

#### **Listing Miscellaneous Expenses**

To populate the main "Miscellaneous Expenses" management page, use this endpoint. It supports comprehensive filtering, sorting, and pagination.

*   **Action:** Retrieve a paginated list of miscellaneous expenses.
*   **Endpoint:** `GET /payments/miscellaneous-expenses`
*   **Query Parameters:**
    *   `page` (int), `per_page` (int)
    *   `sort_by` (string), `sort_order` (string: "asc" or "desc")
    *   `expense_id`, `reference_no`, `category`, `expense_date`, `driver_name`, `lease_id`, `vin_no`, `plate_no`, `medallion_no`

#### **Exporting Miscellaneous Expenses**

To export the filtered list of expenses to a file.

*   **Action:** Download an Excel or PDF file of miscellaneous expense data.
*   **Endpoint:** `GET /payments/miscellaneous-expenses/export`
*   **Query Parameters:**
    *   `format` (string: "excel" or "pdf")
    *   All filtering and sorting parameters from the list endpoint are supported.