## Integration Guide: Interim Payments Workflow

### 1. Overview

This guide outlines the step-by-step process for integrating a client application with the BAT Connect API to handle driver Interim Payments. The entire workflow is orchestrated by the Business Process Management (BPM) engine, meaning the client will primarily interact with the `/case` endpoints to progress through the flow.

The process involves three main stages:
1.  **Initiation**: Creating a new Interim Payment case.
2.  **Data Entry**: Searching for a driver, selecting their active lease, and entering payment details.
3.  **Allocation & Confirmation**: Retrieving the driver's outstanding balances and submitting the final payment allocation.

### 2. Prerequisites

All API requests must include a valid JSON Web Token (JWT) in the `Authorization` header.

**Header Format:**
`Authorization: Bearer <your_access_token>`

### 3. Step-by-Step API Workflow

#### **Step 0: Initiate the Interim Payment Case**

This is the starting point of the entire workflow. The client application calls this endpoint when a user decides to create a new interim payment (e.g., by clicking the "Create Interim Payment" button).

*   **Action:** Create a new BPM case for an interim payment.
*   **Endpoint:** `POST /bpm/case`
*   **Request Body:**

    ```json
    {
      "case_type": "INTPAY"
    }
    ```

*   **Successful Response (200 OK):** The API returns the newly created `case_no` and the initial step information. The client must store the `case_no` as it is required for all subsequent steps.

    ```json
    {
      "case_no": "INTPAY000123",
      "created_by": "Merly",
      "case_created_on": "2024-10-24T10:00:00Z",
      "case_status": "Open",
      "steps": [
        {
          "step_name": "Search Driver & Enter Payment Details",
          "sub_steps": [
            {
              "step_name": "Fetch - Search Driver & Enter Payment Details",
              "step_id": "INTPAY-001",
              // ... other step metadata
            }
          ]
        }
      ]
    }
    ```

#### **Step 1: Search for Driver and Active Leases**

The user enters a driver's TLC License number to find their profile and associated active leases.

*   **Action:** Fetch driver and lease details for the "Search Driver & Enter Payment Details" screen.
*   **Endpoint:** `GET /bpm/case/{case_no}/{step_id}`
*   **Path Parameters:**
    *   `case_no`: The case number obtained from Step 0 (e.g., `INTPAY000123`).
    *   `step_id`: The ID for this step, which is `INTPAY-001`.
*   **Query Parameters:**
    *   `tlc_license_no` (string): The TLC License number entered by the user.

    **Example URL:** `/bpm/case/INTPAY000123/INTPAY-001?tlc_license_no=00504124`

*   **Successful Response (200 OK):** The API returns the driver's details and a list of their active leases.

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
          "vin": "4TALWRZV6YW122447"
        }
      ]
    }
    ```
*   **Error Response (404 Not Found):** If the driver or an active lease cannot be found.

#### **Step 2: Fetch Outstanding Balances for Allocation**

After the user selects a lease and enters the payment details (Total Amount, Method, Date), the client moves to the allocation step. This call fetches all open debts for the selected driver from the ledger.

*   **Action:** Fetch all open financial obligations for the "Allocate Payments" screen.
*   **Endpoint:** `GET /bpm/case/{case_no}/{step_id}`
*   **Path Parameters:**
    *   `case_no`: The case number (`INTPAY000123`).
    *   `step_id`: The ID for this step, which is `INTPAY-002`.
*   **Query Parameters:**
    *   `driver_id` (integer): The primary key ID of the driver selected in the previous step (e.g., `101`).

    **Example URL:** `/bpm/case/INTPAY000123/INTPAY-002?driver_id=101`

*   **Successful Response (200 OK):** The API returns the total outstanding amount and a list of all open obligations.

    ```json
    {
      "total_outstanding": 2156.50,
      "obligations": [
        {
          "category": "Lease",
          "reference_id": "MED-102-LS-08",
          "description": "Lease obligation from 2024-10-20",
          "outstanding": 265.00,
          "due_date": "2024-10-20"
        },
        {
          "category": "Repairs",
          "reference_id": "INV-2457",
          "description": "Repairs obligation from 2024-10-15",
          "outstanding": 450.00,
          "due_date": "2024-10-15"
        }
        // ... other obligations
      ]
    }
    ```

#### **Step 3: Submit the Payment and Allocation**

This is the final action. The client sends the complete payment and allocation details to the backend for processing and posting to the ledger.

*   **Action:** Finalize and process the interim payment.
*   **Endpoint:** `POST /bpm/case/{case_no}`
*   **Request Body:** The body must contain the `step_id` (`INTPAY-002`) and a `data` object that conforms to the `InterimPaymentCreate` schema.

    ```json
    {
      "step_id": "INTPAY-002",
      "data": {
        "driver_id": 101,
        "lease_id": 2054,
        "total_amount": 600.00,
        "payment_method": "ACH",
        "payment_date": "2025-10-20T00:00:00Z",
        "notes": "Partial payment for repairs and lease.",
        "allocations": [
          {
            "category": "Lease",
            "reference_id": "MED-102-LS-08",
            "amount": 265.00
          },
          {
            "category": "Repairs",
            "reference_id": "INV-2457",
            "amount": 335.00
          }
        ]
      }
    }
    ```

*   **Successful Response (200 OK):** The API confirms the successful creation and allocation.

    ```json
    {
      "message": "Interim payment successfully created and allocated."
    }
    ```
*   **Error Responses:**
    *   **400 Bad Request:** If `total_allocated` > `total_amount` or other validation fails.
    *   **500 Internal Server Error:** If the ledger posting fails for any reason.

### 4. Supporting Endpoints

#### **Listing Interim Payments**

To populate the "Interim Payments" management page, use the following endpoint. It supports filtering, sorting, and pagination.

*   **Action:** Retrieve a paginated list of interim payments.
*   **Endpoint:** `GET /payments/interim-payments`
*   **Query Parameters:**
    *   `page` (int), `per_page` (int)
    *   `sort_by` (string), `sort_order` (string: "asc" or "desc")
    *   `payment_id`, `driver_name`, `tlc_license`, `lease_id`, `medallion_no`, `payment_date`

#### **Exporting Interim Payments**

To export the filtered list of payments.

*   **Action:** Download a file (Excel or PDF) of interim payment data.
*   **Endpoint:** `GET /payments/interim-payments/export`
*   **Query Parameters:**
    *   `format` (string: "excel" or "pdf")
    *   All filtering and sorting parameters from the list endpoint are supported.