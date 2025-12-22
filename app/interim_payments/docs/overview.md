## Feature Documentation: Interim Payments

### 1. Overview & Objective

The **Interim Payments** module in BAT Connect provides a dedicated workflow for front-desk and finance staff to process **ad-hoc payments** made by drivers outside of the regular weekly DTR (Driver Transaction Report) cycle. These are typically cash, check, or ACH payments made directly to Big Apple Taxi (BAT).

The primary objective of this module is to allow drivers to reduce their outstanding balances immediately, while giving BAT staff the control to manually allocate these funds to specific obligations. The entire process is captured transparently and immutably within the **Centralized Ledger**, ensuring full auditability and reconciliation.

This feature is distinct from the automated weekly earnings application. It is an event-driven process initiated by a user action at the cashier/front desk.

### 2. Key Principles & Logic

The Interim Payments module is built on the following core principles of the BAT Connect platform:

*   **BPM-Driven Workflow**: Every interim payment is initiated and tracked through a dedicated Business Process Management (BPM) case (prefixed with `INTPAY`). This ensures a consistent, auditable process for every payment.
*   **Ledger as the Source of Truth**: The module does not modify source obligation tables (like `repairs` or `loans`). Instead, it interacts exclusively with the `LedgerService` to create `CREDIT` postings that reduce the real-time balances stored in the `Ledger_Balances` table.
*   **Manual Allocation with Smart Defaults**: While the weekly earnings application follows a strict hierarchical order, interim payments provide flexibility. The cashier can allocate funds to specific obligations as directed by the driver. Any unallocated excess is automatically applied to the driver's outstanding **Lease** balance by default.
*   **Real-Time Balance Updates**: As soon as an interim payment is confirmed, the corresponding `Ledger_Balances` are updated immediately. This provides a live, accurate view of the driver's outstanding debt.
*   **Full Auditability**: Every interim payment generates three key records:
    1.  An `InterimPayment` record capturing the total amount, method, and allocation details.
    2.  One or more `LedgerPosting` records for each allocation.
    3.  An `AuditTrail` entry linked to the BPM case, recording who processed the payment and when.

### 3. Architectural Components

The feature is implemented across several modules, following the established architecture of the application:

| File Path                               | Role & Responsibility                                                                                                     |
| :-------------------------------------- | :------------------------------------------------------------------------------------------------------------------------ |
| `app/interim_payments/models.py`        | Defines the `InterimPayment` SQLAlchemy model, which stores the master record for each payment transaction and its details.   |
| `app/interim_payments/exceptions.py`    | Defines custom, domain-specific exceptions for handling errors related to payment processing and allocation.              |
| `app/interim_payments/repository.py`    | The Data Access Layer. Encapsulates all direct database queries for the `interim_payments` table, such as creating and listing payments. |
| `app/interim_payments/schemas.py`       | Defines the Pydantic schemas for API request validation (`InterimPaymentCreate`) and response formatting (`InterimPaymentResponse`). |
| `app/interim_payments/services.py`      | Contains the core business logic, including generating unique payment IDs and orchestrating the interaction with the `LedgerService`. |
| `app/bpm_flows/interim_payment/flows.py`| Implements the BPM workflow steps, linking the UI actions to the service layer functions.                                   |
| `app/interim_payments/router.py`        | The API layer. Exposes the endpoints for creating BPM cases, listing payments, and exporting data to the frontend.          |

### 4. Workflow & Data Flow

The process follows the user journey depicted in the Figma designs, seamlessly integrating the frontend actions with the backend services.

1.  **Initiation**:
    *   A Finance user clicks "Create Interim Payment" from the main menu.
    *   The frontend calls `POST /payments/interim-payments/create-case`.
    *   The `BPMService` creates a new case with the prefix `INTPAY` and returns the `case_no`.

2.  **Step 1: Driver & Payment Details**:
    *   The UI navigates to the first step of the BPM flow. The user searches for a driver by their TLC License number.
    *   The frontend calls the BPM fetch endpoint for step `INTPAY-001`.
    *   The `fetch_driver_and_lease_details` function is executed, using the `DriverService` and `LeaseService` to find the driver and their active lease(s).
    *   The user selects the correct lease and enters the total payment amount, method, and date.

3.  **Step 2: Allocation**:
    *   The user proceeds to the "Allocate Payments" step.
    *   The frontend calls the BPM fetch endpoint for step `INTPAY-002`, passing the selected `driver_id`.
    *   The `fetch_outstanding_balances` function is executed. It calls the `LedgerRepository`'s `get_open_balances_for_driver` method to retrieve a real-time list of all outstanding obligations.
    *   The UI displays these obligations, allowing the cashier to enter amounts to be paid against each one, as shown in the "Allocate Payments" screen.

4.  **Confirmation & Posting**:
    *   The user confirms the allocation.
    *   The frontend calls the BPM process endpoint for step `INTPAY-002`, sending a payload matching the `InterimPaymentCreate` schema.
    *   The `process_payment_allocation` function is executed:
        *   It calls the `InterimPaymentService`'s `create_interim_payment` method.
        *   This service first creates the master `InterimPayment` record.
        *   Crucially, it then calls the `LedgerService`'s `apply_interim_payment` method. This method creates a `CREDIT` posting in the `Ledger_Postings` table for each allocation and updates the corresponding `Ledger_Balance` record, reducing the outstanding amount.
        *   Finally, the `BPMService` is called to mark the `INTPAY` case as "Closed".

5.  **Completion**:
    *   The UI displays a success message and an option to print a receipt. The workflow is complete.

### 5. Ledger Integration Details

All financial transactions strictly adhere to the rules of the Centralized Ledger.

| Event                     | Trigger                                       | Ledger Posting Logic                                                                                                                              | Example Entry                                                                                                       |
| :------------------------ | :-------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------ | :------------------------------------------------------------------------------------------------------------------ |
| **Payment Collection**    | User confirms allocation in the BPM flow.     | **For each allocation**: <br> • **Credit**: A `CREDIT` posting is created with `Category = INTERIM_PAYMENT` and `reference_id` pointing to the obligation. | Driver pays $100. <br> $75 is allocated to a Repair. <br> Ledger posts: `CREDIT $75`, `Category: INTERIM_PAYMENT`, `Ref: RPR-2025-012-03`. |
| **Excess Payment**        | Unallocated amount remains after allocation.  | **Credit**: An additional `CREDIT` posting is created with `Category = INTERIM_PAYMENT` and `reference_id` pointing to the **Lease ID**.         | The remaining $25 is auto-applied to the Lease. <br> Ledger posts: `CREDIT $25`, `Category: INTERIM_PAYMENT`, `Ref: LS-2054`.    |
| **Reversal / Correction** | A user with finance permissions voids a payment. | The `LedgerService.void_posting()` method is called. A reversing `DEBIT` posting is created, and the original `CREDIT` posting is marked as `VOIDED`. | The $75 payment was a mistake. Voiding it creates a `DEBIT $75` posting, neutralizing the original credit.               |

This implementation ensures that every dollar from an interim payment is accounted for as a `CREDIT` in the ledger, directly reducing the `balance` of an outstanding `DEBIT` obligation, and providing a clear, auditable trail of the entire transaction.