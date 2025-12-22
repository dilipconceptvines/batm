## Feature Documentation: Miscellaneous Expenses

### 1. Overview & Objective

The **Miscellaneous Expenses** module in BAT Connect provides a streamlined workflow for applying **one-off, non-recurring charges** to a driver's account for various operational or penalty-related reasons.

Its purpose is to ensure that ad-hoc charges—which are not part of dedicated modules like Repairs, Loans, EZPass, or PVB—are accurately recorded, immediately posted as a financial obligation, and systematically recovered through the weekly DTR (Driver Transaction Report) process.

Examples of such charges include:
*   Lost Key fees
*   Vehicle Cleaning fees
*   Late Return fees
*   Other administrative charges or adjustments

### 2. Key Principles & Logic

The Miscellaneous Expenses module is built upon the foundational principles of the BAT Connect platform:

*   **BPM-Driven Workflow**: Every new miscellaneous expense is initiated and tracked through a dedicated Business Process Management (BPM) case (prefixed with `MISCEXP`). This ensures a consistent, auditable, and user-guided process for every charge.
*   **Immediate Ledger Posting**: Unlike scheduled installments (for loans or repairs), a miscellaneous expense is posted **immediately** to the Centralized Ledger as a `DEBIT` obligation the moment it is created. This ensures the charge is reflected in the driver's real-time balance and is guaranteed to be included in the next DTR cycle for recovery.
*   **Single Source of Truth**: This feature interacts exclusively with the `LedgerService`. It creates a new financial obligation in the ledger but does not modify any source data tables. The ledger remains the authoritative record for all financial transactions.
*   **Full Auditability**: The creation of a single miscellaneous expense generates a complete, traceable record:
    1.  A master `MiscellaneousExpense` record is created to store the details of the charge.
    2.  An immutable `LedgerPosting` is generated, representing the financial transaction.
    3.  A corresponding `LedgerBalance` is opened to track the real-time status of this new debt.
    4.  An `AuditTrail` entry is created, linking the action back to the user and the BPM case.

### 3. Architectural Components

The feature is implemented across the application's established layers, ensuring a clean separation of concerns.

| File Path                               | Role & Responsibility                                                                                                     |
| :-------------------------------------- | :------------------------------------------------------------------------------------------------------------------------ |
| `app/misc_expenses/models.py`           | Defines the `MiscellaneousExpense` SQLAlchemy model to store the master record for each charge.                            |
| `app/misc_expenses/exceptions.py`       | Defines custom, domain-specific exception classes for handling errors gracefully.                                         |
| `app/misc_expenses/repository.py`       | The Data Access Layer. Encapsulates all direct database queries for creating, listing, and filtering expense records.      |
| `app/misc_expenses/schemas.py`          | Defines the Pydantic schemas for API request validation (`MiscellaneousExpenseCreate`) and response formatting (`MiscellaneousExpenseResponse`). |
| `app/misc_expenses/services.py`         | Contains the core business logic, including generating unique expense IDs and orchestrating the immediate posting to the Centralized Ledger. |
| `app/bpm_flows/misc_expense/flows.py`   | Implements the single-step BPM workflow, linking the UI actions (data entry) to the backend service logic (processing).      |
| `app/misc_expenses/router.py`           | The API layer. Exposes endpoints for creating the BPM case, listing expenses, and exporting data.                         |

### 4. Workflow & Data Flow

The workflow is a simple, single-step process designed for quick and efficient data entry by front-desk or finance staff.

1.  **Initiation**:
    *   A user clicks "Create Misc Expenses" from the main menu.
    *   The frontend calls `POST /payments/miscellaneous-expenses/create-case`.
    *   The `BPMService` creates a new case with the prefix `MISCEXP` and returns the `case_no`, directing the user to the creation screen.

2.  **Data Entry & Submission**:
    *   On the "Create Driver Loans" screen (which is reused for this workflow), the user searches for a driver by TLC License, Medallion, or VIN.
    *   The frontend calls the BPM fetch endpoint `GET /bpm/case/{case_no}/MISCEXP-001` with the search parameters.
    *   The `fetch_driver_and_lease_for_expense` function runs, validating that the driver has an active lease and returning their details.
    *   The user fills in the "Enter Expense Information" form (Category, Amount, etc.) and clicks "Create Miscellaneous Expense".

3.  **Confirmation & Posting**:
    *   The frontend displays a confirmation modal with the expense details.
    *   Upon confirmation, the frontend calls `POST /bpm/case/{case_no}` with `step_id: "MISCEXP-001"` and a data payload matching the `MiscellaneousExpenseCreate` schema.
    *   The `process_misc_expense_creation` function is triggered:
        *   It validates the incoming data.
        *   It calls the `MiscellaneousExpenseService.create_misc_expense` method.
        *   Inside the service, the core logic executes:
            1.  A unique `expense_id` (e.g., `MISC-2025-00123`) is generated.
            2.  The `LedgerService.create_obligation()` method is called. This is an **atomic transaction** that creates both the `DEBIT` record in `Ledger_Postings` and opens a new record in `Ledger_Balances`.
            3.  The master `MiscellaneousExpense` record is saved to the database with a reference to the ledger posting.
            4.  The BPM case is immediately marked as "Closed".

4.  **Completion**:
    *   The API returns a success message, and the UI displays the "Miscellaneous Expenses Creation Successful" modal. The charge is now live in the driver's ledger balance, ready to be recovered in the next DTR.

### 5. Ledger Integration Details

The financial integration is direct, immediate, and adheres to the principles of the Centralized Ledger.

| Event                     | Trigger                                          | Ledger Posting Logic                                                                                                                                                                                                                            | Example Entry                                                                                                       |
| :------------------------ | :----------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------ |
| **Expense Creation**      | User confirms creation in the BPM flow.          | • **Debit**: `LedgerService.create_obligation` is called. <br> • A `DEBIT` posting is created with `Category = MISC`. <br> • A new `LedgerBalance` is opened with the full amount of the expense.                                                      | User creates a $50 'Lost Key' charge. <br> Ledger posts a `DEBIT` of $50 with `Category: MISC` and `Reference: MISC-2025-00123`. |
| **Expense Recovery**      | Automated weekly DTR generation.                 | The `LedgerService.apply_weekly_earnings()` function treats the open `LedgerBalance` for this expense like any other debt. It applies driver earnings to it according to the standard payment hierarchy (after Lease, Repairs, Loans, etc.).        | During DTR generation, the $50 balance for the lost key is paid down by the driver's weekly earnings.                 |
| **Reversal / Voiding**    | A finance user voids the expense record.         | `LedgerService.void_posting()` is called on the original `LedgerPosting`. This creates a reversing `CREDIT` entry of the same amount, which automatically neutralizes the `LedgerBalance`, effectively canceling the debt.                       | The $50 charge was a mistake. Voiding it creates a `CREDIT` of $50, which closes the `LedgerBalance` for that charge.      |