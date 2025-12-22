### Centralized Ledger System: Implementation Overview

#### 1. Executive Summary

This document outlines the technical implementation of the Centralized Ledger system within the BAT Connect application. The ledger is designed to be the authoritative financial engine, providing a single source of truth for all transactions related to drivers, vehicles, and medallions.

The implementation adheres to the core principles specified in the requirements documentation:

*   **Immutability:** All financial entries are permanent. Corrections are handled through auditable reversals, never by editing or deleting records.
*   **Event-Based Architecture:** Every business event (e.g., a lease installment becoming due, a repair invoice creation, an interim payment) generates a distinct, normalized posting in the ledger.
*   **Real-Time Balances:** The system is built to provide an instantaneous, accurate view of every outstanding obligation.
*   **Traceability:** Every posting and balance is linked back to its source entity and transaction, ensuring full reconciliation capabilities.
*   **Extensibility:** The design is modular, allowing new types of obligations or earnings to be integrated with minimal changes to the core ledger.

The system is built as a new, fully asynchronous module (`app/ledger`) that serves as the architectural standard for future development and the target for refactoring existing synchronous modules.

#### 2. Architectural Design

The Centralized Ledger is implemented following a modern, layered, and service-oriented architecture to ensure separation of concerns, testability, and maintainability.

*   **Pattern:** `Model -> Repository -> Service -> Router`.
*   **Concurrency:** The entire module is built using a fully **asynchronous** (`async`/`await`) paradigm to ensure non-blocking I/O operations, aligning with the FastAPI framework for maximum performance and scalability.
*   **Dependency Injection:** FastAPI's dependency injection is used throughout to manage dependencies, particularly for providing the database session and injecting the repository layer into the service layer.

#### 3. Core Components (Data Model)

The foundation of the ledger consists of two primary database tables, implemented in `app/ledger/models.py` using modern SQLAlchemy 2.x `Mapped` syntax.

**3.1. `LedgerPosting` (The Immutable Log)**

*   **Purpose:** To serve as a permanent, append-only log of every financial transaction. This table is the ultimate source of truth for auditing.
*   **Key Fields:**
    *   `id`: A UUID serving as the unique `Posting_ID`.
    *   `category`: An `Enum` (`PostingCategory`) that strictly defines the transaction type (Lease, Repair, Loan, etc.).
    *   `amount`: A `Decimal` value. It is **positive for debits** (obligations) and **negative for credits** (payments/earnings).
    *   `entry_type`: An `Enum` (`EntryType`) specifying `DEBIT` or `CREDIT`.
    *   `status`: An `Enum` (`PostingStatus`) to mark an entry as `POSTED` or `VOIDED`.
    *   `reference_id`: A string that provides traceability by linking the posting back to the source record's ID (e.g., a `LeaseSchedule` ID, a `RepairInvoice` ID).
    *   `reversal_for_id`: A foreign key to itself, linking a reversal posting to the original posting it voids.
    *   `driver_id`, `vehicle_id`, `medallion_id`, `lease_id`: Foreign keys to link the posting to all relevant entities for multi-dimensional reporting.

**3.2. `LedgerBalance` (The Real-Time Snapshot)**

*   **Purpose:** To maintain a rolling, real-time snapshot of the outstanding amount for every single obligation. This table is optimized for fast queries on current financial states.
*   **Key Fields:**
    *   `id`: A UUID serving as the unique `Balance_ID`.
    *   `reference_id`: Links this balance record back to the original source obligation (e.g., a specific `RepairInvoice` ID). There is a one-to-one relationship between an obligation and its `LedgerBalance` record.
    *   `original_amount`: An immutable `Decimal` storing the initial amount of the obligation.
    *   `balance`: A mutable `Decimal` representing the current outstanding amount. This is the only field that is regularly updated.
    *   `status`: An `Enum` (`BalanceStatus`) that is `OPEN` until `balance` becomes zero, at which point it transitions to `CLOSED`.
    *   `applied_payment_refs`: A `JSON` field that stores a list of `LedgerPosting` IDs that have been applied to this balance, ensuring full traceability of payments.

#### 4. Component Deep Dive

**4.1. Repository (`app/ledger/repository.py`)**

*   **Purpose:** This class is the sole data access layer for the ledger. It contains all SQLAlchemy queries and is fully asynchronous.
*   **Key Responsibilities:**
    *   Provides `async` methods for creating, retrieving, and updating `LedgerPosting` and `LedgerBalance` records.
    *   Implements complex queries, such as `get_open_balances_for_driver`, which correctly sorts obligations by the strict hierarchical and chronological order required for earnings application.
    *   Implements filtering, sorting, and pagination logic for the list endpoints (`list_postings`, `list_balances`), which eager-loads related entity data to prevent N+1 query performance issues.

**4.2. Service (`app/ledger/services.py`)**

*   **Purpose:** This class contains all business logic and orchestrates repository methods to perform atomic financial operations. It is the only component that other modules in the application should interact with.
*   **Key Responsibilities:**
    *   **`create_obligation`**: An atomic transaction that creates a `DEBIT` posting and a corresponding `OPEN` balance record.
    *   **`apply_interim_payment`**: Handles the logic for ad-hoc payments, creating `CREDIT` postings and updating the specified balances.
    *   **`apply_weekly_earnings`**: Implements the hierarchical allocation logic by fetching correctly ordered open balances from the repository and applying earnings until they are exhausted.
    *   **`void_posting`**: Implements the immutable void process by creating a reversal posting, marking the original as `VOIDED`, and adjusting the corresponding balance.
    *   **`list_postings` & `list_balances`**: Coordinates fetching data from the repository and mapping it to the Pydantic response schemas.

**4.3. Router (`app/ledger/router.py`)**

*   **Purpose:** Exposes the ledger's functionality as a set of secure, well-defined, asynchronous API endpoints.
*   **Key Endpoints & Features:**
    *   `GET /ledger/balances` & `GET /ledger/postings`: Provides paginated, filterable, and sortable lists of balances and postings, fulfilling the UI requirements from the Figma designs.
    *   `POST /ledger/postings/{posting_id}/void`: Allows authorized users to void a transaction.
    *   `GET /ledger/export`: A single endpoint for exporting either postings or balances to Excel or PDF format, utilizing the `ExporterFactory`.
    *   **Stub Data Generation**: Both list endpoints include a `?use_stubs=true` query parameter, which returns realistic mock data for frontend development and testing.

#### 5. Automated Processes (Celery Tasks)

The ledger system relies on scheduled background tasks for its core automated processes, implemented in `app/ledger/tasks.py` and configured in `app/worker/config.py`.

*   **`post_scheduled_installments_task`**: Scheduled to run every Sunday at 05:00 AM. This task will query source systems (like Driver Loans and Repairs) for all installments that are due for the upcoming week and call `ledger_service.create_obligation()` for each one, posting them as debits to the ledger.
*   **`apply_earnings_for_dtr_task`**: Scheduled to run every Sunday at 05:10 AM, immediately after installments are posted. This task calculates total available earnings for each driver and calls `ledger_service.apply_weekly_earnings()` to automatically pay down obligations according to the defined hierarchy. This finalizes the ledger state for the DTR generation.
*   **`process_expired_deposit_holds_task`**: Scheduled to run daily. This task will be responsible for identifying terminated leases where the 30-day deposit hold has expired, automatically applying the deposit to any outstanding fines, and creating a refund transaction for the remainder.

#### 6. Integration with Other Modules

The Centralized Ledger is designed to be the financial core of the application. Other modules act as "source systems" and will integrate with the ledger exclusively through the `LedgerService`.

*   **Obligation Creation:** Modules like `leases`, `repairs`, and `loans` will call `ledger_service.create_obligation()` whenever a new financial obligation is created or an installment becomes due.
*   **Payment Creation:** The `interim_payments` module will call `ledger_service.apply_interim_payment()`. The `curb` integration module will be responsible for creating `EARNINGS` postings.
*   **Prerequisite:** For this integration to be performant and reliable, it is essential that the calling modules (`leases`, `drivers`, etc.) are refactored to be fully asynchronous, enabling them to properly `await` the `async` methods of the `LedgerService`.

This implementation provides a robust, auditable, and scalable foundation for all financial operations in the BAT Connect system, directly fulfilling the requirements laid out in the design documents.