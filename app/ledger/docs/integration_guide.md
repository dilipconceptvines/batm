### Centralized Ledger System: Integration Guide

#### 1. Introduction and Guiding Principles

This guide details the process of connecting your existing application modules (Leases, Repairs, Driver Loans, etc.) to the newly implemented Centralized Ledger. The ledger is designed to be the single source of truth for all financial data, and proper integration is critical to maintaining data integrity and auditability.

**The Golden Rule of Ledger Integration:**

All interactions with the ledger **must** go through the `LedgerService`. No other part of the application should ever directly create, modify, or delete records in the `ledger_postings` or `ledger_balances` tables. The service layer contains the atomic, transactional logic that guarantees the integrity of the financial system.

#### 2. Critical Prerequisite: The Asynchronous Refactor

Your existing core business modules (`leases`, `drivers`, `repairs`, `loans`, etc.) are implemented using a synchronous, blocking paradigm. The new `LedgerService` is fully asynchronous (`async`/`await`) to ensure high performance. Directly calling an `async` service from a `sync` function is an anti-pattern that negates performance benefits and can lead to complex bugs.

Therefore, **before integration can begin**, any module that needs to interact with the ledger must first be refactored to be fully asynchronous.

**Refactoring Steps for a Module (e.g., `app/repairs`):**

1.  **Update Routers:**
    *   Change all route functions from `def` to `async def`.
    *   Update the database dependency from `Depends(get_db)` to `Depends(get_async_db)`.
    *   If using the `get_db_with_current_user` dependency, create an asynchronous version of it that uses `get_async_db`.

2.  **Update Services:**
    *   Change all service methods from `def` to `async def`.
    *   Replace all synchronous database calls (`db.query(...)` and `db.execute(...)`) with their asynchronous counterparts: `await self.repo.db.execute(select(...))`.

3.  **Implement the Repository Pattern:**
    *   Create an `app/repairs/repository.py`.
    *   Move all SQLAlchemy query logic from the service into this new `RepairRepository`. All repository methods must be `async`.
    *   Inject the `RepairRepository` into the `RepairService`.

This refactoring is not optional; it is the foundational step required for a stable and performant integration with the Centralized Ledger.

#### 3. Core Integration Patterns

Once a module is asynchronous, you can integrate it with the ledger by calling methods on the `LedgerService`. The integration is event-driven, meaning that whenever a business event with a financial impact occurs, the corresponding service calls the ledger.

**Pattern 1: Creating a New Obligation (Debit)**

This is the most common integration pattern. It is used when a new charge is created against a driver.

*   **Trigger:** A new financial obligation is confirmed in a source system. Examples:
    *   A weekly lease installment becomes due.
    *   A new repair invoice is created and a payment plan is generated.
    *   A new driver loan is issued.
    *   A miscellaneous charge (e.g., "Lost Key Fee") is created.
    *   A TLC/PVB ticket is imported and assigned to a driver.
*   **Action:** Call the `ledger_service.create_obligation()` method.
*   **Context:** This call should be made from within the service layer of the source module (e.g., `RepairService`, `LoanService`) immediately after the source record (e.g., `RepairInvoice`, `LoanInstallment`) is successfully saved to the database.

**Concrete Example: Integrating a New Vehicle Repair Invoice**

Imagine you are implementing the "Create Repair Invoice" feature.

**_Before Ledger Integration (Old Synchronous Code):_**

```python
# app/repairs/services.py (Old Version)

class RepairService:
    def create_repair_invoice(self, db: Session, repair_data: dict) -> RepairInvoice:
        # ... validation logic ...
        new_invoice = RepairInvoice(**repair_data)
        db.add(new_invoice)
        db.commit()
        
        # Now generate and save installments
        installments = self._generate_installments(new_invoice)
        for inst in installments:
            db.add(inst)
        db.commit()
        
        return new_invoice
```

**_After Ledger Integration (New Asynchronous Code):_**

```python
# app/repairs/services.py (New Version)

from app.ledger.services import LedgerService
from app.ledger.models import PostingCategory

class RepairService:
    def __init__(self, repo: RepairRepository, ledger_service: LedgerService):
        self.repo = repo
        self.ledger_service = ledger_service

    async def create_repair_invoice(self, repair_data: dict) -> RepairInvoice:
        # ... validation logic ...
        
        # Step 1: Create the source records in the repair module
        new_invoice = await self.repo.create_invoice(repair_data)
        installments = self._generate_installments(new_invoice)
        await self.repo.create_installments(installments)

        # Step 2: Post each installment as an obligation to the ledger
        for inst in installments:
            await self.ledger_service.create_obligation(
                category=PostingCategory.REPAIR,
                amount=inst.amount,
                reference_id=str(inst.id),  # The unique ID of the source installment record
                driver_id=new_invoice.driver_id,
                lease_id=new_invoice.lease_id,
                vehicle_id=new_invoice.vehicle_id
            )
            
        return new_invoice
```

**Pattern 2: Applying an Ad-Hoc Payment (Credit)**

This pattern is used for driver-initiated payments made outside the DTR cycle.

*   **Trigger:** A cashier records an "Interim Payment" (Cash, Check, or ACH).
*   **Action:** Call the `ledger_service.apply_interim_payment()` method.
*   **Context:** This will be triggered from a new API endpoint for interim payments. The frontend will first fetch a driver's open balances (`LedgerBalance` records) and allow the cashier to specify how much of the total payment to allocate to each one.

**Concrete Example: Interim Payment Endpoint**

```python
# app/ledger/router.py (Hypothetical Endpoint)

@router.post("/interim-payment", status_code=status.HTTP_201_CREATED)
async def record_interim_payment(
    payload: InterimPaymentRequest, # A Pydantic model for the request
    ledger_service: LedgerService = Depends(),
):
    # payload would contain:
    # - driver_id
    # - lease_id
    # - total_payment_amount
    # - payment_method
    # - allocations: {"REF-ID-1": 50.00, "REF-ID-2": 25.50}
    
    ledger_service.apply_interim_payment(
        payment_amount=payload.total_payment_amount,
        allocations=payload.allocations,
        driver_id=payload.driver_id,
        lease_id=payload.lease_id,
        payment_method=payload.payment_method
    )
    
    return {"message": "Interim payment successfully applied."}
```

**Pattern 3: Reversing an Incorrect Posting (Void)**

This pattern is used for correcting errors.

*   **Trigger:** A user with the correct permissions identifies an incorrect ledger posting and clicks a "Void" button in the UI.
*   **Action:** Call the `ledger_service.void_posting()` method.
*   **Context:** This is handled by the `POST /ledger/postings/{posting_id}/void` endpoint already implemented in the ledger's router. The frontend will call this endpoint, providing the `posting_id` of the entry to be voided and a reason in the request body.

**Pattern 4: Handling Automated Earnings (Credit)**

This pattern is for automated, system-level credits.

*   **Trigger:** A scheduled Celery task that fetches earnings data (e.g., from CURB).
*   **Action:**
    1.  The task first creates a single `CREDIT` posting of `category=EARNINGS` for the total weekly earnings amount for a driver.
    2.  A separate, subsequent task (`apply_earnings_for_dtr_task`) then calls `ledger_service.apply_weekly_earnings()`. This service method fetches all open balances for the driver and applies the earnings amount according to the strict priority hierarchy (Taxes -> EZPass -> Lease, etc.).

This process is handled entirely by the Celery tasks defined in `app/ledger/tasks.py`.

#### 5. Data Flow for DTR and Reporting

The documentation specifies that DTRs and other reports are derived *from* the ledger. The integration flow for this is read-only.

1.  **Finalize Weekly State:** The scheduled Celery tasks (`post_scheduled_installments_task` and `apply_earnings_for_dtr_task`) run every Sunday morning. They post all new debits (installments) and apply all new credits (earnings), finalizing the ledger's state for the preceding week.
2.  **Generate DTR Snapshot:** A DTR generation service (which you will need to build) queries the `LedgerBalance` table *after* the weekly tasks have completed. It aggregates the balances by category for each driver/lease to build the DTR.
3.  **Store DTR:** The generated DTR report is stored as a static record. **Crucially, the DTR generation process does not write to the ledger.** It is a read-only snapshot.

#### 6. Summary of Integration Points

| Module / Process         | Triggering Event                                | Ledger Service Method Called                | Entry Type |
| ------------------------ | ----------------------------------------------- | ------------------------------------------- | ---------- |
| **Leases**               | Lease installment becomes due (via Celery task) | `create_obligation()`                       | `DEBIT`      |
| **Driver Loans**         | Loan installment becomes due (via Celery task)  | `create_obligation()`                       | `DEBIT`      |
| **Vehicle Repairs**      | Repair installment becomes due (via Celery task)| `create_obligation()`                       | `DEBIT`      |
| **Misc. Expenses**       | User creates a new miscellaneous charge         | `create_obligation()`                       | `DEBIT`      |
| **Tickets/Violations**   | A new ticket (TLC/PVB) is imported/entered      | `create_obligation()`                       | `DEBIT`      |
| **Lease Cancellation**   | A lease is terminated early by a user         | `create_obligation()`                       | `DEBIT`      |
| **Interim Payments**     | Cashier records a payment from a driver       | `apply_interim_payment()`                   | `CREDIT`     |
| **CURB Earnings**        | CURB API data is fetched (via Celery task)      | A service that creates an `EARNINGS` posting| `CREDIT`     |
| **DTR Generation**       | Weekly DTR process runs (via Celery task)       | `apply_weekly_earnings()`                   | `CREDIT`     |
| **Error Correction**     | User voids an incorrect posting               | `void_posting()`                              | `DEBIT` or `CREDIT` |

By following this guide, you can systematically and safely integrate your existing modules with the new Centralized Ledger, ensuring a robust, auditable, and performant financial core for your application.