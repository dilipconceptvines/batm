### app/tlc/services.py

from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from sqlalchemy.orm import Session

from app.bpm.services import bpm_service
from app.ledger.models import PostingCategory
from app.ledger.services import LedgerService
from app.ledger.repository import LedgerRepository
from app.tlc.exceptions import (
    InvalidTLCActionError,
    TLCValidationError,
    TLCViolationNotFoundError,
)
from app.tlc.models import TLCDisposition, TLCViolation, TLCViolationStatus, TLCViolationType
from app.tlc.repository import TLCRepository
from app.tlc.exceptions import TLCLedgerPostingError
from app.utils.logger import get_logger
from app.core.config import settings

logger = get_logger(__name__)


class TLCService:
    """
    Service layer for managing TLC Violations, including manual creation
    and direct integration with the Centralized Ledger.
    """

    def __init__(self, db: Session):
        self.db = db
        self.repo = TLCRepository(db)
        ledger_repo = LedgerRepository(db)
        self.ledger_service = LedgerService(ledger_repo)

    def create_manual_violation(self, case_no: str, violation_data: dict, user_id: int) -> TLCViolation:
        """
        Creates a new TLC Violation from the manual entry BPM workflow.
        This single operation creates the record and immediately posts it to the ledger.
        """
        try:
            summons = violation_data.get("summons_no")
            if not summons:
                raise TLCValidationError("Summons number is required.")

            if self.repo.get_violation_by_summons(summons):
                raise TLCValidationError(f"A violation with summons number '{summons}' already exists.")

            # Calculate total payable
            total_payable = Decimal(violation_data.get("amount", 0)) + Decimal(violation_data.get("service_fee", settings.tlc_service_fee))

            new_violation = TLCViolation(
                case_no=case_no,
                summons_no=summons,
                plate=violation_data["plate"],
                state=violation_data["state"],
                violation_type=TLCViolationType(violation_data["violation_type"]),
                issue_date=violation_data["issue_date"],
                issue_time=violation_data.get("issue_time"),
                description=violation_data.get("description"),
                amount=Decimal(violation_data["amount"]),
                service_fee=Decimal(violation_data.get("service_fee", settings.tlc_service_fee)),
                total_payable=total_payable,
                driver_payable=total_payable,
                disposition=TLCDisposition(violation_data.get("disposition", "Paid")),
                driver_id=violation_data["driver_id"],
                lease_id=violation_data["lease_id"],
                medallion_id=violation_data["medallion_id"],
                vehicle_id=violation_data["vehicle_id"],
                due_date=violation_data["due_date"],
                note=violation_data.get("note"),
                attachment_document_id=violation_data["attachment_document_id"],
                status=TLCViolationStatus.PENDING,
                created_by=user_id,
            )
            
            created_violation = self.repo.create_violation(new_violation)
            
            # Link to BPM case
            bpm_service.create_case_entity(
                self.db, case_no, "tlc_violation", "id", str(created_violation.id)
            )

            # Immediately post to ledger if disposition is 'Paid' or 'Reduced'
            if created_violation.disposition == TLCDisposition.PAID and created_violation.total_payable > 0:
                self.post_to_ledger(created_violation)

            self.db.commit()
            logger.info(f"Successfully created manual TLC Violation {summons}.")
            return created_violation

        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to create manual TLC violation: {e}", exc_info=True)
            raise InvalidTLCActionError(f"Could not create TLC violation: {e}")

    def update_violation_disposition(self, violation_id: int, new_disposition: TLCDisposition, new_amount: Optional[Decimal] = None) -> TLCViolation:
        """
        Updates the disposition of an existing TLC violation and handles the
        necessary ledger reversals and repostings.
        """
        violation = self.repo.get_violation_by_id(violation_id)
        if not violation:
            raise TLCViolationNotFoundError(f"ID {violation_id}")

        original_disposition = violation.disposition
        original_amount = violation.total_payable

        if original_disposition == new_disposition and (new_amount is None or new_amount == original_amount):
            logger.info("No change in disposition or amount for TLC violation %s.", violation.summons_no)
            return violation

        try:
            # --- Step 1: Reverse the original ledger posting if it exists ---
            if violation.original_posting_id:
                logger.info(f"Reversing original ledger posting {violation.original_posting_id} for summons {violation.summons_no}.")
                reversal_posting = self.ledger_service.void_posting(
                    posting_id=violation.original_posting_id,
                    reason=f"Disposition changed from {original_disposition.value} to {new_disposition.value}"
                )
                violation.reversal_posting_id = reversal_posting.id
                violation.status = TLCViolationStatus.REVERSED

            # --- Step 2: Update the violation record ---
            violation.disposition = new_disposition
            if new_amount is not None:
                violation.total_payable = new_amount
            
            # --- Step 3: Create a new ledger posting if required ---
            if new_disposition in [TLCDisposition.PAID, TLCDisposition.REDUCED] and violation.total_payable > 0:
                self.post_to_ledger(violation, is_update=True)
            else:
                # For "Dismissed" or amount=0, no new posting is needed
                violation.original_posting_id = None
                violation.status = TLCViolationStatus.REVERSED if violation.original_posting_id else TLCViolationStatus.PENDING

            self.db.commit()
            logger.info(f"Successfully updated disposition for summons {violation.summons_no} to {new_disposition.value}.")
            return violation

        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to update disposition for summons {violation.summons_no}: {e}", exc_info=True)
            raise InvalidTLCActionError(f"Could not update disposition: {e}")

    def post_to_ledger(self, violation: TLCViolation, is_update: bool = False):
        """
        Internal method to create an obligation in the ledger for a violation.
        """
        try:
            # create_obligation handles commit on its own, so we don't commit here
            # to allow the caller to manage the transaction.
            balance = self.ledger_service.create_obligation(
                category=PostingCategory.TLC,
                amount=violation.total_payable,
                reference_id=violation.summons_no,
                driver_id=violation.driver_id,
                lease_id=violation.lease_id,
                medallion_id=violation.medallion_id,
                vehicle_id=violation.lease.vehicle_id if violation.lease else None,
            )
            
            # The balance object has a corresponding posting via its reference_id.
            # We need to find that posting to get its ID.
            posting = self.ledger_service.repo.get_posting_by_reference_id(violation.summons_no)

            violation.status = TLCViolationStatus.POSTED
            violation.posting_date = datetime.now(timezone.utc)
            violation.original_posting_id = posting.id if posting else None

            if is_update:
                violation.reversal_posting_id = None # Clear reversal ID on successful reposting

            self.db.add(violation)
            self.db.flush()

            logger.info(f"Posted summons {violation.summons_no} to ledger. Posting ID: {violation.original_posting_id}")

        except Exception as e:
            raise TLCLedgerPostingError(violation.summons_no, str(e)) from e