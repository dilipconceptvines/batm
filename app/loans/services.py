### app/loans/services.py

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import List, Tuple, Dict, Optional
from io import BytesIO

from sqlalchemy.orm import Session

from app.bpm.services import bpm_service
from app.core.db import SessionLocal
from app.ledger.models import PostingCategory
from app.ledger.repository import LedgerRepository
from app.ledger.services import LedgerService
from app.loans.exceptions import (
    InvalidLoanOperationError,
    LoanScheduleGenerationError,
)
from app.loans.models import (
    DriverLoan,
    LoanInstallment,
    LoanInstallmentStatus,
    LoanStatus,
)
from app.loans.repository import LoanRepository
from app.loans.schemas import InstallmentPostingResult
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)


# Same repayment matrix as Vehicle Repairs
REPAYMENT_MATRIX = [
    {"min": 0, "max": 200, "installment": "full"},
    {"min": 201, "max": 500, "installment": 100},
    {"min": 501, "max": 1000, "installment": 200},
    {"min": 1001, "max": 3000, "installment": 250},
    {"min": 3001, "max": float('inf'), "installment": 300},
]

class LoanService:
    """
    Service layer for managing Driver Loans, including creation, schedule generation,
    and integration with the Centralized Ledger.
    """

    def __init__(self, db: Session):
        self.db = db
        self.repo = LoanRepository(db)

    def _generate_next_loan_id(self) -> str:
        """Generates a unique Loan ID in the format DLN-YYYY-###."""
        current_year = datetime.utcnow().year
        last_id_record = self.repo.get_last_loan_id_for_year(current_year)
        
        sequence = 1
        if last_id_record:
            last_sequence = int(last_id_record[0].split('-')[-1])
            sequence = last_sequence + 1
            
        return f"DLN-{current_year}-{str(sequence).zfill(3)}"

    def _get_weekly_principal(self, total_amount: Decimal) -> Decimal:
        """Determines the weekly principal installment based on the repayment matrix."""
        for rule in REPAYMENT_MATRIX:
            if rule["min"] <= total_amount <= rule["max"]:
                if rule["installment"] == "full":
                    return total_amount
                return Decimal(str(rule["installment"]))
        return Decimal("300") # Default for amounts over the max defined

    def create_loan_and_schedule(self, case_no: str, loan_data: dict, user_id: int) -> DriverLoan:
        """
        Creates a new Driver Loan, generates its payment schedule, and moves it to OPEN status.
        This is the main entry point from the BPM flow.
        """
        try:
            loan_id = self._generate_next_loan_id()

            new_loan = DriverLoan(
                loan_id=loan_id,
                driver_id=loan_data["driver_id"],
                lease_id=loan_data["lease_id"],
                medallion_id=loan_data["medallion_id"],
                principal_amount=Decimal(loan_data["loan_amount"]),
                interest_rate=Decimal(loan_data.get("interest_rate", 0)),
                loan_date=datetime.utcnow().date(), # Loan date is today
                start_week=loan_data["start_week"],
                notes=loan_data.get("notes"),
                status=LoanStatus.DRAFT,
                created_by=user_id,
            )
            self.repo.create_loan(new_loan)

            self.generate_payment_schedule(new_loan)

            new_loan.status = LoanStatus.OPEN
            self.db.flush()

            bpm_service.create_case_entity(
                self.db, case_no, "driver_loan", "id", str(new_loan.id)
            )

            # NEW: Generate receipt PDF and store in S3
            try:
                receipt_s3_key, receipt_url = self._generate_and_store_receipt(new_loan)
                new_loan.receipt_s3_key = receipt_s3_key
                new_loan.receipt_url = receipt_url
                logger.info(f"Successfully generated receipt for loan {loan_id} at {receipt_s3_key}")
            except Exception as e:
                logger.error(f"Failed to generate receipt for loan {loan_id}: {e}", exc_info=True)
                # Don't fail the entire loan creation if receipt generation fails
                # The receipt can be regenerated later if needed
            
            self.db.commit()
            logger.info(f"Successfully created and opened Driver Loan {loan_id}.")
            return new_loan
        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to create driver loan: {e}", exc_info=True)
            raise InvalidLoanOperationError(f"Could not create driver loan: {e}")
        
    def _generate_and_store_receipt(self, loan: DriverLoan) -> Tuple[str, str]:
        """
        Generates the loan receipt PDF and stores it in S3.
        
        Returns:
            Tuple[str, str]: (s3_key, presigned_url)
        """
        from app.loans.pdf_service import LoanPdfService
        
        # Generate PDF
        pdf_service = LoanPdfService(self.db)
        pdf_content = pdf_service.generate_receipt_pdf(loan.id)
        
        # Prepare S3 key
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"loan_receipts/{loan.loan_id}/{loan.loan_id}_{timestamp}.pdf"
        
        # Upload to S3
        success = s3_utils.upload_file(
            BytesIO(pdf_content),
            s3_key,
            content_type="application/pdf"
        )
        
        if not success:
            raise Exception("Failed to upload receipt to S3")
        
        # Generate presigned URL
        presigned_url = s3_utils.generate_presigned_url(s3_key, expiration=3600)
        
        return s3_key, presigned_url

    def generate_payment_schedule(self, loan: DriverLoan):
        """
        Calculates and stores the full weekly installment schedule for a loan,
        including principal and interest.
        """
        try:
            total_principal = loan.principal_amount
            if total_principal <= 0:
                raise LoanScheduleGenerationError("Loan principal amount must be positive.")

            weekly_principal_payment = self._get_weekly_principal(total_principal)
            
            remaining_principal = total_principal
            current_start_date = loan.start_week
            last_installment_date = loan.loan_date
            installments = []
            seq = 1

            while remaining_principal > 0:
                principal_due = min(weekly_principal_payment, remaining_principal)
                
                due_date = current_start_date + timedelta(days=6) # Due at the end of the week
                accrual_days = (due_date - last_installment_date).days
                
                # Interest = Outstanding Principal * (Annual Rate / 100) * (Accrual Days / 365)
                interest_due = (remaining_principal * (loan.interest_rate / Decimal("100")) * Decimal(accrual_days)) / Decimal("365")
                interest_due = interest_due.quantize(Decimal("0.01"))

                total_due = principal_due + interest_due

                installment = LoanInstallment(
                    loan_id=loan.id,
                    installment_id=f"{loan.loan_id}-{str(seq).zfill(2)}",
                    week_start_date=current_start_date,
                    week_end_date=due_date,
                    principal_amount=principal_due,
                    interest_amount=interest_due,
                    total_due=total_due,
                    status=LoanInstallmentStatus.SCHEDULED,
                )
                installments.append(installment)

                remaining_principal -= principal_due
                last_installment_date = due_date
                current_start_date += timedelta(weeks=1)
                seq += 1

            self.repo.bulk_insert_installments(installments)
            self.db.flush()
            logger.info(f"Generated {len(installments)} installments for Loan ID {loan.loan_id}.")
        except Exception as e:
            logger.error(f"Error generating payment schedule for {loan.loan_id}: {e}", exc_info=True)
            raise LoanScheduleGenerationError(f"Could not generate payment schedule: {e}")

    def post_due_installments_to_ledger(self):
        """
        Finds all due loan installments and posts them as obligations to the ledger.
        Designed to be called by a scheduled Celery task.
        """
        logger.info("Starting task to post due loan installments to ledger.")
        db = SessionLocal()
        ledger_service = LedgerService(db)
        repo = LoanRepository(db)
        
        posted_count, failed_count = 0, 0
        try:
            installments_to_post = repo.get_due_installments_to_post(datetime.utcnow().date())

            if not installments_to_post:
                logger.info("No due loan installments to post.")
                return {"posted": 0, "failed": 0}

            for installment in installments_to_post:
                try:
                    ledger_service.create_obligation(
                        category=PostingCategory.LOAN,
                        amount=installment.total_due,
                        reference_id=installment.installment_id,
                        driver_id=installment.loan.driver_id,
                        lease_id=installment.loan.lease_id,
                        vehicle_id=installment.loan.vehicle_id,
                        medallion_id=installment.loan.medallion_id,
                    )
                    
                    repo.update_installment(installment.id, {
                        "status": LoanInstallmentStatus.POSTED,
                        "posted_on": datetime.utcnow()
                    })
                    posted_count += 1
                except Exception as e:
                    failed_count += 1
                    reason = f"Ledger service error: {str(e)}"
                    repo.update_installment(installment.id, {"status": LoanInstallmentStatus.POSTED, "failure_reason": reason})
                    logger.error(f"Failed to post loan installment {installment.installment_id} to ledger: {e}", exc_info=True)
            
            db.commit()
            logger.info(f"Loan installment posting task finished. Posted: {posted_count}, Failed: {failed_count}")
            return {"posted": posted_count, "failed": failed_count}
        except Exception as e:
            db.rollback()
            logger.error(f"Fatal error in post_due_installments_to_ledger: {e}", exc_info=True)
            raise
        finally:
            db.close()

    def post_installments_to_ledger(
        self, installment_ids: Optional[List[str]] = None,
        post_all_due: bool = False
    ) -> Tuple[List[InstallmentPostingResult], int, int]:
        """Post loan installments to the ledger either by specific IDs or all due installments."""

        if not installment_ids and not post_all_due:
            raise ValueError("Either provide installment_ids or set post_all_due=True")
        
        ledger_repo = LedgerRepository(self.db)
        ledger_service = LedgerService(ledger_repo)

        results = []
        successful_count = 0
        failed_count = 0

        # Determine which installments to post
        if post_all_due:
            installments_to_post = self.repo.get_due_installments_to_post(
                datetime.now(timezone.utc).date()
            )
            logger.info(f"Found {len(installments_to_post)} due installments to post")
        else:
            # Fetch specific installments by their IDs
            installments_to_post = []
            for installment_id in installment_ids:
                installment = self.repo.get_installment_by_installment_id(installment_id)
                if installment:
                    installments_to_post.append(installment)
                else:
                    results.append(InstallmentPostingResult(
                        installment_id=installment_id,
                        success=False,
                        error_message=f"Installment {installment_id} not found"
                    ))
                    failed_count += 1

        # Process each installment
        for installment in installments_to_post:
            try:
                # Validate installment can be posted
                if installment.status != LoanInstallmentStatus.SCHEDULED:
                    results.append(InstallmentPostingResult(
                        installment_id=installment.installment_id,
                        success=False,
                        error_message=f"Installment status is {installment.status.value}, must be SCHEDULES"
                    ))
                    failed_count += 1
                    continue

                # Validate parent loan is OPEN
                if installment.loan.status != LoanStatus.OPEN:
                    results.append(InstallmentPostingResult(
                        installment_id=installment.installment_id,
                        success=False,
                        error_message="Parent loan status is {installment.loan.status.value}, must be OPEN"
                    ))

                    failed_count += 1
                    continue

                if installment.week_start_date > datetime.utcnow().date():
                    results.append(InstallmentPostingResult(
                        installment_id=installment.installment_id,
                        success=False,
                        error_message=f"Installment date {installment.week_start_date} is in the future"
                    ))

                # Create obligation in ledger
                ledger_posting = ledger_service.create_obligation(
                    category=PostingCategory.LOAN,
                    amount=installment.total_due,
                    reference_id=installment.installment_id,
                    driver_id=installment.loan.driver_id,
                    lease_id=installment.loan.lease_id,
                    # vehicle_id=installment.loan.vehicle_id,
                    medallion_id=installment.loan.medallion_id,
                )

                # Update installment status
                posted_on = datetime.now(timezone.utc)
                self.repo.update_installment(installment.id, {
                    "status": LoanInstallmentStatus.POSTED,
                    "posted_on": posted_on,
                    "ledger_posting_ref": ledger_posting.id
                })

                results.append(InstallmentPostingResult(
                    installment_id=installment.installment_id,
                    success=True,
                    logger_posting_id=ledger_posting.id,
                    posted_on=posted_on
                ))
                successful_count += 1

                logger.info(
                    f"Successfully posted installment {installment.installment_id} "
                    f"to ledger with posting ID {ledger_posting.id}"
                )

            except Exception as e:
                results.append(InstallmentPostingResult(
                    installment_id=installment.installment_id,
                    success=False,
                    error_message=f"Ledger posting error: {str(e)}"
                ))

                failed_count += 1
                logger.error(
                    f"Failed to post installment {installment.installment_id}: {e}",
                    exc_info=True
                )

        # Commit all changes if at least one succeeded
        if successful_count > 0:
            try:
                self.db.commit()
                logger.info(
                    f"Committed {successful_count} installment postings to database"
                )
            except Exception as e:
                self.db.rollback()
                logger.error(f"Failed to commit installment postings: {e}", exc_info=True)
                raise
        
        return results, successful_count, failed_count
    
    def mark_installment_paid(self, installment_id: str) -> None:
        """
        Called by LedgerService when a loan installment's balance reaches $0.
        Updates installment status to PAID and checks if loan should be closed.
        
        Args:
            installment_id: The installment reference ID (e.g., "DLN-2025-001-01")
        """
        try:
            installment = self.repo.get_installment_by_installment_id(installment_id)
            
            if not installment:
                logger.warning(f"Loan installment {installment_id} not found for status update")
                return
            
            # Only update if not already marked as PAID
            if installment.status != LoanInstallmentStatus.PAID:
                self.repo.update_installment(installment.id, {
                    "status": LoanInstallmentStatus.PAID
                })
                
                logger.info(
                    f"Marked loan installment as PAID",
                    installment_id=installment_id,
                    loan_id=installment.loan_id
                )
                
                # Check if all installments are now paid
                self._check_and_close_loan(installment.loan_id)
                
        except Exception as e:
            logger.error(
                f"Error marking loan installment {installment_id} as paid",
                error=str(e),
                exc_info=True
            )
            raise

    def mark_installment_reopened(self, installment_id: str) -> None:
        """
        Called by LedgerService when a payment is voided and balance is reopened.
        Updates installment status back to POSTED and reopens loan if needed.
        
        Args:
            installment_id: The installment reference ID
        """
        try:
            installment = self.repo.get_installment_by_installment_id(installment_id)
            
            if not installment:
                logger.warning(f"Loan installment {installment_id} not found for status update")
                return
            
            # Revert to POSTED status if currently PAID
            if installment.status == LoanInstallmentStatus.PAID:
                self.repo.update_installment(installment.id, {
                    "status": LoanInstallmentStatus.POSTED
                })
                
                logger.info(
                    f"Reverted loan installment to POSTED (payment voided)",
                    installment_id=installment_id,
                    loan_id=installment.loan_id
                )
                
                # Reopen loan if it was closed
                if installment.loan.status == LoanStatus.CLOSED:
                    self.repo.update_loan(installment.loan_id, {
                        "status": LoanStatus.OPEN
                    })
                    logger.info(f"Reopened loan {installment.loan.loan_id}")
                    
        except Exception as e:
            logger.error(
                f"Error reopening loan installment {installment_id}",
                error=str(e),
                exc_info=True
            )
            raise

    def _check_and_close_loan(self, loan_id: int) -> None:
        """
        Check if all installments for a loan are PAID.
        If so, mark the loan as CLOSED.
        
        Args:
            loan_id: The loan primary key
        """
        try:
            loan = self.repo.get_loan_by_id(loan_id)
            
            if not loan or loan.status == LoanStatus.CLOSED:
                return
            
            # Get all installments for this loan
            installments = self.db.query(LoanInstallment).filter(
                LoanInstallment.loan_id == loan_id
            ).all()
            
            # Check if ALL installments are PAID
            if all(inst.status == LoanInstallmentStatus.PAID for inst in installments):
                self.repo.update_loan(loan_id, {
                    "status": LoanStatus.CLOSED
                })
                
                logger.info(
                    f"Closed loan (all installments paid)",
                    loan_id=loan.loan_id,
                    total_installments=len(installments)
                )
                
        except Exception as e:
            logger.error(
                f"Error checking/closing loan {loan_id}",
                error=str(e),
                exc_info=True
            )
            raise
    

        