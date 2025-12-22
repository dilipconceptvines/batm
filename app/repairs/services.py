### app/repairs/services.py

from datetime import datetime, timedelta , date , timezone
from decimal import Decimal
from typing import Optional , List , Tuple
from io import BytesIO

from sqlalchemy.orm import Session

from app.bpm.services import bpm_service
from app.core.db import SessionLocal
from app.ledger.models import PostingCategory
from app.ledger.services import LedgerService
from app.ledger.repository import LedgerRepository
from app.repairs.exceptions import (
    InvalidRepairOperationError,
    PaymentScheduleGenerationError,
)
from app.repairs.models import (
    RepairInstallment,
    RepairInstallmentStatus,
    RepairInvoice,
    RepairInvoiceStatus,
    WorkshopType,
)
from app.utils.s3_utils import s3_utils
from app.repairs.repository import RepairRepository
from app.utils.logger import get_logger
from app.loans.schemas import PostInstallmentResponse , InstallmentPostingResult

logger = get_logger(__name__)

REPAYMENT_MATRIX = [
    {"min": 0, "max": 200, "installment": "full"},
    {"min": 201, "max": 500, "installment": 100},
    {"min": 501, "max": 1000, "installment": 200},
    {"min": 1001, "max": 3000, "installment": 250},
    {"min": 3001, "max": float('inf'), "installment": 300},
]

class RepairService:
    """
    Service layer for managing the lifecycle of vehicle repair invoices and their
    integration with the Centralized Ledger.
    """
    def __init__(self, db: Session):
        self.db = db
        self.repo = RepairRepository(db)

    def _generate_next_repair_id(self) -> str:
        """Generates a new, unique Repair ID in the format RPR-YYYY-#####."""
        current_year = datetime.utcnow().year
        last_id_record = self.repo.get_last_repair_id_for_year(current_year)
        
        sequence = 1
        if last_id_record:
            last_sequence = int(last_id_record[0].split('-')[-1])
            sequence = last_sequence + 1
            
        return f"RPR-{current_year}-{str(sequence).zfill(5)}"

    def _get_weekly_principal(self, total_amount: Decimal) -> Decimal:
        """Determines the weekly installment amount based on the repayment matrix."""
        for rule in REPAYMENT_MATRIX:
            if rule["min"] <= total_amount <= rule["max"]:
                if rule["installment"] == "full":
                    return total_amount
                return Decimal(str(rule["installment"]))
        return Decimal("300")  # Default for amounts over the max defined

    def create_repair_invoice(self, case_no: str, invoice_data: dict, user_id: int) -> RepairInvoice:
        """
        Creates a new Repair Invoice, generates its payment schedule, generates receipt PDF,
        stores it in S3, and moves the invoice to OPEN status.
        
        This is the main entry point from the BPM flow.
        
        NEW: Generates receipt PDF and stores in S3 with presigned URL
        """
        try:
            # 1. Generate unique Repair ID
            repair_id = self._generate_next_repair_id()

            # 2. Create the master invoice record in DRAFT status
            new_invoice = RepairInvoice(
                repair_id=repair_id,
                invoice_number=invoice_data["invoice_number"],
                invoice_date=invoice_data["invoice_date"],
                driver_id=invoice_data["driver_id"],
                lease_id=invoice_data["lease_id"],
                vehicle_id=invoice_data["vehicle_id"],
                medallion_id=invoice_data["medallion_id"],
                workshop_type=WorkshopType(invoice_data["workshop_type"]),
                description=invoice_data.get("notes"),
                total_amount=Decimal(invoice_data["total_amount"]),
                start_week=invoice_data["start_week"],
                status=RepairInvoiceStatus.DRAFT,
                created_by=user_id,
            )
            self.repo.create_invoice(new_invoice)

            # 3. Generate the payment schedule
            self.generate_payment_schedule(new_invoice)

            # 4. Finalize invoice status to OPEN
            new_invoice.status = RepairInvoiceStatus.OPEN
            self.db.flush()

            # 5. Link invoice to the BPM case
            bpm_service.create_case_entity(
                self.db, case_no, "repair_invoice", "id", str(new_invoice.id)
            )
            
            # NEW: 6. Generate receipt PDF and store in S3
            try:
                receipt_s3_key, receipt_url = self._generate_and_store_receipt(new_invoice)
                new_invoice.receipt_s3_key = receipt_s3_key
                new_invoice.receipt_url = receipt_url
                logger.info(f"Successfully generated receipt for repair {repair_id} at {receipt_s3_key}")
            except Exception as e:
                logger.error(f"Failed to generate receipt for repair {repair_id}: {e}", exc_info=True)
                # Don't fail the entire invoice creation if receipt generation fails
                # The receipt can be regenerated later if needed
            
            self.db.commit()
            logger.info(f"Successfully created and opened Repair Invoice {repair_id} with receipt.")
            return new_invoice
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to create repair invoice: {e}", exc_info=True)
            raise InvalidRepairOperationError(f"Could not create repair invoice: {e}") from e
        
    def _generate_and_store_receipt(self, invoice: RepairInvoice) -> Tuple[str, str]:
        """
        Generates the repair receipt PDF and stores it in S3.
        
        Returns:
            Tuple[str, str]: (s3_key, presigned_url)
        """
        from app.repairs.pdf_service import RepairPdfService
        
        # Generate PDF
        pdf_service = RepairPdfService(self.db)
        pdf_content = pdf_service.generate_receipt_pdf(invoice.id)
        
        # Prepare S3 key
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"repair_receipts/{invoice.repair_id}/{invoice.repair_id}_{timestamp}.pdf"
        
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

    def generate_payment_schedule(self, invoice: RepairInvoice):
        """
        Calculates and stores the weekly installment schedule for a given invoice.
        """
        try:
            total_amount = invoice.total_amount
            if total_amount <= 0:
                raise PaymentScheduleGenerationError("Total amount must be positive.")

            weekly_principal = self._get_weekly_principal(total_amount)
            
            remaining_balance = total_amount
            current_start_date = invoice.start_week
            installments = []
            seq = 1

            while remaining_balance > 0:
                installment_amount = min(weekly_principal, remaining_balance)
                
                installment = RepairInstallment(
                    invoice_id=invoice.id,
                    installment_id=f"{invoice.repair_id}-{str(seq).zfill(2)}",
                    week_start_date=current_start_date,
                    week_end_date=current_start_date + timedelta(days=6),
                    principal_amount=installment_amount,
                    status=RepairInstallmentStatus.SCHEDULED,
                )
                installments.append(installment)

                remaining_balance -= installment_amount
                current_start_date += timedelta(weeks=1)
                seq += 1

            self.repo.bulk_insert_installments(installments)
            self.db.flush()
            logger.info(f"Generated {len(installments)} installments for Repair ID {invoice.repair_id}.")
        except Exception as e:
            logger.error(f"Error generating payment schedule for {invoice.repair_id}: {e}", exc_info=True)
            raise PaymentScheduleGenerationError(f"Could not generate payment schedule: {e}")
        
    def closed_repair(self):
        try:
            repairs = self.db.query(RepairInvoice).filter(RepairInvoice.status == RepairInvoiceStatus.OPEN).all()
            for repair in repairs:
                installments = self.db.query(RepairInstallment).filter(RepairInstallment.invoice_id == repair.id).all()
                if all(inst.status == RepairInstallmentStatus.POSTED for inst in installments):
                        repair.status = RepairInvoiceStatus.OPEN
                if all(inst.status == RepairInstallmentStatus.PAID for inst in installments):
                    repair.status = RepairInvoiceStatus.CLOSED
            
            self.db.commit()
            logger.info(f"Closed {len(repairs)} repairs.")
        except Exception as e:
            logger.error(f"Error closing repairs: {e}", exc_info=True)
            raise

    def post_due_installments_to_ledger(
        self ,
        installment_ids:Optional[list[str]]= None,
        post_all_due:bool = False
        )->Tuple[List[InstallmentPostingResult], int, int]:
        """Post loan installments to the ledger either by specific IDs or all due installments."""

        logger.info("Starting task to post due repair installments to ledger.")

        if not installment_ids and not post_all_due:
            raise ValueError("Either provide installment_ids or set post_all_due=True")
        
        db = SessionLocal()
        
        ledger_repo = LedgerRepository(self.db)
        ledger_service = LedgerService(ledger_repo)
        repo = RepairRepository(self.db)
        
        results = []
        posted_count,failed_count = 0, 0
        try:

            if post_all_due:
                # Post for today's date to catch all due installments
                installments_to_post = repo.get_due_installments_to_post(datetime.utcnow().date())
                logger.info(f"Found {len(installments_to_post)} due installments to post")
            else:
                installments_to_post = []
                for installment_id in installment_ids:
                    installment = repo.get_installment_by_installment_id(installment_id)
                    if installment:
                        installments_to_post.append(installment)
                    else:
                        results.append(InstallmentPostingResult(
                        installment_id=installment_id,
                        success=False,
                        error_message=f"Installment {installment_id} not found"
                        ))
                        failed_count += 1


            for installment in installments_to_post:
                try:
                    # Validate installment can be posted
                    if installment.status != RepairInstallmentStatus.SCHEDULED:
                        results.append(InstallmentPostingResult(
                            installment_id=installment.installment_id,
                            success=False,
                            error_message=f"Installment status is {installment.status.value}, must be SCHEDULES"
                        ))
                        failed_count += 1
                        continue

                    if installment.invoice.status != RepairInvoiceStatus.OPEN:
                        results.append(InstallmentPostingResult(
                            installment_id=installment.installment_id,
                            success=False,
                            error_message=f"Parent invoice status is {installment.invoice.status.value}, must be OPEN"
                        ))
                        failed_count += 1
                        continue

                    if installment.week_start_date > datetime.utcnow().date():
                        results.append(InstallmentPostingResult(
                            installment_id=installment.installment_id,
                            success=False,
                            error_message=f"Installment date {installment.week_start_date} is in the future"
                        ))
                        failed_count += 1
                        continue

                    ledger_posting = ledger_service.create_obligation(
                                    category=PostingCategory.REPAIR,
                                    amount=installment.principal_amount,
                                    reference_id=installment.installment_id,
                                    driver_id=installment.invoice.driver_id,
                                    lease_id=installment.invoice.lease_id,
                                    vehicle_id=installment.invoice.vehicle_id,
                                    medallion_id=installment.invoice.medallion_id,
                                )
                    
                    posted_on = datetime.now(timezone.utc)
                    self.repo.update_installment(installment.id, {
                        "status": RepairInstallmentStatus.POSTED,
                        "posted_on": posted_on,
                        "ledger_posting_ref": ledger_posting.id
                    })

                    results.append(InstallmentPostingResult(
                        installment_id=installment.installment_id,
                        success=True,
                        ledger_posting_ref=ledger_posting.id,
                        posted_on=posted_on
                    ))
                    posted_count += 1

                    logger.info(
                        f"Successfully posted installment {installment.installment_id} "
                        f"to ledger with posting ID {ledger_posting.id}"
                    )
                except Exception as e:
                    results.append(InstallmentPostingResult(
                        installment_id=installment.installment_id,
                        success=False,
                        error_message=f"Error posting installment to ledger: {e}"
                    ))
                    failed_count += 1
                    logger.error(f"Error posting installment to ledger: {e}", exc_info=True)


            if posted_count > 0:
                try:
                    self.db.commit()
                    logger.info(
                        f"Committed {posted_count} installment postings to database"
                    )
                except Exception as e:
                    self.db.rollback()
                    logger.error(f"Failed to commit installment postings: {e}", exc_info=True)
                    raise

            self.closed_repair()

            return results , posted_count , failed_count
        
        except Exception as e:
            self.db.rollback()
            logger.error(f"Fatal error in post_due_installments_to_ledger: {e}", exc_info=True)
            raise
        finally:
            self.db.close()

    def get_repair_invoice(self,
                           lookup_id: int = None,
                           repair_id: int = None,
                           vehicle_id: int = None,
                           lease_id: int = None,
                           driver_id: int = None,
                           medallion_id: int = None,
                           invoice_number: str = None,
                           invoice_date: date = None,
                           sort_by: str = None,
                           sort_order: str = None,
                           multiple: bool = False               
                           ) -> RepairInvoice:
        
        """
        Retrieves a Repair Invoice based on various identifiers.
        """

        try:
            query = self.db.query(RepairInvoice)

            if lookup_id:
                query = query.filter(RepairInvoice.id == lookup_id)
            if repair_id:
                query = query.filter(RepairInvoice.repair_id == repair_id)
            if vehicle_id:
                query = query.filter(RepairInvoice.vehicle_id == vehicle_id)
            if lease_id:
                query = query.filter(RepairInvoice.lease_id == lease_id)
            if driver_id:
                query = query.filter(RepairInvoice.driver_id == driver_id)
            if medallion_id:
                query = query.filter(RepairInvoice.medallion_id == medallion_id)
            if invoice_number:
                query = query.filter(RepairInvoice.invoice_number == invoice_number)
            if invoice_date:
                query = query.filter(RepairInvoice.invoice_date == invoice_date)

            if sort_by:
                sort_column = getattr(RepairInvoice, sort_by, None)
                if sort_column is not None:
                    if sort_order and sort_order.lower() == "desc":
                        query = query.order_by(sort_column.desc())
                    else:
                        query = query.order_by(sort_column.asc())
            else:
                query = query.order_by(RepairInvoice.created_on.desc())
            
            if multiple:
                invoices = query.all()
                return invoices
            
            invoice = query.first()
            return invoice
        except Exception as e:
            logger.error(f"Error retrieving repair invoice: {e}", exc_info=True)
            raise InvalidRepairOperationError(f"Could not retrieve repair invoice: {e}") from e
        
    def mark_installment_paid(self, installment_id: str) -> None:
        """
        Called by LedgerService when an installment's balance reaches $0.
        Updates installment status to PAID and checks if invoice should be closed.
        
        Args:
            installment_id: The installment reference ID (e.g., "RPR-2025-001-01")
        """
        try:
            installment = self.repo.get_installment_by_installment_id(installment_id)
            
            if not installment:
                logger.warning(f"Installment {installment_id} not found for status update")
                return
            
            # Only update if not already marked as PAID
            if installment.status != RepairInstallmentStatus.PAID:
                self.repo.update_installment(installment.id, {
                    "status": RepairInstallmentStatus.PAID
                })
                
                logger.info(
                    f"Marked repair installment as PAID",
                    installment_id=installment_id,
                    invoice_id=installment.invoice_id
                )
                
                # Check if all installments are now paid
                self._check_and_close_invoice(installment.invoice_id)
                
        except Exception as e:
            logger.error(
                f"Error marking installment {installment_id} as paid",
                error=str(e),
                exc_info=True
            )
            raise

    def mark_installment_reopened(self, installment_id: str) -> None:
        """
        Called by LedgerService when a payment is voided and balance is reopened.
        Updates installment status back to POSTED and reopens invoice if needed.
        
        Args:
            installment_id: The installment reference ID
        """
        try:
            installment = self.repo.get_installment_by_installment_id(installment_id)
            
            if not installment:
                logger.warning(f"Installment {installment_id} not found for status update")
                return
            
            # Revert to POSTED status if currently PAID
            if installment.status == RepairInstallmentStatus.PAID:
                self.repo.update_installment(installment.id, {
                    "status": RepairInstallmentStatus.POSTED
                })
                
                logger.info(
                    f"Reverted repair installment to POSTED (payment voided)",
                    installment_id=installment_id,
                    invoice_id=installment.invoice_id
                )
                
                # Reopen invoice if it was closed
                if installment.invoice.status == RepairInvoiceStatus.CLOSED:
                    self.repo.update_invoice(installment.invoice_id, {
                        "status": RepairInvoiceStatus.OPEN
                    })
                    logger.info(f"Reopened repair invoice {installment.invoice.repair_id}")
                    
        except Exception as e:
            logger.error(
                f"Error reopening installment {installment_id}",
                error=str(e),
                exc_info=True
            )
            raise

    def _check_and_close_invoice(self, invoice_id: int) -> None:
        """
        Check if all installments for an invoice are PAID.
        If so, mark the invoice as CLOSED.
        
        Args:
            invoice_id: The invoice primary key
        """
        try:
            invoice = self.repo.get_invoice_by_id(invoice_id)
            
            if not invoice or invoice.status == RepairInvoiceStatus.CLOSED:
                return
            
            # Get all installments for this invoice
            installments = self.repo.get_installments_by_invoice(invoice_id)
            
            # Check if ALL installments are PAID
            if all(inst.status == RepairInstallmentStatus.PAID for inst in installments):
                self.repo.update_invoice(invoice_id, {
                    "status": RepairInvoiceStatus.CLOSED
                })
                
                logger.info(
                    f"Closed repair invoice (all installments paid)",
                    repair_id=invoice.repair_id,
                    invoice_id=invoice_id,
                    total_installments=len(installments)
                )
                
        except Exception as e:
            logger.error(
                f"Error checking/closing invoice {invoice_id}",
                error=str(e),
                exc_info=True
            )
            raise