### app/pvb/services.py

import csv
import io
from datetime import datetime, time, timedelta, date , timezone
from decimal import Decimal
from typing import Dict, List, Optional

from celery import shared_task
from sqlalchemy.orm import Session

from app.core.db import SessionLocal
from app.curb.models import CurbTrip
from app.pvb.exceptions import (
    PVBAssociationError,
    PVBCSVParseError,
    PVBError,
    PVBImportInProgressError,
    PVBLedgerPostingError,
    PVBValidationError,
)
from app.pvb.models import (
    PVBImport,
    PVBImportStatus,
    PVBSource,
    PVBViolation,
    PVBViolationStatus,
)
from app.pvb.repository import PVBRepository
from app.ledger.models import PostingCategory
from app.ledger.services import LedgerService
from app.utils.logger import get_logger
from app.vehicles.models import VehicleRegistration
from app.utils.general import parse_custom_time , clean_value

logger = get_logger(__name__)

# In-memory flag to prevent concurrent imports. A distributed lock (Redis) is recommended for multi-worker environments.
IMPORT_IN_PROGRESS_FLAG = False


class PVBService:
    """
    Service layer for PVB violations, handling CSV imports, manual creation,
    association logic, and ledger posting.
    """

    def __init__(self, db: Session):
        self.db = db
        self.repo = PVBRepository(db)

    def process_uploaded_csv(self, file_stream: io.BytesIO, file_name: str, user_id: int):
        """
        Parses an uploaded PVB CSV, creates an import record, saves raw transaction
        data, and triggers the asynchronous association task.
        """
        global IMPORT_IN_PROGRESS_FLAG
        if IMPORT_IN_PROGRESS_FLAG:
            raise PVBImportInProgressError()

        IMPORT_IN_PROGRESS_FLAG = True
        import_record = None
        try:
            logger.info(f"Starting PVB CSV import for file: {file_name}")
            content = file_stream.read().decode("utf-8-sig")  # Use utf-8-sig to handle potential BOM
            csv_reader = csv.reader(io.StringIO(content))
            header = next(csv_reader)
            rows = list(csv_reader)

            if not rows:
                logger.warning(f"PVB CSV file '{file_name}' is empty.")
                return {"message": "File is empty, no violations were imported."}

            import_record = self.repo.create_import_record(file_name, len(rows))

            violations_to_insert = []
            failed_rows_count = 0
            faild_reasons = []
            inserted_summons = set()

            for i, row in enumerate(rows):
                try:
                    if len(row) < 29:
                        raise ValueError(f"Expected at least 29 columns, but got {len(row)}")
                    
                    issue_date_str = row[6].strip() if row[6] else None
                    issue_time_str = row[7].strip() if row[7] else None

                    issue_time_str = parse_custom_time(issue_time_str)

                    summons = clean_value(row[4])

                    exiting_summaons = self.repo.get_violation_by_summons(summons) if summons else None

                    if exiting_summaons:
                        logger.info(f"Skipping duplicate summons {summons} at row {i+2} in {file_name}")
                        failed_rows_count += 1
                        faild_reasons.append(f"Row {i+2}: Duplicate summons {summons}")
                        continue

                    if summons in inserted_summons:
                        logger.info(f"Skipping duplicate summons {summons} at row {i+2} in {file_name}")
                        failed_rows_count += 1
                        faild_reasons.append(f"Row {i+2}: Duplicate summons {summons}")
                        continue

                    inserted_summons.add(summons)

                    def parse_date(date_str: str) -> Optional[date]:
                        try:
                            if not date_str:
                                return None
                            date_str = date_str.strip()

                            for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
                                try:
                                    return datetime.strptime(date_str, fmt).date()
                                except ValueError:
                                    continue
                            raise ValueError("Invalid date format")
                        except Exception as e:
                            raise ValueError(f"Error parsing date '{date_str}': {e}")
                        
                    issue_date = parse_date(issue_date_str) if issue_date_str else None
                    issue_time = issue_time_str if issue_time_str else None

                    fine = Decimal(row[14] or "0")
                    processing_fee = fine * Decimal("0.025")
                    amount_due = Decimal(row[20] or "0") + processing_fee
            
                    violation_data = {
                        "import_id": import_record.id,
                        "source": PVBSource.CSV_IMPORT,
                        "plate": clean_value(row[0]),
                        "state": clean_value(row[1]),
                        "type": clean_value(row[2]),
                        "is_terminated": clean_value(row[3] , True),
                        "summons": summons,
                        "non_program": clean_value(row[5] , True),
                        "issue_date": issue_date,
                        "issue_time": issue_time,
                        "fine": fine,
                        "system_entry_date": parse_date(row[8]),
                        "new_issue": clean_value(row[9] , True),
                        "violation_code": clean_value(row[10]),
                        "hearing_ind": clean_value(row[11]),
                        "penalty_warning": clean_value(row[12]),
                        "judgement": clean_value(row[13] , True),
                        "penalty": Decimal(row[15] or "0"),
                        "interest": Decimal(row[16] or "0"),
                        "reduction": Decimal(row[17] or "0"),
                        "payment": Decimal(row[18] or "0"),
                        "ng_pmt": clean_value(row[19] , True),
                        "processing_fee": processing_fee,
                        "amount_due": amount_due,
                        "violation_country": clean_value(row[21]),
                        "front_or_opp": clean_value(row[22]),
                        "house_number": clean_value(row[23]),
                        "street_name": clean_value(row[24]),
                        "intersect_street": clean_value(row[25]),
                        "geo_location": clean_value(row[26]),
                        "street_code_1": clean_value(row[27]),
                        "street_code_2": clean_value(row[28]),
                        "street_code_3": clean_value(row[29]),
                        "created_by": user_id,
                        "status": PVBViolationStatus.IMPORTED,
                    }
                    violations_to_insert.append(violation_data)
                except (ValueError, IndexError) as e:
                    logger.warning(f"Skipping malformed row {i+2} in {file_name}: {e}. Data: {row}")
                    failed_rows_count += 1
                    faild_reasons.append(f"Row {i+2}: {e}")

            self.repo.bulk_insert_violations(violations_to_insert)
            self.repo.update_import_record_status(
                import_id=import_record.id,
                status=PVBImportStatus.COMPLETED,
                successful=len(violations_to_insert),
                failed=failed_rows_count,
            )
            self.db.commit()

            logger.info(f"Imported {len(violations_to_insert)} records from {file_name}. Triggering association task.")
            self.associate_violations()

            return {
                "message": "File uploaded and import process initiated.",
                "import_id": import_record.id,
                "total_rows": len(rows),
                "imported_records": len(violations_to_insert),
                "failed_rows": failed_rows_count,
                "failure_reasons": faild_reasons,
            }
        except Exception as e:
            self.db.rollback()
            logger.error(f"Fatal error during PVB CSV processing for {file_name}: {e}", exc_info=True)
            if import_record:
                self.repo.update_import_record_status(import_record.id, PVBImportStatus.FAILED, 0, len(rows) if 'rows' in locals() else 0)
                self.db.commit()
            raise PVBError(f"Could not process PVB file: {e}") from e
        finally:
            IMPORT_IN_PROGRESS_FLAG = False

    def associate_violations(self):
        """
        Background task logic to associate imported PVB violations with drivers/leases
        by cross-referencing vehicle and CURB trip data.
        """
        logger.info("Starting PVB violation association task.")
        violations_to_process = self.repo.get_violations_by_status(PVBViolationStatus.IMPORTED)
        
        successful_count, failed_count = 0, 0

        for violation in violations_to_process:
            updates = {"status": PVBViolationStatus.ASSOCIATION_FAILED}
            try:
                # 1. Find Vehicle by Plate
                vehicle_reg = self.db.query(VehicleRegistration).filter(
                    VehicleRegistration.plate_number.ilike(f"%{violation.plate}%")
                ).first()
                if not vehicle_reg or not vehicle_reg.vehicle:
                    raise PVBAssociationError(violation.summons, f"No vehicle found for plate '{violation.plate}'")
                
                vehicle = vehicle_reg.vehicle
                updates["vehicle_id"] = vehicle.id

                # 2. Find CURB trip to identify Driver and Lease
                violation_datetime = datetime.combine(violation.issue_date, violation.issue_time or time.min)
                time_buffer = timedelta(hours=2) # A wider buffer for violations
                trip_start = violation_datetime - time_buffer
                trip_end = violation_datetime + time_buffer

                curb_trip = self.db.query(CurbTrip).filter(
                    CurbTrip.vehicle_id == vehicle.id,
                    CurbTrip.start_time <= trip_end,
                    CurbTrip.end_time >= trip_start
                ).order_by(CurbTrip.start_time.desc()).first()

                if not curb_trip or not curb_trip.driver_id:
                    raise PVBAssociationError(violation.summons, f"No active CURB trip found for vehicle {vehicle.id} around {violation_datetime}")

                updates.update({
                    "driver_id": curb_trip.driver_id,
                    "lease_id": curb_trip.lease_id,
                    "medallion_id": curb_trip.medallion_id,
                    "status": PVBViolationStatus.ASSOCIATED,
                    "failure_reason": None
                })
                successful_count += 1
                
            except PVBAssociationError as e:
                updates["failure_reason"] = e.reason
                failed_count += 1
                logger.warning(f"Association failed for summons {violation.summons}: {e.reason}")
            except Exception as e:
                updates["failure_reason"] = f"An unexpected error occurred: {str(e)}"
                failed_count += 1
                logger.error(f"Error associating summons {violation.summons}: {e}", exc_info=True)
            finally:
                self.repo.update_violation(violation.id, updates)
        
        self.db.commit()
        logger.info(f"Association task finished. Processed: {len(violations_to_process)}, Successful: {successful_count}, Failed: {failed_count}")

        return {"processed": len(violations_to_process), "successful": successful_count, "failed": failed_count}

    def post_violations_to_ledger(self, ledger_service: LedgerService):
        """
        Posts successfully associated PVB violations to the Centralized Ledger.
        """
        logger.info("Starting task to post PVB violations to ledger.")
        violations_to_post = self.repo.get_violations_by_status(PVBViolationStatus.ASSOCIATED)

        posted_count, failed_count = 0, 0

        for violation in violations_to_post:
            updates = {"status": PVBViolationStatus.POSTING_FAILED}
            try:
                if not all([violation.driver_id, violation.lease_id, violation.amount_due > 0]):
                    raise PVBLedgerPostingError(violation.summons, "Missing required driver, lease, or positive amount due.")

                ledger_service.create_obligation(
                    category=PostingCategory.PVB,
                    amount=violation.amount_due,
                    reference_id=violation.summons,
                    driver_id=violation.driver_id,
                    lease_id=violation.lease_id,
                    vehicle_id=violation.vehicle_id,
                    medallion_id=violation.medallion_id,
                )
                
                updates.update({
                    "status": PVBViolationStatus.POSTED_TO_LEDGER,
                    "failure_reason": None,
                    "posting_date": datetime.utcnow()
                })
                posted_count += 1
            except Exception as e:
                updates["failure_reason"] = f"Ledger service error: {str(e)}"
                failed_count += 1
                logger.error(f"Failed to post PVB summons {violation.summons} to ledger: {e}", exc_info=True)
            finally:
                self.repo.update_violation(violation.id, updates)

        self.db.commit()
        logger.info(f"Ledger posting for PVB finished. Posted: {posted_count}, Failed: {failed_count}")
        return {"posted": posted_count, "failed": failed_count}

    def create_manual_violation(self, case_no: str, violation_data: dict, user_id: int) -> PVBViolation:
        """
        Creates a PVB violation from the manual entry (BPM) workflow.
        """
        try:
            summons = violation_data.get("summons")
            if not summons:
                raise PVBValidationError("Summons number is required.")

            if self.repo.get_violation_by_summons(summons):
                raise PVBValidationError(f"A violation with summons number '{summons}' already exists.")

            new_violation = PVBViolation(
                source=PVBSource.MANUAL_ENTRY,
                case_no=case_no,
                created_by=user_id,
                **violation_data
            )
            self.db.add(new_violation)
            self.db.flush() # Let the service commit
            
            self.manual_post_to_ledger([new_violation.id])
            logger.info(f"Manual PVB violation created with summons {summons} for case {case_no}.")
            return new_violation
        except Exception as e:
            logger.error(f"Error creating manual PVB violation: {e}", exc_info=True)
            raise

    def manual_post_to_ledger(self, transaction_ids: List[int] , all_transactions: bool = False) -> dict:
        """
        Manually post EZPass transactions to the centralized ledger.
        Used to force posting of ASSOCIATED transactions.
        """
        from app.ledger.services import LedgerService
        from app.ledger.repository import LedgerRepository

        logger.info("Manual posting of transactions to ledger", transactions_count=len(transaction_ids))

        ledger_repo = LedgerRepository(self.db)
        ledger_service = LedgerService(ledger_repo)
        success_count = 0
        failed_count = 0
        errors = []

        if all_transactions:
            transactions = self.repo.get_violations_by_status(PVBViolationStatus.ASSOCIATED)
            for transaction in transactions:
                if not all([transaction.driver_id, transaction.lease_id, transaction.amount_due > 0]):
                    errors.append({
                        "transaction_id": transaction.id,
                        "error": "Missing required fields"
                    })
                    failed_count += 1
                    continue
                
                # Post to ledger
                ledger_service.create_obligation(
                    category=PostingCategory.PVB,
                    amount=transaction.amount_due,
                    reference_id=transaction.summons,
                    driver_id=transaction.driver_id,
                    lease_id=transaction.lease_id,
                    vehicle_id=transaction.vehicle_id,
                    medallion_id=transaction.medallion_id,
                )

                # Update transaction status
                updates = {
                    "status": PVBViolationStatus.POSTED_TO_LEDGER,
                    "failure_reason": None,
                    "posting_date": datetime.now(timezone.utc)
                }
                self.repo.update_violation(transaction.id, updates)
                success_count += 1
                logger.info("Transaction posted to ledger", transaction_id=transaction.id)

        else:

            for txn_id in transaction_ids:
                try:
                    transaction = self.repo.get_violation_by_id(txn_id)
                    if not transaction:
                        errors.append({
                            "transaction_id": txn_id,
                            "error": "Transaction not found"
                        })
                        failed_count += 1
                        continue

                    # Validate transaction can be posted
                    if transaction.status == PVBViolationStatus.POSTED_TO_LEDGER:
                        errors.append({
                            "transaction_id": txn_id,
                            "error": "Already posted to ledger"
                        })
                        failed_count += 1
                        continue

                    if transaction.status != PVBViolationStatus.ASSOCIATED:
                        errors.append({
                            "transaction_id": txn_id,
                            "error": f"Cannot post - transaction status is {transaction.status.value}"
                        })
                        failed_count += 1
                        continue

                    if not all([transaction.driver_id, transaction.lease_id, transaction.amount_due > 0]):
                        errors.append({
                            "transaction_id": txn_id,
                            "error": "Missing required fields (driver_id, lease_id, or valid amount)"
                        })
                        failed_count += 1
                        continue

                    # Post to ledger
                    ledger_service.create_obligation(
                        category=PostingCategory.PVB,
                        amount=transaction.amount_due,
                        reference_id=transaction.summons,
                        driver_id=transaction.driver_id,
                        lease_id=transaction.lease_id,
                        vehicle_id=transaction.vehicle_id,
                        medallion_id=transaction.medallion_id,
                    )

                    # Update transaction status
                    updates = {
                        "status": PVBViolationStatus.POSTED_TO_LEDGER,
                        "failure_reason": None,
                        "posting_date": datetime.now(timezone.utc)
                    }
                    self.repo.update_violation(transaction.id, updates)
                    success_count += 1
                    logger.info("Transaction posted to ledger", transaction_id=transaction.id)

                except Exception as e:
                    # Update transaction with error
                    if transaction:
                        self.repo.update_violation(transaction.id, {
                            "status": PVBViolationStatus.POSTING_FAILED,
                            "failure_reason": f"Manual posting error: {str(e)}"
                        })

                    errors.append({
                        "transaction_id": txn_id,
                        "error": str(e)
                    })
                    failed_count += 1
                    logger.error(f"Failed to post transaction {txn_id}: {e}", exc_info=True)

        self.db.commit()

        return {
            "success_count": success_count,
            "failed_count": failed_count,
            "errors": errors,
            "message": f"Successfully posted {success_count} transactions, {failed_count} failed."
        }

    def reassign_transactions(
            self, transaction_ids: List[int], new_driver_id: int, new_lease_id: int,
            new_medallion_id: Optional[int] = None, new_vehicle_id: Optional[int] = None
        ) -> dict:
            """
            Reassign EZPass transactions from one driver to another.
            This allows correcting incorrect associations.
            Can only reassign transactions that haven't been posted to ledger.
            """
            from app.drivers.models import Driver
            from app.leases.models import Lease

            logger.info("Reassigning transactions for driver", transactions_count=len(transaction_ids), driver_id=new_driver_id)

            # Validate new driver and lease
            new_driver = self.db.query(Driver).filter(Driver.id == new_driver_id).first()
            if not new_driver:
                raise PVBError(f"New driver with ID {new_driver_id} not found.")
            
            new_lease = self.db.query(Lease).filter(Lease.id == new_lease_id).first()
            if not new_lease:
                raise PVBError(f"New lease with ID {new_lease_id} not found.")
            
            # Validate new lease is an active lease for the new driver
            lease_drivers = new_lease.lease_driver
            is_primary_driver = False
            for ld in lease_drivers:
                if ld.driver_id == new_driver.driver_id and not ld.is_additional_driver:
                    is_primary_driver = True
                    break

            if not is_primary_driver:
                raise PVBError(f"Lease {new_lease_id} does not belong to the driver {new_driver_id}")
            
            success_count = 0
            failed_count = 0
            errors = []

            for txn_id in transaction_ids:
                try:
                    transaction = self.repo.get_violation_by_id(txn_id)
                    if not transaction:
                        errors.append({
                            "transaction_id": txn_id,
                            "error": "Transaction not found"
                        })
                        failed_count += 1
                        continue

                    # Cannot reassign if already posted to ledger
                    if transaction.status == PVBViolationStatus.POSTED_TO_LEDGER:
                        errors.append({
                            "transaction_id": txn_id,
                            "error": "Cannot reassign - transaction already posted to ledger"
                        })
                        failed_count += 1
                        continue

                    # Store old assignment for logging
                    old_driver_id = transaction.driver_id
                    old_lease_id = transaction.lease_id

                    # Reassign to new driver/lease
                    updates = {
                        "driver_id": new_driver_id,
                        "lease_id": new_lease_id,
                        "medallion_id": new_medallion_id or new_lease.medallion_id,
                        "vehicle_id": new_vehicle_id or new_lease.vehicle_id,
                        "status": PVBViolationStatus.ASSOCIATED,
                        "failure_reason": None
                    }

                    self.repo.update_violation(transaction.id, updates)
                    success_count += 1
                    logger.info(
                        f"Transaction {transaction.id} reassigned from "
                        f"driver {old_driver_id}/lease {old_lease_id} to "
                        f"driver {new_driver_id}/lease {new_lease_id}"
                    )

                    if success_count > 0:
                        self.post_violations_to_ledger()
                        
                except Exception as e:
                    errors.append({
                        "transaction_id": txn_id,
                        "error": str(e)
                    })
                    failed_count += 1
                    logger.error(f"Failed to reassign transaction {txn_id}: {e}", exc_info=True)

            self.db.commit()

            return {
                "success_count": success_count,
                "failed_count": failed_count,
                "errors": errors,
                "message": f"Successfully reassigned {success_count} transactions, {failed_count} failed"
            }
    
    def retry_failed_associations(self, transaction_ids: Optional[List[int]] = None) -> dict:
        """
        Retry automatic association logic for failed or specific transactions.
        This uses the SAME association logic as the initial automatic process.
        
        If transaction_ids provided: Only retry those specific transactions
        If transaction_ids is None: Retry ALL ASSOCIATION_FAILED transactions
        
        Business Logic (same as automatic association):
        1. Extract plate number from tag_or_plate
        2. Find Vehicle via plate number
        3. Find CURB trip on that vehicle ±30 minutes of toll time
        4. If found: Associate driver_id, lease_id, medallion_id from CURB trip
        5. Update status to ASSOCIATED or ASSOCIATION_FAILED
        """
        logger.info(f"Retrying association for transactions: {transaction_ids or 'all failed'}")
        
        # Get transactions to retry
        if transaction_ids:
            # Retry specific transactions
            transactions_to_process = [
                self.repo.get_violation_by_id(txn_id) 
                for txn_id in transaction_ids
            ]
            transactions_to_process = [t for t in transactions_to_process if t is not None]
        else:
            # Retry all ASSOCIATION_FAILED transactions
            transactions_to_process = (
                self.db.query(PVBViolation)
                .filter(PVBViolation.status.in_([
                    PVBViolationStatus.ASSOCIATION_FAILED,
                    PVBViolationStatus.IMPORTED
                    ])).all()
                )
        
        if not transactions_to_process:
            return {
                "processed": 0,
                "successful": 0,
                "failed": 0,
                "message": "No transactions to retry association"
            }
        
        successful_count = 0
        failed_count = 0
        
        for trans in transactions_to_process:
            updates = {"status": PVBViolationStatus.ASSOCIATION_FAILED}
            try:
                # 1. Find the vehicle using the plate number (same logic as automatic)
                plate_number_full = trans.plate
                plate_number = plate_number_full.split(' ')[1] if ' ' in plate_number_full else plate_number_full
                
                vehicle_reg = self.db.query(VehicleRegistration).filter(
                    VehicleRegistration.plate_number.ilike(f"%{plate_number}%")
                ).first()

                if not vehicle_reg or not vehicle_reg.vehicle:
                    raise ValueError(trans.id, f"No vehicle found for plate '{plate_number}'")
                
                vehicle = vehicle_reg.vehicle
                updates["vehicle_id"] = vehicle.id

                # 2. Find the corresponding CURB trip to identify the driver
                # Look for a trip within a time window around the toll time
                time_buffer = timedelta(minutes=30)
                trip_start = datetime.combine(trans.issue_date, trans.issue_time)- time_buffer
                trip_end = datetime.combine(trans.issue_date, trans.issue_time)+ time_buffer

                curb_trip = self.db.query(CurbTrip).filter(
                    CurbTrip.vehicle_id == vehicle.id,
                    CurbTrip.start_time <= trip_end,
                    CurbTrip.end_time >= trip_start
                ).order_by(CurbTrip.start_time.desc()).first()

                if not curb_trip or not curb_trip.driver_id:
                    raise ValueError(
                        trans.id, 
                        f"No active CURB trip found for vehicle {vehicle.id} around {trans.issue_date}"
                    )
                
                # SUCCESS - Associate with driver/lease from CURB trip
                updates["driver_id"] = curb_trip.driver_id
                updates["lease_id"] = curb_trip.lease_id
                updates["medallion_id"] = curb_trip.medallion_id
                updates["status"] = PVBViolationStatus.ASSOCIATED
                updates["failure_reason"] = None
                successful_count += 1
                
            except Exception as e:
                updates["failure_reason"] = e
                failed_count += 1
                logger.warning(f"Association retry failed for transaction {trans.id}: {e}")

            except Exception as e:
                updates["failure_reason"] = f"Unexpected error during retry: {str(e)}"
                failed_count += 1
                logger.error(f"Unexpected error retrying transaction {trans.id}: {e}", exc_info=True)

            finally:
                self.repo.update_violation(trans.id, updates)
        
        self.db.commit()
        logger.info(
            f"Association retry finished. Processed: {len(transactions_to_process)}, "
            f"Successful: {successful_count}, Failed: {failed_count}"
        )
        
        # If successful associations exist, trigger posting task
        if successful_count > 0:
            self.post_violations_to_ledger()
        
        return {
            "processed": len(transactions_to_process),
            "successful": successful_count,
            "failed": failed_count,
            "message": f"Retried {len(transactions_to_process)} transactions: {successful_count} succeeded, {failed_count} failed"
        }
    
    def get_pvb_logs(
            self ,
            import_id: Optional[int]=None,
            file_name: Optional[str]=None,
            from_date: Optional[date]=None,
            to_date: Optional[date]=None,
            status: Optional[PVBImportStatus]=None,
            multiple: Optional[bool]=False,
            sort_by: Optional[str] = None,
            sort_order: Optional[str] = None,
            page : Optional[int]= None,
            per_page : Optional[int]= None
    ) -> List[Dict]:
        """
        Retrieve logs for a specific PVB import.
        """
        try:
            logs = self.db.query(PVBImport)

            if import_id:
                logs = logs.filter(PVBImport.id == import_id)
            if file_name:
                logs = logs.filter(PVBImport.file_name.ilike(f"%{file_name}%"))
            if from_date:
                logs = logs.filter(PVBImport.import_timestamp >= datetime.combine(from_date, time.min))
            if to_date:
                logs = logs.filter(PVBImport.import_timestamp <= datetime.combine(to_date, time.max))
            if status:
                logs = logs.filter(PVBImport.status == status)

            # Sorting
            if sort_by and sort_order:
                sort_column = getattr(PVBImport, sort_by, None)
                if sort_column is not None:
                    if sort_order.lower() == "desc":
                        logs = logs.order_by(sort_column.desc())
                    else:
                        logs = logs.order_by(sort_column.asc())
                else:
                    raise ValueError(f"Invalid sort column: {sort_by}")
            else:
                logs = logs.order_by(PVBImport.import_timestamp.desc())

            # Pagination
            if page and per_page:
                logs = logs.offset((page - 1) * per_page).limit(per_page)

            if multiple:
                return [log.to_dict() for log in logs.all()]
            else:
                log = logs.first()
                return log.to_dict() if log else None
        except Exception as e:
            logger.error(f"Error retrieving PVB logs: {e}", exc_info=True)
            raise e

# --- Celery Tasks ---

@shared_task(name="pvb.associate_violations")
def associate_pvb_violations_task():
    """Background task to associate imported PVB violations."""
    logger.info("Executing Celery task: associate_pvb_violations_task")
    db: Session = SessionLocal()
    try:
        service = PVBService(db)
        result = service.associate_violations()
        return result
    except Exception as e:
        logger.error(f"Celery task associate_pvb_violations_task failed: {e}", exc_info=True)
        db.rollback()
        raise
    finally:
        db.close()

@shared_task(name="pvb.post_violations_to_ledger")
def post_pvb_violations_to_ledger_task():
    """Background task to post associated PVB violations to the ledger."""
    logger.info("Executing Celery task: post_pvb_violations_to_ledger_task")
    db: Session = SessionLocal()
    try:
        ledger_service = LedgerService(db)
        pvb_service = PVBService(db)
        result = pvb_service.post_violations_to_ledger(ledger_service)
        return result
    except Exception as e:
        logger.error(f"Celery task post_pvb_violations_to_ledger_task failed: {e}", exc_info=True)
        db.rollback()
        raise
    finally:
        db.close()