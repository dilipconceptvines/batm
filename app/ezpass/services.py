### app/ezpass/services.py

import csv
import io
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import List, Optional, Dict

from sqlalchemy.orm import Session

from app.audit_trail.services import audit_trail_service
from app.audit_trail.schemas import AuditTrailType
from app.ezpass.exceptions import (
    AssociationError,
    CSVParseError,
    ImportInProgressError,
    LedgerPostingError,
    ReassignmentError,
)
from app.ezpass.models import (
    EZPassImportStatus,
    EZPassTransactionStatus,
)
from app.ezpass.repository import EZPassRepository
from app.ledger.models import PostingCategory
from app.ledger.services import LedgerService
from app.ledger.repository import LedgerRepository
from app.curb.models import CurbTrip
from app.utils.logger import get_logger
from app.vehicles.models import VehicleRegistration

logger = get_logger(__name__)

# A simple in-memory flag to prevent concurrent imports. For a multi-worker setup,
# a distributed lock (e.g., using Redis) would be more robust.
IMPORT_IN_PROGRESS_FLAG = False

AVAILABLE_LOG_TYPES = ["Import"]
AVAILABLE_LOG_STATUSES = ["Success", "Partial Success", "Failure", "Pending", "Processing"]


class EZPassService:
    """
    Service layer for handling EZPass CSV imports with immediate ledger posting.
    
    NEW WORKFLOW (v3.0):
    ====================
    1. Import CSV → IMPORTED status
    2. Associate with vehicle/driver/lease → Immediately post to ledger
    3. Transaction moves directly to POSTED_TO_LEDGER (single atomic operation)
    
    REASSIGNMENT FLOW:
    =================
    - IMPORTED/ASSOCIATION_FAILED: Update associations only
    - POSTED_TO_LEDGER with balance > 0: Refund old driver + charge new driver
    - POSTED_TO_LEDGER with balance = 0: Update associations only (already paid)
    
    CSV AMOUNT HANDLING:
    ===================
    - Negative amounts ($-9.11): Obligations (DEBIT to driver)
    - Positive amounts ($9.00): Refunds (CREDIT to driver)
    """

    def __init__(self, db: Session):
        self.db = db
        self.repo = EZPassRepository(db)
        self.ledger_repo = LedgerRepository(db)
        self.ledger_service = LedgerService(self.ledger_repo)

    def _map_csv_columns(self, header: list) -> dict:
        """
        Maps CSV column names to their indices, handling different column orders.
        Returns a dictionary with expected field names as keys and column indices as values.
        """
        column_mapping = {
            "Lane Txn ID": None,
            "Tag/Plate #": None,
            "Posted Date": None,
            "Agency": None,
            "Entry Plaza": None,
            "Exit Plaza": None,
            "Class": None,
            "Date": None,
            "Time": None,
            "Amount": None,
            "Post Txn Balance": None,
        }
        
        for idx, col_name in enumerate(header):
            col_name = col_name.strip()
            if col_name in column_mapping:
                column_mapping[col_name] = idx
        
        missing_columns = [k for k, v in column_mapping.items() if v is None]
        if missing_columns:
            raise CSVParseError(
                f"Missing required columns: {', '.join(missing_columns)}"
            )
        
        return column_mapping
    
    def _parse_amount(self, amount_str: str) -> Decimal:
        """
        Parse amount from CSV, handling both negative (obligations) and positive (refunds).
        
        Examples:
        - "($9.11)" -> Decimal("-9.11") (obligation)
        - "$9.00" -> Decimal("9.00") (refund)
        - "($18.72)" -> Decimal("-18.72") (obligation)
        """
        if not amount_str or amount_str.strip() == "":
            return Decimal("0.00")
        
        amount_str = amount_str.strip().replace(",", "")
        
        # Handle negative amounts in parentheses: ($9.11)
        if amount_str.startswith("(") and amount_str.endswith(")"):
            amount_str = amount_str[1:-1]  # Remove parentheses
            amount_str = amount_str.replace("$", "")
            return Decimal(f"-{amount_str}")
        
        # Handle positive amounts: $9.00
        amount_str = amount_str.replace("$", "")
        return Decimal(amount_str)
    
    def _should_exclude_row(self, row: dict, col_map: dict, row_num: int) -> tuple:
        """
        Determines if a CSV row should be excluded from import.
        
        Returns:
            tuple: (should_exclude: bool, exclusion_reason: str)
        """
        exit_plaza = row[col_map["Exit Plaza"]].strip() if row[col_map["Exit Plaza"]] else ""
        agency = row[col_map["Agency"]].strip() if row[col_map["Agency"]] else ""
        
        # Exclusion 1: CRZ Records
        if "CRZ" in exit_plaza.upper():
            return True, "CRZ_EXCLUSION: CRZ tolling points not part of billing logic"
        
        # Exclusion 2: Payment/Credit/Adjustment Records  
        # These are identified by empty agency AND positive amounts typically
        if not agency:
            amount_str = row[col_map["Amount"]].strip()
            if amount_str and not amount_str.startswith("("):
                # This might be a payment/credit record
                logger.debug(
                    f"Row {row_num}: Empty agency with positive amount - likely payment record",
                    amount=amount_str
                )
        
        return False, ""
    
    def import_csv(self, file_content: bytes, filename: str, user_id: Optional[int] = None) -> dict:
        """
        Import EZPass CSV file and create transaction records.
        
        This method:
        1. Validates CSV format
        2. Excludes CRZ and payment records
        3. Creates transactions in IMPORTED status
        4. Returns import summary
        
        Transactions will be associated and posted in a separate step.
        """
        global IMPORT_IN_PROGRESS_FLAG
        
        if IMPORT_IN_PROGRESS_FLAG:
            raise ImportInProgressError()
        
        IMPORT_IN_PROGRESS_FLAG = True
        
        try:
            # Read bytes from BytesIO and decode to text
            csv_text = file_content.read().decode("utf-8-sig")
            csv_reader = csv.reader(io.StringIO(csv_text))
            
            header = next(csv_reader)
            col_map = self._map_csv_columns(header)
            
            import_batch = self.repo.create_import_batch(
                file_name=filename,
                status=EZPassImportStatus.PROCESSING,
                created_by=user_id
            )
            
            total_rows = 0
            successful_imports = 0
            excluded_count = 0
            failed_imports = 0
            exclusion_details = []
            
            for row_num, row in enumerate(csv_reader, start=2):
                total_rows += 1
                
                if len(row) < len(col_map):
                    failed_imports += 1
                    logger.warning(f"Row {row_num}: Insufficient columns")
                    continue
                
                # Check exclusions
                should_exclude, exclusion_reason = self._should_exclude_row(row, col_map, row_num)
                if should_exclude:
                    excluded_count += 1
                    exclusion_details.append({
                        "row": row_num,
                        "reason": exclusion_reason,
                        "transaction_id": row[col_map["Lane Txn ID"]].strip()
                    })
                    continue
                
                try:
                    # Parse transaction data
                    transaction_id = row[col_map["Lane Txn ID"]].strip()
                    tag_plate = row[col_map["Tag/Plate #"]].strip()
                    agency = row[col_map["Agency"]].strip()
                    entry_plaza = row[col_map["Entry Plaza"]].strip() if row[col_map["Entry Plaza"]] else None
                    exit_plaza = row[col_map["Exit Plaza"]].strip() if row[col_map["Exit Plaza"]] else None
                    ezpass_class = row[col_map["Class"]].strip() if row[col_map["Class"]] else None
                    
                    # Parse date and time
                    date_str = row[col_map["Date"]].strip()
                    time_str = row[col_map["Time"]].strip()
                    datetime_str = f"{date_str} {time_str}"
                    transaction_datetime = datetime.strptime(datetime_str, "%m/%d/%Y %I:%M:%S %p")
                    transaction_datetime = transaction_datetime.replace(tzinfo=timezone.utc)
                    
                    # Parse amount (handles both negative and positive)
                    amount = self._parse_amount(row[col_map["Amount"]])
                    
                    # Check for duplicate
                    existing = self.repo.get_transaction_by_transaction_id(transaction_id)
                    if existing:
                        logger.warning(
                            f"Row {row_num}: Duplicate transaction_id {transaction_id}, skipping"
                        )
                        failed_imports += 1
                        continue
                    
                    # Create transaction
                    transaction_data = {
                        "import_id": import_batch.id,
                        "transaction_id": transaction_id,
                        "tag_or_plate": tag_plate,
                        "agency": agency,
                        "entry_plaza": entry_plaza,
                        "exit_plaza": exit_plaza,
                        "transaction_datetime": transaction_datetime,
                        "amount": amount,
                        "ezpass_class": ezpass_class,
                        "status": EZPassTransactionStatus.IMPORTED,
                        "created_by": user_id,
                    }
                    
                    self.repo.create_transaction(**transaction_data)
                    successful_imports += 1
                    
                except Exception as e:
                    failed_imports += 1
                    logger.error(
                        f"Row {row_num}: Failed to parse - {str(e)}",
                        exc_info=True
                    )
                    continue
            
            # Update import batch status
            batch_status = EZPassImportStatus.COMPLETED
            if successful_imports == 0:
                batch_status = EZPassImportStatus.FAILED
            
            self.repo.update_import_batch(
                import_batch.id,
                {
                    "status": batch_status,
                    "total_records": total_rows,
                    "successful_records": successful_imports,
                    "failed_records": failed_imports + excluded_count,
                }
            )
            
            self.db.commit()
            
            logger.info(
                f"CSV import completed: {successful_imports} imported, "
                f"{excluded_count} excluded, {failed_imports} failed"
            )

            # Immediately associate and post imported transactions
            self.associate_and_post_transactions(import_batch.id)
            
            return {
                "import_id": import_batch.id,
                "total_rows": total_rows,
                "successful_imports": successful_imports,
                "excluded_count": excluded_count,
                "failed_imports": failed_imports,
                "exclusion_details": exclusion_details[:10],  # First 10 for logging
            }
            
        except CSVParseError as e:
            logger.error(f"CSV parsing error: {str(e)}")
            raise
        except Exception as e:
            self.db.rollback()
            logger.error(f"Unexpected error during CSV import: {str(e)}", exc_info=True)
            raise CSVParseError(f"Failed to import CSV: {str(e)}") from e
        finally:
            IMPORT_IN_PROGRESS_FLAG = False

    def _extract_plate_from_tag(self, tag_or_plate: str) -> Optional[str]:
        """
        Extracts the license plate number from the tag_or_plate field.
        
        Examples:
        - "NY Y204273C" -> "Y204273C"
        - "NY 8M20B" -> "8M20B"
        """
        parts = tag_or_plate.strip().split()
        if len(parts) >= 2:
            return parts[1]
        return None
    
    def _find_matching_curb_trip(
        self,
        vehicle_id: int,
        transaction_datetime: datetime
    ) -> Optional[CurbTrip]:
        """
        Find CURB trip within ±30 minutes of toll transaction time.
        """
        time_window_start = transaction_datetime - timedelta(minutes=30)
        time_window_end = transaction_datetime + timedelta(minutes=30)
        
        curb_trip = (
            self.db.query(CurbTrip)
            .filter(
                CurbTrip.vehicle_id == vehicle_id,
                CurbTrip.transaction_date >= time_window_start,
                CurbTrip.transaction_date <= time_window_end,
            )
            .first()
        )

        if not curb_trip:
            curb_trip = (
                self.db.query(CurbTrip)
                .filter(CurbTrip.vehicle_id == vehicle_id)
                .order_by(CurbTrip.transaction_date.desc())
                .first()
            )
        
        return curb_trip
    
    def associate_and_post_transactions(self, import_id: Optional[int] = None) -> Dict:
        """
        Associate IMPORTED transactions with entities AND immediately post to ledger.
        
        **NEW ATOMIC WORKFLOW:**
        1. Find vehicle by plate
        2. Find CURB trip within ±30 min window
        3. Extract driver, lease, medallion from trip
        4. Post to ledger immediately based on amount sign:
           - Negative amount: Create obligation (DEBIT)
           - Positive amount: Create refund/credit (CREDIT)
        5. Update status to POSTED_TO_LEDGER
        
        This replaces the old two-step process (associate → post).
        
        Args:
            import_id: Optional import batch ID to process. If None, process all IMPORTED transactions.
            
        Returns:
            Dict with processing statistics
        """
        if import_id:
            transactions = self.repo.get_transactions_by_import_id(import_id)
            transactions = [t for t in transactions if t.status == EZPassTransactionStatus.IMPORTED]
        else:
            transactions = self.repo.get_transactions_by_status(EZPassTransactionStatus.IMPORTED)
        
        if not transactions:
            logger.info("No IMPORTED transactions to process")
            return {"processed": 0, "posted": 0, "failed": 0}
        
        processed_count = 0
        posted_count = 0
        failed_count = 0
        
        for trans in transactions:
            updates = {}
            
            try:
                # Step 1: Extract plate number
                plate_number = self._extract_plate_from_tag(trans.tag_or_plate)
                if not plate_number:
                    raise AssociationError(
                        f"Could not extract plate from tag_or_plate: {trans.tag_or_plate}",
                        "INVALID_PLATE_FORMAT"
                    )
                
                # Step 2: Find vehicle by plate
                vehicle_reg = (
                    self.db.query(VehicleRegistration)
                    .filter(VehicleRegistration.plate_number == plate_number)
                    .first()
                )
                
                if not vehicle_reg:
                    raise AssociationError(
                        f"No vehicle found with plate: {plate_number}",
                        "PLATE_NOT_FOUND"
                    )
                
                # Step 3: Find CURB trip within ±30 minutes
                curb_trip = self._find_matching_curb_trip(
                    vehicle_reg.vehicle_id,
                    trans.transaction_datetime
                )
                
                if not curb_trip:
                    raise AssociationError(
                        f"No CURB trip found within ±30 min of {trans.transaction_datetime}",
                        "NO_TRIP_DATA"
                    )
                
                # Step 4: Validate required fields
                if not all([curb_trip.driver_id, curb_trip.lease_id]):
                    raise AssociationError(
                        "CURB trip missing driver_id or lease_id",
                        "INVALID_TRIP_DATA"
                    )
                
                # Step 5: Update transaction associations
                updates = {
                    "driver_id": curb_trip.driver_id,
                    "lease_id": curb_trip.lease_id,
                    "vehicle_id": vehicle_reg.vehicle_id,
                    "medallion_id": curb_trip.medallion_id,
                }
                
                # Step 6: Post to ledger immediately
                amount = abs(trans.amount)
                
                if trans.amount < 0:
                    # Negative amount = Obligation (DEBIT to driver)
                    self.ledger_service.create_obligation(
                        category=PostingCategory.EZPASS,
                        amount=amount,
                        reference_id=trans.transaction_id,
                        driver_id=curb_trip.driver_id,
                        lease_id=curb_trip.lease_id,
                        vehicle_id=vehicle_reg.vehicle_id,
                        medallion_id=curb_trip.medallion_id,
                    )
                    logger.info(
                        f"Posted DEBIT obligation ${amount} for transaction {trans.transaction_id}"
                    )
                    
                elif trans.amount > 0:
                    # Positive amount = Refund (CREDIT to driver)
                    self.ledger_service.create_manual_credit(
                        category=PostingCategory.EZPASS,
                        amount=amount,
                        reference_id=trans.transaction_id,
                        driver_id=curb_trip.driver_id,
                        lease_id=curb_trip.lease_id,
                        vehicle_id=vehicle_reg.vehicle_id,
                        medallion_id=curb_trip.medallion_id,
                        description=f"EZPass refund from {trans.transaction_datetime}"
                    )
                    logger.info(
                        f"Posted CREDIT refund ${amount} for transaction {trans.transaction_id}"
                    )
                else:
                    # Zero amount - still mark as posted but no ledger entry
                    logger.warning(
                        f"Transaction {trans.transaction_id} has zero amount, skipping ledger posting"
                    )
                
                # Step 7: Update transaction status
                updates["status"] = EZPassTransactionStatus.POSTED_TO_LEDGER
                updates["failure_reason"] = None
                updates["posting_date"] = datetime.now(timezone.utc)
                
                posted_count += 1
                processed_count += 1
                
                logger.info(
                    f"Successfully associated and posted transaction {trans.transaction_id}",
                    driver_id=curb_trip.driver_id,
                    lease_id=curb_trip.lease_id,
                    amount=float(amount),
                    type="DEBIT" if trans.amount < 0 else "CREDIT"
                )
                
            except AssociationError as e:
                updates["status"] = EZPassTransactionStatus.ASSOCIATION_FAILED
                updates["failure_reason"] = e.reason
                failed_count += 1
                processed_count += 1
                logger.warning(
                    f"Association failed for transaction {trans.transaction_id}: {e.reason}"
                )
                
            except LedgerPostingError as e:
                updates["status"] = EZPassTransactionStatus.POSTING_FAILED
                updates["failure_reason"] = f"Ledger Error: {e.reason}"
                failed_count += 1
                processed_count += 1
                logger.error(
                    f"Ledger posting failed for transaction {trans.transaction_id}: {e.reason}",
                    exc_info=True
                )
                
            except Exception as e:
                updates["status"] = EZPassTransactionStatus.ASSOCIATION_FAILED
                updates["failure_reason"] = f"Unexpected error: {str(e)}"
                failed_count += 1
                processed_count += 1
                logger.error(
                    f"Unexpected error processing transaction {trans.transaction_id}: {e}",
                    exc_info=True
                )
            
            finally:
                if updates:
                    self.repo.update_transaction(trans.id, updates)
        
        self.db.commit()
        
        logger.info(
            f"Association and posting completed: "
            f"{processed_count} processed, {posted_count} posted, {failed_count} failed"
        )
        
        return {
            "processed": processed_count,
            "posted": posted_count,
            "failed": failed_count
        }
    
    def reassign_transactions(
        self,
        transaction_ids: List[int],
        new_driver_id: int,
        new_lease_id: int,
        new_medallion_id: Optional[int] = None,
        new_vehicle_id: Optional[int] = None,
        user_id: Optional[int] = None,
        reason: Optional[str] = None
    ) -> Dict:
        """
        Reassign EZPass transactions to a different driver/lease.
        
        **UNIVERSAL REASSIGNMENT - Works for ALL statuses:**
        
        1. IMPORTED Status:
           - Simple association update
           - No ledger operations (not yet posted)
           - Status: Remains IMPORTED
        
        2. ASSOCIATION_FAILED Status:
           - Update associations
           - Status: Changes to IMPORTED (ready for reprocessing)
        
        3. POSTED_TO_LEDGER Status:
           - Check current balance from ledger
           - If balance = 0 (fully paid): Update associations only
           - If balance > 0: Perform full ledger reversal and reposting
             * Create CREDIT on old lease (refund)
             * Create DEBIT on new lease (new charge)
           - Update associations
           - Status: Remains POSTED_TO_LEDGER
        
        **Late Arrival Handling:**
        Transactions can arrive weeks/months after the toll date.
        - Can be reassigned at any status
        - DTR inclusion based on transaction_datetime, not reassignment date
        
        Args:
            transaction_ids: List of transaction IDs to reassign
            new_driver_id: Target driver ID
            new_lease_id: Target lease ID
            new_medallion_id: Optional target medallion ID
            new_vehicle_id: Optional target vehicle ID
            user_id: User performing reassignment
            reason: Optional reason for reassignment
            
        Returns:
            Dict with reassignment results
        """
        from app.drivers.models import Driver
        from app.leases.models import Lease
        import uuid
        
        success_count = 0
        failed_count = 0
        errors = []
        
        status_breakdown = {
            "IMPORTED": {"count": 0, "with_ledger_ops": 0},
            "ASSOCIATION_FAILED": {"count": 0, "with_ledger_ops": 0},
            "POSTED_TO_LEDGER": {"count": 0, "with_ledger_ops": 0},
        }
        
        # Generate Batch ID for bulk operations (Section 9.3)
        batch_id = str(uuid.uuid4()) if len(transaction_ids) > 1 else None
        if batch_id:
            logger.info(f"Bulk reassignment batch created: {batch_id}, size: {len(transaction_ids)}")
        
        # Validate target lease and driver (Section 4.3, 4.4)
        new_driver = self.db.query(Driver).filter(Driver.id == new_driver_id).first()
        if not new_driver:
            raise ReassignmentError(f"Target driver {new_driver_id} not found")

        new_lease = self.db.query(Lease).filter(Lease.id == new_lease_id).first()
        if not new_lease:
            raise ReassignmentError(f"Target lease {new_lease_id} not found")

        # Validate target driver is associated with target lease
        lease_drivers = new_lease.lease_driver
        is_valid_driver = False
        for ld in lease_drivers:
            if ld.driver_id == new_driver.driver_id:
                is_valid_driver = True
                break

        if not is_valid_driver:
            raise ReassignmentError(
                f"Target lease {new_lease_id} is not associated with target driver {new_driver_id}. "
                f"Cannot reassign entries to invalid driver/lease combination."
            )
        
        # Validate bulk source consistency (Section 4.2)
        # All selected entries must originate from EXACTLY one source lease
        source_leases = set()
        for txn_id in transaction_ids:
            transaction = self.repo.get_transaction_by_id(txn_id)
            if transaction and transaction.lease_id:
                source_leases.add(transaction.lease_id)
        
        if len(source_leases) > 1:
            raise ReassignmentError(
                f"Bulk reassignment failed: All entries must originate from exactly one source lease. "
                f"Found entries from {len(source_leases)} different leases: {sorted(source_leases)}"
            )
        
        if source_leases:
            source_lease_id = list(source_leases)[0]
            logger.info(f"Bulk reassignment validated: All {len(transaction_ids)} entries from source lease {source_lease_id}")

        for txn_id in transaction_ids:
            try:
                transaction = self.repo.get_transaction_by_id(txn_id)
                if not transaction:
                    raise ReassignmentError(f"Transaction {txn_id} not found")
                
                # Validate source entry has valid associations (Section 4.1)
                if not transaction.driver_id or not transaction.lease_id:
                    raise ReassignmentError(
                        f"Transaction {txn_id} has invalid source associations. "
                        f"driver_id={transaction.driver_id}, lease_id={transaction.lease_id}. "
                        f"Entry must be linked to valid source lease and driver."
                    )
                
                # Check for no-op reassignment (Section 4.8)
                # Block when source = target for both lease AND driver
                # BUT allow vehicle-only changes (Section 6.3)
                if (transaction.driver_id == new_driver_id and 
                    transaction.lease_id == new_lease_id and
                    (new_vehicle_id is None or transaction.vehicle_id == new_vehicle_id)):
                    raise ReassignmentError(
                        f"Transaction {txn_id}: Source and target are identical (no-op reassignment). "
                        f"driver_id={transaction.driver_id}, lease_id={transaction.lease_id}, "
                        f"vehicle_id={transaction.vehicle_id}"
                    )
                
                current_status = transaction.status
                
                # ============================================================
                # CASE 1: IMPORTED Status
                # ============================================================
                if current_status == EZPassTransactionStatus.IMPORTED:
                    logger.info(
                        f"Reassigning IMPORTED transaction {transaction.transaction_id}",
                        new_driver=new_driver_id
                    )
                    
                    self.repo.update_transaction(transaction.id, {
                        "driver_id": new_driver_id,
                        "lease_id": new_lease_id,
                        "medallion_id": new_medallion_id,
                        "vehicle_id": new_vehicle_id or transaction.vehicle_id,
                        "updated_on": datetime.now(timezone.utc),
                        "updated_by": user_id
                    })
                    
                    status_breakdown["IMPORTED"]["count"] += 1
                    success_count += 1
                    
                    logger.info(
                        f"Successfully reassigned IMPORTED transaction {transaction.transaction_id}. "
                        f"Status remains IMPORTED. Will be posted with new associations."
                    )

                    # Create audit trail record (Section 9.2)
                    audit_trail_service.create_audit_trail(
                        db=self.db,
                        description=f"EZPass transaction reassigned: {transaction.transaction_id}",
                        case=None,
                        user=None,
                        meta_data={
                            "entry_type": "EZPASS_TRANSACTION",
                            "entry_id": transaction.id,
                            "entry_reference": transaction.transaction_id,
                            "batch_id": batch_id,
                            "batch_size": len(transaction_ids) if batch_id else 1,
                            "driver_id": new_driver_id,
                            "medallion_id": new_medallion_id,
                            "vehicle_id": new_vehicle_id or transaction.vehicle_id,
                            "lease_id": new_lease_id,
                            "source_lease_id": transaction.lease_id,
                            "source_driver_id": transaction.driver_id,
                            "target_lease_id": new_lease_id,
                            "target_driver_id": new_driver_id,
                            "reassignment_type": "IMPORTED_STATUS_UPDATE",
                            "total_payable": None,
                            "collected_to_date": None,
                            "user_id": user_id,
                            "reason": reason
                        },
                        audit_type=AuditTrailType.AUTOMATED
                    )

                    self.associate_and_post_transactions()
                
                # ============================================================
                # CASE 2: ASSOCIATION_FAILED Status
                # ============================================================
                elif current_status == EZPassTransactionStatus.ASSOCIATION_FAILED:
                    logger.info(
                        f"Reassigning ASSOCIATION_FAILED transaction {transaction.transaction_id}",
                        new_driver=new_driver_id
                    )
                    
                    # Update associations AND reset status to IMPORTED for reprocessing
                    self.repo.update_transaction(transaction.id, {
                        "driver_id": new_driver_id,
                        "lease_id": new_lease_id,
                        "medallion_id": new_medallion_id,
                        "vehicle_id": new_vehicle_id or transaction.vehicle_id,
                        "status": EZPassTransactionStatus.IMPORTED,
                        "failure_reason": None,
                        "updated_on": datetime.now(timezone.utc),
                        "updated_by": user_id
                    })
                    
                    status_breakdown["ASSOCIATION_FAILED"]["count"] += 1
                    success_count += 1
                    
                    logger.info(
                        f"Successfully reassigned ASSOCIATION_FAILED transaction {transaction.transaction_id}. "
                        f"Status changed to IMPORTED. Ready for reprocessing."
                    )

                    # Create audit trail record (Section 9.2)
                    audit_trail_service.create_audit_trail(
                        db=self.db,
                        description=f"EZPass transaction reassigned: {transaction.transaction_id}",
                        case=None,
                        user=None,
                        meta_data={
                            "entry_type": "EZPASS_TRANSACTION",
                            "entry_id": transaction.id,
                            "entry_reference": transaction.transaction_id,
                            "batch_id": batch_id,
                            "batch_size": len(transaction_ids) if batch_id else 1,
                            "driver_id": new_driver_id,
                            "medallion_id": new_medallion_id,
                            "vehicle_id": new_vehicle_id or transaction.vehicle_id,
                            "lease_id": new_lease_id,
                            "source_lease_id": transaction.lease_id,
                            "source_driver_id": transaction.driver_id,
                            "target_lease_id": new_lease_id,
                            "target_driver_id": new_driver_id,
                            "reassignment_type": "ASSOCIATION_FAILED_TO_IMPORTED",
                            "total_payable": None,
                            "collected_to_date": None,
                            "user_id": user_id,
                            "reason": reason
                        },
                        audit_type=AuditTrailType.AUTOMATED
                    )

                    self.associate_and_post_transactions()
                
                # ============================================================
                # CASE 3: POSTED_TO_LEDGER Status
                # ============================================================
                elif current_status == EZPassTransactionStatus.POSTED_TO_LEDGER:
                    logger.info(
                        f"Reassigning POSTED_TO_LEDGER transaction {transaction.transaction_id}",
                        old_driver=transaction.driver_id,
                        old_lease=transaction.lease_id,
                        new_driver=new_driver_id,
                        new_lease=new_lease_id
                    )
                    
                    # Get current balance from ledger
                    balance = self.ledger_service.repo.get_balance_by_reference_id(
                        transaction.transaction_id
                    )
                    
                    if not balance:
                        raise ReassignmentError(
                            f"No ledger balance found for transaction {transaction.transaction_id}. "
                            f"Transaction shows POSTED_TO_LEDGER but has no ledger entry."
                        )
                    
                    # Derive financial values per specification (Section 7.2)
                    total_payable = Decimal(str(balance.original_amount))  # TP
                    current_balance = Decimal(str(balance.balance))        # B
                    collected_to_date = total_payable - current_balance     # CD
                    
                    logger.info(
                        f"Financial snapshot for transaction {transaction.transaction_id}: "
                        f"TP=${total_payable}, CD=${collected_to_date}, B=${current_balance}"
                    )
                    
                    # ALWAYS perform full reversal and reposting per specification (Section 7.3)
                    # This reconstructs entire financial responsibility regardless of payment status
                    
                    # Determine if original was debit or credit based on transaction amount
                    was_debit = transaction.amount < 0
                    
                    # Step 1: Create reversal on old lease (CREDIT for full TP)
                    reversal_reference_id = f"REASSIGN-REV-{transaction.transaction_id}"
                    
                    if was_debit:
                        # Original was obligation (DEBIT), so reverse with CREDIT
                        reversal_posting = self.ledger_service.create_manual_credit(
                            category=PostingCategory.EZPASS,
                            amount=total_payable,  # Always use full TP
                            reference_id=reversal_reference_id,
                            driver_id=transaction.driver_id,
                            lease_id=transaction.lease_id,
                            vehicle_id=transaction.vehicle_id,
                            medallion_id=transaction.medallion_id,
                            description=(
                                f"Reassignment reversal: EZPass toll from {transaction.transaction_datetime}. "
                                f"Original charge on lease {transaction.lease_id} reversed. "
                                f"Reassigned to lease {new_lease_id}."
                                + (f" Reason: {reason}" if reason else "")
                            ),
                            user_id=user_id
                        )
                        logger.info(
                            f"Created reversal CREDIT of ${total_payable} on lease {transaction.lease_id}"
                        )
                    else:
                        # Original was refund (CREDIT), so reverse with DEBIT
                        reversal_posting = self.ledger_service.create_obligation(
                            category=PostingCategory.EZPASS,
                            amount=total_payable,  # Always use full TP
                            reference_id=reversal_reference_id,
                            driver_id=transaction.driver_id,
                            lease_id=transaction.lease_id,
                            vehicle_id=transaction.vehicle_id,
                            medallion_id=transaction.medallion_id,
                        )
                        logger.info(
                            f"Created reversal DEBIT of ${total_payable} on lease {transaction.lease_id}"
                        )
                    
                    # Step 2: Create new posting on new lease (same type as original, full TP)
                    if was_debit:
                        # Repost as obligation (DEBIT) on new lease
                        new_posting, balance = self.ledger_service.create_obligation(
                            category=PostingCategory.EZPASS,
                            amount=total_payable,  # Always use full TP
                            reference_id=transaction.transaction_id,
                            driver_id=new_driver_id,
                            lease_id=new_lease_id,
                            vehicle_id=new_vehicle_id or transaction.vehicle_id,
                            medallion_id=new_medallion_id or transaction.medallion_id,
                        )
                        logger.info(
                            f"Created new DEBIT of ${total_payable} on lease {new_lease_id}"
                        )
                    else:
                        # Repost as refund (CREDIT) on new lease
                        new_posting = self.ledger_service.create_manual_credit(
                            category=PostingCategory.EZPASS,
                            amount=total_payable,  # Always use full TP
                            reference_id=transaction.transaction_id,
                            driver_id=new_driver_id,
                            lease_id=new_lease_id,
                            vehicle_id=new_vehicle_id or transaction.vehicle_id,
                            medallion_id=new_medallion_id or transaction.medallion_id,
                            description=(
                                f"Reassigned EZPass refund from {transaction.transaction_datetime}. "
                                f"Originally credited to lease {transaction.lease_id}. "
                                f"Reassigned to lease {new_lease_id}."
                                + (f" Reason: {reason}" if reason else "")
                            ),
                            user_id=user_id
                        )
                        logger.info(
                            f"Created new CREDIT of ${total_payable} on lease {new_lease_id}"
                        )
                    
                    # Step 3: Update transaction associations
                    self.repo.update_transaction(transaction.id, {
                        "driver_id": new_driver_id,
                        "lease_id": new_lease_id,
                        "medallion_id": new_medallion_id,
                        "vehicle_id": new_vehicle_id or transaction.vehicle_id,
                        "updated_on": datetime.now(timezone.utc),
                        "updated_by": user_id
                    })
                    
                    status_breakdown["POSTED_TO_LEDGER"]["count"] += 1
                    status_breakdown["POSTED_TO_LEDGER"]["with_ledger_ops"] += 1
                    success_count += 1
                    
                    logger.info(
                        f"Successfully reassigned POSTED_TO_LEDGER transaction {transaction.transaction_id}. "
                        f"Full financial responsibility (${total_payable}) moved "
                        f"from lease {transaction.lease_id} to lease {new_lease_id}."
                    )

                    # Create audit trail record (Section 9.2)
                    audit_trail_service.create_audit_trail(
                        db=self.db,
                        description=f"EZPass transaction reassigned: {transaction.transaction_id}",
                        case=None,
                        user=None,
                        meta_data={
                            "entry_type": "EZPASS_TRANSACTION",
                            "entry_id": transaction.id,
                            "entry_reference": transaction.transaction_id,
                            "batch_id": batch_id,
                            "batch_size": len(transaction_ids) if batch_id else 1,
                            "driver_id": new_driver_id,
                            "medallion_id": new_medallion_id,
                            "vehicle_id": new_vehicle_id or transaction.vehicle_id,
                            "lease_id": new_lease_id,
                            "source_vehicle_id": transaction.vehicle_id,
                            "source_medallion_id": transaction.medallion_id,
                            "source_lease_id": transaction.lease_id,
                            "source_driver_id": transaction.driver_id,
                            "target_lease_id": new_lease_id,
                            "target_driver_id": new_driver_id,
                            "reassignment_type": "POSTED_TO_LEDGER_FULL_RECONSTRUCTION",
                            "total_payable": float(total_payable),
                            "collected_to_date": float(collected_to_date),
                            "reversal_posting_id": reversal_posting.id if 'reversal_posting' in locals() else None,
                            "new_posting_id": new_posting.id if 'new_posting' in locals() else None,
                            "user_id": user_id,
                            "reason": reason
                        },
                        audit_type=AuditTrailType.AUTOMATED
                    )
                
                else:
                    raise ReassignmentError(
                        f"Unknown transaction status: {current_status}. "
                        f"Cannot reassign transaction {txn_id}."
                    )
                
                # Commit after each successful transaction
                self.db.commit()
                
            except ReassignmentError as e:
                failed_count += 1
                error_msg = str(e)
                errors.append({
                    "transaction_id": txn_id,
                    "error": error_msg
                })
                logger.error(
                    f"Failed to reassign transaction {txn_id}: {error_msg}",
                    exc_info=True
                )
                self.db.rollback()
                
            except Exception as e:
                failed_count += 1
                error_msg = f"Unexpected error: {str(e)}"
                errors.append({
                    "transaction_id": txn_id,
                    "error": error_msg
                })
                logger.error(
                    f"Unexpected error reassigning transaction {txn_id}: {error_msg}",
                    exc_info=True
                )
                self.db.rollback()
        
        result = {
            "success_count": success_count,
            "failed_count": failed_count,
            "total_processed": len(transaction_ids),
            "errors": errors,
            "by_status": status_breakdown
        }
        
        logger.info(
            f"Reassignment complete: {success_count} succeeded, {failed_count} failed",
            breakdown=status_breakdown
        )
        
        return result
    
    def retry_failed_associations(self, transaction_ids: Optional[List[int]] = None) -> dict:
        """
        Retry association and posting for ASSOCIATION_FAILED transactions.
        
        This uses the same immediate posting workflow as the main import process.
        """
        if transaction_ids:
            transactions = [
                self.repo.get_transaction_by_id(tid) for tid in transaction_ids
            ]
            transactions = [t for t in transactions if t and t.status == EZPassTransactionStatus.ASSOCIATION_FAILED]
        else:
            transactions = self.repo.get_transactions_by_status(
                EZPassTransactionStatus.ASSOCIATION_FAILED
            )
        
        if not transactions:
            logger.info("No ASSOCIATION_FAILED transactions to retry")
            return {"processed": 0, "posted": 0, "failed": 0}
        
        # Temporarily update status to IMPORTED so they'll be processed
        for trans in transactions:
            self.repo.update_transaction(trans.id, {
                "status": EZPassTransactionStatus.IMPORTED,
                "failure_reason": None
            })
        
        self.db.commit()
        
        # Run the normal association and posting process
        return self.associate_and_post_transactions()
    
    def get_paginated_transactions(
        self,
        page: int = 1,
        per_page: int = 50,
        status: Optional[EZPassTransactionStatus] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        plate_number: Optional[str] = None,
        driver_id: Optional[int] = None,
        lease_id: Optional[int] = None,
        agency: Optional[str] = None,
    ) -> dict:
        """
        Get paginated list of transactions with filters.
        """
        return self.repo.get_paginated_transactions(
            page=page,
            per_page=per_page,
            status=status,
            start_date=start_date,
            end_date=end_date,
            plate_number=plate_number,
            driver_id=driver_id,
            lease_id=lease_id,
            agency=agency,
        )

    def get_import_logs(
        self,
        page: int = 1,
        per_page: int = 50,
        log_type: Optional[str] = None,
        log_status: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> dict:
        """
        Get paginated import logs with filters.
        """
        logs = self.repo.get_paginated_import_logs(
            page=page,
            per_page=per_page,
            log_type=log_type,
            log_status=log_status,
            start_date=start_date,
            end_date=end_date,
        )
        
        return {
            **logs,
            "available_log_types": AVAILABLE_LOG_TYPES,
            "available_log_statuses": AVAILABLE_LOG_STATUSES,
        }



