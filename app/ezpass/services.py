### app/ezpass/services.py

import csv
import io
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import List, Optional, Dict

from celery import shared_task
from sqlalchemy.orm import Session

from app.core.db import SessionLocal
from app.curb.models import CurbTrip
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
from app.utils.logger import get_logger
from app.vehicles.models import VehicleRegistration

logger = get_logger(__name__)

# A simple in-memory flag to prevent concurrent imports. For a multi-worker setup,
# a distributed lock (e.g., using Redis) would be more robust.
IMPORT_IN_PROGRESS_FLAG = False

# Available log types for filtering
AVAILABLE_LOG_TYPES = [
    "Import",
    # Future: "Associate", "Post"
]

# Available log statuses for filtering
AVAILABLE_LOG_STATUSES = [
    "Success",
    "Partial Success", 
    "Failure",
    "Pending",
    "Processing"
]


class EZPassService:
    """
    Service layer for handling EZPass CSV imports, transaction association,
    and ledger posting.
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
        # Define possible column name variations for each field
        column_mappings = {
            'transaction_id': ['transaction id', 'trans id', 'id', 'transaction_id', 'txn_id', 'lane txn id', 'lane transaction id'],
            'tag_or_plate': ['tag/plate', 'tag or plate', 'plate', 'tag', 'tag_or_plate', 'license_plate', 'tag/plate #', 'tag/plate number'],
            'agency': ['agency', 'toll agency', 'authority', 'agency_name'],
            'entry_plaza': ['entry plaza', 'entry', 'entry_plaza', 'on_plaza', 'entrance'],
            'exit_plaza': ['exit plaza', 'exit', 'exit_plaza', 'off_plaza', 'exit_point'],
            'ezpass_class': ['class', 'vehicle class', 'ezpass class', 'ezpass_class', 'veh_class'],
            'date': ['date', 'transaction date', 'trans date', 'txn_date', 'travel_date'],
            'time': ['time', 'transaction time', 'trans time', 'txn_time', 'travel_time'],
            'amount': ['amount', 'toll amount', 'charge', 'cost', 'fee', 'price'],
            'medallion': ['medallion', 'med', 'cab', 'medallion_no', 'med_no', 'cab_no'],
            'posted_date': ['posted date', 'posting date', 'post date'],
            'balance': ['balance', 'post txn balance', 'transaction balance', 'account balance']
        }
        
        # Normalize header names (lowercase, strip whitespace)
        normalized_header = [col.strip().lower() for col in header]
        
        # Find the index for each required field
        field_indices = {}
        
        for field, possible_names in column_mappings.items():
            index_found = None
            for i, col_name in enumerate(normalized_header):
                if any(possible_name in col_name for possible_name in possible_names):
                    index_found = i
                    break
            
            if index_found is not None:
                field_indices[field] = index_found
                logger.debug(f"Mapped field '{field}' to column index {index_found} ('{header[index_found]}')")
            else:
                logger.warning(f"Could not find column for field '{field}' in header: {header}")
        
        return field_indices

    def _parse_transaction_row(self, row: list, column_indices: dict, row_num: int) -> Optional[dict]:
        """
        Parse a single CSV row into transaction data.
        
        Args:
            row: List of CSV values
            column_indices: Dictionary mapping field names to column indices
            row_num: Row number (for error reporting)
            
        Returns:
            Dictionary with parsed transaction data, or None if row should be excluded
            
        Raises:
            CSVParseError: If row validation fails
        """
        try:
            # Validate row has enough columns
            max_index = max(column_indices.values())
            if len(row) <= max_index:
                raise CSVParseError(
                    f"Row {row_num} has {len(row)} columns but needs at least {max_index + 1}"
                )
            
            # Extract required fields
            transaction_id = row[column_indices['transaction_id']].strip()
            tag_or_plate = row[column_indices['tag_or_plate']].strip()
            agency = row[column_indices['agency']].strip()
            entry_plaza = row[column_indices['entry_plaza']].strip()
            exit_plaza = row[column_indices['exit_plaza']].strip()
            ezpass_class = row[column_indices['ezpass_class']].strip()
            date_str = row[column_indices['date']].strip()
            time_str = row[column_indices['time']].strip()
            amount_str = row[column_indices['amount']].strip()
            
            # Validate required fields are not empty
            if not all([transaction_id, tag_or_plate, agency, entry_plaza, exit_plaza, 
                       ezpass_class, date_str, time_str, amount_str]):
                raise CSVParseError(f"Row {row_num} has empty required fields")
            
            # Skip CRZ exit plaza transactions
            if exit_plaza.upper() == "CRZ":
                logger.debug(f"Excluding row {row_num} with CRZ exit plaza")
                return None
            
            # Process amount (handle parentheses for negative values, remove $ signs)
            amount_str = amount_str.replace("(", "-").replace(")", "").replace("$", "").replace(",", "")
            
            try:
                amount = Decimal(amount_str)
            except (ValueError, TypeError) as e:
                raise CSVParseError(f"Row {row_num}: Invalid amount '{amount_str}': {e}")
            
            # Process datetime - combine date and time
            transaction_datetime_str = f"{date_str} {time_str}"
            transaction_datetime = None
            
            # Try parsing with different datetime formats
            datetime_formats = [
                "%m/%d/%Y %I:%M:%S %p",  # 10/28/2025 11:29:22 AM
                "%m/%d/%Y %I:%M %p",     # 10/28/2025 11:29 AM
                "%Y-%m-%d %H:%M:%S",     # 2025-10-28 11:29:22
                "%Y-%m-%d %H:%M",        # 2025-10-28 11:29
                "%m/%d/%Y %H:%M:%S",     # 10/28/2025 23:29:22
                "%m/%d/%Y %H:%M",        # 10/28/2025 23:29
                "%m-%d-%Y %I:%M:%S %p",  # 10-28-2025 11:29:22 AM
                "%m-%d-%Y %H:%M:%S",     # 10-28-2025 23:29:22
            ]
            
            for fmt in datetime_formats:
                try:
                    transaction_datetime = datetime.strptime(transaction_datetime_str, fmt)
                    break
                except ValueError:
                    continue
            
            if transaction_datetime is None:
                raise CSVParseError(
                    f"Row {row_num}: Unable to parse datetime '{transaction_datetime_str}' "
                    f"with any known format"
                )
            
            # Get optional medallion field
            medallion = None
            if 'medallion' in column_indices and len(row) > column_indices['medallion']:
                medallion_value = row[column_indices['medallion']].strip()
                medallion = medallion_value if medallion_value else None
            
            # Get optional posting date field
            posting_date = None
            if 'posted_date' in column_indices and len(row) > column_indices['posted_date']:
                posted_date_str = row[column_indices['posted_date']].strip()
                if posted_date_str:
                    posting_date_formats = [
                        "%m/%d/%Y",      # 10/28/2025
                        "%Y-%m-%d",      # 2025-10-28
                        "%m-%d-%Y",      # 10-28-2025
                        "%d/%m/%Y",      # 28/10/2025
                    ]
                    
                    for fmt in posting_date_formats:
                        try:
                            posting_date = datetime.strptime(posted_date_str, fmt)
                            break
                        except ValueError:
                            continue
            
            # Return parsed transaction data
            return {
                "transaction_id": transaction_id,
                "tag_or_plate": tag_or_plate,
                "agency": agency,
                "entry_plaza": entry_plaza,
                "exit_plaza": exit_plaza,
                "ezpass_class": ezpass_class,
                "transaction_datetime": transaction_datetime,
                "amount": amount,
                "med_from_csv": medallion,
                "posting_date": posting_date,
            }
            
        except CSVParseError:
            raise  # Re-raise CSVParseError as-is
        except (ValueError, IndexError, KeyError) as e:
            raise CSVParseError(f"Row {row_num}: Parse error: {e}") from e
        except Exception as e:
            raise CSVParseError(f"Row {row_num}: Unexpected error: {e}") from e

    def associate_transactions(self):
        """
        Business logic to associate imported EZPass transactions with drivers, leases, etc.
        This method is designed to be run in a background task.
        """
        logger.info("Starting EZPass transaction association task.")
        transactions_to_process = self.repo.get_transactions_by_status(EZPassTransactionStatus.IMPORTED)
        
        if not transactions_to_process:
            logger.info("No imported EZPass transactions to associate.")
            return {"processed": 0, "successful": 0, "failed": 0}

        successful_count = 0
        failed_count = 0

        for trans in transactions_to_process:
            updates = {"status": EZPassTransactionStatus.ASSOCIATION_FAILED}
            try:
                # 1. Find the vehicle using the plate number
                plate_number_full = trans.tag_or_plate
                plate_number = plate_number_full.split(' ')[1] if ' ' in plate_number_full else plate_number_full
                
                vehicle_reg = self.db.query(VehicleRegistration).filter(
                    VehicleRegistration.plate_number.ilike(f"%{plate_number}%")
                ).first()

                if not vehicle_reg or not vehicle_reg.vehicle:
                    raise AssociationError(trans.transaction_id, f"No vehicle found for plate '{plate_number}'")
                
                vehicle = vehicle_reg.vehicle
                updates["vehicle_id"] = vehicle.id

                # 2. Find the corresponding CURB trip to identify the driver
                # Look for a trip within a time window around the toll time
                time_buffer = timedelta(minutes=30)
                trip_start = trans.transaction_datetime - time_buffer
                trip_end = trans.transaction_datetime + time_buffer

                curb_trip = self.db.query(CurbTrip).filter(
                    CurbTrip.vehicle_id == vehicle.id,
                    CurbTrip.start_time <= trip_end,
                    CurbTrip.end_time >= trip_start
                ).order_by(CurbTrip.start_time.desc()).first()
                
                if not curb_trip or not curb_trip.driver_id:
                    curb_trip = self.db.query(CurbTrip).filter(
                        CurbTrip.vehicle_id == vehicle.id
                    ).order_by(CurbTrip.start_time.desc()).first()

                if not curb_trip or not curb_trip.driver_id:
                    raise AssociationError(trans.transaction_id, f"No active CURB trip found for vehicle {vehicle.id} around {trans.transaction_datetime}")
                
                updates["driver_id"] = curb_trip.driver_id
                updates["lease_id"] = curb_trip.lease_id
                updates["medallion_id"] = curb_trip.medallion_id
                updates["status"] = EZPassTransactionStatus.ASSOCIATED
                updates["failure_reason"] = None
                successful_count += 1
                
            except AssociationError as e:
                updates["failure_reason"] = e.reason
                failed_count += 1
                logger.warning(f"Association failed for transaction {trans.transaction_id}: {e.reason}")

            except Exception as e:
                updates["failure_reason"] = f"An unexpected error occurred: {str(e)}"
                failed_count += 1
                logger.error(f"Unexpected error associating transaction {trans.transaction_id}: {e}", exc_info=True)

            finally:
                self.repo.update_transaction(trans.id, updates)
        
        self.db.commit()
        logger.info(f"Association task finished. Processed: {len(transactions_to_process)}, Successful: {successful_count}, Failed: {failed_count}")

        return {"processed": len(transactions_to_process), "successful": successful_count, "failed": failed_count}

    def post_tolls_to_ledger(self):
        """
        Posts successfully associated EZPass tolls as obligations to the Centralized Ledger.
        This is designed to be run as a background task.
        """
        logger.info("Starting task to post EZPass tolls to ledger.")
        transactions_to_post = self.repo.get_transactions_by_status(EZPassTransactionStatus.ASSOCIATED)

        if not transactions_to_post:
            logger.info("No associated EZPass transactions to post to ledger.")
            return {"posted": 0, "failed": 0}

        posted_count = 0
        failed_count = 0

        ledger_repo = LedgerRepository(self.db)
        ledger_service = LedgerService(ledger_repo)

        for trans in transactions_to_post:
            updates = {"status": EZPassTransactionStatus.POSTING_FAILED}
            try:
                if not all([trans.driver_id, trans.lease_id, trans.amount != 0]):
                    raise LedgerPostingError(trans.transaction_id, "Missing required driver, lease, or positive amount.")

                if trans.amount < 0:
                    trans.amount = trans.amount * -1

                # The create_obligation method is atomic and handles both posting and balance creation
                ledger_service.create_obligation(
                    category=PostingCategory.EZPASS,
                    amount=trans.amount,
                    reference_id=trans.transaction_id,
                    driver_id=trans.driver_id,
                    lease_id=trans.lease_id,
                    vehicle_id=trans.vehicle_id,
                    medallion_id=trans.medallion_id,
                )
                
                updates["status"] = EZPassTransactionStatus.POSTED_TO_LEDGER
                updates["failure_reason"] = None
                updates["posting_date"] = datetime.utcnow()
                posted_count += 1

            except Exception as e:
                updates["failure_reason"] = f"Ledger service error: {str(e)}"
                failed_count += 1
                logger.error(f"Failed to post EZPass transaction {trans.transaction_id} to ledger: {e}", exc_info=True)
            
            finally:
                self.repo.update_transaction(trans.id, updates)

        self.db.commit()
        logger.info(f"Ledger posting task finished. Posted: {posted_count}, Failed: {failed_count}")
        return {"posted": posted_count, "failed": failed_count}
    
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
                self.repo.get_transaction_by_id(txn_id) 
                for txn_id in transaction_ids
            ]
            transactions_to_process = [t for t in transactions_to_process if t is not None]
        else:
            # Retry all ASSOCIATION_FAILED transactions
            transactions_to_process = self.repo.get_transactions_by_status(
                EZPassTransactionStatus.ASSOCIATION_FAILED
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
            updates = {"status": EZPassTransactionStatus.ASSOCIATION_FAILED}
            try:
                # 1. Find the vehicle using the plate number (same logic as automatic)
                plate_number_full = trans.tag_or_plate
                plate_number = plate_number_full.split(' ')[1] if ' ' in plate_number_full else plate_number_full
                
                vehicle_reg = self.db.query(VehicleRegistration).filter(
                    VehicleRegistration.plate_number.ilike(f"%{plate_number}%")
                ).first()

                if not vehicle_reg or not vehicle_reg.vehicle:
                    raise AssociationError(trans.transaction_id, f"No vehicle found for plate '{plate_number}'")
                
                vehicle = vehicle_reg.vehicle
                updates["vehicle_id"] = vehicle.id

                # 2. Find the corresponding CURB trip to identify the driver
                # Look for a trip within a time window around the toll time
                time_buffer = timedelta(minutes=30)
                trip_start = trans.transaction_datetime - time_buffer
                trip_end = trans.transaction_datetime + time_buffer

                curb_trip = self.db.query(CurbTrip).filter(
                    CurbTrip.vehicle_id == vehicle.id,
                    CurbTrip.start_time <= trip_end,
                    CurbTrip.end_time >= trip_start
                ).order_by(CurbTrip.start_time.desc()).first()

                if not curb_trip or not curb_trip.driver_id:
                    raise AssociationError(
                        trans.transaction_id, 
                        f"No active CURB trip found for vehicle {vehicle.id} around {trans.transaction_datetime}"
                    )
                
                # SUCCESS - Associate with driver/lease from CURB trip
                updates["driver_id"] = curb_trip.driver_id
                updates["lease_id"] = curb_trip.lease_id
                updates["medallion_id"] = curb_trip.medallion_id
                updates["status"] = EZPassTransactionStatus.ASSOCIATED
                updates["failure_reason"] = None
                successful_count += 1
                
            except AssociationError as e:
                updates["failure_reason"] = e.reason
                failed_count += 1
                logger.warning(f"Association retry failed for transaction {trans.transaction_id}: {e.reason}")

            except Exception as e:
                updates["failure_reason"] = f"Unexpected error during retry: {str(e)}"
                failed_count += 1
                logger.error(f"Unexpected error retrying transaction {trans.transaction_id}: {e}", exc_info=True)

            finally:
                self.repo.update_transaction(trans.id, updates)
        
        self.db.commit()
        logger.info(
            f"Association retry finished. Processed: {len(transactions_to_process)}, "
            f"Successful: {successful_count}, Failed: {failed_count}"
        )
        
        # If successful associations exist, trigger posting task
        if successful_count > 0:
            from app.ezpass.services import post_ezpass_tolls_to_ledger_task
            post_ezpass_tolls_to_ledger_task.delay()
        
        return {
            "processed": len(transactions_to_process),
            "successful": successful_count,
            "failed": failed_count,
            "message": f"Retried {len(transactions_to_process)} transactions: {successful_count} succeeded, {failed_count} failed"
        }

    # ====================== Updated service methods ===================================

    def process_uploaded_csv(
        self, file_stream: io.BytesIO, file_name: str, user_id: int
    ) -> dict:
        """
        Triggers the NEW COMBINED task that does both association and posting in a single
        atomic operation.

        Triggers associate_and_post_ezpass_transactions_task.delay()
        """
        global IMPORT_IN_PROGRESS_FLAG
        if IMPORT_IN_PROGRESS_FLAG:
            raise ImportInProgressError()
        
        IMPORT_IN_PROGRESS_FLAG = True
        try:
            logger.info(f"Starting EZPass CSV import for file: {file_name}")

            # Read and decode the file stream
            try:
                content = file_stream.read().decode("utf-8")
                csv_reader = csv.reader(io.StringIO(content))
                header = next(csv_reader)
                rows = list(csv_reader)
            except Exception as e:
                raise CSVParseError(f"Failed to read or decode CSV content: {e}") from e
            
            if not rows:
                logger.warning(f"EZPass CSV file '{file_name}' is empty or has no data rows.")
                return {"message": "File is empty, no transactions were imported."}
            
            # Map column names to indices dynamically
            column_indices = self._map_csv_columns(header)
            logger.info(f"Column mapping for {file_name}: {column_indices}")

            # Validate required columns
            required_columns = [
                'transaction_id', 'tag_or_plate', 'agency', 'entry_plaza', 'exit_plaza',
                'ezpass_class', 'date', 'time', 'amount'
            ]
            missing_fields = [field for field in required_columns if field not in column_indices]
            if missing_fields:
                raise CSVParseError(
                    f"Missing required columns: {missing_fields}. "
                    f"Found columns: {list(column_indices.keys())}"
                )
            
            # Create import record
            import_record = self.repo.create_import_record(
                file_name=file_name,
                total_records=len(rows)
            )
            self.db.commit()

            # Parse and bulk insert transactions
            transactions_to_insert = []
            excluded_count = 0
            failed_count = 0

            for row_num, row in enumerate(rows, start=2):
                try:
                    # Parse transaction data
                    parsed_transaction = self._parse_transaction_row(
                        row, column_indices, row_num
                    )

                    if parsed_transaction:
                        parsed_transaction["import_id"] = import_record.id
                        parsed_transaction["status"] = EZPassTransactionStatus.IMPORTED
                        transactions_to_insert.append(parsed_transaction)
                    else:
                        excluded_count += 1

                except CSVParseError as e:
                    failed_count += 1
                    logger.warning(f"Row {row_num} failed validation: {e}")

            # Bulk insert
            if transactions_to_insert:
                self.repo.bulk_insert_transactions(transactions_to_insert)
                self.db.commit()

            # Update import record
            successful_count = len(transactions_to_insert)
            self.repo.update_import_record_status(
                import_record.id,
                status=EZPassImportStatus.COMPLETED,
                successful=successful_count,
                failed=failed_count + excluded_count
            )
            self.db.commit()

            # ====== Trigger combined task ======
            from app.ezpass.tasks import associate_and_post_ezpass_transactions_task

            associate_and_post_ezpass_transactions_task.delay()

            logger.info(
                f"EZPass CSV import completed for {file_name}. "
                f"Triggered combined association and posting task. "
                f"Total: {len(rows)}, Success: {successful_count}, "
                f"Failed: {failed_count}, Excluded: {excluded_count}"
            )

            return {
                "message": "File uploaded and import initiated successfully",
                "import_id": import_record.id,
                "file_name": file_name,
                "total_rows": len(rows),
                "successful": successful_count,
                "failed": failed_count,
                "excluded": excluded_count,
            }
        
        finally:
            IMPORT_IN_PROGRESS_FLAG = False

    def associate_and_post_transactions(self) -> Dict[str, int]:
        """
        Associates EZPass transactions with drivers and IMMEDIATELY posts to ledger.

        This replaces the previous two-step process (associate, then post) with a single
        atomic operation that:
        1. Associates the transaction with driver/lease via CURB trip matching
        2. Immediately posts to ledger if association is successful
        3. Updates status to POSTED_TO_LEDGER on success

        Returns:
            Dict with counts: {
                "processed": int,
                "associated": int,
                "posted": int,
                "failed": int
            }
        """
        logger.info("Starting EZPass association and immediate ledger posting task.")

        transactions_to_process = self.repo.get_transactions_by_status(
            EZPassTransactionStatus.IMPORTED
        )

        if not transactions_to_process:
            logger.info("No imported EZPass transactions to process")
            return {
                "processed": 0,
                "associated": 0,
                "posted": 0,
                "failed": 0
            }
        
        associated_count = 0
        posted_count = 0
        failed_count = 0

        for trans in transactions_to_process:
            updates = {
                "status": EZPassTransactionStatus.ASSOCIATION_FAILED
            }

            try:
                # Step 1: Extract plate from tag_or_plate field
                plate_number_full = trans.tag_or_plate.strip()
                # Extract just the plate number (remove state prefix if present)
                plate = plate_number_full.split(' ')[1] if ' ' in plate_number_full else plate_number_full
                
                if not plate:
                    raise AssociationError(
                        trans.transaction_id, "Empty plate number in tag_or_plate field"
                    )
                
                # Step 2: Find vehicle by plate number
                vehicle_reg = self.db.query(VehicleRegistration).filter(
                    VehicleRegistration.plate_number.ilike(f"%{plate}%")
                ).first()

                logger.info("Looking up vehicle for plate", plate=plate, transaction_id=trans.transaction_id, vehicle_reg=vehicle_reg.vehicle)

                if not vehicle_reg or not vehicle_reg.vehicle:
                    raise AssociationError(
                        trans.transaction_id, f"No vehicle found for plate '{plate}'"
                    )
                
                vehicle = vehicle_reg.vehicle
                updates["vehicle_id"] = vehicle.id

                # Step 3: Find CURB trip within time window
                time_buffer = timedelta(minutes=30)
                trip_start = trans.transaction_datetime - time_buffer
                trip_end = trans.transaction_datetime + time_buffer

                curb_trip = self.db.query(CurbTrip).filter(
                    CurbTrip.vehicle_id == vehicle.id,
                    CurbTrip.start_time <= trip_end,
                    CurbTrip.end_time >= trip_start
                ).order_by(CurbTrip.start_time.desc()).first()

                # Fallback to most recent trip if no trip in window
                if not curb_trip or not curb_trip.driver_id:
                    curb_trip = self.db.query(CurbTrip).filter(
                        CurbTrip.vehicle_id == vehicle.id
                    ).order_by(CurbTrip.start_time.desc()).first()

                if not curb_trip or not curb_trip.driver_id:
                    raise AssociationError(
                        trans.transaction_id,
                        f"No active CURB trip found for vehicle {vehicle.id} "
                        f"around {trans.transaction_datetime}"
                    )
                
                # Step 4: Update transaction with association details
                updates.update({
                    "driver_id": curb_trip.driver_id,
                    "lease_id": curb_trip.lease_id,
                    "medallion_id": curb_trip.medallion_id,
                    "status": EZPassTransactionStatus.ASSOCIATED,
                    "failure_reason": None
                })
                associated_count += 1

                # Step 5: Immediately Post to Ledger
                if not all([
                    curb_trip.driver_id, curb_trip.lease_id, trans.amount != 0
                ]):
                    raise LedgerPostingError(
                        trans.transaction_id, "Missing required driver, lease or valid amount for posting"
                    )
                
                # Ensure amount is positive for obligation
                amount = abs(trans.amount)

                # Create obligation in ledger (atomic operation)
                self.ledger_service.create_obligation(
                    category=PostingCategory.EZPASS,
                    amount=amount,
                    reference_id=trans.transaction_id,
                    driver_id=curb_trip.driver_id,
                    lease_id=curb_trip.lease_id,
                    vehicle_id=vehicle.id,
                    medallion_id=curb_trip.medallion_id,
                )

                # Update to Posted_to_ledger status
                updates["status"] = EZPassTransactionStatus.POSTED_TO_LEDGER
                updates["posting_date"] = datetime.now(timezone.utc)
                posted_count += 1

                logger.info(
                    f"Successfully associated and posted EZPass transaction "
                    f"{trans.transaction_id} to ledger",
                    driver_id=curb_trip.driver_id,
                    lease_id=curb_trip.lease_id,
                    amount=amount
                )
            except AssociationError as e:
                updates["failure_reason"] = e.reason
                failed_count += 1
                logger.warning(
                    f"Association failed for transaction {trans.transaction_id}: "
                    f"{e.reason}"
                )
            except LedgerPostingError as e:
                # Association succeeded but posting failed
                updates["status"] = EZPassTransactionStatus.POSTING_FAILED
                updates["failure_reason"] = f"Ledger Posting Error: {e.reason}"
                failed_count += 1
                logger.error(
                    f"Ledger posting failed for transaction {trans.transaction_id}: "
                    f"{e.reason}",
                    exc_info=True
                )

            except Exception as e:
                updates["failure_reason"] = f"Unexpected error: {str(e)}"
                failed_count += 1
                logger.error(
                    f"Unexpected error processing transaction {trans.transaction_id}: "
                    f"{e}",
                    exc_info=True
                )
            
            finally:
                self.repo.update_transaction(trans.id, updates)

        self.db.commit()

        logger.info(
            f"EZPass association and posting task finished. "
            f"Processed: {len(transactions_to_process)}, "
            f"Associated: {associated_count}, "
            f"Posted: {posted_count}, "
            f"Failed: {failed_count}"
        )

        return {
            "processed": len(transactions_to_process),
            "associated": associated_count,
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
        
        **CRITICAL: Works for ALL transaction statuses**
        - IMPORTED: Simple association update
        - ASSOCIATION_FAILED: Association update + status change to ASSOCIATED
        - ASSOCIATED: Simple association update
        - POSTED_TO_LEDGER: Balance-driven ledger operations + association update
        
        **Late Arrival Handling:**
        When transactions arrive late (weeks/months after transaction date):
        - They can be reassigned at any status
        - For POSTED_TO_LEDGER: Full ledger reversal and reposting
        - For other statuses: Simple association updates
        - DTR consideration: Transaction date determines which week's DTR includes it
        
        **Status-Specific Logic:**
        
        1. IMPORTED Status:
        - Update: driver_id, lease_id, medallion_id, vehicle_id
        - No ledger operations (not yet posted)
        - Status: Remains IMPORTED
        - Next step: Normal association/posting workflow continues
        
        2. ASSOCIATION_FAILED Status:
        - Update: driver_id, lease_id, medallion_id, vehicle_id
        - No ledger operations (not yet posted)
        - Status: Changes to ASSOCIATED (manual correction complete)
        - Next step: Can be posted to ledger with corrected associations
        
        3. ASSOCIATED Status:
        - Update: driver_id, lease_id, medallion_id, vehicle_id
        - No ledger operations (not yet posted)
        - Status: Remains ASSOCIATED
        - Next step: Can be posted to ledger with new associations
        
        4. POSTED_TO_LEDGER Status:
        - Retrieve current outstanding balance
        - If balance > 0:
            * Create CREDIT (reversal) on source lease
            * Create DEBIT (new charge) on target lease
        - Update: driver_id, lease_id, medallion_id, vehicle_id
        - Status: Remains POSTED_TO_LEDGER
        
        Args:
            transaction_ids: List of transaction IDs to reassign
            new_driver_id: Target driver ID
            new_lease_id: Target lease ID
            new_medallion_id: Optional target medallion ID
            new_vehicle_id: Optional target vehicle ID
            user_id: User performing the reassignment
            reason: Optional reason for reassignment
            
        Returns:
            Dict with reassignment results:
            {
                "success_count": int,
                "failed_count": int,
                "total_processed": int,
                "errors": [{"transaction_id": int, "error": str}],
                "by_status": {
                    "IMPORTED": {"count": int, "action": "association_update"},
                    "ASSOCIATED": {"count": int, "action": "association_update"},
                    "ASSOCIATION_FAILED": {"count": int, "action": "association_update_with_status_fix"},
                    "POSTED_TO_LEDGER": {"count": int, "action": "ledger_operations"}
                }
            }
            
        Raises:
            ReassignmentError: If validation fails
        """
        logger.info(
            f"Starting reassignment of {len(transaction_ids)} EZPass transactions",
            new_driver_id=new_driver_id,
            new_lease_id=new_lease_id
        )
        
        # ================================================================
        # VALIDATION: Driver and Lease
        # ================================================================
        from app.drivers.models import Driver
        from app.leases.models import Lease
        
        new_driver = self.db.query(Driver).filter(Driver.id == new_driver_id).first()
        if not new_driver:
            raise ReassignmentError(f"Driver with ID {new_driver_id} not found")
        
        new_lease = self.db.query(Lease).filter(Lease.id == new_lease_id).first()
        if not new_lease:
            raise ReassignmentError(f"Lease with ID {new_lease_id} not found")
        
        # Verify new driver is the primary driver on new lease
        if new_lease.driver_id != new_driver_id:
            raise ReassignmentError(
                f"Driver {new_driver_id} is not the primary driver on lease {new_lease_id}. "
                f"Lease primary driver is {new_lease.driver_id}"
            )
        
        # ================================================================
        # REASSIGNMENT PROCESSING
        # ================================================================
        success_count = 0
        failed_count = 0
        errors = []
        status_breakdown = {
            "IMPORTED": {"count": 0, "action": "association_update"},
            "ASSOCIATED": {"count": 0, "action": "association_update"},
            "ASSOCIATION_FAILED": {"count": 0, "action": "association_update_with_status_fix"},
            "POSTED_TO_LEDGER": {"count": 0, "action": "ledger_operations"}
        }
        
        for txn_id in transaction_ids:
            try:
                transaction = self.repo.get_transaction_by_id(txn_id)
                if not transaction:
                    raise ReassignmentError(f"Transaction {txn_id} not found")
                
                # Block no-op reassignment (same driver and lease)
                if (transaction.driver_id == new_driver_id and 
                    transaction.lease_id == new_lease_id):
                    logger.info(
                        f"Skipping no-op reassignment for transaction {txn_id} "
                        f"(already assigned to driver {new_driver_id}, lease {new_lease_id})"
                    )
                    continue
                
                current_status = transaction.status
                
                # ============================================================
                # CASE 1: IMPORTED Status
                # ============================================================
                if current_status == EZPassTransactionStatus.IMPORTED:
                    logger.info(
                        f"Reassigning IMPORTED transaction {transaction.transaction_id}",
                        old_driver=transaction.driver_id,
                        new_driver=new_driver_id
                    )
                    
                    self.repo.update_transaction(transaction.id, {
                        "driver_id": new_driver_id,
                        "lease_id": new_lease_id,
                        "medallion_id": new_medallion_id,
                        "vehicle_id": new_vehicle_id or transaction.vehicle_id,
                        "updated_on": datetime.now(timezone.utc)
                    })
                    
                    status_breakdown["IMPORTED"]["count"] += 1
                    success_count += 1
                    
                    logger.info(
                        f"Successfully reassigned IMPORTED transaction {transaction.transaction_id}. "
                        f"Status remains IMPORTED. Will be processed normally."
                    )
                
                # ============================================================
                # CASE 2: ASSOCIATION_FAILED Status
                # ============================================================
                elif current_status == EZPassTransactionStatus.ASSOCIATION_FAILED:
                    logger.info(
                        f"Reassigning ASSOCIATION_FAILED transaction {transaction.transaction_id}",
                        old_driver=transaction.driver_id,
                        new_driver=new_driver_id
                    )
                    
                    # Update associations AND fix status to ASSOCIATED
                    self.repo.update_transaction(transaction.id, {
                        "driver_id": new_driver_id,
                        "lease_id": new_lease_id,
                        "medallion_id": new_medallion_id,
                        "vehicle_id": new_vehicle_id or transaction.vehicle_id,
                        "status": EZPassTransactionStatus.ASSOCIATED,  # Fix status
                        "failure_reason": None,  # Clear failure reason
                        "updated_on": datetime.now(timezone.utc)
                    })
                    
                    status_breakdown["ASSOCIATION_FAILED"]["count"] += 1
                    success_count += 1
                    
                    logger.info(
                        f"Successfully reassigned ASSOCIATION_FAILED transaction {transaction.transaction_id}. "
                        f"Status changed to ASSOCIATED. Ready for posting."
                    )
                
                # ============================================================
                # CASE 3: ASSOCIATED Status
                # ============================================================
                elif current_status == EZPassTransactionStatus.ASSOCIATED:
                    logger.info(
                        f"Reassigning ASSOCIATED transaction {transaction.transaction_id}",
                        old_driver=transaction.driver_id,
                        new_driver=new_driver_id
                    )
                    
                    self.repo.update_transaction(transaction.id, {
                        "driver_id": new_driver_id,
                        "lease_id": new_lease_id,
                        "medallion_id": new_medallion_id,
                        "vehicle_id": new_vehicle_id or transaction.vehicle_id,
                        "updated_on": datetime.now(timezone.utc)
                    })
                    
                    status_breakdown["ASSOCIATED"]["count"] += 1
                    success_count += 1
                    
                    logger.info(
                        f"Successfully reassigned ASSOCIATED transaction {transaction.transaction_id}. "
                        f"Status remains ASSOCIATED. Ready for posting with new associations."
                    )
                
                # ============================================================
                # CASE 4: POSTED_TO_LEDGER Status
                # ============================================================
                elif current_status == EZPassTransactionStatus.POSTED_TO_LEDGER:
                    logger.info(
                        f"Reassigning POSTED_TO_LEDGER transaction {transaction.transaction_id}",
                        old_driver=transaction.driver_id,
                        old_lease=transaction.lease_id,
                        new_driver=new_driver_id,
                        new_lease=new_lease_id
                    )
                    
                    # Get current outstanding balance from ledger
                    balance = self.ledger_service.repo.get_balance_by_reference_id(
                        transaction.transaction_id
                    )
                    
                    if not balance:
                        raise ReassignmentError(
                            f"No ledger balance found for transaction {transaction.transaction_id}. "
                            f"Transaction shows POSTED_TO_LEDGER but has no ledger entry."
                        )
                    
                    outstanding_balance = balance.balance
                    
                    # If fully paid (balance = 0), only update associations
                    if outstanding_balance == Decimal('0.00'):
                        logger.info(
                            f"Transaction {transaction.transaction_id} is fully paid (balance: $0.00). "
                            f"Updating associations only, no ledger changes needed."
                        )
                        
                        self.repo.update_transaction(transaction.id, {
                            "driver_id": new_driver_id,
                            "lease_id": new_lease_id,
                            "medallion_id": new_medallion_id,
                            "vehicle_id": new_vehicle_id or transaction.vehicle_id,
                            "updated_on": datetime.now(timezone.utc)
                        })
                        
                        status_breakdown["POSTED_TO_LEDGER"]["count"] += 1
                        success_count += 1
                        continue
                    
                    # Balance > 0: Perform ledger operations
                    logger.info(
                        f"Transaction {transaction.transaction_id} has outstanding balance: "
                        f"${outstanding_balance}. Performing ledger reversal and reposting."
                    )
                    
                    # Step 1: Create reversal (CREDIT) on source lease
                    reversal_description = (
                        f"Reassignment reversal: EZPass toll from {transaction.transaction_datetime}. "
                        f"Original charge on lease {transaction.lease_id} reversed. "
                        f"Reassigned to lease {new_lease_id}."
                    )
                    
                    reversal_reference_id = f"REASSIGN-REV-{transaction.transaction_id}"
                    
                    self.ledger_service.create_obligation(
                        driver_id=transaction.driver_id,
                        lease_id=transaction.lease_id,
                        amount=outstanding_balance,
                        category=PostingCategory.EZPASS,
                        reference_id=reversal_reference_id,
                    )
                    
                    logger.info(
                        f"Created reversal CREDIT of ${outstanding_balance} on lease {transaction.lease_id}"
                    )
                    
                    # Step 2: Create new posting (DEBIT) on target lease
                    new_posting_description = (
                        f"Reassigned EZPass toll from {transaction.transaction_datetime}. "
                        f"Originally charged to lease {transaction.lease_id}. "
                        f"Reassigned to lease {new_lease_id}."
                        + (f" Reason: {reason}" if reason else "")
                    )
                    
                    self.ledger_service.create_obligation(
                        driver_id=new_driver_id,
                        lease_id=new_lease_id,
                        amount=outstanding_balance,
                        category=PostingCategory.EZPASS,
                        reference_id=transaction.transaction_id,  # Use original reference
                    )
                    
                    logger.info(
                        f"Created new DEBIT of ${outstanding_balance} on lease {new_lease_id}"
                    )
                    
                    # Step 3: Update transaction associations
                    self.repo.update_transaction(transaction.id, {
                        "driver_id": new_driver_id,
                        "lease_id": new_lease_id,
                        "medallion_id": new_medallion_id,
                        "vehicle_id": new_vehicle_id or transaction.vehicle_id,
                        "updated_on": datetime.now(timezone.utc)
                    })
                    
                    status_breakdown["POSTED_TO_LEDGER"]["count"] += 1
                    success_count += 1
                    
                    logger.info(
                        f"Successfully reassigned POSTED_TO_LEDGER transaction {transaction.transaction_id}. "
                        f"Ledger operations complete. Outstanding balance ${outstanding_balance} moved "
                        f"from lease {transaction.lease_id} to lease {new_lease_id}."
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

    def manual_post_to_ledger(self, transaction_ids: List[int]) -> Dict[str, any]:
        """
        Manually post ASSOCIATED transactions to ledger.
        
        This method is for backward compatibility and edge cases where
        transactions might be in ASSOCIATED status without being posted.
        Under the new flow, this should rarely be needed since posting
        happens immediately after association.
        
        Args:
            transaction_ids: List of transaction IDs to post
            
        Returns:
            Dict with posting results
        """
        logger.info(
            f"Manual posting of {len(transaction_ids)} EZPass transactions to ledger"
        )
        
        success_count = 0
        failed_count = 0
        errors = []
        
        for txn_id in transaction_ids:
            try:
                transaction = self.repo.get_transaction_by_id(txn_id)
                if not transaction:
                    raise LedgerPostingError(str(txn_id), "Transaction not found")
                
                # Validate transaction status
                if transaction.status == EZPassTransactionStatus.POSTED_TO_LEDGER:
                    errors.append({
                        "transaction_id": txn_id,
                        "error": "Already posted to ledger"
                    })
                    failed_count += 1
                    continue
                
                if transaction.status != EZPassTransactionStatus.ASSOCIATED:
                    errors.append({
                        "transaction_id": txn_id,
                        "error": f"Cannot post - status is {transaction.status.value}"
                    })
                    failed_count += 1
                    continue
                
                if not all([
                    transaction.driver_id,
                    transaction.lease_id,
                    transaction.amount != 0
                ]):
                    errors.append({
                        "transaction_id": txn_id,
                        "error": "Missing required fields (driver_id, lease_id, or valid amount)"
                    })
                    failed_count += 1
                    continue
                
                # Post to ledger
                amount = abs(transaction.amount)
                self.ledger_service.create_obligation(
                    category=PostingCategory.EZPASS,
                    amount=amount,
                    reference_id=transaction.transaction_id,
                    driver_id=transaction.driver_id,
                    lease_id=transaction.lease_id,
                    vehicle_id=transaction.vehicle_id,
                    medallion_id=transaction.medallion_id,
                )
                
                # Update transaction status
                self.repo.update_transaction(transaction.id, {
                    "status": EZPassTransactionStatus.POSTED_TO_LEDGER,
                    "failure_reason": None,
                    "posting_date": datetime.now(timezone.utc)
                })
                
                success_count += 1
                logger.info(
                    f"Successfully posted EZPass transaction {transaction.transaction_id} "
                    f"to ledger"
                )
                
            except Exception as e:
                failed_count += 1
                errors.append({
                    "transaction_id": txn_id,
                    "error": str(e)
                })
                logger.error(
                    f"Failed to post transaction {txn_id} to ledger: {e}",
                    exc_info=True
                )
        
        self.db.commit()
        
        result = {
            "success_count": success_count,
            "failed_count": failed_count,
            "total_processed": len(transaction_ids),
            "errors": errors if errors else None
        }
        
        logger.info(
            f"Manual EZPass posting completed. "
            f"Success: {success_count}, Failed: {failed_count}"
        )
        
        return result
    

# --- Celery Tasks ---
from app.worker.app import app as celery_app

@celery_app.task(name="ezpass.associate_transactions")
def associate_ezpass_transactions_task():
    """
    Background task to find the correct driver/lease for imported EZPass transactions.
    """
    logger.info("Executing Celery task: associate_ezpass_transactions_task")
    db: Session = SessionLocal()
    try:
        service = EZPassService(db)
        result = service.associate_transactions()
        return result
    except Exception as e:
        logger.error(f"Celery task associate_ezpass_transactions_task failed: {e}", exc_info=True)
        db.rollback()
        raise
    finally:
        db.close()

    

@celery_app.task(name="ezpass.post_tolls_to_ledger")
def post_ezpass_tolls_to_ledger_task():
    """
    Background task to post successfully associated EZPass tolls to the ledger.
    """
    logger.info("Executing Celery task: post_ezpass_tolls_to_ledger_task")
    db: Session = SessionLocal()
    try:
        # ledger_service = LedgerService(db)
        ezpass_service = EZPassService(db)
        result = ezpass_service.post_tolls_to_ledger()
        return result
    except Exception as e:
        logger.error(f"Celery task post_ezpass_tolls_to_ledger_task failed: {e}", exc_info=True)
        db.rollback()
        raise
    finally:
        db.close()




