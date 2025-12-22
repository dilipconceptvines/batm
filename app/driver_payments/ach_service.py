# app/driver_payments/ach_service.py

import math
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.driver_payments.models import (
    ACHBatch,
    ACHBatchStatus,
    CompanyBankConfiguration,
)
from app.dtr.models import DTR, DTRStatus, PaymentMethod
from app.drivers.models import Driver
from app.utils.logger import get_logger

logger = get_logger(__name__)


class ACHBatchService:
    """
    Service for ACH batch processing and NACHA file generation.
    Handles local file storage, database tracking, and strict NACHA file formatting.
    """

    def __init__(self, db: Session):
        self.db = db
        # Ensure local directory exists for NACHA files
        self.nacha_directory = Path("nacha_files")
        self.nacha_directory.mkdir(exist_ok=True)

    # ==========================================
    # Public Methods
    # ==========================================

    def create_ach_batch(
        self,
        dtr_ids: List[int],
        effective_date: Optional[date] = None
    ) -> ACHBatch:
        """
        Create an ACH batch from selected DTRs, generate the NACHA file, 
        save it locally, and update DTR statuses.
        
        Args:
            dtr_ids: List of DTR IDs to include in the batch.
            effective_date: The date funds are intended to settle.
            
        Returns:
            The created ACHBatch object with the local file path populated.
        """
        logger.info(f"Initiating ACH batch creation for {len(dtr_ids)} DTRs")
        
        try:
            # 1. Validation: Ensure DTRs exist, are finalized, and eligible for ACH
            dtrs = self._get_and_validate_dtrs(dtr_ids)
            
            # 2. Configuration: Get company bank details (Seeds default if missing)
            company_config = self._get_company_bank_config()
            
            # 3. Batch Identification: Generate YYMM-XXX format
            batch_number = self._generate_batch_number()
            
            # 4. Timing: Determine effective entry date
            if not effective_date:
                effective_date = self._calculate_effective_date()
            
            # 5. Financials: Calculate total
            total_amount = sum(dtr.total_due_to_driver for dtr in dtrs)
            
            # 6. Create Batch Record (Draft status initially)
            ach_batch = ACHBatch(
                batch_number=batch_number,
                batch_date=datetime.utcnow(),
                effective_date=effective_date,
                status=ACHBatchStatus.DRAFT,
                total_payments=len(dtrs),
                total_amount=total_amount,
                created_by=self.db.info.get("current_user_id")
            )
            
            self.db.add(ach_batch)
            self.db.flush() # Flush to get the ID
            
            # 7. Link DTRs to Batch and Update Status
            for dtr in dtrs:
                dtr.ach_batch_id = ach_batch.id
                dtr.ach_batch_number = batch_number
                dtr.status = DTRStatus.PAID
                dtr.payment_date = datetime.utcnow()
                # Note: We don't set check_number for ACH
            
            # 8. Generate NACHA File and Save to Disk
            # Returns the Path object
            file_path = self._generate_nacha_file(
                ach_batch=ach_batch,
                dtrs=dtrs,
                company_config=company_config
            )
            
            # 9. Finalize Batch Record
            ach_batch.nacha_file_path = str(file_path)
            ach_batch.nacha_generated_at = datetime.utcnow()
            ach_batch.status = ACHBatchStatus.NACHA_GENERATED
            
            self.db.commit()
            self.db.refresh(ach_batch)
            
            logger.info(
                f"Successfully created ACH Batch {batch_number}. "
                f"Total: ${total_amount}. File: {file_path}"
            )
            
            return ach_batch
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to create ACH batch: {e}", exc_info=True)
            raise

    def reverse_ach_batch(self, batch_id: int, reason: str) -> ACHBatch:
        """
        Reverse an ACH batch.
        Updates batch status to REVERSED and reverts linked DTR statuses to FINALIZED.
        
        Args:
            batch_id: ID of the batch to reverse.
            reason: Mandatory reason string.
        """
        logger.info(f"Reversing ACH batch {batch_id}")
        
        try:
            batch = self.db.query(ACHBatch).filter(ACHBatch.id == batch_id).first()
            
            if not batch:
                raise ValueError("Batch not found")
            
            if batch.is_reversed:
                raise ValueError("Batch is already reversed")
            
            # Revert DTRs
            dtrs = self.db.query(DTR).filter(DTR.ach_batch_id == batch_id).all()
            
            for dtr in dtrs:
                dtr.status = DTRStatus.FINALIZED
                dtr.ach_batch_id = None
                dtr.ach_batch_number = None
                dtr.payment_date = None
            
            # Mark batch as reversed
            batch.is_reversed = True
            batch.reversed_at = datetime.utcnow()
            batch.reversal_reason = reason
            batch.status = ACHBatchStatus.REVERSED
            
            self.db.commit()
            self.db.refresh(batch)
            
            logger.info(f"Reversed ACH batch {batch.batch_number}, reverted {len(dtrs)} DTRs")
            
            return batch
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to reverse ACH batch: {e}", exc_info=True)
            raise

    def list_batches(
        self,
        page: int = 1,
        per_page: int = 50,
        status: Optional[ACHBatchStatus] = None
    ) -> Tuple[List[ACHBatch], int]:
        """
        List ACH batches with pagination and optional status filtering.
        """
        query = self.db.query(ACHBatch)
        
        if status:
            query = query.filter(ACHBatch.status == status)
        
        total = query.count()
        
        batches = (
            query.order_by(desc(ACHBatch.batch_date))
            .offset((page - 1) * per_page)
            .limit(per_page)
            .all()
        )
        
        return batches, total
    
    def get_batch_by_id(self, batch_id: int) -> Optional[ACHBatch]:
        """
        Get ACH batch by ID.
        """
        return self.db.query(ACHBatch).filter(ACHBatch.id == batch_id).first()

    def get_batch_by_number(self, batch_number: str) -> Optional[ACHBatch]:
        """
        Get ACH batch by its batch number (e.g., '2510-001').
        """
        return (
            self.db.query(ACHBatch)
            .filter(ACHBatch.batch_number == batch_number)
            .first()
        )

    # ==========================================
    # NACHA File Generation (Local Disk)
    # ==========================================

    def _generate_nacha_file(
        self,
        ach_batch: ACHBatch,
        dtrs: List[DTR],
        company_config: CompanyBankConfiguration
    ) -> Path:
        """
        Generates NACHA content strings, writes to local disk, and returns the Path.
        Strict 94-character fixed width lines.
        """
        logger.info(f"Generating NACHA content for batch {ach_batch.batch_number}")
        
        # Pre-calculate entry hash and totals
        entry_hash = 0
        total_credit_amount = 0
        entry_records = []
        trace_sequence = 1
        odfi_prefix = company_config.bank_routing_number[:8]

        # --- Generate Entry Details (Type 6) ---
        for dtr in dtrs:
            driver = self.db.query(Driver).filter(Driver.id == dtr.primary_driver_id).first()
            bank = getattr(driver, 'driver_bank_account', None)
            
            if not bank:
                raise ValueError(f"Missing bank info for driver {dtr.primary_driver_id}")
            
            routing = str(getattr(bank, 'bank_routing_number', '')).strip()
            if len(routing) != 9 or not routing.isdigit():
                raise ValueError(f"Invalid routing number '{routing}' for driver {driver.id}")
            
            account = str(getattr(bank, 'bank_account_number', '')).strip()
            # Amount in Cents
            amount_cents = int(dtr.total_due_to_driver * 100)
            
            # Entry Hash Calculation (Sum of first 8 digits)
            entry_hash += int(routing[:8])
            total_credit_amount += amount_cents
            
            # Determine Transaction Code (22=Checking Credit, 32=Savings Credit)
            acct_type = getattr(bank, 'bank_account_type', 'Checking')
            # Assuming 'S' or 'Savings' indicates savings
            is_savings = str(acct_type).strip().upper() in ['S', 'SAVINGS']
            txn_code = '32' if is_savings else '22'

            entry_record = self._generate_entry_detail(
                txn_code=txn_code,
                routing=routing,
                account=account,
                amount=amount_cents,
                driver_id=str(driver.id),
                receipt_number=str(dtr.receipt_number),
                driver_name=f"{driver.first_name} {driver.last_name}",
                trace_seq=trace_sequence,
                odfi_prefix=odfi_prefix
            )
            entry_records.append(entry_record)
            trace_sequence += 1

        # --- Build File Content ---
        lines = []
        
        # 1. File Header (Type 1)
        lines.append(self._generate_file_header(ach_batch, company_config))
        
        # 2. Batch Header (Type 5)
        # Batch number typically increments, using ID for uniqueness within 7 digits
        batch_num = ach_batch.id % 10000000 
        lines.append(self._generate_batch_header(ach_batch, company_config, batch_num, odfi_prefix))
        
        # 3. Entries (Type 6)
        lines.extend(entry_records)
        
        # 4. Batch Control (Type 8)
        lines.append(self._generate_batch_control(
            len(entry_records), 
            entry_hash, 
            total_credit_amount, 
            company_config, 
            batch_num,
            odfi_prefix
        ))
        
        # 5. File Control (Type 9)
        # Calculate blocks (10 records per block)
        # Total lines so far + 1 for the File Control line itself
        total_records = len(lines) + 1 
        block_count = math.ceil(total_records / 10)
        
        lines.append(self._generate_file_control(
            1, # Batch count (system does 1 batch per file usually)
            block_count,
            len(entry_records),
            entry_hash,
            total_credit_amount
        ))
        
        # 6. Padding (94 chars of '9')
        final_line_count = len(lines)
        lines_needed = block_count * 10
        padding_lines = lines_needed - final_line_count
        
        for _ in range(padding_lines):
            lines.append('9' * 94)
            
        # --- Write to Local Disk ---
        file_path = self.nacha_directory / f"{ach_batch.batch_number}.ach"
        
        logger.info(f"Writing NACHA file to disk: {file_path}")
        
        with open(file_path, 'w', newline='\r\n') as f:
             f.write('\n'.join(lines))
             
        return file_path

    # ==========================================
    # Validation & Calculation Helpers
    # ==========================================

    def _get_and_validate_dtrs(self, dtr_ids: List[int]) -> List[DTR]:
        """Validates that DTRs exist, are finalized, are ACH payment method, and are not already paid."""
        dtrs = (
            self.db.query(DTR)
            .filter(DTR.id.in_(dtr_ids))
            .all()
        )

        if not dtrs:
            raise ValueError("No DTRs found for the provided IDs.")

        if len(dtrs) != len(dtr_ids):
            found_ids = {d.id for d in dtrs}
            missing = set(dtr_ids) - found_ids
            raise ValueError(f"DTRs not found: {missing}")

        for dtr in dtrs:
            if dtr.status != DTRStatus.FINALIZED:
                raise ValueError(
                    f"DTR {dtr.receipt_number} is not in FINALIZED status (Current: {dtr.status})."
                )
            
            if dtr.payment_method != PaymentMethod.ACH:
                raise ValueError(
                    f"DTR {dtr.receipt_number} is not marked for ACH payment."
                )
            
            if dtr.ach_batch_id or dtr.ach_batch_number:
                raise ValueError(
                    f"DTR {dtr.receipt_number} is already assigned to batch {dtr.ach_batch_number}."
                )
                
            # Validate Driver Bank Info existence
            driver = self.db.query(Driver).filter(Driver.id == dtr.primary_driver_id).first()
            if not driver or not driver.driver_bank_account:
                raise ValueError(
                    f"Driver for DTR {dtr.receipt_number} has no bank account configuration."
                )
                
        return dtrs

    def _get_company_bank_config(self) -> CompanyBankConfiguration:
        """
        Retrieves the active company bank configuration.
        If not found, seeds it with default values from Documentation Appendix C.
        """
        config = (
            self.db.query(CompanyBankConfiguration)
            .filter(CompanyBankConfiguration.is_active == True)
            .first()
        )
        
        if not config:
            logger.info("No active Company Bank Configuration found. Seeding default configuration from Appendix C.")
            # Default values based on Documentation Appendix C
            default_config = CompanyBankConfiguration(
                company_name="AAP Credit Card LLC",
                company_tax_id="0103214763",
                bank_name="ConnectOne Bank",
                bank_routing_number="021213944",
                # Placeholder for debit account, as it's required by DB but not specified in static value list
                bank_account_number="0000000000", 
                immediate_origin="P963014763",
                immediate_destination=" 021213944", # Note the leading space required by Type 1 record
                company_entry_description="DRVPAY",
                is_active=True
            )
            self.db.add(default_config)
            self.db.commit()
            self.db.refresh(default_config)
            return default_config
            
        return config

    def _generate_batch_number(self) -> str:
        """
        Generates the next batch number in format YYMM-XXX.
        Example: 2510-001
        """
        now = datetime.now()
        prefix = now.strftime('%y%m') # e.g., 2510
        
        # Find last batch for this month
        last_batch = (
            self.db.query(ACHBatch)
            .filter(ACHBatch.batch_number.like(f"{prefix}-%"))
            .order_by(ACHBatch.id.desc())
            .first()
        )
        
        if last_batch:
            try:
                # Extract sequence number from "2510-005" -> 5
                last_seq = int(last_batch.batch_number.split('-')[1])
                next_seq = last_seq + 1
            except (IndexError, ValueError):
                # Fallback if format was manually messed up
                next_seq = 1
        else:
            next_seq = 1
            
        return f"{prefix}-{next_seq:03d}"

    def _calculate_effective_date(self) -> date:
        """
        Calculates the default effective entry date (next business day).
        """
        dt = date.today() + timedelta(days=1)
        
        # If Saturday (5), add 2 days -> Monday
        if dt.weekday() == 5:
            dt += timedelta(days=2)
        # If Sunday (6), add 1 day -> Monday
        elif dt.weekday() == 6:
            dt += timedelta(days=1)
            
        return dt

    # ==========================================
    # NACHA Record Generators (94 Char Fixed)
    # ==========================================

    def _generate_file_header(self, batch: ACHBatch, config: CompanyBankConfiguration) -> str:
        """Record Type 1"""
        now = datetime.now()
        # Dest: " 021213944" (space + 9 digits)
        dest = f" {config.bank_routing_number}"[:10]
        # Origin: "P963014763"
        origin = (config.immediate_origin or "P963014763")[:10].ljust(10)
        
        line = "1"                              # 01: Record Type
        line += "01"                            # 02-03: Priority
        line += dest                            # 04-13: Immediate Dest
        line += origin                          # 14-23: Immediate Origin
        line += now.strftime('%y%m%d')          # 24-29: File Date
        line += now.strftime('%H%M')            # 30-33: File Time
        line += "A"                             # 34: File ID Modifier
        line += "094"                           # 35-37: Record Size
        line += "10"                            # 38-39: Blocking Factor
        line += "1"                             # 40: Format Code
        line += f"{config.bank_name[:23]:<23}"  # 41-63: Dest Name
        line += f"{config.company_name[:23]:<23}" # 64-86: Origin Name
        line += " " * 8                         # 87-94: Ref Code
        return line

    def _generate_batch_header(
        self, 
        batch: ACHBatch, 
        config: CompanyBankConfiguration, 
        batch_num: int, 
        odfi_prefix: str
    ) -> str:
        """Record Type 5"""
        cid = (config.company_tax_id or "0103214763")[:10]
        eff = batch.effective_date.strftime('%y%m%d')
        
        line = "5"                              # 01: Record Type
        line += "220"                           # 02-04: Service Class (Credits)
        line += f"{config.company_name[:16]:<16}" # 05-20: Company Name
        line += " " * 20                        # 21-40: Discretionary
        line += f"{cid:<10}"                    # 41-50: Company ID
        line += "PPD"                           # 51-53: SEC Code
        line += f"{config.company_entry_description[:10]:<10}" # 54-63: Entry Desc
        line += eff                             # 64-69: Descriptive Date
        line += eff                             # 70-75: Effective Entry Date
        line += "   "                           # 76-78: Settlement Date
        line += "1"                             # 79: Originator Status
        line += f"{odfi_prefix[:8]:<8}"         # 80-87: Originating DFI
        line += f"{batch_num:07d}"              # 88-94: Batch Number
        return line

    def _generate_entry_detail(
        self, 
        txn_code: str, 
        routing: str, 
        account: str, 
        amount: int, 
        driver_id: str, 
        receipt_number: str, 
        driver_name: str, 
        trace_seq: int, 
        odfi_prefix: str
    ) -> str:
        """Record Type 6"""
        # Individual ID: DRV<ID>-R<RCPT>
        ind_id = f"DRV{driver_id}-R{receipt_number}"[:15]
        
        line = "6"                              # 01: Record Type
        line += txn_code                        # 02-03: Txn Code
        line += f"{routing[:8]}"                # 04-11: Receiving DFI
        line += f"{routing[-1]}"                # 12: Check Digit
        line += f"{account:<17}"                # 13-29: DFI Account
        line += f"{amount:010d}"                # 30-39: Amount
        line += f"{ind_id:<15}"                 # 40-54: Individual ID
        line += f"{driver_name.upper()[:22]:<22}" # 55-76: Individual Name
        line += "  "                            # 77-78: Discretionary
        line += "0"                             # 79: Addenda Indicator
        line += f"{odfi_prefix[:8]}"            # 80-87: Trace (ODFI)
        line += f"{trace_seq:07d}"              # 88-94: Trace (Seq)
        return line

    def _generate_batch_control(
        self, 
        entry_count: int, 
        entry_hash: int, 
        total_credit: int, 
        config: CompanyBankConfiguration, 
        batch_num: int, 
        odfi_prefix: str
    ) -> str:
        """Record Type 8"""
        # Use alternate Tax ID for control if specified, or fallback to hardcoded 9083733001 per docs
        company_id = "9083733001"
        # Entry Hash: Sum of routing numbers, mod 10^10
        hash_str = str(entry_hash)[-10:].zfill(10)

        line = "8"                              # 01: Record Type
        line += "220"                           # 02-04: Service Class
        line += f"{entry_count:06d}"            # 05-10: Entry Count
        line += hash_str                        # 11-20: Entry Hash
        line += f"{0:012d}"                     # 21-32: Total Debit (0)
        line += f"{total_credit:012d}"          # 33-44: Total Credit
        line += f"{company_id:<10}"             # 45-54: Company ID
        line += " " * 19                        # 55-73: Message Auth
        line += " " * 6                         # 74-79: Reserved
        line += f"{odfi_prefix[:8]:<8}"         # 80-87: Originating DFI
        line += f"{batch_num:07d}"              # 88-94: Batch Number
        return line

    def _generate_file_control(
        self, 
        batch_count: int, 
        block_count: int, 
        entry_count: int, 
        entry_hash: int, 
        total_credit: int
    ) -> str:
        """Record Type 9"""
        hash_str = str(entry_hash)[-10:].zfill(10)

        line = "9"                              # 01: Record Type
        line += f"{batch_count:06d}"            # 02-07: Batch Count
        line += f"{block_count:06d}"            # 08-13: Block Count
        line += f"{entry_count:08d}"            # 14-21: Entry Count
        line += hash_str                        # 22-31: Entry Hash
        line += f"{0:012d}"                     # 32-43: Total Debit (0)
        line += f"{total_credit:012d}"          # 44-55: Total Credit
        line += " " * 39                        # 56-94: Reserved
        return line