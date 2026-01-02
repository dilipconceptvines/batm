### app/ezpass/repository.py

from datetime import datetime, date, time
from typing import List, Optional, Tuple
from decimal import Decimal

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import and_, or_, func

from app.ezpass.models import (
    EZPassImport,
    EZPassImportStatus,
    EZPassTransaction,
    EZPassTransactionStatus,
)
from app.vehicles.models import Vehicle, VehicleRegistration
from app.drivers.models import Driver
from app.medallions.models import Medallion
from app.utils.general import apply_multi_filter
from app.utils.logger import get_logger

logger = get_logger(__name__)


class EZPassRepository:
    """
    Data Access Layer for EZPass Imports and Transactions.
    """

    def __init__(self, db: Session):
        self.db = db

    def create_import_batch(
        self,
        file_name: str,
        status: EZPassImportStatus = EZPassImportStatus.PENDING,
        created_by: Optional[int] = None
    ) -> EZPassImport:
        """Create a new import batch record."""
        import_batch = EZPassImport(
            file_name=file_name,
            status=status,
            created_by=created_by
        )
        self.db.add(import_batch)
        self.db.flush()
        return import_batch

    def update_import_batch(self, import_id: int, updates: dict) -> EZPassImport:
        """Update an import batch with new data."""
        import_batch = self.db.query(EZPassImport).filter(EZPassImport.id == import_id).first()
        if import_batch:
            for key, value in updates.items():
                setattr(import_batch, key, value)
            self.db.flush()
        return import_batch

    def create_transaction(self, **kwargs) -> EZPassTransaction:
        """Create a new EZPass transaction."""
        transaction = EZPassTransaction(**kwargs)
        self.db.add(transaction)
        self.db.flush()
        return transaction
    
    def update_transaction(self, transaction_id: int, updates: dict) -> EZPassTransaction:
        """Update a transaction with new data."""
        transaction = self.db.query(EZPassTransaction).filter(
            EZPassTransaction.id == transaction_id
        ).first()
        
        if transaction:
            for key, value in updates.items():
                setattr(transaction, key, value)
            self.db.flush()
        return transaction

    def get_transaction_by_id(self, transaction_id: int) -> Optional[EZPassTransaction]:
        """Get transaction by internal ID."""
        return self.db.query(EZPassTransaction).filter(
            EZPassTransaction.id == transaction_id
        ).first()

    def get_transaction_by_transaction_id(self, transaction_id: str) -> Optional[EZPassTransaction]:
        """Get transaction by Lane Txn ID."""
        return self.db.query(EZPassTransaction).filter(
            EZPassTransaction.transaction_id == transaction_id
        ).first()
    
    def get_transactions_by_status(self, status: EZPassTransactionStatus) -> List[EZPassTransaction]:
        """Get all transactions with a specific status."""
        return self.db.query(EZPassTransaction).filter(
            EZPassTransaction.status == status
        ).all()

    def get_transactions_by_import_id(self, import_id: int) -> List[EZPassTransaction]:
        """Get all transactions from a specific import batch."""
        return self.db.query(EZPassTransaction).filter(
            EZPassTransaction.import_id == import_id
        ).all()

    def get_paginated_transactions(
        self,
        page: int = 1,
        per_page: int = 50,
        sort_by: str = "transaction_datetime",
        sort_order: str = "desc",
        
        # Date range filters
        from_posting_date: Optional[date] = None,
        to_posting_date: Optional[date] = None,
        from_transaction_date: Optional[date] = None,
        to_transaction_date: Optional[date] = None,
        from_transaction_time: Optional[time] = None,
        to_transaction_time: Optional[time] = None,
        
        # Comma-separated multi-value filters
        plate_number: Optional[str] = None,
        transaction_id: Optional[str] = None,
        entry_lane: Optional[str] = None,
        exit_lane: Optional[str] = None,
        entry_plaza: Optional[str] = None,
        exit_plaza: Optional[str] = None,
        vin: Optional[str] = None,
        medallion_no: Optional[str] = None,
        driver_id: Optional[str] = None,
        driver_name: Optional[str] = None,
        lease_id: Optional[str] = None,
        status: Optional[str] = None,
        
        # Amount range filters
        from_amount: Optional[Decimal] = None,
        to_amount: Optional[Decimal] = None,
        from_ledger_balance: Optional[Decimal] = None,
        to_ledger_balance: Optional[Decimal] = None,
        
        # Other filters
        agency: Optional[str] = None,
        ezpass_class: Optional[str] = None,
    ) -> Tuple[List[EZPassTransaction], int]:
        """
        Get paginated EZPass transactions with comprehensive filtering.
        
        OPTIMIZATION STRATEGY:
        - Uses indexed columns for filtering (composite indexes)
        - Conditional joins - only join tables when filters require them
        - Count query optimization with same filters
        - Proper use of query.options() for eager loading
        
        Returns:
            Tuple of (filtered_transactions, total_count)
        """
        
        # ==================================================================
        # STEP 1: Build base query with strategic eager loading
        # ==================================================================
        query = self.db.query(EZPassTransaction)
        
        # Track which joins we need
        needs_vehicle_join = vin is not None or plate_number is not None
        needs_driver_join = driver_id is not None or driver_name is not None
        needs_medallion_join = medallion_no is not None
        needs_lease_join = lease_id is not None
        
        # Always eager load these relationships for display
        query = query.options(
            joinedload(EZPassTransaction.driver),
            joinedload(EZPassTransaction.vehicle),
            joinedload(EZPassTransaction.medallion),
            joinedload(EZPassTransaction.lease),
        )
        
        # Conditionally join for filtering
        if needs_vehicle_join:
            query = query.outerjoin(Vehicle, EZPassTransaction.vehicle_id == Vehicle.id)
            if plate_number:
                query = query.outerjoin(
                    VehicleRegistration,
                    and_(
                        Vehicle.id == VehicleRegistration.vehicle_id,
                        VehicleRegistration.is_active == True
                    )
                )
        
        if needs_driver_join:
            query = query.outerjoin(Driver, EZPassTransaction.driver_id == Driver.id)
        
        if needs_medallion_join:
            query = query.outerjoin(Medallion, EZPassTransaction.medallion_id == Medallion.id)
        
        if needs_lease_join:
            from app.leases.models import Lease
            query = query.outerjoin(Lease, EZPassTransaction.lease_id == Lease.id)
        
        # ==================================================================
        # STEP 2: Apply filters using indexed columns where possible
        # ==================================================================
        
        # 1. Posting date range filter - uses idx_ezpass_posting_date
        if from_posting_date:
            from_posting_datetime = datetime.combine(from_posting_date, datetime.min.time())
            query = query.filter(EZPassTransaction.posting_date >= from_posting_datetime)
        
        if to_posting_date:
            to_posting_datetime = datetime.combine(to_posting_date, datetime.max.time())
            query = query.filter(EZPassTransaction.posting_date <= to_posting_datetime)
        
        # 2. Transaction date range filter - uses idx_ezpass_transaction_datetime
        if from_transaction_date:
            from_transaction_datetime = datetime.combine(from_transaction_date, datetime.min.time())
            query = query.filter(EZPassTransaction.transaction_datetime >= from_transaction_datetime)
        
        if to_transaction_date:
            to_transaction_datetime = datetime.combine(to_transaction_date, datetime.max.time())
            query = query.filter(EZPassTransaction.transaction_datetime <= to_transaction_datetime)
        
        # 3. Transaction time range filter (combine with date for precise filtering)
        if from_transaction_time:
            if from_transaction_date:
                from_datetime_with_time = datetime.combine(from_transaction_date, from_transaction_time)
                query = query.filter(EZPassTransaction.transaction_datetime >= from_datetime_with_time)
            else:
                query = query.filter(func.time(EZPassTransaction.transaction_datetime) >= from_transaction_time)
        
        if to_transaction_time:
            if to_transaction_date:
                to_datetime_with_time = datetime.combine(to_transaction_date, to_transaction_time)
                query = query.filter(EZPassTransaction.transaction_datetime <= to_datetime_with_time)
            else:
                query = query.filter(func.time(EZPassTransaction.transaction_datetime) <= to_transaction_time)
        
        # 4. Plate number filter (comma-separated) - uses idx_ezpass_tag_plate
        if plate_number:
            query = apply_multi_filter(query, EZPassTransaction.tag_or_plate, plate_number)
        
        # 5. Transaction ID filter (comma-separated) - uses unique index
        if transaction_id:
            query = apply_multi_filter(query, EZPassTransaction.transaction_id, transaction_id)
        
        # 6. Entry lane filter (comma-separated) - partial match support
        if entry_lane:
            entry_lanes = [lane.strip() for lane in entry_lane.split(',') if lane.strip()]
            if entry_lanes:
                # Support for multi-lane filtering with LIKE for partial matches
                query = query.filter(
                    or_(*[
                        EZPassTransaction.entry_plaza.ilike(f"%{lane}%") 
                        for lane in entry_lanes
                    ])
                )
        
        # 7. Exit lane filter (comma-separated) - partial match support
        if exit_lane:
            exit_lanes = [lane.strip() for lane in exit_lane.split(',') if lane.strip()]
            if exit_lanes:
                query = query.filter(
                    or_(*[
                        EZPassTransaction.exit_plaza.ilike(f"%{lane}%") 
                        for lane in exit_lanes
                    ])
                )
        
        # 8. Entry plaza filter (comma-separated) - uses idx_ezpass_entry_plaza
        if entry_plaza:
            query = apply_multi_filter(query, EZPassTransaction.entry_plaza, entry_plaza)
        
        # 9. Exit plaza filter (comma-separated) - uses idx_ezpass_exit_plaza
        if exit_plaza:
            query = apply_multi_filter(query, EZPassTransaction.exit_plaza, exit_plaza)
        
        # 10. Amount range filter - uses idx_ezpass_amount
        if from_amount is not None:
            query = query.filter(EZPassTransaction.amount >= from_amount)
        
        if to_amount is not None:
            query = query.filter(EZPassTransaction.amount <= to_amount)
        
        # 11. VIN filter (comma-separated)
        if vin:
            query = apply_multi_filter(query, Vehicle.vin, vin)
        
        # 12. Medallion number filter (comma-separated)
        if medallion_no:
            query = apply_multi_filter(query, Medallion.medallion_number, medallion_no)
        
        # 13. Driver ID filter (comma-separated)
        if driver_id:
            query = apply_multi_filter(query, Driver.driver_id, driver_id)
        
        # 14. Driver name filter (comma-separated)
        if driver_name:
            query = apply_multi_filter(query, Driver.full_name, driver_name)
        
        # 15. Lease ID filter (comma-separated)
        if lease_id:
            from app.leases.models import Lease
            query = apply_multi_filter(query, Lease.lease_id, lease_id)
        
        # 16. Ledger balance range filter
        # TODO: Implement ledger balance filtering once balance calculation is available
        if from_ledger_balance is not None:
            # query = query.filter(...)  # Placeholder for balance filtering
            pass
        
        if to_ledger_balance is not None:
            # query = query.filter(...)  # Placeholder for balance filtering
            pass
        
        # 17. Status filter (comma-separated) - uses idx_ezpass_status
        if status:
            statuses = [s.strip() for s in status.split(',') if s.strip()]
            if statuses:
                # Convert string statuses to enum values
                valid_statuses = []
                for s in statuses:
                    try:
                        valid_statuses.append(EZPassTransactionStatus[s.upper()])
                    except KeyError:
                        logger.warning(f"Invalid status: {s}")
                
                if valid_statuses:
                    query = query.filter(EZPassTransaction.status.in_(valid_statuses))
        
        # 15. Agency filter - uses idx_ezpass_agency
        if agency:
            query = query.filter(EZPassTransaction.agency.ilike(f"%{agency}%"))
        
        # 16. EZPass class filter
        if ezpass_class:
            query = query.filter(EZPassTransaction.ezpass_class.ilike(f"%{ezpass_class}%"))
        
        # ==================================================================
        # STEP 3: Get total count (with same filters, before pagination)
        # ==================================================================
        total_count = query.with_entities(func.count(EZPassTransaction.id)).scalar()
        
        # ==================================================================
        # STEP 4: Apply sorting
        # ==================================================================
        sort_column_map = {
            "transaction_date": EZPassTransaction.transaction_datetime,
            "transaction_datetime": EZPassTransaction.transaction_datetime,
            "transaction_id": EZPassTransaction.transaction_id,
            "plate_number": EZPassTransaction.tag_or_plate,
            "posting_date": EZPassTransaction.posting_date,
            "amount": EZPassTransaction.amount,
            "status": EZPassTransaction.status,
            "entry_plaza": EZPassTransaction.entry_plaza,
            "exit_plaza": EZPassTransaction.exit_plaza,
            "agency": EZPassTransaction.agency,
            "driver_name": Driver.full_name,
            "lease_id": EZPassTransaction.lease_id,
            # "ledger_balance": ...  # TODO: Add when balance calculation is implemented
        }
        
        sort_column = sort_column_map.get(sort_by, EZPassTransaction.transaction_datetime)
        
        if sort_order.lower() == "asc":
            query = query.order_by(sort_column.asc())
        else:
            query = query.order_by(sort_column.desc())
        
        # ==================================================================
        # STEP 5: Apply pagination
        # ==================================================================
        offset = (page - 1) * per_page
        transactions = query.offset(offset).limit(per_page).all()
        
        logger.info(
            f"Retrieved {len(transactions)} EZPass transactions "
            f"(page {page}, total: {total_count})"
        )
        
        return transactions, total_count

    def get_paginated_import_logs(
        self,
        page: int = 1,
        per_page: int = 50,
        log_type: Optional[str] = None,
        log_status: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> dict:
        """
        Get paginated import logs.
        """
        query = self.db.query(EZPassImport)

        # Apply filters
        if log_status:
            if log_status == "Success":
                query = query.filter(EZPassImport.status == EZPassImportStatus.COMPLETED)
            elif log_status == "Failure":
                query = query.filter(EZPassImport.status == EZPassImportStatus.FAILED)
            elif log_status == "Pending":
                query = query.filter(EZPassImport.status == EZPassImportStatus.PENDING)
            elif log_status == "Processing":
                query = query.filter(EZPassImport.status == EZPassImportStatus.PROCESSING)
        
        if start_date:
            query = query.filter(EZPassImport.import_timestamp >= start_date)
        
        if end_date:
            query = query.filter(EZPassImport.import_timestamp <= end_date)

        # Count total
        total_items = query.count()

        # Paginate
        offset = (page - 1) * per_page
        items = query.order_by(EZPassImport.import_timestamp.desc()).offset(offset).limit(per_page).all()

        total_pages = (total_items + per_page - 1) // per_page

        return {
            "items": items,
            "total_items": total_items,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
        }