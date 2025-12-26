### app/ezpass/repository.py

from datetime import datetime
from typing import List, Optional

from sqlalchemy.orm import Session, joinedload
from app.ezpass.models import (
    EZPassImport,
    EZPassImportStatus,
    EZPassTransaction,
    EZPassTransactionStatus,
)
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
        status: Optional[EZPassTransactionStatus] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        plate_number: Optional[str] = None,
        driver_id: Optional[int] = None,
        lease_id: Optional[int] = None,
        agency: Optional[str] = None,
    ) -> dict:
        """
        Get paginated transactions with filters.
        """
        query = self.db.query(EZPassTransaction).options(
            joinedload(EZPassTransaction.driver),
            joinedload(EZPassTransaction.vehicle),
            joinedload(EZPassTransaction.medallion),
            joinedload(EZPassTransaction.lease),
        )

        # Apply filters
        if status:
            query = query.filter(EZPassTransaction.status == status)
        
        if start_date:
            query = query.filter(EZPassTransaction.transaction_datetime >= start_date)
        
        if end_date:
            query = query.filter(EZPassTransaction.transaction_datetime <= end_date)
        
        if plate_number:
            query = query.filter(EZPassTransaction.tag_or_plate.contains(plate_number))
        
        if driver_id:
            query = query.filter(EZPassTransaction.driver_id == driver_id)
        
        if lease_id:
            query = query.filter(EZPassTransaction.lease_id == lease_id)
        
        if agency:
            query = query.filter(EZPassTransaction.agency == agency)

        # Count total
        total_items = query.count()

        # Paginate
        offset = (page - 1) * per_page
        items = query.order_by(EZPassTransaction.transaction_datetime.desc()).offset(offset).limit(per_page).all()

        total_pages = (total_items + per_page - 1) // per_page

        return {
            "items": items,
            "total_items": total_items,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
        }

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