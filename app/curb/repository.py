# app/curb/repository.py

"""
CURB Repository - Data Access Layer

Handles all database operations for CURB accounts and trips with optimized queries.
"""

from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Tuple

from sqlalchemy import and_, func
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, joinedload

from app.curb.exceptions import CurbAccountNotFoundError, CurbTripNotFoundError
from app.curb.models import CurbAccount, CurbTrip, CurbTripStatus, PaymentType
from app.utils.logger import get_logger

logger = get_logger(__name__)


class CurbRepository:
    """Data Access Layer for CURB operations"""
    
    def __init__(self, db: Session):
        self.db = db

    # --- CURB ACCOUNT OPERATIONS --- #
    def get_active_accounts(self) -> List[CurbAccount]:
        """Get all active CURB accounts"""
        return self.db.query(CurbAccount).filter(
            CurbAccount.is_active == True
        ).all()
    
    def get_account_by_id(self, account_id: int) -> CurbAccount:
        """Get a specific CURB account by ID"""
        account = self.db.query(CurbAccount).filter(
            CurbAccount.id == account_id
        ).first()
        
        if not account:
            raise CurbAccountNotFoundError(f"CURB account with ID {account_id} not found")
        
        return account
    
    def get_account_by_name(self, account_name: str) -> Optional[CurbAccount]:
        """Get a CURB account by name"""
        return self.db.query(CurbAccount).filter(
            CurbAccount.account_name == account_name
        ).first()
    
    def create_account(self, account_data: dict) -> CurbAccount:
        """Create a new CURB account"""
        account = CurbAccount(**account_data)
        self.db.add(account)
        self.db.flush()
        logger.info(f"Created new CURB account: {account.account_name} (ID: {account.id})")
        return account
    
    def update_account(self, account_id: int, update_data: dict) -> CurbAccount:
        """Update an existing CURB account"""
        account = self.get_account_by_id(account_id)
        
        for key, value in update_data.items():
            if value is not None and hasattr(account, key):
                setattr(account, key, value)
        
        self.db.flush()
        logger.info(f"Updated CURB account: {account.account_name} (ID: {account.id})")
        return account
    
    # --- CURB TRIP BULK OPERATIONS --- #

    def bulk_insert_or_update_trips(self, trips_data: List[dict]) -> Tuple[int, int]:
        """
        Insert/update trips individually with savepoints (like EZPass pattern)
        
        Args:
            trips_data: List of trip dictionaries
        
        Returns:
            Tuple of (inserted_count, updated_count)
        """
        if not trips_data:
            return 0, 0
        
        # Deduplicate by curb_trip_id
        unique_trips = {}
        for trip in trips_data:
            trip_id = trip.get("curb_trip_id")
            if trip_id:
                unique_trips[trip_id] = trip
        
        trips_to_process = list(unique_trips.values())
        
        if not trips_to_process:
            return 0, 0
        
        try:
            logger.info(f"Bulk inserting/updating {len(trips_to_process)} CURB trips")
            
            # Process in smaller batches to avoid lock contention
            batch_size = 100
            total_processed = 0
            
            for i in range(0, len(trips_to_process), batch_size):
                batch = trips_to_process[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (len(trips_to_process) + batch_size - 1) // batch_size
                
                # Use MySQL's INSERT ... ON DUPLICATE KEY UPDATE
                stmt = insert(CurbTrip)
                
                # Define which columns to update on duplicate
                update_dict = {
                    "end_time": stmt.inserted.end_time,
                    "fare": stmt.inserted.fare,
                    "tips": stmt.inserted.tips,
                    "tolls": stmt.inserted.tolls,
                    "extras": stmt.inserted.extras,
                    "total_amount": stmt.inserted.total_amount,
                    "surcharge": stmt.inserted.surcharge,
                    "improvement_surcharge": stmt.inserted.improvement_surcharge,
                    "congestion_fee": stmt.inserted.congestion_fee,
                    "airport_fee": stmt.inserted.airport_fee,
                    "cbdt_fee": stmt.inserted.cbdt_fee,
                    "distance_miles": stmt.inserted.distance_miles,
                    "num_passengers": stmt.inserted.num_passengers,
                    "transaction_date": stmt.inserted.transaction_date,
                    "start_lat": stmt.inserted.start_lat,
                    "start_long": stmt.inserted.start_long,
                    "end_lat": stmt.inserted.end_lat,
                    "end_long": stmt.inserted.end_long,
                    "num_service": stmt.inserted.num_service,
                    "updated_on": datetime.now(),
                }
                
                stmt = stmt.on_duplicate_key_update(**update_dict)
                
                # Execute batch (no flush - commit happens in service layer)
                logger.info(f"Processing batch {batch_num}/{total_batches}: {len(batch)} trips")
                self.db.execute(stmt, batch)
                total_processed += len(batch)
            
            logger.info(f"Bulk operation complete: {total_processed} trips processed")
            return total_processed, 0
            
        except IntegrityError as e:
            logger.error(f"Integrity error during bulk insert: {e}")
            self.db.rollback()
            raise
        except SQLAlchemyError as e:
            logger.error(f"Database error during bulk insert: {e}")
            self.db.rollback()
            raise

    # --- CURB TRIP QUERY OPERATIONS --- #

    def get_trip_by_id(self, trip_id: int) -> CurbTrip:
        """Get a specific trip by internal ID"""
        trip = self.db.query(CurbTrip).options(
            joinedload(CurbTrip.account),
            joinedload(CurbTrip.driver),
            joinedload(CurbTrip.lease),
        ).filter(CurbTrip.id == trip_id).first()
        
        if not trip:
            raise CurbTripNotFoundError(f"Trip with ID {trip_id} not found")
        
        return trip
    
    def get_trip_by_curb_id(self, curb_trip_id: str) -> Optional[CurbTrip]:
        """Get a trip by its CURB trip ID"""
        return self.db.query(CurbTrip).filter(
            CurbTrip.curb_trip_id == curb_trip_id
        ).first()
    
    def get_trips_by_datetime_range(
        self,
        start_datetime: datetime,
        end_datetime: datetime,
        account_ids: Optional[List[int]] = None,
        status: Optional[CurbTripStatus] = None,
        payment_type: Optional[PaymentType] = None,
    ) -> List[CurbTrip]:
        """
        Get trips within a datetime range with optional filters.
        Uses optimized composite index: idx_curb_trip_time_status
        """
        query = self.db.query(CurbTrip).filter(
            and_(
                CurbTrip.start_time >= start_datetime,
                CurbTrip.start_time < end_datetime
            )
        )
        
        if account_ids:
            query = query.filter(CurbTrip.account_id.in_(account_ids))
        
        if status:
            query = query.filter(CurbTrip.status == status)
        
        if payment_type:
            query = query.filter(CurbTrip.payment_type == payment_type)
        
        return query.order_by(CurbTrip.start_time.asc()).all()
    
    def get_trips_ready_for_ledger(
        self,
        start_date: datetime,
        end_date: datetime,
        driver_ids: Optional[List[int]] = None,
        lease_ids: Optional[List[int]] = None,
    ) -> List[CurbTrip]:
        """
        Get IMPORTED trips ready to be posted to ledger.
        Uses optimized composite index: idx_curb_ready_for_ledger
        """
        query = self.db.query(CurbTrip).filter(
            and_(
                CurbTrip.status == CurbTripStatus.IMPORTED,
                CurbTrip.start_time >= start_date,
                CurbTrip.start_time < end_date,
                CurbTrip.driver_id.isnot(None),  # Must be mapped to a driver
                CurbTrip.lease_id.isnot(None),    # Must be mapped to a lease
            )
        )
        
        if driver_ids:
            query = query.filter(CurbTrip.driver_id.in_(driver_ids))
        
        if lease_ids:
            query = query.filter(CurbTrip.lease_id.in_(lease_ids))
        
        return query.order_by(CurbTrip.driver_id, CurbTrip.start_time).all()
    
    def list_trips_paginated(
        self,
        page: int = 1,
        per_page: int = 50,
        account_ids: Optional[List[int]] = None,
        driver_ids: Optional[List[int]] = None,
        lease_ids: Optional[List[int]] = None,
        status: Optional[CurbTripStatus] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        sort_by: str = "start_time",
        sort_order: str = "desc",
    ) -> Tuple[List[CurbTrip], int]:
        """
        List trips with pagination and filters.
        Returns (trips, total_count)
        """
        query = self.db.query(CurbTrip).options(
            joinedload(CurbTrip.account),
            joinedload(CurbTrip.driver),
            joinedload(CurbTrip.lease),
        )
        
        # Apply filters
        if account_ids:
            query = query.filter(CurbTrip.account_id.in_(account_ids))
        
        if driver_ids:
            query = query.filter(CurbTrip.driver_id.in_(driver_ids))
        
        if lease_ids:
            query = query.filter(CurbTrip.lease_id.in_(lease_ids))
        
        if status:
            query = query.filter(CurbTrip.status == status)
        
        if start_date:
            query = query.filter(CurbTrip.start_time >= start_date)
        
        if end_date:
            query = query.filter(CurbTrip.start_time < end_date)
        
        # Get total count before pagination
        total_count = query.count()
        
        # Apply sorting
        sort_column = getattr(CurbTrip, sort_by, CurbTrip.start_time)
        if sort_order == "asc":
            query = query.order_by(sort_column.asc())
        else:
            query = query.order_by(sort_column.desc())
        
        # Apply pagination
        offset = (page - 1) * per_page
        trips = query.offset(offset).limit(per_page).all()
        
        return trips, total_count
    
    # --- CURB TRIP UPDATE OPERATIONS --- #

    def update_trip_status(
        self,
        trip_id: int,
        new_status: CurbTripStatus,
        ledger_posting_ref: Optional[str] = None,
    ) -> CurbTrip:
        """Update the status of a trip"""
        trip = self.get_trip_by_id(trip_id)
        trip.status = new_status
        
        if new_status == CurbTripStatus.POSTED_TO_LEDGER:
            trip.posted_to_ledger_at = datetime.now()
            if ledger_posting_ref:
                trip.ledger_posting_ref = ledger_posting_ref
        
        self.db.flush()
        return trip
    
    def mark_trips_as_reconciled(
        self,
        trip_ids: List[int],
        reconciliation_id: str,
    ) -> int:
        """Mark multiple trips as reconciled"""
        count = self.db.query(CurbTrip).filter(
            CurbTrip.id.in_(trip_ids)
        ).update(
            {
                "reconciliation_id": reconciliation_id,
                "reconciled_at": datetime.now(),
            },
            synchronize_session=False
        )
        
        self.db.flush()
        logger.info(f"Marked {count} trips as reconciled with ID: {reconciliation_id}")
        return count
    
    # --- STATISTICS & AGGREGATIONS --- #

    def get_account_statistics(self, account_id: int) -> dict:
        """Get statistics for a specific CURB account"""
        stats = self.db.query(
            func.count(CurbTrip.id).label("total_trips"),
            func.count(func.nullif(CurbTrip.status == CurbTripStatus.IMPORTED, False)).label("trips_imported"),
            func.count(func.nullif(CurbTrip.status == CurbTripStatus.POSTED_TO_LEDGER, False)).label("trips_posted"),
            func.sum(CurbTrip.total_amount).label("total_earnings"),
            func.max(CurbTrip.created_on).label("last_import")
        ).filter(
            CurbTrip.account_id == account_id
        ).first()
        
        return {
            "total_trips": stats.total_trips or 0,
            "trips_imported": stats.trips_imported or 0,
            "trips_posted": stats.trips_posted or 0,
            "total_earnings": stats.total_earnings or Decimal("0.00"),
            "last_import": stats.last_import,
        }
    
    def get_system_statistics(self) -> dict:
        """Get overall CURB system statistics"""
        return {
            "total_accounts": self.db.query(func.count(CurbAccount.id)).scalar(),
            "active_accounts": self.db.query(func.count(CurbAccount.id)).filter(
                CurbAccount.is_active == True
            ).scalar(),
            "total_trips": self.db.query(func.count(CurbTrip.id)).scalar(),
            "trips_pending_post": self.db.query(func.count(CurbTrip.id)).filter(
                CurbTrip.status == CurbTripStatus.IMPORTED
            ).scalar(),
        }