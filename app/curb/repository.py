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
from app.drivers.models import Driver
from app.vehicles.models import Vehicle, VehicleRegistration
from app.medallions.models import Medallion
from app.drivers.models import TLCLicense
from app.utils.general import apply_multi_filter
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
        
        logger.info(f"Processing {len(trips_to_process)} CURB trips individually")
        
        inserted_count = 0
        updated_count = 0
        failed_count = 0
        
        for i, trip_data in enumerate(trips_to_process, 1):
            try:
                # Use savepoint for each trip
                savepoint = self.db.begin_nested()
                
                # Check if trip already exists
                existing_trip = self.db.query(CurbTrip).filter(
                    CurbTrip.curb_trip_id == trip_data['curb_trip_id']
                ).first()
                
                if existing_trip:
                    # Update existing trip
                    for key, value in trip_data.items():
                        if hasattr(existing_trip, key) and key not in ['id', 'created_on']:
                            setattr(existing_trip, key, value)
                    existing_trip.updated_on = datetime.now()
                    updated_count += 1
                else:
                    # Insert new trip
                    new_trip = CurbTrip(**trip_data)
                    self.db.add(new_trip)
                    inserted_count += 1
                
                # Flush to trigger any constraint violations
                self.db.flush()
                
                # Commit the savepoint if successful
                savepoint.commit()
                
                # Log progress every 100 trips
                if i % 100 == 0:
                    logger.info(f"Processed {i}/{len(trips_to_process)} trips (inserted: {inserted_count}, updated: {updated_count})")
                
            except Exception as e:
                # Rollback only this savepoint
                savepoint.rollback()
                failed_count += 1
                logger.warning(f"Failed to process trip {trip_data.get('curb_trip_id', 'unknown')}: {e}")
                
                # Log first few failures in detail
                if failed_count <= 5:
                    logger.error(f"Trip data that failed: {trip_data}", exc_info=True)
                
                continue
        
        logger.info(
            f"Completed: {inserted_count} inserted, {updated_count} updated, "
            f"{failed_count} failed out of {len(trips_to_process)} total"
        )
        
        return inserted_count, updated_count

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
    
    def list_trips_with_enhanced_filters(
        self,
        page: int = 1,
        per_page: int = 50,
        # Comma-separated ID/Text filters
        trip_ids: Optional[str] = None,
        driver_ids: Optional[str] = None,
        vehicle_plates: Optional[str] = None,
        medallion_numbers: Optional[str] = None,
        tlc_license_numbers: Optional[str] = None,
        # Date range filters
        trip_start_from: Optional[datetime] = None,
        trip_start_to: Optional[datetime] = None,
        trip_end_from: Optional[datetime] = None,
        trip_end_to: Optional[datetime] = None,
        transaction_date_from: Optional[datetime] = None,
        transaction_date_to: Optional[datetime] = None,
        # Amount range filters
        total_amount_from: Optional[Decimal] = None,
        total_amount_to: Optional[Decimal] = None,
        # Payment modes (comma-separated)
        payment_modes: Optional[str] = None,
        # Status filter
        status: Optional[str] = None,
        # Account filter
        account_ids: Optional[List[int]] = None,
        # Sorting
        sort_by: str = "start_time",
        sort_order: str = "desc",
    ) -> Tuple[List[CurbTrip], int]:
        """
        Enhanced trip listing with comprehensive filters optimized for millions of records.
        
        **Optimization Strategy:**
        - Uses indexed columns for filtering (composite indexes)
        - Minimal joins - only loads relationships needed
        - Count query optimization with same filters
        - Proper use of query.options() for eager loading
        
        Returns:
            Tuple of (filtered_trips, total_count)
        """
        
        # Base query with optimized joins
        # Only join tables that are actually needed for filtering or display
        query = self.db.query(CurbTrip).options(
            joinedload(CurbTrip.account),  # Always needed for display
            joinedload(CurbTrip.driver),    # Always needed for display
            joinedload(CurbTrip.lease),     # Always needed for display
        )
        
        # Track which additional joins we need
        needs_vehicle_join = vehicle_plates is not None
        needs_medallion_join = medallion_numbers is not None
        needs_tlc_join = tlc_license_numbers is not None
        
        # Conditionally join only when needed for filtering
        if needs_vehicle_join:
            query = query.outerjoin(Vehicle, CurbTrip.vehicle_id == Vehicle.id)
            query = query.outerjoin(
                VehicleRegistration,
                and_(
                    Vehicle.id == VehicleRegistration.vehicle_id,
                    VehicleRegistration.is_active == True
                )
            )
        
        if needs_medallion_join:
            query = query.outerjoin(Medallion, CurbTrip.medallion_id == Medallion.id)
        
        if needs_tlc_join:
            query = query.outerjoin(Driver, CurbTrip.driver_id == Driver.id)
            query = query.outerjoin(TLCLicense, Driver.tlc_license_id == TLCLicense.id)
        
        # ==================================================================
        # APPLY FILTERS (using indexed columns where possible)
        # ==================================================================
        
        # 1. Trip IDs filter (comma-separated) - uses unique index
        if trip_ids:
            query = apply_multi_filter(query, CurbTrip.curb_trip_id, trip_ids)
        
        # 2. Driver IDs filter (comma-separated) - uses indexed column
        if driver_ids:
            driver_id_list = [int(did.strip()) for did in driver_ids.split(',') if did.strip().isdigit()]
            if driver_id_list:
                query = query.filter(CurbTrip.driver_id.in_(driver_id_list))
        
        # 3. Vehicle plates filter (comma-separated)
        if vehicle_plates:
            query = apply_multi_filter(query, VehicleRegistration.plate_number, vehicle_plates)
        
        # 4. Medallion numbers filter (comma-separated) - uses curb_cab_number (indexed)
        if medallion_numbers:
            query = apply_multi_filter(query, CurbTrip.curb_cab_number, medallion_numbers)
        
        # 5. TLC License numbers filter (comma-separated)
        if tlc_license_numbers:
            query = apply_multi_filter(query, TLCLicense.tlc_license_number, tlc_license_numbers)
        
        # 6. Trip start date range filter - uses composite index idx_curb_trip_time_status
        if trip_start_from:
            query = query.filter(CurbTrip.start_time >= trip_start_from)
        if trip_start_to:
            query = query.filter(CurbTrip.start_time <= trip_start_to)
        
        # 7. Trip end date range filter
        if trip_end_from:
            query = query.filter(CurbTrip.end_time >= trip_end_from)
        if trip_end_to:
            query = query.filter(CurbTrip.end_time <= trip_end_to)
        
        # 8. Transaction date range filter
        if transaction_date_from:
            query = query.filter(CurbTrip.transaction_date >= transaction_date_from)
        if transaction_date_to:
            query = query.filter(CurbTrip.transaction_date <= transaction_date_to)
        
        # 9. Total amount range filter
        if total_amount_from:
            query = query.filter(CurbTrip.total_amount >= total_amount_from)
        if total_amount_to:
            query = query.filter(CurbTrip.total_amount <= total_amount_to)
        
        # 10. Payment modes filter (comma-separated) - uses indexed column
        if payment_modes:
            payment_mode_list = [pm.strip().upper() for pm in payment_modes.split(',') if pm.strip()]
            # Convert string values to PaymentType enum
            valid_payment_types = []
            for pm in payment_mode_list:
                try:
                    valid_payment_types.append(PaymentType[pm])
                except KeyError:
                    logger.warning(f"Invalid payment mode: {pm}")
            
            if valid_payment_types:
                query = query.filter(CurbTrip.payment_type.in_(valid_payment_types))
        
        # 11. Status filter - uses composite index idx_curb_trip_time_status
        if status:
            try:
                status_enum = CurbTripStatus[status.upper()]
                query = query.filter(CurbTrip.status == status_enum)
            except KeyError:
                logger.warning(f"Invalid status: {status}")
        
        # 12. Account IDs filter - uses indexed column
        if account_ids:
            query = query.filter(CurbTrip.account_id.in_(account_ids))
        
        # ==================================================================
        # GET TOTAL COUNT (before pagination, with same filters)
        # ==================================================================
        total_count = query.with_entities(func.count(CurbTrip.id)).scalar()
        
        # ==================================================================
        # APPLY SORTING
        # ==================================================================
        sort_column_map = {
            "trip_id": CurbTrip.curb_trip_id,
            "start_time": CurbTrip.start_time,
            "end_time": CurbTrip.end_time,
            "transaction_date": CurbTrip.transaction_date,
            "driver_id": CurbTrip.driver_id,
            "total_amount": CurbTrip.total_amount,
            "payment_type": CurbTrip.payment_type,
            "status": CurbTrip.status,
        }
        
        sort_column = sort_column_map.get(sort_by, CurbTrip.start_time)
        
        if sort_order.lower() == "asc":
            query = query.order_by(sort_column.asc())
        else:
            query = query.order_by(sort_column.desc())
        
        # ==================================================================
        # APPLY PAGINATION
        # ==================================================================
        offset = (page - 1) * per_page
        trips = query.offset(offset).limit(per_page).all()
        
        logger.info(f"Retrieved {len(trips)} CURB trips (page {page}, total: {total_count})")
        
        return trips, total_count