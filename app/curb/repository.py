### app/curb/repository.py

from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

from sqlalchemy import or_, update
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from app.curb.models import CurbTrip, CurbTripStatus
from app.curb.schemas import PaymentType
from app.drivers.models import Driver
from app.medallions.models import Medallion
from app.utils.logger import get_logger

logger = get_logger(__name__)


class CurbRepository:
    """
    Data Access Layer for CURB Trip data.
    Handles all database interactions for the CurbTrip model.
    """

    def __init__(self, db: Session):
        self.db = db

    def get_trip_by_id(self, trip_id: int) -> Optional[CurbTrip]:
        """Fetches a single CURB trip by its internal database primary key."""
        return (
            self.db.query(CurbTrip)
            .options(
                joinedload(CurbTrip.driver).joinedload(Driver.tlc_license),
                joinedload(CurbTrip.medallion),
                joinedload(CurbTrip.vehicle),
            )
            .filter(CurbTrip.id == trip_id)
            .first()
        )

    def get_trip_by_curb_id(self, curb_trip_id: str) -> Optional[CurbTrip]:
        """Fetches a single CURB trip by its unique CURB-provided ID."""
        return (
            self.db.query(CurbTrip)
            .filter(CurbTrip.curb_trip_id == curb_trip_id)
            .first()
        )

    def bulk_insert_or_update(self, trips_data: List[dict]) -> Tuple[int, int]:
        """
        Efficiently inserts new trips and updates existing ones using MySQL's 
        INSERT ... ON DUPLICATE KEY UPDATE to handle race conditions and concurrent writes.

        Args:
            trips_data: A list of dictionaries, where each dict represents a trip.

        Returns:
            A tuple containing the count of (inserted_records, updated_records).
        """
        if not trips_data:
            return 0, 0

        # Additional deduplication safety check within this batch
        seen_ids = set()
        deduplicated_data = []
        for trip in trips_data:
            trip_id = trip["curb_trip_id"]
            if trip_id not in seen_ids:
                deduplicated_data.append(trip)
                seen_ids.add(trip_id)
            else:
                logger.warning(f"Duplicate trip ID in batch detected: {trip_id}")
        
        if len(deduplicated_data) != len(trips_data):
            logger.warning(f"Removed {len(trips_data) - len(deduplicated_data)} duplicate entries from batch")
            trips_data = deduplicated_data

        try:
            # Use MySQL's INSERT ... ON DUPLICATE KEY UPDATE for atomic upsert
            stmt = insert(CurbTrip)
            
            # Get the actual columns that will be in the INSERT statement
            # by using the first trip's keys
            if not trips_data:
                return 0, 0
                
            sample_trip = trips_data[0]
            data_columns = set()
            for trip in trips_data:
                data_columns.update(trip.keys())
            table_columns = set(CurbTrip.__table__.columns.keys())
            
            # Log the data structure for debugging
            logger.debug(f"Sample trip keys: {sorted(data_columns)}")
            logger.debug(f"Table columns: {sorted(table_columns)}")
            
            # Filter trips_data to only include columns that exist in the table
            # This prevents issues with extra fields that don't match the schema
            filtered_data = []
            valid_columns = data_columns.intersection(table_columns)
            
            for trip in trips_data:
                filtered_trip = {k: v for k, v in trip.items() if k in valid_columns}
                # Ensure ALL valid columns are present with explicit None if missing
                for col in valid_columns:
                    if col not in filtered_trip:
                        filtered_trip[col] = None
                filtered_data.append(filtered_trip)
            
            # Update the data_columns to reflect only the valid columns
            data_columns = valid_columns
            
            # Build the ON DUPLICATE KEY UPDATE clause only for columns that are:
            # 1. Present in the filtered data being inserted
            # 2. Exist in the database table 
            # 3. Are not primary key or unique key fields
            update_dict = {}
            updateable_columns = []
            for column in data_columns:
                if column not in ['id', 'curb_trip_id']:  # Exclude primary/unique keys
                    update_dict[column] = stmt.inserted[column]
                    updateable_columns.append(column)
            
            # Validate that we have at least some columns to update
            if not update_dict:
                logger.warning("No updateable columns found, performing INSERT IGNORE instead")
                # Fall back to INSERT IGNORE if no updateable columns
                ignore_stmt = stmt.prefix_with("IGNORE")
                result = self.db.execute(ignore_stmt, filtered_data)
                return len(filtered_data), 0  # All considered inserts
            
            # Log the columns being updated for debugging
            logger.debug(f"UPSERT filtered data columns: {sorted(data_columns)}")
            logger.debug(f"UPSERT updateable columns: {sorted(updateable_columns)}")
            
            # Execute the upsert with ON DUPLICATE KEY UPDATE
            upsert_stmt = stmt.on_duplicate_key_update(**update_dict)
            result = self.db.execute(upsert_stmt, filtered_data)
            
            # Log successful upsert
            logger.info(f"Successfully processed {len(filtered_data)} trip records with MySQL upsert")
            
            # Since we can't easily distinguish inserts from updates with ON DUPLICATE KEY UPDATE,
            # we'll estimate based on MySQL rowcount behavior
            if hasattr(result, 'rowcount') and result.rowcount > 0:
                # MySQL rowcount behavior: 1 for insert, 2 for update, 0 for no change
                if result.rowcount >= len(filtered_data):
                    # Likely mostly updates
                    estimated_updates = len(filtered_data)
                    estimated_inserts = 0
                else:
                    # Mixed or mostly inserts
                    estimated_inserts = len(filtered_data) // 2
                    estimated_updates = len(filtered_data) - estimated_inserts
            else:
                # Fallback estimation
                estimated_inserts = len(filtered_data) // 2
                estimated_updates = len(filtered_data) - estimated_inserts
            
            return estimated_inserts, estimated_updates
            
        except (SQLAlchemyError, IntegrityError) as e:
            logger.error(f"Bulk upsert failed, falling back to individual processing: {str(e)}")
            # Rollback the failed transaction
            self.db.rollback()
            # Fallback to individual record processing for better error handling
            return self._fallback_individual_processing(trips_data)

    def _fallback_individual_processing(self, trips_data: List[dict]) -> Tuple[int, int]:
        """
        Fallback method to process trips individually when bulk operation fails.
        """
        inserted_count = 0
        updated_count = 0
        
        for trip_data in trips_data:
            curb_trip_id = trip_data["curb_trip_id"]
            
            try:
                # Check if trip already exists
                existing_trip = self.db.query(CurbTrip).filter(
                    CurbTrip.curb_trip_id == curb_trip_id
                ).first()
                
                if existing_trip:
                    # Update existing record
                    for key, value in trip_data.items():
                        if hasattr(existing_trip, key):
                            setattr(existing_trip, key, value)
                    updated_count += 1
                else:
                    # Create new record
                    new_trip = CurbTrip(**trip_data)
                    self.db.add(new_trip)
                    inserted_count += 1
                    
            except (SQLAlchemyError, IntegrityError) as e:
                logger.error(f"Failed to process trip {curb_trip_id}: {str(e)}")
                # Skip this record and continue
                continue
        
        return inserted_count, updated_count

    def get_unreconciled_trips(self) -> List[CurbTrip]:
        """Fetches all trips that have not yet been successfully reconciled with CURB."""
        return (
            self.db.query(CurbTrip)
            .filter(CurbTrip.status == CurbTripStatus.UNRECONCILED)
            .all()
        )

    def update_trip_status(
        self,
        trip_id: int,
        status: CurbTripStatus,
        reconciliation_id: Optional[str] = None,
    ):
        """Updates the status and optionally the reconciliation ID of a single trip."""
        stmt = (
            update(CurbTrip)
            .where(CurbTrip.id == trip_id)
            .values(status=status, reconciliation_id=reconciliation_id)
        )
        self.db.execute(stmt)

    def get_unposted_credit_card_trips_for_period(
        self, start_date: date, end_date: date
    ) -> List[CurbTrip]:
        """
        Fetches all reconciled, credit card trips within a specific date range
        that have not yet been posted to the ledger.
        """
        return (
            self.db.query(CurbTrip)
            .filter(
                CurbTrip.status == CurbTripStatus.RECONCILED,
                CurbTrip.payment_type == "CREDIT_CARD",
                CurbTrip.start_time >= start_date,
                CurbTrip.end_time <= end_date,
            )
            .all()
        )

    def list_trips(
        self,
        page: int,
        per_page: int,
        sort_by: str,
        sort_order: str,
        trip_id: Optional[str] = None,
        driver_id_tlc: Optional[str] = None,
        medallion_no: Optional[str] = None,
        plate_no: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        transaction_date: Optional[date] = None,
    ) -> Tuple[List[CurbTrip], int]:
        """
        Retrieves a paginated, sorted, and filtered list of CURB trips.
        This is the primary query engine for both 'View Trips' and 'View Curb Data'.
        Supports comma-separated values for driver and medallion filters.
        """
        query = (
            self.db.query(CurbTrip)
            .options(
                joinedload(CurbTrip.driver).joinedload(Driver.tlc_license),
                joinedload(CurbTrip.medallion),
                joinedload(CurbTrip.vehicle),
            )
            .outerjoin(Driver, CurbTrip.driver_id == Driver.driver_id)
            .outerjoin(Medallion, CurbTrip.medallion_id == Medallion.id)
        )

        # Apply filters
        if trip_id:
            query = query.filter(CurbTrip.curb_trip_id.ilike(f"%{trip_id}%"))

        # **MODIFICATION START**
        if driver_id_tlc:
            # Handle comma-separated list of driver IDs/TLC numbers
            driver_filters = [f.strip() for f in driver_id_tlc.split(",") if f.strip()]
            if driver_filters:
                query = query.filter(
                    or_(
                        *[
                            CurbTrip.curb_driver_id.ilike(f"%{filt}%")
                            for filt in driver_filters
                        ]
                    )
                )

        if medallion_no:
            # Handle comma-separated list of medallion numbers
            medallion_filters = [
                f.strip() for f in medallion_no.split(",") if f.strip()
            ]
            if medallion_filters:
                query = query.filter(
                    or_(
                        *[
                            CurbTrip.curb_cab_number.ilike(f"%{filt}%")
                            for filt in medallion_filters
                        ]
                    )
                )
        # **MODIFICATION END**

        if plate_no:
            query = query.filter(CurbTrip.plate.ilike(f"%{plate_no}%"))
        if start_date:
            start_datetime = datetime.combine(start_date, datetime.min.time())
            query = query.filter(CurbTrip.start_time >= start_datetime)
        if end_date:
            end_datetime = datetime.combine(end_date, datetime.max.time())
            query = query.filter(CurbTrip.start_time <= end_datetime)
        if transaction_date:
            # Filter by transaction_date (exact date match)
            transaction_start = datetime.combine(transaction_date, datetime.min.time())
            transaction_end = datetime.combine(transaction_date, datetime.max.time())
            query = query.filter(
                CurbTrip.transaction_date >= transaction_start,
                CurbTrip.transaction_date <= transaction_end,
            )

        # Determine total items before pagination
        total_items = query.count()

        # Apply sorting
        sort_column_map = {
            "trip_id": CurbTrip.curb_trip_id,
            "driver_id_tlc": CurbTrip.curb_driver_id,
            "cab_no": CurbTrip.curb_cab_number,
            "vehicle_plate": CurbTrip.plate,
            "start_time": CurbTrip.start_time,
            "end_time": CurbTrip.end_time,
            "trip_start_date": CurbTrip.start_time,
            "trip_end_date": CurbTrip.end_time,
            "total_amount": CurbTrip.total_amount,
            "payment_mode": CurbTrip.payment_type,
            "status": CurbTrip.status,
            "medallion_no": CurbTrip.curb_cab_number,
            "transaction_date": CurbTrip.transaction_date,
        }

        sort_column = sort_column_map.get(sort_by, CurbTrip.start_time)
        if sort_order.lower() == "desc":
            query = query.order_by(sort_column.desc())
        else:
            query = query.order_by(sort_column.asc())

        # Apply pagination
        query = query.offset((page - 1) * per_page).limit(per_page)

        return query.all(), total_items

    def get_trips_by_driver_and_date_range(
        self,
        driver_id: Optional[str],
        tlc_license_no: Optional[str],
        start_date: date,
        end_date: date,
    ) -> List[CurbTrip]:
        """
        Get all trips for a specific driver within a date range.
        Can search by either internal driver_id or TLC license number.
        """
        query = self.db.query(CurbTrip)

        if driver_id:
            # Join with driver table to filter by internal driver_id
            query = query.join(Driver, CurbTrip.driver_id == Driver.id).filter(
                Driver.driver_id == driver_id
            )
        elif tlc_license_no:
            # Filter by TLC license (curb_driver_id field)
            query = query.filter(CurbTrip.curb_driver_id == tlc_license_no)
        else:
            return []

        query = query.filter(
            CurbTrip.start_time >= start_date,
            CurbTrip.end_time < (end_date + timedelta(days=1)),
        )

        return query.all()

    def get_trips_by_medallion_and_date_range(
        self, medallion_number: str, start_date: date, end_date: date
    ) -> List[CurbTrip]:
        """
        Get all trips for a specific medallion within a date range.
        """
        return (
            self.db.query(CurbTrip)
            .filter(
                CurbTrip.curb_cab_number == medallion_number,
                CurbTrip.start_time >= start_date,
                CurbTrip.end_time < (end_date + timedelta(days=1)),
            )
            .all()
        )

    def get_existing_trip_ids_for_filters(
        self,
        start_date: date,
        end_date: date,
        driver_ids: Optional[List[str]] = None,
        medallion_numbers: Optional[List[str]] = None,
    ) -> List[str]:
        """
        Get existing CURB trip IDs for given filters to avoid re-importing.
        """
        query = self.db.query(CurbTrip.curb_trip_id).filter(
            CurbTrip.start_time >= start_date,
            CurbTrip.end_time < (end_date + timedelta(days=1)),
        )

        if driver_ids:
            query = query.filter(CurbTrip.curb_driver_id.in_(driver_ids))

        if medallion_numbers:
            query = query.filter(CurbTrip.curb_cab_number.in_(medallion_numbers))

        return [row[0] for row in query.all()]

    def get_mapped_credit_card_trips_for_posting(
        self,
        start_date: date,
        end_date: date,
        lease_id: Optional[int] = None,
        driver_id: Optional[int] = None,
    ) -> List[CurbTrip]:
        """
        Fetches all MAPPED, credit card trips within a specific date range
        that have not yet been posted to the ledger, with optional filters.
        """
        end_datetime = datetime.combine(end_date, datetime.max.time())

        query = self.db.query(CurbTrip).filter(
            CurbTrip.status == CurbTripStatus.MAPPED,
            CurbTrip.payment_type == PaymentType.CREDIT_CARD,
            CurbTrip.start_time >= start_date,
            CurbTrip.start_time <= end_datetime,
        )

        if lease_id:
            query = query.filter(CurbTrip.lease_id == lease_id)

        if driver_id:
            query = query.filter(CurbTrip.driver_id == driver_id)

        return query.all()

    def count_trips_by_status_and_filters(
        self,
        status: CurbTripStatus,
        start_date: date,
        end_date: date,
        driver_id: Optional[str] = None,
        medallion_number: Optional[str] = None,
    ) -> int:
        """
        Count trips by status with optional filters for monitoring/reporting.
        """
        query = self.db.query(CurbTrip).filter(
            CurbTrip.status == status,
            CurbTrip.start_time >= start_date,
            CurbTrip.end_time < (end_date + timedelta(days=1)),
        )

        if driver_id:
            query = query.filter(CurbTrip.curb_driver_id == driver_id)

        if medallion_number:
            query = query.filter(CurbTrip.curb_cab_number == medallion_number)

        return query.count()

    def find_trips_by_status(self, status: CurbTripStatus) -> List[CurbTrip]:
        """
        Fetches all trips with the specified status.
        """
        return self.db.query(CurbTrip).filter(CurbTrip.status == status).all()
