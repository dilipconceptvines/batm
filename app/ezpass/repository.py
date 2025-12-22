### app/ezpass/repository.py

from datetime import date, datetime , time
from typing import List, Optional, Tuple

from sqlalchemy import update , or_ , func, asc, desc
from sqlalchemy.orm import Session, joinedload
from app.drivers.models import Driver
from app.ezpass.models import (
    EZPassImport,
    EZPassImportStatus,
    EZPassTransaction,
    EZPassTransactionStatus,
)
from app.medallions.models import Medallion
from app.vehicles.models import Vehicle
from app.utils.logger import get_logger

logger = get_logger(__name__)


class EZPassRepository:
    """
    Data Access Layer for EZPass Imports and Transactions.
    """

    def __init__(self, db: Session):
        self.db = db

    def create_import_record(self, file_name: str, total_records: int) -> EZPassImport:
        """Creates a new parent record for a CSV import batch."""
        import_record = EZPassImport(
            file_name=file_name,
            total_records=total_records,
            status=EZPassImportStatus.PROCESSING,
        )
        self.db.add(import_record)
        self.db.flush()  # Flush to get the ID for the transactions
        return import_record

    def update_import_record_status(
        self, import_id: int, status: EZPassImportStatus, successful: int, failed: int
    ):
        """Updates the status and counts of an import record upon completion."""
        stmt = (
            update(EZPassImport)
            .where(EZPassImport.id == import_id)
            .values(
                status=status,
                successful_records=successful,
                failed_records=failed,
                updated_on=datetime.utcnow(),
            )
        )
        self.db.execute(stmt)

    def bulk_insert_transactions(self, transactions_data: List[dict]):
        """
        Performs a bulk insert of new EZPassTransaction records with individual error handling.
        Returns the number of successfully inserted transactions.
        """
        if not transactions_data:
            return 0
        
        successful_count = 0
        
        # Process each transaction individually to handle constraint violations
        for transaction_data in transactions_data:
            try:
                # Use a savepoint for each transaction
                savepoint = self.db.begin_nested()
                
                # Check if transaction already exists
                existing = self.db.query(EZPassTransaction).filter(
                    EZPassTransaction.transaction_id == transaction_data['transaction_id']
                ).first()
                
                if existing:
                    savepoint.rollback()
                    logger.debug(f"Skipping duplicate transaction: {transaction_data['transaction_id']}")
                    continue
                
                # Create new transaction
                new_transaction = EZPassTransaction(**transaction_data)
                self.db.add(new_transaction)
                
                # Flush to trigger any constraint violations
                self.db.flush()
                
                # Commit the savepoint if successful
                savepoint.commit()
                successful_count += 1
                
            except Exception as e:
                # Rollback only this savepoint, not the entire session
                savepoint.rollback()
                logger.debug(f"Failed to insert transaction {transaction_data.get('transaction_id', 'unknown')}: {e}")
                # Don't re-raise - let the caller handle the error tracking
                continue
        
        logger.info(f"Successfully inserted {successful_count} out of {len(transactions_data)} transactions.")
        return successful_count


    def get_transactions_by_status(
        self, status: EZPassTransactionStatus
    ) -> List[EZPassTransaction]:
        """Retrieves all transactions currently in a specific status."""
        return (
            self.db.query(EZPassTransaction)
            .filter(EZPassTransaction.status == status)
            .all()
        )

    def update_transaction(self, transaction_id: int, updates: dict):
        """Updates specific fields of a single transaction record."""
        updates["updated_on"] = datetime.utcnow()
        stmt = (
            update(EZPassTransaction)
            .where(EZPassTransaction.id == transaction_id)
            .values(**updates)
        )
        self.db.execute(stmt)

    def list_transactions(
        self,
        page: int,
        per_page: int,
        sort_by: str,
        sort_order: str,
        from_transaction_date: Optional[date] = None,
        to_transaction_date: Optional[date] = None,
        from_transaction_time: Optional[time] = None,
        to_transaction_time: Optional[time] = None,
        from_posting_date: Optional[date] = None,
        to_posting_date: Optional[date] = None,
        from_amount: Optional[float] = None,
        to_amount: Optional[float] = None,
        transaction_id: Optional[str] = None,
        entry_plaza: Optional[str] = None,
        exit_plaza: Optional[str] = None,
        ezpass_class: Optional[str] = None,
        agency: Optional[str] = None,
        vin: Optional[str] = None,
        medallion_no: Optional[str] = None,
        driver_id: Optional[str] = None,
        plate_number: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Tuple[List[EZPassTransaction], int]:
        """
        Retrieves a paginated, sorted, and filtered list of EZPass transactions.
        """
        from sqlalchemy.orm import selectinload
        
        query = (
            self.db.query(EZPassTransaction)
            .options(
                selectinload(EZPassTransaction.driver),
                selectinload(EZPassTransaction.medallion),
                selectinload(EZPassTransaction.vehicle),
            )
            .outerjoin(Driver, EZPassTransaction.driver_id == Driver.id)
            .outerjoin(Medallion, EZPassTransaction.medallion_id == Medallion.id)
            .outerjoin(Vehicle, EZPassTransaction.vehicle_id == Vehicle.id)
        )

        # Apply filters
        # if transaction_date:
        #     start_of_day = datetime.combine(transaction_date, datetime.min.time())
        #     end_of_day = datetime.combine(transaction_date, datetime.max.time())
        #     query = query.filter(EZPassTransaction.transaction_datetime.between(start_of_day, end_of_day))

        if from_transaction_date:
            from_transaction_date = datetime.combine(from_transaction_date, datetime.min.time())
            query = query.filter(EZPassTransaction.transaction_datetime >= from_transaction_date)

        if to_transaction_date:
            to_transaction_date = datetime.combine(to_transaction_date, datetime.max.time())
            query = query.filter(EZPassTransaction.transaction_datetime <= to_transaction_date)

        if from_transaction_time:
            query = query.filter(
                func.time(EZPassTransaction.transaction_datetime) >= from_transaction_time
            )
        if to_transaction_time:
            query = query.filter(
                func.time(EZPassTransaction.transaction_datetime) <= to_transaction_time
            )

        if medallion_no:
            numbers = [num.strip() for num in medallion_no.split(',') if num.strip()]            
            query = query.filter(or_(*[Medallion.medallion_number.ilike(f"%{n}%") for n in numbers]))

        if driver_id:
            ids = [id.strip() for id in driver_id.split(',') if id.strip()]
            query = query.filter(or_(*[Driver.driver_id.ilike(f"%{i}%") for i in ids]))

        if plate_number:
            plates = [plate.strip() for plate in plate_number.split(',') if plate.strip()]
            query = query.filter(or_(*[EZPassTransaction.tag_or_plate.ilike(f"%{p}%") for p in plates]))

        if from_posting_date:
            from_posting_date = datetime.combine(from_posting_date, datetime.min.time())
            query = query.filter(EZPassTransaction.posting_date >= from_posting_date)

        if to_posting_date:
            to_posting_date = datetime.combine(to_posting_date, datetime.max.time())
            query = query.filter(EZPassTransaction.posting_date <= to_posting_date)

        if from_amount:
            query = query.filter(EZPassTransaction.amount >= from_amount)

        if to_amount:
            query = query.filter(EZPassTransaction.amount <= to_amount)

        if agency:
            agencies = [agency.strip().upper() for agency in agency.split(',') if agency.strip()]
            query = query.filter(or_(*[EZPassTransaction.agency.ilike(f"%{a}%") for a in agencies]))

        if vin:
            vins = [vin.strip() for vin in vin.split(',') if vin.strip()]
            query = query.filter(or_(*[Vehicle.vin.ilike(f"%{v}%") for v in vins]))

        if ezpass_class:
            classes = [cls.strip() for cls in ezpass_class.split(',') if cls.strip()]
            query = query.filter(EZPassTransaction.ezpass_class.in_(classes))
        if transaction_id:
            tr_ids = [id.strip() for id in transaction_id.split(',') if id.strip()]
            query = query.filter(or_(*[EZPassTransaction.transaction_id.ilike(f"%{i}%") for i in tr_ids]))
        if entry_plaza:
            entry_plazas = [plaza.strip().upper() for plaza in entry_plaza.split(',') if plaza.strip()]
            query = query.filter(or_(*[EZPassTransaction.entry_plaza.ilike(f"%{p}%") for p in entry_plazas]))

        if exit_plaza:
            exit_plazas = [plaza.strip().upper() for plaza in exit_plaza.split(',') if plaza.strip()]
            query = query.filter(or_(*[EZPassTransaction.exit_plaza.ilike(f"%{p}%") for p in exit_plazas]))


        if status:
            query = query.filter(EZPassTransaction.status == status)

        total_items = query.with_entities(func.count(EZPassTransaction.id)).scalar()

        sort_column_map = {
            "transaction_date": EZPassTransaction.transaction_datetime,
            "medallion_no": Medallion.medallion_number,
            "driver_id": Driver.driver_id,
            "vin": Vehicle.vin,
            "ezpass_class": EZPassTransaction.ezpass_class,
            "transaction_id": EZPassTransaction.transaction_id,
            "entry_plaza": EZPassTransaction.entry_plaza,
            "exit_plaza": EZPassTransaction.exit_plaza,
            "amount": EZPassTransaction.amount,
            "agency": EZPassTransaction.agency,
            "plate_number": EZPassTransaction.tag_or_plate,
            "posting_date": EZPassTransaction.posting_date,
            "status": EZPassTransaction.status,
        }

        sort_column = sort_column_map.get(sort_by, EZPassTransaction.transaction_datetime)
        if sort_order.lower() == "desc":
            query = query.order_by(sort_column.desc())
        else:
            query = query.order_by(sort_column.asc())

        query = query.offset((page - 1) * per_page).limit(per_page)

        return query.all(), total_items
    
    def get_transaction_by_id(self, transaction_id: int) -> Optional[EZPassTransaction]:
        """Retrieves a single EZPass transaction by its internal ID."""
        return (
            self.db.query(EZPassTransaction)
            .options(
                joinedload(EZPassTransaction.driver),
                joinedload(EZPassTransaction.medallion),
                joinedload(EZPassTransaction.vehicle),
            )
            .filter(EZPassTransaction.id == transaction_id)
            .first()
        )
    
    def list_import_logs(
        self,
        page: int = 1,
        per_page: int = 10,
        sort_by: str = "import_timestamp",
        sort_order: str = "desc",
        from_log_date: Optional[date] = None,
        to_log_date: Optional[date] = None,
        log_type: Optional[str] = None,
        log_status: Optional[str] = None,
        file_name: Optional[str] = None,
    ) -> tuple[List[EZPassImport], int]:
        """
        List EZPass import logs with filtering, sorting, and pagination.
        
        Date Range Filter:
        - from_log_date: Start date (inclusive)
        - to_log_date: End date (inclusive)
        
        Returns:
            Tuple of (import_logs, total_count)
        """
        from app.ezpass.models import EZPassImport, EZPassImportStatus
        
        query = self.db.query(EZPassImport)
        
        # Apply date range filter
        if from_log_date:
            query = query.filter(
                EZPassImport.import_timestamp >= datetime.combine(from_log_date, datetime.min.time())
            )
        
        if to_log_date:
            query = query.filter(
                EZPassImport.import_timestamp <= datetime.combine(to_log_date, datetime.max.time())
            )
        
        # Apply file name filter
        if file_name:
            query = query.filter(EZPassImport.file_name.ilike(f"%{file_name}%"))
        
        # Apply log status filter
        # Map UI values to DB enum values
        if log_status:
            status_mapping = {
                "Success": EZPassImportStatus.COMPLETED,
                "Partial Success": EZPassImportStatus.COMPLETED,  # Will filter in Python
                "Failure": EZPassImportStatus.FAILED,
                "Pending": EZPassImportStatus.PENDING,
                "Processing": EZPassImportStatus.PROCESSING,
            }
            if log_status in status_mapping:
                db_status = status_mapping[log_status]
                query = query.filter(EZPassImport.status == db_status)
        
        # Note: log_type filter not applied since all records are "Import" type currently
        # Can be extended when association/posting logs are added
        
        # Get total count before pagination
        total_items = query.count()
        
        # Apply sorting
        sort_column = getattr(EZPassImport, sort_by, EZPassImport.import_timestamp)
        if sort_order == "asc":
            query = query.order_by(asc(sort_column))
        else:
            query = query.order_by(desc(sort_column))
        
        # Apply pagination
        offset = (page - 1) * per_page
        query = query.offset(offset).limit(per_page)
        
        # Execute query
        import_logs = query.all()
        
        # Post-process for "Partial Success" filtering
        if log_status == "Partial Success":
            import_logs = [
                log for log in import_logs 
                if log.status == EZPassImportStatus.COMPLETED 
                and log.failed_records > 0
            ]
            total_items = len(import_logs)  # Recalculate total after filtering
        
        return import_logs, total_items