### app/pvb/repository.py

from datetime import date, datetime, time
from typing import List, Optional, Tuple

from sqlalchemy import func, update , or_
from sqlalchemy.orm import Session, joinedload

from app.drivers.models import Driver
from app.medallions.models import Medallion
from app.vehicles.models import Vehicle
from app.leases.models import Lease
from app.pvb.models import (
    PVBImport,
    PVBImportStatus,
    PVBViolation,
    PVBViolationStatus,
)
from app.utils.logger import get_logger
from app.utils.general import apply_multi_filter

logger = get_logger(__name__)


class PVBRepository:
    """
    Data Access Layer for PVB Imports and Violations.
    """

    def __init__(self, db: Session):
        self.db = db

    def create_import_record(self, file_name: str, total_records: int) -> PVBImport:
        """Creates a new parent record for a CSV import batch."""
        import_record = PVBImport(
            file_name=file_name,
            total_records=total_records,
            status=PVBImportStatus.PROCESSING,
        )
        self.db.add(import_record)
        self.db.flush()
        return import_record

    def update_import_record_status(
        self, import_id: int, status: PVBImportStatus, successful: int, failed: int
    ):
        """Updates the status and counts of an import record upon completion."""
        stmt = (
            update(PVBImport)
            .where(PVBImport.id == import_id)
            .values(
                status=status,
                successful_records=successful,
                failed_records=failed,
                updated_on=datetime.utcnow(),
            )
        )
        self.db.execute(stmt)

    def bulk_insert_violations(self, violations_data: List[dict]):
        """Performs a bulk insert of new PVBViolation records, skipping duplicates."""
        if not violations_data:
            return
            
        incoming_summons = {v['summons'] for v in violations_data}
        existing_summons = {
            res[0] for res in self.db.query(PVBViolation.summons)
            .filter(PVBViolation.summons.in_(incoming_summons))
        }
        
        new_violations = [
            PVBViolation(**data)
            for data in violations_data
            if data['summons'] not in existing_summons
        ]

        if new_violations:
            self.db.add_all(new_violations)
        
        logger.info(f"Prepared {len(new_violations)} new PVB violations for insertion. Skipped {len(existing_summons)} duplicates.")


    def get_violation_by_summons(self, summons: str) -> Optional[PVBViolation]:
        """Fetches a single violation by its unique summons number."""
        return self.db.query(PVBViolation).filter(PVBViolation.summons == summons).first()
    
    def get_violation_by_id(self, violation_id: int) -> Optional[PVBViolation]:
        """Retrieves a single violation record by its ID."""
        return self.db.query(PVBViolation).filter(PVBViolation.id == violation_id).first()

    def get_violations_by_status(
        self, status: PVBViolationStatus
    ) -> List[PVBViolation]:
        """Retrieves all violations currently in a specific status."""
        return (
            self.db.query(PVBViolation)
            .filter(PVBViolation.status == status)
            .all()
        )

    def update_violation(self, violation_id: int, updates: dict):
        """Updates specific fields of a single violation record."""
        updates["updated_on"] = datetime.utcnow()
        stmt = (
            update(PVBViolation)
            .where(PVBViolation.id == violation_id)
            .values(**updates)
        )
        self.db.execute(stmt)
        self.db.commit()

    def list_violations(
        self,
        page: int,
        per_page: int,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        plate: Optional[str] = None,
        state: Optional[str] = None,
        type: Optional[str] = None,
        summons: Optional[str] = None,
        from_issue_date: Optional[date] = None,
        to_issue_date: Optional[date] = None,
        from_issue_time: Optional[time] = None,
        to_issue_time: Optional[time] = None,
        from_posting_date: Optional[date] = None,
        to_posting_date: Optional[date] = None,
        from_amount: Optional[float] = None,
        to_amount: Optional[float] = None,
        from_fine: Optional[float] = None,
        to_fine: Optional[float] = None,
        from_penalty: Optional[float] = None,
        to_penalty: Optional[float] = None,
        from_interest: Optional[float] = None,
        to_interest: Optional[float] = None,
        from_reduction: Optional[float] = None,
        to_reduction: Optional[float] = None,
        from_processing_fee: Optional[float] = None,
        to_processing_fee: Optional[float] = None,
        failure_reason: Optional[str] = None,
        lease_id: Optional[str] = None,
        vin: Optional[str] = None,
        driver_id: Optional[str] = None,
        medallion_no: Optional[str] = None,
        status: Optional[str] = None,
        source: Optional[str] = None,
        violation_code: Optional[str] = None,
        violation_country: Optional[str] = None,
        street_name: Optional[str] = None
    ) -> Tuple[List[PVBViolation], int]:
        """
        Retrieves a paginated, sorted, and filtered list of PVB violations.
        """
        query = (
            self.db.query(PVBViolation)
            .options(
                joinedload(PVBViolation.driver),
                joinedload(PVBViolation.medallion),
                joinedload(PVBViolation.lease),
                joinedload(PVBViolation.vehicle),
            )
            .outerjoin(Driver, PVBViolation.driver_id == Driver.id)
            .outerjoin(Medallion, PVBViolation.medallion_id == Medallion.id)
            .outerjoin(Vehicle, PVBViolation.vehicle_id == Vehicle.id)
            .outerjoin(Lease, PVBViolation.lease_id == Lease.id)
        )

        # Apply filters
        if plate:
            query = apply_multi_filter(query, PVBViolation.plate, plate)

        if state:
            query = apply_multi_filter(query, PVBViolation.state, state)

        if type:
            query = apply_multi_filter(query, PVBViolation.type, type)
            
        if summons:
            query = apply_multi_filter(query, PVBViolation.summons, summons)

        if from_issue_date:
            from_issue_date = datetime.combine(from_issue_date, datetime.min.time())
            query = query.filter(PVBViolation.issue_date >= from_issue_date)

        if to_issue_date:
            to_issue_date = datetime.combine(to_issue_date, datetime.max.time())
            query = query.filter(PVBViolation.issue_date <= to_issue_date)

        if from_issue_time:
            query = query.filter(PVBViolation.issue_time >= from_issue_time)

        if to_issue_time:
            query = query.filter(PVBViolation.issue_time <= to_issue_time)

        if from_posting_date:
            from_posting_date = datetime.combine(from_posting_date, datetime.min.time())
            query = query.filter(PVBViolation.posting_date >= from_posting_date)

        if to_posting_date:
            to_posting_date = datetime.combine(to_posting_date, datetime.max.time())
            query = query.filter(PVBViolation.posting_date <= to_posting_date)

        if from_amount:
            query = query.filter(PVBViolation.amount_due >= from_amount)
            
        if to_amount:
            query = query.filter(PVBViolation.amount_due <= to_amount)

        if from_fine:
            query = query.filter(PVBViolation.fine >= from_fine)

        if to_fine:
            query = query.filter(PVBViolation.fine <= to_fine)

        if from_penalty:
            query = query.filter(PVBViolation.penalty >= from_penalty)

        if to_penalty:
            query = query.filter(PVBViolation.penalty <= to_penalty)
        
        if from_interest:
            query = query.filter(PVBViolation.interest >= from_interest)

        if to_interest:
            query = query.filter(PVBViolation.interest <= to_interest)

        if from_reduction:
            query = query.filter(PVBViolation.reduction >= from_reduction)

        if to_reduction:
            query = query.filter(PVBViolation.reduction <= to_reduction)

        if from_processing_fee:
            query = query.filter(PVBViolation.processing_fee >= from_processing_fee)

        if to_processing_fee:
            query = query.filter(PVBViolation.processing_fee <= to_processing_fee)

        if failure_reason:
            query = apply_multi_filter(query, PVBViolation.failure_reason, failure_reason)

        if lease_id:
            query = apply_multi_filter(query, Lease.lease_id, lease_id)

        if vin:
            query = apply_multi_filter(query, Vehicle.vin, vin)

        if driver_id:
            query = apply_multi_filter(query, Driver.driver_id, driver_id)

        if medallion_no:
            query = apply_multi_filter(query, Medallion.medallion_number, medallion_no)

        if status:
            statuses = [s.strip() for s in status.split(',') if s.strip()]
            query = query.filter(PVBViolation.status.in_(statuses))

        if source:
            sources = [s.strip() for s in source.split(',') if s.strip()]
            query = query.filter(PVBViolation.source.in_(sources))

        if violation_code:
            query = apply_multi_filter(query, PVBViolation.violation_code, violation_code)

        if violation_country:
            query = apply_multi_filter(query, PVBViolation.violation_country, violation_country)

        if street_name:
            query = apply_multi_filter(query, PVBViolation.street_name, street_name)


        total_items = query.with_entities(func.count(PVBViolation.id)).scalar()

        # Apply sorting
        sort_column_map = {
            "plate": PVBViolation.plate,
            "state": PVBViolation.state,
            "type": PVBViolation.type,
            "summons": PVBViolation.summons,
            "issue_datetime": PVBViolation.issue_date,
            "medallion_no": Medallion.medallion_number,
            "driver_id": Driver.driver_id,
            "lease_id": Lease.lease_id,
            "vin": Vehicle.vin,
            "status": PVBViolation.status,
            "amount": PVBViolation.amount_due,
            "fine": PVBViolation.fine,
            "penalty": PVBViolation.penalty,
            "interest": PVBViolation.interest,
            "reduction": PVBViolation.reduction,
            "processing_fee": PVBViolation.processing_fee,
            "failure_reason": PVBViolation.failure_reason,
            "posting_date": PVBViolation.posting_date,
            "source": PVBViolation.source,
            "violation_code": PVBViolation.violation_code,
            "violation_country": PVBViolation.violation_country,
            "street_name": PVBViolation.street_name,
        }
        
        if sort_by and sort_order:
            sort_column = sort_column_map.get(sort_by, PVBViolation.issue_date)
            if sort_order.lower() == "desc":
                query = query.order_by(sort_column.desc())
            else:
                query = query.order_by(sort_column.asc())
        else:
            query = query.order_by(PVBViolation.updated_on.desc() , PVBViolation.created_on.desc())

        # Apply pagination

        query = query.offset((page - 1) * per_page).limit(per_page)

        states = [
            row[0]
            for row in self.db.query(PVBViolation.state)
                .filter(PVBViolation.state.isnot(None))
                .filter(func.length(func.trim(PVBViolation.state)) > 0)
                .distinct()
                .all()
            ]
        
        types = [
            row[0]
            for row in self.db.query(PVBViolation.type)
                .filter(PVBViolation.type.isnot(None))
                .filter(func.length(func.trim(PVBViolation.type)) > 0)
                .distinct()
                .all()
        ]

        return query.all(), total_items , states , types