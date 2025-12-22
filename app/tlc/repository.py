### app/tlc/repository.py

from datetime import date, time, datetime
from typing import List, Optional, Tuple

from sqlalchemy import func, update
from sqlalchemy.orm import Session, joinedload

from app.drivers.models import Driver
from app.medallions.models import Medallion
from app.leases.models import Lease
from app.vehicles.models import Vehicle
from app.tlc.models import TLCViolation, TLCViolationType
from app.utils.logger import get_logger
from app.utils.general import apply_multi_filter

logger = get_logger(__name__)


class TLCRepository:
    """
    Data Access Layer for TLC Violations.
    Handles all database interactions for the TLCViolation model.
    """

    def __init__(self, db: Session):
        self.db = db

    def create_violation(self, violation: TLCViolation) -> TLCViolation:
        """Adds a new TLCViolation record to the session."""
        self.db.add(violation)
        self.db.flush()
        self.db.refresh(violation)
        logger.info("Created new TLCViolation", summons_no=violation.summons_no)
        return violation

    def get_violation_by_id(self, violation_id: int) -> Optional[TLCViolation]:
        """Fetches a single TLC violation by its primary key."""
        return self.db.query(TLCViolation).filter(TLCViolation.id == violation_id).first()

    def get_violation_by_summons(self, summons_no: str) -> Optional[TLCViolation]:
        """Fetches a single TLC violation by its unique summons number."""
        return self.db.query(TLCViolation).filter(TLCViolation.summons_no == summons_no).first()

    def update_violation(self, violation_id: int, updates: dict):
        """Updates specific fields of a single violation record."""
        stmt = (
            update(TLCViolation)
            .where(TLCViolation.id == violation_id)
            .values(**updates)
        )
        self.db.execute(stmt)

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
        from_due_date: Optional[date] = None,
        to_due_date: Optional[date] = None,
        from_penalty_amount: Optional[float] = None,
        to_penalty_amount: Optional[float] = None,
        from_service_fee: Optional[float] = None,
        to_service_fee: Optional[float] = None,
        from_total_payable: Optional[float] = None,
        to_total_payable: Optional[float] = None,
        from_driver_payable: Optional[float] = None,
        to_driver_payable: Optional[float] = None,
        disposition: Optional[str] = None,
        status: Optional[str] = None,
        description: Optional[str] = None,
        driver_name: Optional[str] = None,
        driver_email: Optional[str] = None,
        lease_id: Optional[str] = None,
        lease_type: Optional[str] = None,
        vin: Optional[str] = None,
        note: Optional[str] = None,
        driver_id: Optional[str] = None,
        medallion_no: Optional[str] = None,
    ) -> Tuple[List[TLCViolation], int]:
        """
        Retrieves a paginated, sorted, and filtered list of TLC violations.
        """
        from app.drivers.models import Driver
        
        query = (
            self.db.query(TLCViolation)
            .options(
                joinedload(TLCViolation.driver),
                joinedload(TLCViolation.medallion),
                joinedload(TLCViolation.lease),
            )
            .outerjoin(Driver, TLCViolation.driver_id == Driver.id)
            .outerjoin(Medallion, TLCViolation.medallion_id == Medallion.id)
            .outerjoin(Lease, TLCViolation.lease_id == Lease.id)
            .outerjoin(Vehicle, TLCViolation.vehicle_id == Vehicle.id)
        )

        # Apply filters
        if plate:
            query = apply_multi_filter(query, TLCViolation.plate, plate)
        
        if state:
            query = apply_multi_filter(query, TLCViolation.state, state)
        
        if type:
            try:
                types = [tp.strip().upper() for tp in type.split(",") if tp.strip()]
                query = query.filter(TLCViolation.violation_type.in_(types))
            except KeyError:
                logger.warning(f"Invalid type filter for TLC violations: {type}")
        
        if summons:
            query = apply_multi_filter(query, TLCViolation.summons_no, summons)

        if from_issue_date:
            from_issue_date = datetime.combine(from_issue_date, time.min)
            query = query.filter(TLCViolation.issue_date >= from_issue_date)
        
        if to_issue_date:
            to_issue_date = datetime.combine(to_issue_date, time.max)
            query = query.filter(TLCViolation.issue_date <= to_issue_date)
        
        if from_issue_time:
            query = query.filter(TLCViolation.issue_time >= from_issue_time)
        
        if to_issue_time:
            query = query.filter(TLCViolation.issue_time <= to_issue_time)
        
        if from_due_date:
            from_due_date = datetime.combine(from_due_date, time.min)
            query = query.filter(TLCViolation.due_date >= from_due_date)
        
        if to_due_date:
            to_due_date = datetime.combine(to_due_date, time.max)
            query = query.filter(TLCViolation.due_date <= to_due_date)
        
        if from_penalty_amount:
            query = query.filter(TLCViolation.amount >= from_penalty_amount)

        if to_penalty_amount:
            query = query.filter(TLCViolation.amount <= to_penalty_amount)

        if from_service_fee:
            query = query.filter(TLCViolation.service_fee >= from_service_fee)
        
        if to_service_fee:
            query = query.filter(TLCViolation.service_fee <= to_service_fee)
        
        if from_total_payable:
            query = query.filter(TLCViolation.total_payable >= from_total_payable)
        
        if to_total_payable:
            query = query.filter(TLCViolation.total_payable <= to_total_payable)

        if from_driver_payable:
            query = query.filter(TLCViolation.driver_payable >= from_driver_payable)

        if to_driver_payable:
            query = query.filter(TLCViolation.driver_payable <= to_driver_payable)
        
        if disposition:
            dps = [d.strip() for d in disposition.split(",") if d.strip()]
            query = query.filter(TLCViolation.disposition.in_(dps))

        if status:
            statuses = [s.strip() for s in status.split(",") if s.strip()]
            query = query.filter(TLCViolation.status.in_(statuses))
        
        if description:
            query = apply_multi_filter(query, TLCViolation.description, description)
        
        if driver_name:
            query = apply_multi_filter(query, Driver.full_name, driver_name)

        if driver_email:
            query = apply_multi_filter(query, Driver.email_address, driver_email)
        
        if lease_id:
            query = apply_multi_filter(query, Lease.lease_id, lease_id)
        
        if lease_type:
            query = apply_multi_filter(query, Lease.lease_type, lease_type)

        if vin:
            query = apply_multi_filter(query, Vehicle.vin, vin)
        
        if note:
            query = apply_multi_filter(query, TLCViolation.note, note)
        

        if driver_id:
            query = apply_multi_filter(query, Driver.driver_id, driver_id)
        
        if medallion_no:
            query = apply_multi_filter(query, Medallion.medallion_number, medallion_no)


        total_items = query.with_entities(func.count(TLCViolation.id)).scalar()

        # Apply sorting
        sort_column_map = {
            "summons_no": TLCViolation.summons_no,
            "plate": TLCViolation.plate,
            "state": TLCViolation.state,
            "type": TLCViolation.violation_type,
            "issue_date": TLCViolation.issue_date,
            "issue_time": TLCViolation.issue_time,
            "due_date": TLCViolation.due_date,
            "description": TLCViolation.description,
            "driver_id": Driver.driver_id,
            "medallion_no": Medallion.medallion_number,
            "penalty_amount": TLCViolation.amount,
            "service_fee": TLCViolation.service_fee,
            "total_payable": TLCViolation.total_payable,
            "driver_payable": TLCViolation.driver_payable,
            "disposition": TLCViolation.disposition,
            "status": TLCViolation.status,
            "driver_name": Driver.full_name,
            "lease_id": Lease.lease_id,
            "lease_type": Lease.lease_type,
            "vin": Vehicle.vin,
            "note": TLCViolation.note,
        }
        
        if sort_by and sort_order:
            sort_column = sort_column_map.get(sort_by, TLCViolation.issue_date)
            if sort_order.lower() == "desc":
                query = query.order_by(sort_column.desc())
            else:
                query = query.order_by(sort_column.asc())
        else:
            query = query.order_by(TLCViolation.updated_on.desc() , TLCViolation.created_on.desc())

        # Apply pagination

        query = query.offset((page - 1) * per_page).limit(per_page)

        return query.all(), total_items