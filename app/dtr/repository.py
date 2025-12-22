# app/dtr/repository.py

from datetime import date
from typing import List, Optional, Tuple, Dict, Any
from decimal import Decimal

from sqlalchemy import and_, or_, desc, asc, func, case
from sqlalchemy.orm import Session, joinedload

from app.dtr.models import DTR, DTRStatus, PaymentMethod
from app.drivers.models import Driver, TLCLicense
from app.vehicles.models import Vehicle, VehicleRegistration
from app.medallions.models import Medallion
from app.utils.logger import get_logger

logger = get_logger(__name__)


class DTRRepository:
    """Repository for DTR data access"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_by_id(self, dtr_id: int) -> Optional[DTR]:
        """Get DTR by ID with all relationships loaded"""
        return (
            self.db.query(DTR)
            .options(
                joinedload(DTR.lease),
                joinedload(DTR.primary_driver),
                joinedload(DTR.vehicle),
                joinedload(DTR.medallion),
                joinedload(DTR.ach_batch)
            )
            .filter(DTR.id == dtr_id)
            .first()
        )
    
    def get_by_receipt_number(self, receipt_number: str) -> Optional[DTR]:
        """Get DTR by receipt number"""
        return self.db.query(DTR).filter(DTR.receipt_number == receipt_number).first()
    
    def get_by_dtr_number(self, dtr_number: str) -> Optional[DTR]:
        """Get DTR by DTR number"""
        return self.db.query(DTR).filter(DTR.dtr_number == dtr_number).first()
    
    def get_by_lease_and_period(
        self,
        lease_id: int,
        week_start: date
    ) -> Optional[DTR]:
        """Get DTR for a specific lease and week"""
        return (
            self.db.query(DTR)
            .filter(
                and_(
                    DTR.lease_id == lease_id,
                    DTR.week_start_date == week_start
                )
            )
            .first()
        )
    
    def list_with_filters(
        self,
        page: int = 1,
        per_page: int = 50,
        receipt_number: Optional[str] = None,
        status: Optional[DTRStatus] = None,
        payment_method: Optional[PaymentMethod] = None,
        week_start_date_from: Optional[date] = None,
        week_start_date_to: Optional[date] = None,
        week_end_date_from: Optional[date] = None,
        week_end_date_to: Optional[date] = None,
        ach_batch_number: Optional[str] = None,
        total_due_min: Optional[float] = None,
        total_due_max: Optional[float] = None,
        receipt_type: Optional[str] = None,
        medallion_number: Optional[str] = None,
        tlc_license: Optional[str] = None,
        driver_name: Optional[str] = None,
        plate_number: Optional[str] = None,
        check_number: Optional[str] = None,
        sort_by: str = 'generation_date',
        sort_order: str = 'desc'
    ) -> Tuple[List[DTR], int]:
        """
        List DTRs with comprehensive filtering and sorting.
        
        Returns: (list_of_dtrs, total_count)
        """
        query = self.db.query(DTR).options(
            joinedload(DTR.lease),
            joinedload(DTR.primary_driver),
            joinedload(DTR.vehicle),
            joinedload(DTR.medallion)
        )
        
        # Apply filters
        if receipt_number:
            query = query.filter(DTR.receipt_number.ilike(f'%{receipt_number}%'))
        
        if status:
            query = query.filter(DTR.status == status)
        
        if payment_method:
            query = query.filter(DTR.payment_method == payment_method)
        
        # Date range filters for week_start_date
        if week_start_date_from:
            query = query.filter(DTR.week_start_date >= week_start_date_from)
        
        if week_start_date_to:
            query = query.filter(DTR.week_start_date <= week_start_date_to)
        
        # Date range filters for week_end_date
        if week_end_date_from:
            query = query.filter(DTR.week_end_date >= week_end_date_from)
        
        if week_end_date_to:
            query = query.filter(DTR.week_end_date <= week_end_date_to)
        
        if ach_batch_number:
            query = query.filter(DTR.ach_batch_number == ach_batch_number)
        
        # Total due to driver range filters
        if total_due_min is not None:
            query = query.filter(DTR.total_due_to_driver >= total_due_min)
        
        if total_due_max is not None:
            query = query.filter(DTR.total_due_to_driver <= total_due_max)
        
        # Receipt type filter (currently only "DTR" is supported)
        if receipt_type:
            if receipt_type != "DTR":
                # Return empty result if invalid receipt type
                return [], 0
        
        if check_number:
            query = query.filter(DTR.check_number.ilike(f'%{check_number}%'))
        
        # Join filters
        if medallion_number:
            med_vals = [m.strip() for m in medallion_number.split(',') if m.strip()]
            if med_vals:
                query = query.join(DTR.medallion).filter(
                    or_(*[Medallion.medallion_number.ilike(f'%{m}%') for m in med_vals])
                )
        
        if tlc_license:
            tlc_vals = [t.strip() for t in tlc_license.split(',') if t.strip()]
            if tlc_vals:
                query = query.join(DTR.primary_driver).join(
                    TLCLicense, Driver.tlc_license_number_id == TLCLicense.id, isouter=True
                ).filter(
                    or_(*[TLCLicense.tlc_license_number.ilike(f'%{t}%') for t in tlc_vals])
                )
        
        if driver_name:
            name_vals = [n.strip() for n in driver_name.split(',') if n.strip()]
            if name_vals:
                exprs = []
                for n in name_vals:
                    like = f'%{n}%'
                    exprs.extend([
                        Driver.first_name.ilike(like),
                        Driver.last_name.ilike(like),
                        func.concat(Driver.first_name, ' ', Driver.last_name).ilike(like),
                        func.concat(Driver.last_name, ' ', Driver.first_name).ilike(like)
                    ])

                query = query.join(DTR.primary_driver).filter(or_(*exprs))
        
        if plate_number:
            plate_vals = [p.strip() for p in plate_number.split(',') if p.strip()]
            if plate_vals:
                query = query.join(DTR.vehicle).join(
                    VehicleRegistration,
                    VehicleRegistration.vehicle_id == Vehicle.id
                ).filter(
                    or_(*[VehicleRegistration.plate_number.ilike(f'%{p}%') for p in plate_vals])
                )
        
        # Get total count before pagination
        total = query.count()
        
        # Apply sorting
        sort_column = getattr(DTR, sort_by, DTR.generation_date)
        if sort_order == 'desc':
            query = query.order_by(desc(sort_column))
        else:
            query = query.order_by(asc(sort_column))
        
        # Apply pagination
        offset = (page - 1) * per_page
        dtrs = query.offset(offset).limit(per_page).all()
        
        return dtrs, total
    
    def get_unpaid_dtrs_for_ach(self) -> List[DTR]:
        """Get all unpaid DTRs eligible for ACH payment"""
        return (
            self.db.query(DTR)
            .filter(
                and_(
                    DTR.status == DTRStatus.FINALIZED,
                    DTR.payment_method == PaymentMethod.ACH,
                    DTR.ach_batch_id.is_(None),
                    DTR.payment_date.is_(None)
                )
            )
            .all()
        )
    
    def get_dtrs_by_ids(self, dtr_ids: List[int]) -> List[DTR]:
        """Get multiple DTRs by IDs"""
        return (
            self.db.query(DTR)
            .options(
                joinedload(DTR.primary_driver),
                joinedload(DTR.lease)
            )
            .filter(DTR.id.in_(dtr_ids))
            .all()
        )
    
    def update_check_number(
        self,
        dtr_id: int,
        check_number: str,
        payment_date: Optional[date] = None
    ) -> DTR:
        """Update check number for a DTR"""
        dtr = self.get_by_id(dtr_id)
        
        if not dtr:
            raise ValueError(f"DTR {dtr_id} not found")
        
        dtr.check_number = check_number
        dtr.status = DTRStatus.PAID
        dtr.payment_date = payment_date or date.today()
        
        self.db.commit()
        self.db.refresh(dtr)
        
        return dtr
    
    def finalize_dtr(self, dtr_id: int, user_id: int) -> DTR:
        """Manually finalize a DRAFT DTR (when all charges confirmed)"""
        dtr = self.get_by_id(dtr_id)
        
        if not dtr:
            raise ValueError(f"DTR {dtr_id} not found")
        
        if dtr.status != DTRStatus.DRAFT:
            raise ValueError(f"DTR {dtr_id} is not in DRAFT status")
        
        dtr.status = DTRStatus.FINALIZED
        dtr.has_pending_charges = False
        dtr.pending_charge_categories = None
        dtr.finalized_at = date.today()
        dtr.finalized_by = user_id
        
        self.db.commit()
        self.db.refresh(dtr)
        
        return dtr
    
    def get_summary_stats(
        self,
        week_start: Optional[date] = None,
        week_end: Optional[date] = None
    ) -> Dict[str, Any]:
        """Get summary statistics for DTRs"""
        query = self.db.query(
            func.count(DTR.id).label('total_count'),
            func.sum(DTR.total_due_to_driver).label('total_amount'),
            func.sum(
                case((DTR.status == DTRStatus.PAID, DTR.total_due_to_driver), else_=Decimal('0.00'))
            ).label('paid_amount'),
            func.sum(
                case((DTR.status != DTRStatus.PAID, DTR.total_due_to_driver), else_=Decimal('0.00'))
            ).label('unpaid_amount')
        )
        
        if week_start:
            query = query.filter(DTR.week_start_date >= week_start)
        
        if week_end:
            query = query.filter(DTR.week_end_date <= week_end)
        
        result = query.first()
        
        return {
            'total_count': result.total_count or 0,
            'total_amount': result.total_amount or Decimal('0.00'),
            'paid_amount': result.paid_amount or Decimal('0.00'),
            'unpaid_amount': result.unpaid_amount or Decimal('0.00')
        }