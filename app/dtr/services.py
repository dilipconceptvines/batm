# app/dtr/services.py

"""
DTR Service - Business Logic for DTR Generation and Management

CRITICAL BUSINESS RULES:
1. ONE DTR PER LEASE (not per driver)
2. Additional drivers consolidated into primary driver's DTR
3. Lease amounts from lease_schedules (pro-rated if mid-week termination)
4. Payment hierarchy: Taxes → EZPass → Lease → PVB → TLC → Repairs → Loans → Misc
5. DRAFT status if charges still pending, FINALIZED when all confirmed
"""

from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import List, Optional, Dict, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func

from app.dtr.models import DTR, DTRStatus, PaymentMethod
from app.leases.models import Lease, LeaseSchedule
from app.leases.schemas import LeaseStatus
from app.drivers.models import Driver
from app.utils.logger import get_logger

logger = get_logger(__name__)


class DTRService:
    """Service for DTR generation and management"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def generate_dtr_for_lease(
        self,
        lease_id: int,
        week_start: date,
        week_end: date,
        force_final: bool = False
    ) -> DTR:
        """
        Generate DTR for a LEASE (not for individual driver).
        
        Args:
            lease_id: Lease ID
            week_start: Sunday date (start of week)
            week_end: Saturday date (end of week)
            force_final: Force generation as final DTR (for terminations)
            
        Returns:
            Generated DTR
            
        Raises:
            ValueError: If lease not found or already has DTR for period
        """
        logger.info(f"Generating DTR for lease {lease_id}, period {week_start} to {week_end}")
        
        # 1. Validate and get lease
        lease = self._get_and_validate_lease(lease_id)
        
        # 2. Check if DTR already exists
        existing = self.db.query(DTR).filter(
            and_(
                DTR.lease_id == lease_id,
                DTR.week_start_date == week_start
            )
        ).first()
        
        if existing:
            raise ValueError(f"DTR already exists for lease {lease_id} for week {week_start}")
        
        # 3. Get all drivers on this lease
        drivers_info = self._get_lease_drivers(lease)
        primary_driver = drivers_info['primary']
        additional_drivers = drivers_info['additional']
        
        logger.info(
            f"Lease {lease_id}: Primary driver {primary_driver.id}, "
            f"{len(additional_drivers)} additional drivers"
        )
        
        # 4. Check for mid-week termination
        is_terminated, termination_date, active_days = self._check_mid_week_termination(
            lease, week_start, week_end
        )
        
        if is_terminated:
            logger.info(f"Lease {lease_id} terminated on {termination_date}, active days: {active_days}")
            force_final = True
            week_end = termination_date
        
        # 5. Calculate all charges and earnings
        dtr_data = self._calculate_dtr_amounts(
            lease=lease,
            primary_driver=primary_driver,
            additional_drivers=additional_drivers,
            week_start=week_start,
            week_end=week_end,
            active_days=active_days
        )
        
        # 6. Generate DTR and receipt numbers
        dtr_number = self._generate_dtr_number()
        receipt_number = self._generate_receipt_number()
        
        # 7. Create DTR
        dtr = DTR(
            dtr_number=dtr_number,
            receipt_number=receipt_number,
            week_start_date=week_start,
            week_end_date=week_end,
            generation_date=datetime.now(),
            lease_id=lease_id,
            primary_driver_id=primary_driver.id,
            vehicle_id=lease.vehicle_id,
            medallion_id=lease.medallion_id,
            additional_driver_ids=[d.id for d in additional_drivers] if additional_drivers else None,
            
            # Earnings
            credit_card_earnings=dtr_data['earnings'],
            
            # Taxes
            mta_fees_total=dtr_data['taxes']['total'],
            mta_fee_mta=dtr_data['taxes']['mta'],
            mta_fee_tif=dtr_data['taxes']['tif'],
            mta_fee_congestion=dtr_data['taxes']['congestion'],
            mta_fee_cbdt=dtr_data['taxes']['cbdt'],
            mta_fee_airport=dtr_data['taxes']['airport'],
            
            # Charges
            ezpass_tolls=dtr_data['ezpass'],
            lease_amount=dtr_data['lease']['amount'],
            is_lease_prorated=dtr_data['lease']['is_prorated'],
            active_days=active_days,
            pvb_violations=dtr_data['pvb'],
            tlc_tickets=dtr_data['tlc'],
            repairs=dtr_data['repairs'],
            driver_loans=dtr_data['loans'],
            misc_charges=dtr_data['misc'],
            
            # Prior balance
            prior_balance=dtr_data['prior_balance'],
            
            # Calculations
            subtotal_deductions=dtr_data['subtotal_deductions'],
            net_earnings=dtr_data['net_earnings'],
            total_due_to_driver=dtr_data['total_due_to_driver'],
            
            # Status
            status=DTRStatus.FINALIZED if (force_final and not dtr_data['has_pending']) else DTRStatus.DRAFT,
            has_pending_charges=dtr_data['has_pending'],
            pending_charge_categories=dtr_data['pending_categories'],
            
            # Termination
            is_final_dtr=is_terminated,
            termination_date=termination_date if is_terminated else None,
            cancellation_fee=dtr_data['cancellation_fee'] if is_terminated else None,
            
            # Payment method (from lease configuration)
            payment_method=PaymentMethod.ACH if primary_driver.pay_to_mode == 'ACH' else PaymentMethod.CHECK
        )
        
        self.db.add(dtr)
        self.db.commit()
        self.db.refresh(dtr)
        
        logger.info(f"Created DTR {dtr_number} for lease {lease_id}, status: {dtr.status}")
        
        return dtr
    
    def _get_and_validate_lease(self, lease_id: int) -> Lease:
        """Get and validate lease"""
        lease = self.db.query(Lease).filter(Lease.id == lease_id).first()
        
        if not lease:
            raise ValueError(f"Lease {lease_id} not found")
        
        return lease
    
    def _get_lease_drivers(self, lease: Lease) -> Dict:
        """
        Get primary and additional drivers for a lease.
        
        Returns:
            {
                'primary': Driver (leaseholder),
                'additional': List[Driver] (additional drivers on lease)
            }
        """
        primary_driver = None
        additional_drivers = []
        lease_drivers = lease.lease_driver
        for ld in lease_drivers:
            if not ld.is_additional_driver:
                primary_driver = ld.driver
            else:
                additional_drivers.append(ld.driver)
        
        return {
            'primary': primary_driver,
            'additional': additional_drivers
        }
    
    def _check_mid_week_termination(
        self,
        lease: Lease,
        week_start: date,
        week_end: date
    ) -> Tuple[bool, Optional[date], Optional[int]]:
        """
        Check if lease was terminated mid-week.
        
        Returns:
            (is_terminated, termination_date, active_days)
        """
        if lease.lease_status not in [LeaseStatus.TERMINATED, LeaseStatus.EXPIRED]:
            return False, None, None
        
        if not lease.lease_end_date:
            return False, None, None
        
        # Check if termination date is within the week
        if week_start <= lease.lease_end_date <= week_end:
            active_days = (lease.lease_end_date - week_start).days + 1
            return True, lease.lease_end_date, active_days
        
        return False, None, None
    
    def _generate_dtr_number(self) -> str:
        """Generate unique DTR number: DTR-YYYY-XXXX"""
        year = datetime.now().year
        
        # Get last sequence for this year
        last_dtr = (
            self.db.query(DTR)
            .filter(DTR.dtr_number.like(f'DTR-{year}-%'))
            .order_by(DTR.id.desc())
            .first()
        )
        
        if last_dtr:
            last_seq = int(last_dtr.dtr_number.split('-')[-1])
            new_seq = last_seq + 1
        else:
            new_seq = 1
        
        return f"DTR-{year}-{new_seq:04d}"
    
    def _generate_receipt_number(self) -> str:
        """Generate unique receipt number: RCP-YYYYMM-XXXX"""
        now = datetime.now()
        year_month = now.strftime('%Y%m')
        
        # Get last sequence for this month
        last_receipt = (
            self.db.query(DTR)
            .filter(DTR.receipt_number.like(f'RCP-{year_month}-%'))
            .order_by(DTR.id.desc())
            .first()
        )
        
        if last_receipt:
            last_seq = int(last_receipt.receipt_number.split('-')[-1])
            new_seq = last_seq + 1
        else:
            new_seq = 1
        
        return f"RCP-{year_month}-{new_seq:04d}"
    
    def _calculate_dtr_amounts(
        self,
        lease: Lease,
        primary_driver: Driver,
        additional_drivers: List[Driver],
        week_start: date,
        week_end: date,
        active_days: Optional[int] = None
    ) -> Dict:
        """
        Calculate all DTR amounts (earnings, charges, deductions).
        
        CRITICAL: Consolidates earnings and charges from ALL drivers on the lease.
        
        Returns:
            Dictionary with all calculated amounts
        """
        all_drivers = [primary_driver] + additional_drivers
        driver_ids = [d.id for d in all_drivers]
        
        # 1. Calculate consolidated earnings (from CURB for all drivers)
        earnings = self._calculate_consolidated_earnings(driver_ids, week_start, week_end)
        
        # 2. Calculate consolidated taxes (from CURB for all drivers)
        taxes = self._calculate_consolidated_taxes(driver_ids, week_start, week_end)
        
        # 3. Calculate EZPass (all outstanding as of week_end)
        ezpass = self._calculate_ezpass_charges(lease.medallion_id, driver_ids, week_end)
        
        # 4. Calculate lease amount (from lease_schedules, pro-rated if needed)
        lease_charge = self._calculate_lease_charge(lease, week_start, week_end, active_days)
        
        # 5. Calculate PVB violations (all outstanding)
        pvb = self._calculate_pvb_violations(lease.vehicle_id, driver_ids, week_end)
        
        # 6. Calculate TLC tickets (all outstanding)
        tlc = self._calculate_tlc_tickets(driver_ids, week_end)
        
        # 7. Calculate repairs (WTD due only)
        repairs = self._calculate_repairs_wtd(lease.vehicle_id, week_start, week_end)
        
        # 8. Calculate driver loans (WTD due only)
        loans = self._calculate_loans_wtd(primary_driver.id, week_start, week_end)
        
        # 9. Calculate miscellaneous charges
        misc = self._calculate_misc_charges(lease.id, driver_ids, week_start, week_end)
        
        # 10. Get prior balance (from previous DTR)
        prior_balance = self._get_prior_balance(lease.id, week_start)
        
        # 11. Check for pending charges
        has_pending, pending_categories = self._check_pending_charges(
            lease.id, driver_ids, week_end
        )
        
        # 12. Calculate cancellation fee if terminated
        cancellation_fee = Decimal('0.00')
        if active_days is not None and active_days < 7:
            cancellation_fee = self._calculate_cancellation_fee(lease, active_days)
        
        # 13. Calculate totals
        subtotal_deductions = (
            taxes['total'] + ezpass + lease_charge['amount'] +
            pvb + tlc + repairs + loans + misc + cancellation_fee
        )
        
        net_earnings = earnings - subtotal_deductions - prior_balance
        total_due_to_driver = max(net_earnings, Decimal('0.00'))
        
        return {
            'earnings': earnings,
            'taxes': taxes,
            'ezpass': ezpass,
            'lease': lease_charge,
            'pvb': pvb,
            'tlc': tlc,
            'repairs': repairs,
            'loans': loans,
            'misc': misc,
            'prior_balance': prior_balance,
            'cancellation_fee': cancellation_fee,
            'subtotal_deductions': subtotal_deductions,
            'net_earnings': net_earnings,
            'total_due_to_driver': total_due_to_driver,
            'has_pending': has_pending,
            'pending_categories': pending_categories if has_pending else None
        }
    
    def _calculate_consolidated_earnings(
        self,
        driver_ids: List[int],
        week_start: date,
        week_end: date
    ) -> Decimal:
        """
        Calculate total CC earnings from CURB for all drivers on lease.
        
        Consolidates trips from primary driver AND additional drivers.
        """
        from app.curb.models import CurbTrip as Trip, PaymentType

        # Use `total_amount` on CurbTrip and PaymentType.CREDIT_CARD to compute
        # credit-card earnings for the week. Use start_time bounds to compare datetimes.
        start_dt = datetime.combine(week_start, datetime.min.time())
        end_dt = datetime.combine(week_end, datetime.max.time())

        total = (
            self.db.query(func.coalesce(func.sum(Trip.total_amount), 0))
            .filter(
                and_(
                    Trip.driver_id.in_(driver_ids),
                    Trip.transaction_date >= start_dt,
                    Trip.transaction_date <= end_dt,
                    Trip.payment_type == PaymentType.CREDIT_CARD,
                )
            )
            .scalar()
        ) or Decimal('0.00')

        return Decimal(total)
    
    def _calculate_consolidated_taxes(
        self,
        driver_ids: List[int],
        week_start: date,
        week_end: date
    ) -> Dict[str, Decimal]:
        """
        Calculate all tax components from CURB trips for all drivers.
        
        Returns breakdown by tax type.
        """
        from app.curb.models import CurbTrip as Trip

        # Use DB aggregates for each tax component. Field names on CurbTrip:
        # surcharge, improvement_surcharge, congestion_fee, cbdt_fee, airport_fee
        start_dt = datetime.combine(week_start, datetime.min.time())
        end_dt = datetime.combine(week_end, datetime.max.time())

        mta = self.db.query(func.coalesce(func.sum(Trip.surcharge), 0)).filter(
            and_(
                Trip.driver_id.in_(driver_ids),
                Trip.transaction_date >= start_dt,
                Trip.transaction_date <= end_dt,
            )
        ).scalar() or Decimal('0.00')

        tif = self.db.query(func.coalesce(func.sum(Trip.improvement_surcharge), 0)).filter(
            and_(
                Trip.driver_id.in_(driver_ids),
                Trip.transaction_date >= start_dt,
                Trip.transaction_date <= end_dt,
            )
        ).scalar() or Decimal('0.00')

        congestion = self.db.query(func.coalesce(func.sum(Trip.congestion_fee), 0)).filter(
            and_(
                Trip.driver_id.in_(driver_ids),
                Trip.transaction_date >= start_dt,
                Trip.transaction_date <= end_dt,
            )
        ).scalar() or Decimal('0.00')

        cbdt = self.db.query(func.coalesce(func.sum(Trip.cbdt_fee), 0)).filter(
            and_(
                Trip.driver_id.in_(driver_ids),
                Trip.transaction_date >= start_dt,
                Trip.transaction_date <= end_dt,
            )
        ).scalar() or Decimal('0.00')

        airport = self.db.query(func.coalesce(func.sum(Trip.airport_fee), 0)).filter(
            and_(
                Trip.driver_id.in_(driver_ids),
                Trip.transaction_date >= start_dt,
                Trip.transaction_date <= end_dt,
            )
        ).scalar() or Decimal('0.00')

        total = (mta or Decimal('0.00')) + (tif or Decimal('0.00')) + (congestion or Decimal('0.00')) + (cbdt or Decimal('0.00')) + (airport or Decimal('0.00'))

        return {
            'mta': Decimal(mta),
            'tif': Decimal(tif),
            'congestion': Decimal(congestion),
            'cbdt': Decimal(cbdt),
            'airport': Decimal(airport),
            'total': Decimal(total)
        }
    
    def _calculate_ezpass_charges(
        self,
        medallion_id: Optional[int],
        driver_ids: List[int],
        as_of_date: date
    ) -> Decimal:
        """
        Calculate ALL outstanding EZPass charges as of the given date.
        
        IMPORTANT: Includes all unpaid tolls, not just current week.
        """
        from app.ezpass.models import EZPassTransaction, EZPassTransactionStatus

        # Use `amount` and `transaction_datetime` fields. Consider transactions
        # outstanding if they have not been posted to the ledger.
        cutoff = datetime.combine(as_of_date, datetime.max.time())

        total = (
            self.db.query(func.coalesce(func.sum(EZPassTransaction.amount), 0))
            .filter(
                and_(
                    or_(
                        EZPassTransaction.medallion_id == medallion_id,
                        EZPassTransaction.driver_id.in_(driver_ids),
                    ),
                    EZPassTransaction.transaction_datetime <= cutoff,
                    EZPassTransaction.status != EZPassTransactionStatus.POSTED_TO_LEDGER,
                )
            )
            .scalar()
        ) or Decimal('0.00')

        return total
    
    def _calculate_lease_charge(
        self,
        lease: Lease,
        week_start: date,
        week_end: date,
        active_days: Optional[int] = None
    ) -> Dict:
        """
        Calculate lease charge from lease_schedules (NOT from ledger).
        
        Pro-rates if mid-week termination (active_days < 7).
        
        CRITICAL: Lease amounts MUST come from lease schedules, not ledger postings.
        """
        # Get lease schedule for this period. The LeaseSchedule model
        # uses `period_start_date` / `period_end_date` and `installment_amount`.
        schedule = self.db.query(LeaseSchedule).filter(
            and_(
                LeaseSchedule.lease_id == lease.id,
                LeaseSchedule.period_start_date <= week_start,
                LeaseSchedule.period_end_date >= week_end,
            )
        ).first()

        if schedule and schedule.installment_amount is not None:
            weekly_amount = Decimal(str(schedule.installment_amount))
        else:
            # Fallback order: overridden_weekly_rate -> preset_weekly_rate -> 0
            if getattr(lease, 'overridden_weekly_rate', None):
                weekly_amount = Decimal(str(lease.overridden_weekly_rate))
            elif getattr(lease, 'preset_weekly_rate', None):
                weekly_amount = Decimal(str(lease.preset_weekly_rate))
            else:
                logger.warning(f"No lease schedule or preset rate for lease {lease.id}; defaulting to 0")
                weekly_amount = Decimal('0.00')
        
        # Pro-rate if terminated mid-week
        if active_days is not None and active_days < 7:
            daily_rate = (weekly_amount / Decimal('7')).quantize(Decimal('0.0001'))
            prorated_amount = (daily_rate * Decimal(str(active_days))).quantize(Decimal('0.01'))

            return {
                'amount': prorated_amount,
                'is_prorated': True,
                'active_days': active_days,
                'weekly_amount': weekly_amount.quantize(Decimal('0.01')),
                'daily_rate': daily_rate
            }
        
        return {
            'amount': weekly_amount.quantize(Decimal('0.01')),
            'is_prorated': False,
            'active_days': 7,
            'weekly_amount': weekly_amount.quantize(Decimal('0.01')),
            'daily_rate': None
        }
    
    def _calculate_pvb_violations(
        self,
        vehicle_id: Optional[int],
        driver_ids: List[int],
        as_of_date: date
    ) -> Decimal:
        """
        Calculate ALL outstanding PVB violations as of date.
        
        Includes all unpaid violations, not just current week.
        """
        from app.pvb.models import PVBViolation, PVBViolationStatus

        # PVBViolation uses `amount_due` and `issue_date`.
        # Consider a violation outstanding if it has not been posted to the ledger
        # (i.e. status != POSTED_TO_LEDGER). This mirrors the async posting flow
        # in the PVB service which posts ASSOCIATED violations to ledger.
        total = (
            self.db.query(func.coalesce(func.sum(PVBViolation.amount_due), 0))
            .filter(
                and_(
                    or_(
                        PVBViolation.vehicle_id == vehicle_id,
                        PVBViolation.driver_id.in_(driver_ids)
                    ),
                    PVBViolation.issue_date <= as_of_date,
                    PVBViolation.status != PVBViolationStatus.POSTED_TO_LEDGER,
                )
            )
            .scalar()
        ) or Decimal('0.00')

        return total
    
    def _calculate_tlc_tickets(
        self,
        driver_ids: List[int],
        as_of_date: date
    ) -> Decimal:
        """Calculate ALL outstanding TLC tickets as of date"""
        from app.tlc.models import TLCViolation, TLCViolationStatus

        # TLC violations use `total_payable` and `issue_date`.
        # Treat any violation that is not REVERSED as outstanding (PENDING or POSTED).
        total = (
            self.db.query(func.coalesce(func.sum(TLCViolation.total_payable), 0))
            .filter(
                and_(
                    TLCViolation.driver_id.in_(driver_ids),
                    TLCViolation.issue_date <= as_of_date,
                    TLCViolation.status != TLCViolationStatus.REVERSED,
                )
            )
            .scalar()
        ) or Decimal('0.00')

        return total
    
    def _calculate_repairs_wtd(
        self,
        vehicle_id: Optional[int],
        week_start: date,
        week_end: date
    ) -> Decimal:
        """
        Calculate repair charges DUE this week only (WTD).
        
        NOT total outstanding - only installments due in this period.
        """
        # Repairs are modelled as RepairInvoice and RepairInstallment. Each
        # installment has `week_start_date` and `principal_amount`.
        # Join the installment to its invoice to filter by vehicle_id.
        from app.repairs.models import (
            RepairInstallment, RepairInvoice, RepairInstallmentStatus
        )

        # If no vehicle specified, nothing to charge
        if vehicle_id is None:
            return Decimal('0.00')

        total = (
            self.db.query(func.sum(RepairInstallment.principal_amount))
            .join(RepairInvoice, RepairInstallment.invoice_id == RepairInvoice.id)
            .filter(
                and_(
                    RepairInvoice.vehicle_id == vehicle_id,
                    RepairInstallment.week_start_date >= week_start,
                    RepairInstallment.week_start_date <= week_end,
                    RepairInstallment.status != RepairInstallmentStatus.PAID,
                )
            )
            .scalar()
        ) or Decimal('0.00')

        return total
    
    def _calculate_loans_wtd(
        self,
        driver_id: int,
        week_start: date,
        week_end: date
    ) -> Decimal:
        """
        Calculate loan installments DUE this week only (WTD).
        
        NOT total outstanding - only installments due in this period.
        """
        # Use the loan models' actual fields: LoanInstallment.total_due,
        # LoanInstallment.week_start_date and LoanInstallment.status.
        from app.loans.models import (
            LoanInstallment, DriverLoan, LoanInstallmentStatus
        )

        # Sum installments whose week_start_date falls within the given
        # week and which are not already paid. Join to DriverLoan to
        # filter by the driver_id on the master loan record.
        total = (
            self.db.query(func.sum(LoanInstallment.total_due))
            .join(DriverLoan, LoanInstallment.loan_id == DriverLoan.id)
            .filter(
                and_(
                    DriverLoan.driver_id == driver_id,
                    LoanInstallment.week_start_date >= week_start,
                    LoanInstallment.week_start_date <= week_end,
                    LoanInstallment.status != LoanInstallmentStatus.PAID,
                )
            )
            .scalar()
        ) or Decimal('0.00')

        return total
    
    def _calculate_misc_charges(
        self,
        lease_id: int,
        driver_ids: List[int],
        week_start: date,
        week_end: date
    ) -> Decimal:
        """Calculate miscellaneous charges for the week"""
        from app.misc_expenses.models import (
            MiscellaneousExpense as MiscExpense, MiscellaneousExpenseStatus
        )
        
        total = self.db.query(func.sum(MiscExpense.amount)).filter(
            and_(
                or_(
                    MiscExpense.lease_id == lease_id,
                    MiscExpense.driver_id.in_(driver_ids)
                ),
                MiscExpense.expense_date >= week_start,
                MiscExpense.expense_date <= week_end,
                MiscExpense.status == MiscellaneousExpenseStatus.OPEN
            )
        ).scalar() or Decimal('0.00')
        
        return total
    
    def _get_prior_balance(self, lease_id: int, current_week_start: date) -> Decimal:
        """Get unpaid balance from previous DTR"""
        prev_dtr = (
            self.db.query(DTR)
            .filter(
                and_(
                    DTR.lease_id == lease_id,
                    DTR.week_start_date < current_week_start
                )
            )
            .order_by(DTR.week_start_date.desc())
            .first()
        )
        
        if not prev_dtr:
            return Decimal('0.00')
        
        # If previous DTR was not fully paid, carry forward the balance
        if prev_dtr.status != DTRStatus.PAID:
            return prev_dtr.total_due_to_driver
        
        return Decimal('0.00')
    
    def _check_pending_charges(
        self,
        lease_id: int,
        driver_ids: List[int],
        week_end: date
    ) -> Tuple[bool, Optional[List[str]]]:
        """
        Check if there are charges that might still arrive for this period.
        
        Returns: (has_pending, list_of_categories)
        """
        pending_categories = []
        
        # Check if EZPass charges are still being posted (grace period: 3 days)
        grace_period = week_end + timedelta(days=3)
        if datetime.now().date() < grace_period:
            pending_categories.append('EZPASS')
        
        # Check if PVB violations are still being posted (grace period: 5 days)
        pvb_grace = week_end + timedelta(days=5)
        if datetime.now().date() < pvb_grace:
            pending_categories.append('PVB')
        
        return len(pending_categories) > 0, pending_categories if pending_categories else None
    
    def _calculate_cancellation_fee(self, lease: Lease, active_days: int) -> Decimal:
        """
        Calculate cancellation fee for mid-week termination.
        
        Business rule: 
        - Early termination within first 4 weeks: $500 fee
        - Mid-week termination after 4 weeks: No fee
        - Emergency termination: No fee
        """
        if not hasattr(lease, 'termination_reason'):
            return Decimal('0.00')
        
        # Emergency terminations have no fee
        if lease.termination_reason == 'EMERGENCY':
            return Decimal('0.00')
        
        # Check if within first 4 weeks
        lease_duration = (lease.lease_end_date - lease.lease_start_date).days
        if lease_duration <= 28:  # 4 weeks
            return Decimal('500.00')
        
        return Decimal('0.00')