"""
app/current_balances/services.py

Business logic for Current Balances feature
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import List, Optional, Tuple

from sqlalchemy import func, or_
from sqlalchemy.orm import Session, joinedload

from app.curb.models import CurbTrip
from app.current_balances.schemas import (
    CurrentBalancesFilter,
    DailyBreakdown,
    DelayedCharge,
    DriverStatusEnum,
    DTRStatusEnum,
    LeaseStatusEnum,
    PaymentTypeEnum,
    WeeklyBalanceRow,
    WeekPeriod,
)
from app.drivers.models import Driver, TLCLicense
from app.dtr.models import DTR
from app.dtr.models import DTRStatus as DTRStatusModel
from app.ezpass.models import EZPassTransaction
from app.leases.models import Lease, LeaseDriver
from app.leases.schemas import LeaseStatus
from app.ledger.models import BalanceStatus, LedgerBalance, PostingCategory
from app.medallions.models import Medallion
from app.pvb.models import PVBViolation
from app.tlc.models import TLCViolation
from app.utils.logger import get_logger
from app.vehicles.models import Vehicle, VehicleRegistration

logger = get_logger(__name__)


class CurrentBalancesService:
    """Service for managing current balances view"""

    def __init__(self, db: Session):
        self.db = db

    def get_current_week(self) -> Tuple[date, date]:
        """Get current week's Sunday to Saturday range"""
        today = date.today()
        days_since_sunday = (today.weekday() + 1) % 7
        week_start = today - timedelta(days=days_since_sunday)
        week_end = week_start + timedelta(days=6)
        return week_start, week_end

    def get_week_for_date(self, target_date: date) -> Tuple[date, date]:
        """Get week range for a specific date"""
        days_since_sunday = (target_date.weekday() + 1) % 7
        week_start = target_date - timedelta(days=days_since_sunday)
        week_end = week_start + timedelta(days=6)
        return week_start, week_end

    def create_week_period(self, week_start: date, week_end: date) -> WeekPeriod:
        """Create WeekPeriod object"""
        current_week_start, _ = self.get_current_week()
        is_current = week_start == current_week_start

        week_label = (
            f"{week_start.strftime('%b %d')} - {week_end.strftime('%b %d, %Y')}"
        )

        return WeekPeriod(
            week_start=week_start,
            week_end=week_end,
            week_label=week_label,
            is_current_week=is_current,
        )

    def get_lease_balances(
        self,
        week_start: date,
        week_end: date,
        page: int = 1,
        per_page: int = 25,
        filters: Optional[CurrentBalancesFilter] = None,
    ) -> Tuple[List[WeeklyBalanceRow], int]:
        """
        Get current balances for all active leases for a specific week.

        For current week: Real-time calculated data
        For past weeks: Data from finalized DTR
        """
        current_week_start, _ = self.get_current_week()
        is_current_week = week_start == current_week_start

        if is_current_week:
            return self._get_live_balances(
                week_start, week_end, page, per_page, filters
            )
        else:
            return self._get_historical_balances(
                week_start, week_end, page, per_page, filters
            )

    def _get_live_balances(
        self,
        week_start: date,
        week_end: date,
        page: int,
        per_page: int,
        filters: Optional[CurrentBalancesFilter],
    ) -> Tuple[List[WeeklyBalanceRow], int]:
        """Get live balances for current week"""

        query = (
            self.db.query(Lease)
            .options(
                joinedload(Lease.lease_driver)
                .joinedload(LeaseDriver.driver)
                .joinedload(Driver.tlc_license),
                joinedload(Lease.vehicle),
                joinedload(Lease.medallion),
            )
            .outerjoin(LeaseDriver)
            .outerjoin(Driver)
            .outerjoin(Vehicle)
            .outerjoin(Medallion)
            .outerjoin(TLCLicense)
            .filter(
                Lease.lease_status.in_([LeaseStatus.ACTIVE, LeaseStatus.TERMINATED])
            )
        )

        # Apply filters
        if filters:
            # General search (existing)
            if filters.search:
                search_term = f"%{filters.search}%"
                query = query.filter(
                    or_(
                        Lease.lease_id.ilike(search_term),
                        Driver.full_name.ilike(search_term),
                        Vehicle.registrations.any(
                            VehicleRegistration.plate_number.ilike(search_term)
                        ),
                        Medallion.medallion_number.ilike(search_term),
                    )
                )

            # Individual column searches with comma-separated support
            if filters.lease_id_search:
                lease_ids = [
                    term.strip()
                    for term in filters.lease_id_search.split(",")
                    if term.strip()
                ]
                if lease_ids:
                    lease_id_conditions = [
                        Lease.lease_id.ilike(f"%{term}%") for term in lease_ids
                    ]
                    query = query.filter(or_(*lease_id_conditions))

            if filters.driver_name_search:
                driver_names = [
                    term.strip()
                    for term in filters.driver_name_search.split(",")
                    if term.strip()
                ]
                if driver_names:
                    driver_name_conditions = [
                        Driver.full_name.ilike(f"%{term}%") for term in driver_names
                    ]
                    query = query.filter(or_(*driver_name_conditions))

            if filters.tlc_license_search:
                tlc_licenses = [
                    term.strip()
                    for term in filters.tlc_license_search.split(",")
                    if term.strip()
                ]
                if tlc_licenses:
                    logger.info(f"TLC License Search - Terms: {tlc_licenses}")

                    # Use a fresh subquery to avoid join conflicts
                    tlc_subquery = (
                        self.db.query(Lease.id)
                        .join(LeaseDriver)
                        .join(Driver)
                        .join(TLCLicense)
                        .filter(
                            LeaseDriver.is_additional_driver == False,
                            or_(
                                *[
                                    TLCLicense.tlc_license_number.ilike(f"%{term}%")
                                    for term in tlc_licenses
                                ]
                            ),
                        )
                    )

                    # Execute subquery to get lease IDs
                    matching_lease_ids = [row[0] for row in tlc_subquery.all()]
                    logger.info(
                        f"TLC License Search - Found {len(matching_lease_ids)} matching lease IDs: {matching_lease_ids}"
                    )

                    if matching_lease_ids:
                        query = query.filter(Lease.id.in_(matching_lease_ids))
                    else:
                        # If no matches found, filter to return no results
                        query = query.filter(Lease.id == -1)

            if filters.medallion_search:
                medallions = [
                    term.strip()
                    for term in filters.medallion_search.split(",")
                    if term.strip()
                ]
                if medallions:
                    medallion_conditions = [
                        Medallion.medallion_number.ilike(f"%{term}%")
                        for term in medallions
                    ]
                    query = query.filter(or_(*medallion_conditions))

            if filters.plate_search:
                plates = [
                    term.strip()
                    for term in filters.plate_search.split(",")
                    if term.strip()
                ]
                if plates:
                    plate_conditions = [
                        Vehicle.registrations.any(
                            VehicleRegistration.plate_number.ilike(f"%{term}%")
                        )
                        for term in plates
                    ]
                    query = query.filter(or_(*plate_conditions))

            if filters.vin_search:
                vins = [
                    term.strip()
                    for term in filters.vin_search.split(",")
                    if term.strip()
                ]
                if vins:
                    vin_conditions = [Vehicle.vin.ilike(f"%{term}%") for term in vins]
                    query = query.filter(or_(*vin_conditions))

            # Status filters (existing)
            if filters.lease_status:
                query = query.filter(Lease.lease_status == filters.lease_status.value)

            if filters.driver_status:
                if filters.driver_status == DriverStatusEnum.ACTIVE:
                    # Filter for active drivers (drivers with active TLC licenses)
                    query = query.filter(
                        Driver.driver_status == DriverStatusEnum.ACTIVE.value
                    )
                elif filters.driver_status == DriverStatusEnum.SUSPENDED:
                    # Filter for inactive drivers (drivers with expired or no TLC licenses)
                    query = query.filter(
                        or_(Driver.driver_status == DriverStatusEnum.SUSPENDED.value)
                    )

            if filters.payment_type:
                query = query.filter(Driver.pay_to_mode == filters.payment_type.value)

        # Apply sorting
        if filters and filters.sort_by:
            sort_column = None
            if filters.sort_by == "lease_id":
                sort_column = Lease.lease_id
            elif filters.sort_by == "driver_name":
                sort_column = Driver.full_name
            elif filters.sort_by == "medallion_number":
                sort_column = Medallion.medallion_number
            elif filters.sort_by == "plate_number":
                # Can't sort by plate_number at database level due to relationship
                sort_column = None
            elif filters.sort_by == "vin_number":
                sort_column = Vehicle.vin
            # Note: Financial columns (cc_earnings, net_earnings, etc.) will be sorted post-query
            # as they are calculated fields, not database columns

            if sort_column is not None:
                if filters.sort_order == "desc":
                    query = query.order_by(sort_column.desc())
                else:
                    query = query.order_by(sort_column.asc())

        # Handle pagination differently for financial column sorting
        if (
            filters
            and filters.sort_by
            and filters.sort_by
            in [
                "cc_earnings",
                "weekly_lease_fee",
                "mta_tif",
                "ezpass_tolls",
                "pvb_violations",
                "tlc_tickets",
                "repairs_wtd",
                "loans_wtd",
                "misc_charges",
                "subtotal_deductions",
                "prior_balance",
                "net_earnings",
            ]
        ):
            # For financial columns, get all data first, then sort and paginate
            all_leases = query.all()
            all_balance_rows = []
            for lease in all_leases:
                row = self._calculate_live_balance_for_lease(
                    lease, week_start, week_end
                )
                if filters and filters.dtr_status:
                    if row.dtr_status != filters.dtr_status:
                        continue
                all_balance_rows.append(row)

            # Sort by financial column
            reverse_order = filters.sort_order == "desc"
            all_balance_rows.sort(
                key=lambda x: getattr(x, filters.sort_by, Decimal("0")),
                reverse=reverse_order,
            )

            # Apply pagination after sorting
            start_idx = (page - 1) * per_page
            end_idx = start_idx + per_page
            balance_rows = all_balance_rows[start_idx:end_idx]
            total_items = len(all_balance_rows)
        else:
            # For non-financial columns, need to get all data to apply calculated filters
            all_leases = query.all()
            all_balance_rows = []
            for lease in all_leases:
                row = self._calculate_live_balance_for_lease(
                    lease, week_start, week_end
                )
                if filters and filters.dtr_status:
                    if row.dtr_status != filters.dtr_status:
                        continue
                all_balance_rows.append(row)

            # Apply pagination
            start_idx = (page - 1) * per_page
            end_idx = start_idx + per_page
            balance_rows = all_balance_rows[start_idx:end_idx]
            total_items = len(all_balance_rows)

        return balance_rows, total_items

    def _calculate_live_balance_for_lease(
        self, lease: Lease, week_start: date, week_end: date
    ) -> WeeklyBalanceRow:
        """Calculate real-time balance for a single lease"""

        # Get the primary driver (not additional driver)
        driver = None
        if lease.lease_driver:
            primary_drivers = [
                ld.driver for ld in lease.lease_driver if not ld.is_additional_driver
            ]
            driver = (
                primary_drivers[0]
                if primary_drivers
                else (lease.lease_driver[0].driver if lease.lease_driver else None)
            )

        lease_amount = 0
        lease_confis = lease.lease_configuration

        if lease_confis:
            config = config = next(
                (c for c in lease_confis if c.lease_breakup_type == "lease_amount"),
                None,
            )
            if config and config.lease_limit:
                lease_amount = float(config.lease_limit)

        vehicle = lease.vehicle
        medallion = lease.medallion

        # Get CURB earnings for the week
        cc_earnings = self._get_curb_earnings(lease.id, week_start, week_end)

        # Get weekly lease fee
        weekly_lease_fee = self._get_weekly_lease_fee(lease)

        # Get MTA/TIF charges
        mta_tif = self._get_mta_tif_charges(lease.id, week_start, week_end)

        # Get EZPass tolls (all outstanding as of week end)
        ezpass_tolls = self._get_ezpass_outstanding(
            lease.id, medallion.id if medallion else None, week_end
        )

        # Get PVB violations (all outstanding as of week end)
        pvb_violations = self._get_pvb_outstanding(lease.id, week_end)

        # Get TLC tickets (all outstanding as of week end)
        tlc_tickets = self._get_tlc_outstanding(lease.id, week_end)

        # Get repairs WTD due
        repairs_wtd = self._get_repairs_wtd(lease.id, week_start, week_end)

        # Get loans WTD due
        loans_wtd = self._get_loans_wtd(lease.id, week_start, week_end)

        # Get misc charges
        misc_charges = self._get_misc_charges(lease.id, week_start, week_end)

        # Get prior balance from last DTR
        prior_balance = self._get_prior_balance(lease.id, week_start)

        # Get deposit amount
        deposit_amount = lease_amount or Decimal("0")

        # Calculate subtotal and net
        subtotal = (
            weekly_lease_fee
            + mta_tif
            + ezpass_tolls
            + pvb_violations
            + tlc_tickets
            + repairs_wtd
            + loans_wtd
            + misc_charges
        )

        net_earnings = cc_earnings - subtotal - prior_balance

        # Determine payment type
        payment_type = (
            PaymentTypeEnum.ACH
            if driver and driver.pay_to_mode == "ACH"
            else PaymentTypeEnum.CASH
        )

        # Convert lease status from database enum to current balances enum
        lease_status_mapping = {
            "Active": LeaseStatusEnum.ACTIVE,
            "Terminated": LeaseStatusEnum.TERMINATED,
            "Termination Requested": LeaseStatusEnum.TERMINATION_REQUESTED,
        }
        lease_status = lease_status_mapping.get(
            lease.lease_status, LeaseStatusEnum.ACTIVE
        )

        # Determine driver status based on TLC license expiration
        # driver_status = DriverStatusEnum.SUSPENDED
        # if driver and driver.tlc_license and driver.tlc_license.tlc_license_expiry_date:
        #     current_date = date.today()
        #     if driver.tlc_license.tlc_license_expiry_date >= current_date:
        #         driver_status = DriverStatusEnum.ACTIVE

        return WeeklyBalanceRow(
            lease_id=lease.lease_id,
            driver_name=driver.full_name if driver else "Unknown",
            tlc_license=driver.tlc_license.tlc_license_number
            if driver and driver.tlc_license
            else None,
            medallion_number=medallion.medallion_number if medallion else "N/A",
            plate_number=vehicle.get_active_plate_number() if vehicle else "N/A",
            vin_number=vehicle.vin if vehicle else None,
            lease_status=lease_status,
            driver_status=DriverStatusEnum(driver.driver_status.upper())
            if driver and driver.driver_status
            else DriverStatusEnum.ACTIVE,
            dtr_status=DTRStatusEnum.NOT_GENERATED,
            payment_type=payment_type,
            cc_earnings=cc_earnings,
            weekly_lease_fee=weekly_lease_fee,
            mta_tif=mta_tif,
            ezpass_tolls=ezpass_tolls,
            pvb_violations=pvb_violations,
            tlc_tickets=tlc_tickets,
            repairs_wtd=repairs_wtd,
            loans_wtd=loans_wtd,
            misc_charges=misc_charges,
            subtotal_deductions=subtotal,
            prior_balance=prior_balance,
            deposit_amount=deposit_amount,
            net_earnings=net_earnings,
            last_updated=datetime.now(timezone.utc),
        )

    def _get_historical_balances(
        self,
        week_start: date,
        week_end: date,
        page: int,
        per_page: int,
        filters: Optional[CurrentBalancesFilter],
    ) -> Tuple[List[WeeklyBalanceRow], int]:
        """Get historical balances from finalized DTRs"""

        query = (
            self.db.query(DTR)
            .options(
                joinedload(DTR.driver).joinedload(Driver.tlc_license),
                joinedload(DTR.lease),
                joinedload(DTR.vehicle),
                joinedload(DTR.medallion),
            )
            .filter(
                DTR.week_start_date == week_start,
                DTR.week_end_date == week_end,
                DTR.status == DTRStatusModel.FINALIZED,
            )
        )

        # Apply filters
        if filters:
            # General search
            if filters.search:
                search_term = f"%{filters.search}%"
                query = (
                    query.outerjoin(Driver)
                    .outerjoin(Vehicle)
                    .outerjoin(Medallion)
                    .filter(
                        or_(
                            DTR.lease.has(Lease.lease_id.ilike(search_term)),
                            Driver.full_name.ilike(search_term),
                            # Note: plate_number search removed as it requires VehicleRegistration join
                            Medallion.medallion_number.ilike(search_term),
                            Vehicle.vin.ilike(search_term),  # Added VIN search
                        )
                    )
                )

            # Individual column searches with comma-separated support
            if filters.lease_id_search:
                lease_ids = [
                    term.strip()
                    for term in filters.lease_id_search.split(",")
                    if term.strip()
                ]
                if lease_ids:
                    lease_id_conditions = [
                        DTR.lease.has(Lease.lease_id.ilike(f"%{term}%"))
                        for term in lease_ids
                    ]
                    query = query.filter(or_(*lease_id_conditions))

            if filters.driver_name_search:
                driver_names = [
                    term.strip()
                    for term in filters.driver_name_search.split(",")
                    if term.strip()
                ]
                if driver_names:
                    driver_name_conditions = [
                        Driver.full_name.ilike(f"%{term}%") for term in driver_names
                    ]
                    query = query.filter(or_(*driver_name_conditions))

            if filters.tlc_license_search:
                tlc_licenses = [
                    term.strip()
                    for term in filters.tlc_license_search.split(",")
                    if term.strip()
                ]
                if tlc_licenses:
                    tlc_conditions = [
                        TLCLicense.tlc_license_number.ilike(f"%{term}%")
                        for term in tlc_licenses
                    ]
                    query = (
                        query.join(Driver).join(TLCLicense).filter(or_(*tlc_conditions))
                    )

            if filters.medallion_search:
                medallions = [
                    term.strip()
                    for term in filters.medallion_search.split(",")
                    if term.strip()
                ]
                if medallions:
                    medallion_conditions = [
                        Medallion.medallion_number.ilike(f"%{term}%")
                        for term in medallions
                    ]
                    query = query.filter(or_(*medallion_conditions))

            if filters.plate_search:
                plates = [
                    term.strip()
                    for term in filters.plate_search.split(",")
                    if term.strip()
                ]
                if plates:
                    plate_conditions = [
                        Vehicle.registrations.any(
                            VehicleRegistration.plate_number.ilike(f"%{term}%")
                        )
                        for term in plates
                    ]
                    query = query.filter(or_(*plate_conditions))

            if filters.vin_search:
                vins = [
                    term.strip()
                    for term in filters.vin_search.split(",")
                    if term.strip()
                ]
                if vins:
                    vin_conditions = [Vehicle.vin.ilike(f"%{term}%") for term in vins]
                    query = query.filter(or_(*vin_conditions))

            # Driver status filter
            if filters.driver_status:
                if filters.driver_status == DriverStatusEnum.ACTIVE:
                    # Filter for active drivers (drivers with active TLC licenses)
                    query = query.filter(
                        Driver.tlc_license.has(
                            TLCLicense.tlc_license_expiration_date
                            >= func.current_date()
                        )
                    )
                elif filters.driver_status == DriverStatusEnum.INACTIVE:
                    # Filter for inactive drivers (drivers with expired or no TLC licenses)
                    query = query.filter(
                        or_(
                            Driver.tlc_license_id.is_(None),
                            Driver.tlc_license.has(
                                TLCLicense.tlc_license_expiration_date
                                < func.current_date()
                            ),
                        )
                    )

        # Apply sorting for database columns
        if filters and filters.sort_by:
            sort_column = None
            if filters.sort_by == "lease_id":
                sort_column = Lease.lease_id
            elif filters.sort_by == "driver_name":
                sort_column = Driver.full_name
            elif filters.sort_by == "medallion_number":
                sort_column = Medallion.medallion_number
            elif filters.sort_by == "plate_number":
                # Can't sort by plate_number at database level due to relationship
                sort_column = None
            elif filters.sort_by == "vin_number":
                sort_column = Vehicle.vin
            elif filters.sort_by == "cc_earnings":
                sort_column = DTR.credit_card_earnings
            elif filters.sort_by == "net_earnings":
                sort_column = DTR.net_earnings
            # Add other DTR financial columns as needed

            if sort_column is not None:
                if filters.sort_order == "desc":
                    query = query.order_by(sort_column.desc())
                else:
                    query = query.order_by(sort_column.asc())

        total_items = query.count()
        dtrs = query.offset((page - 1) * per_page).limit(per_page).all()

        balance_rows = []
        for dtr in dtrs:
            row = self._convert_dtr_to_balance_row(dtr)
            balance_rows.append(row)

        return balance_rows, total_items

    def _convert_dtr_to_balance_row(self, dtr: DTR) -> WeeklyBalanceRow:
        """Convert a finalized DTR to a balance row"""

        driver = dtr.driver
        lease = dtr.lease
        vehicle = dtr.vehicle
        medallion = dtr.medallion

        payment_type = (
            PaymentTypeEnum.ACH
            if driver and driver.pay_to_mode == "ACH"
            else PaymentTypeEnum.CASH
        )

        # Convert lease status from database enum to current balances enum
        lease_status_mapping = {
            "Active": LeaseStatusEnum.ACTIVE,
            "Terminated": LeaseStatusEnum.TERMINATED,
            "Termination Requested": LeaseStatusEnum.TERMINATION_REQUESTED,
        }
        lease_status = (
            lease_status_mapping.get(lease.lease_status, LeaseStatusEnum.ACTIVE)
            if lease
            else LeaseStatusEnum.TERMINATED
        )

        # Determine driver status based on TLC license expiration
        driver_status = DriverStatusEnum.INACTIVE
        if (
            driver
            and driver.tlc_license
            and driver.tlc_license.tlc_license_expiration_date
        ):
            # Use the DTR date or current date for historical comparison
            comparison_date = (
                dtr.generation_date.date() if dtr.generation_date else date.today()
            )
            if driver.tlc_license.tlc_license_expiration_date >= comparison_date:
                driver_status = DriverStatusEnum.ACTIVE

        return WeeklyBalanceRow(
            lease_id=lease.lease_id if lease else "N/A",
            driver_name=driver.full_name if driver else "Unknown",
            tlc_license=driver.tlc_license.tlc_license_number
            if driver and driver.tlc_license
            else None,
            medallion_number=medallion.medallion_number if medallion else "N/A",
            plate_number=vehicle.get_active_plate_number() if vehicle else "N/A",
            vin_number=vehicle.vin if vehicle else None,
            lease_status=lease_status,
            driver_status=driver_status,
            dtr_status=DTRStatusEnum.GENERATED,
            payment_type=payment_type,
            cc_earnings=dtr.credit_card_earnings or Decimal("0"),
            weekly_lease_fee=dtr.lease_amount or Decimal("0"),
            mta_tif=dtr.mta_fees_total or Decimal("0"),
            ezpass_tolls=dtr.ezpass_tolls or Decimal("0"),
            pvb_violations=dtr.pvb_violations or Decimal("0"),
            tlc_tickets=dtr.tlc_tickets or Decimal("0"),
            repairs_wtd=dtr.repairs or Decimal("0"),
            loans_wtd=dtr.driver_loans or Decimal("0"),
            misc_charges=dtr.misc_charges or Decimal("0"),
            subtotal_deductions=dtr.subtotal_deductions or Decimal("0"),
            prior_balance=dtr.prior_balance or Decimal("0"),
            deposit_amount=lease.deposit_amount_paid if lease else Decimal("0"),
            net_earnings=dtr.net_earnings or Decimal("0"),
            last_updated=dtr.generation_date or datetime.now(timezone.utc),
        )

    # Helper methods for calculating individual components

    def _get_curb_earnings(
        self, lease_id: int, week_start: date, week_end: date
    ) -> Decimal:
        """Get credit card earnings from CURB for the week"""
        from app.curb.models import PaymentType

        result = (
            self.db.query(func.coalesce(func.sum(CurbTrip.total_amount), 0))
            .filter(
                CurbTrip.lease_id == lease_id,
                CurbTrip.start_time >= week_start,
                CurbTrip.start_time <= week_end + timedelta(days=1),
                CurbTrip.payment_type == PaymentType.CREDIT_CARD,
            )
            .scalar()
        )
        return Decimal(str(result))

    def _get_weekly_lease_fee(self, lease: Lease) -> Decimal:
        """Get weekly lease fee"""
        # Use overridden rate if available, otherwise use preset rate
        weekly_rate = lease.overridden_weekly_rate or lease.preset_weekly_rate
        return Decimal(str(weekly_rate)) if weekly_rate else Decimal("0")

    def _get_mta_tif_charges(
        self, lease_id: int, week_start: date, week_end: date
    ) -> Decimal:
        """Get MTA/TIF charges for the week"""
        result = (
            self.db.query(
                func.coalesce(
                    func.sum(
                        func.coalesce(CurbTrip.surcharge, 0)
                        + func.coalesce(CurbTrip.improvement_surcharge, 0)
                        + func.coalesce(CurbTrip.congestion_fee, 0)
                        + func.coalesce(CurbTrip.cbdt_fee, 0)
                        + func.coalesce(CurbTrip.airport_fee, 0)
                    ),
                    0,
                )
            )
            .filter(
                CurbTrip.lease_id == lease_id,
                CurbTrip.start_time >= week_start,
                CurbTrip.start_time <= week_end + timedelta(days=1),
            )
            .scalar()
        )
        return Decimal(str(result))

    def _get_ezpass_outstanding(
        self, lease_id: int, _medallion_id: Optional[int], as_of_date: date
    ) -> Decimal:
        """Get all outstanding EZPass tolls as of the given date"""
        result = (
            self.db.query(func.coalesce(func.sum(LedgerBalance.balance), 0))
            .filter(
                LedgerBalance.lease_id == lease_id,
                LedgerBalance.category == PostingCategory.EZPASS,
                LedgerBalance.status == BalanceStatus.OPEN,
                LedgerBalance.created_on <= as_of_date,
            )
            .scalar()
        )
        return Decimal(str(result))

    def _get_pvb_outstanding(self, lease_id: int, as_of_date: date) -> Decimal:
        """Get all outstanding PVB violations as of the given date"""
        result = (
            self.db.query(func.coalesce(func.sum(LedgerBalance.balance), 0))
            .filter(
                LedgerBalance.lease_id == lease_id,
                LedgerBalance.category == PostingCategory.PVB,
                LedgerBalance.status == BalanceStatus.OPEN,
                LedgerBalance.created_on <= as_of_date,
            )
            .scalar()
        )
        return Decimal(str(result))

    def _get_tlc_outstanding(self, lease_id: int, as_of_date: date) -> Decimal:
        """Get all outstanding TLC tickets as of the given date"""
        result = (
            self.db.query(func.coalesce(func.sum(LedgerBalance.balance), 0))
            .filter(
                LedgerBalance.lease_id == lease_id,
                LedgerBalance.category == PostingCategory.TLC,
                LedgerBalance.status == BalanceStatus.OPEN,
                LedgerBalance.created_on <= as_of_date,
            )
            .scalar()
        )
        return Decimal(str(result))

    def _get_repairs_wtd(
        self, lease_id: int, week_start: date, week_end: date
    ) -> Decimal:
        """Get repairs due this week only"""
        # Note: LedgerBalance doesn't have due_date, so getting all open repair balances
        # TODO: Join with repair tables to get actual due dates if needed
        result = (
            self.db.query(func.coalesce(func.sum(LedgerBalance.balance), 0))
            .filter(
                LedgerBalance.lease_id == lease_id,
                LedgerBalance.category == PostingCategory.REPAIR,
                LedgerBalance.status == BalanceStatus.OPEN,
            )
            .scalar()
        )
        return Decimal(str(result))

    def _get_loans_wtd(
        self, lease_id: int, week_start: date, week_end: date
    ) -> Decimal:
        """Get loan installments due this week only"""
        # Note: LedgerBalance doesn't have due_date, so getting all open loan balances
        # TODO: Join with loan tables to get actual due dates if needed
        result = (
            self.db.query(func.coalesce(func.sum(LedgerBalance.balance), 0))
            .filter(
                LedgerBalance.lease_id == lease_id,
                LedgerBalance.category == PostingCategory.LOAN,
                LedgerBalance.status == BalanceStatus.OPEN,
            )
            .scalar()
        )
        return Decimal(str(result))

    def _get_misc_charges(
        self, lease_id: int, week_start: date, week_end: date
    ) -> Decimal:
        """Get miscellaneous charges for the week"""
        result = (
            self.db.query(func.coalesce(func.sum(LedgerBalance.balance), 0))
            .filter(
                LedgerBalance.lease_id == lease_id,
                LedgerBalance.category.in_([PostingCategory.MISCELLANEOUS_EXPENSE, PostingCategory.MISCELLANEOUS_CREDIT]),
                LedgerBalance.status == BalanceStatus.OPEN,
                LedgerBalance.created_on >= week_start,
                LedgerBalance.created_on <= week_end,
            )
            .scalar()
        )
        return Decimal(str(result))

    def _get_prior_balance(self, lease_id: int, current_week_start: date) -> Decimal:
        """Get prior balance from last DTR"""
        last_week_start = current_week_start - timedelta(days=7)
        last_week_end = current_week_start - timedelta(days=1)

        last_dtr = (
            self.db.query(DTR)
            .filter(
                DTR.lease_id == lease_id,
                DTR.week_start_date == last_week_start,
                DTR.week_end_date == last_week_end,
                DTR.status == DTRStatusModel.FINALIZED,
            )
            .first()
        )

        if last_dtr:
            # Prior balance is negative of total_due_to_driver (if driver owes, it's positive here)
            return (
                -last_dtr.total_due_to_driver
                if last_dtr.total_due_to_driver < 0
                else Decimal("0")
            )

        return Decimal("0")

    def get_lease_detail_with_daily_breakdown(
        self, lease_id: str, week_start: date, week_end: date
    ) -> Optional[dict]:
        """Get detailed balance for a lease including daily breakdown"""

        lease = self.db.query(Lease).filter(Lease.lease_id == lease_id).first()
        if not lease:
            return None

        # Get base balance row
        balance_row = self._calculate_live_balance_for_lease(
            lease, week_start, week_end
        )

        # Calculate daily breakdown
        daily_breakdown = self._get_daily_breakdown(lease.id, week_start, week_end)

        # Get delayed charges
        delayed_charges = self._get_delayed_charges(lease.id, week_start, week_end)

        return {
            **balance_row.model_dump(),
            "daily_breakdown": daily_breakdown,
            "delayed_charges": delayed_charges,
        }

    def _get_daily_breakdown(
        self, lease_id: int, week_start: date, week_end: date
    ) -> List[DailyBreakdown]:
        """Get daily breakdown of earnings and charges"""

        days = []
        current_date = week_start
        day_names = [
            "Sunday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
        ]

        while current_date <= week_end:
            day_name = day_names[
                current_date.weekday() if current_date.weekday() != 6 else 0
            ]

            # Get daily CURB earnings
            cc_earnings = self._get_curb_earnings(lease_id, current_date, current_date)

            # Get daily charges
            mta_tif = self._get_mta_tif_charges(lease_id, current_date, current_date)

            # Get daily EZPass (if any with specific dates)
            ezpass = self._get_daily_ezpass(lease_id, current_date)

            # Get daily PVB
            pvb = self._get_daily_pvb(lease_id, current_date)

            # Get daily TLC
            tlc = self._get_daily_tlc(lease_id, current_date)

            net_daily = cc_earnings - (mta_tif + ezpass + pvb + tlc)

            days.append(
                DailyBreakdown(
                    date=current_date,
                    day_of_week=day_name,
                    breakdown_date=current_date,
                    cc_earnings=cc_earnings,
                    mta_tif=mta_tif,
                    ezpass=ezpass,
                    violations=pvb,
                    tlc_tickets=tlc,
                    net_daily=net_daily,
                )
            )

            current_date += timedelta(days=1)

        return days

    def _get_daily_ezpass(self, lease_id: int, date_param: date) -> Decimal:
        """Get EZPass charges for a specific day"""
        result = (
            self.db.query(func.coalesce(func.sum(EZPassTransaction.amount), 0))
            .filter(
                EZPassTransaction.lease_id == lease_id,
                EZPassTransaction.posting_date == date_param,
            )
            .scalar()
        )
        return Decimal(str(result))

    def _get_daily_pvb(self, lease_id: int, date_param: date) -> Decimal:
        """Get PVB violations for a specific day"""
        result = (
            self.db.query(func.coalesce(func.sum(PVBViolation.fine), 0))
            .filter(
                PVBViolation.lease_id == lease_id,
                PVBViolation.posting_date == date_param,
            )
            .scalar()
        )
        return Decimal(str(result))

    def _get_daily_tlc(self, lease_id: int, date_param: date) -> Decimal:
        """Get TLC tickets for a specific day"""
        result = (
            self.db.query(func.coalesce(func.sum(TLCViolation.total_payable), 0))
            .filter(
                TLCViolation.lease_id == lease_id, TLCViolation.issue_date == date_param
            )
            .scalar()
        )
        return Decimal(str(result))

    def _get_delayed_charges(
        self, lease_id: int, week_start: date, week_end: date
    ) -> List[DelayedCharge]:
        """Get charges entered this week but belonging to previous weeks"""

        delayed = []

        # Get delayed EZPass charges
        ezpass_charges = (
            self.db.query(EZPassTransaction)
            .filter(
                EZPassTransaction.lease_id == lease_id,
                EZPassTransaction.created_on >= week_start,
                EZPassTransaction.created_on <= week_end,
                EZPassTransaction.posting_date < week_start,
            )
            .all()
        )

        for charge in ezpass_charges:
            delayed.append(
                DelayedCharge(
                    category="EZPass",
                    amount=charge.amount,
                    original_date=charge.posting_date,
                    system_entry_date=charge.created_on.date(),
                    description=f"Toll: {charge.exit_plaza or 'Unknown'}",
                )
            )

        # Get delayed PVB violations
        pvb_charges = (
            self.db.query(PVBViolation)
            .filter(
                PVBViolation.lease_id == lease_id,
                PVBViolation.created_on >= week_start,
                PVBViolation.created_on <= week_end,
                PVBViolation.posting_date < week_start,
            )
            .all()
        )

        for charge in pvb_charges:
            delayed.append(
                DelayedCharge(
                    category="PVB",
                    amount=charge.fine,
                    original_date=charge.posting_date,
                    system_entry_date=charge.created_on.date(),
                    description=f"Violation: 'Unknown'",
                )
            )

        # Get delayed TLC tickets
        tlc_charges = (
            self.db.query(TLCViolation)
            .filter(
                TLCViolation.lease_id == lease_id,
                TLCViolation.created_on >= week_start,
                TLCViolation.created_on <= week_end,
                TLCViolation.issue_date < week_start,
            )
            .all()
        )

        for charge in tlc_charges:
            delayed.append(
                DelayedCharge(
                    category="TLC",
                    amount=charge.total_payable,
                    original_date=charge.issue_date,
                    system_entry_date=charge.created_on.date(),
                    description=f"Ticket: {charge.type or 'Unknown'}",
                )
            )

        return delayed
