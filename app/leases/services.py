# app/leases/services.py

import os
import tempfile
from decimal import Decimal
from datetime import date, datetime, timezone
from typing import List, Optional, Tuple, Union

from sqlalchemy import and_, asc, desc, func, or_, select
from sqlalchemy.orm import Session, joinedload

from app.drivers.models import Driver, TLCLicense
from app.drivers.schemas import DOVLease
from app.drivers.services import driver_service
from app.esign.esign_client import ESignClient
from app.leases.models import (
    Lease,
    LeaseConfiguration,
    LeaseDriver,
    LeaseDriverDocument,
    LeasePaymentConfiguration,
    LeasePreset,
    LeaseSchedule,
)
from app.leases.schemas import (
    LeasePresetCreate,
    LeasePresetUpdate,
    LeaseStatus,
    LongTermLease,
    MedallionOnlyLease,
    ShiftLease,
    ShortTermLease,
)
from app.medallions.models import Medallion
from app.uploads.models import Document
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.vehicles.models import Vehicle, VehicleRegistration
from app.vehicles.schemas import VehicleStatus

logger = get_logger(__name__)


class LeaseService:
    """Service for managing lease operations"""

    def get_lease(
        self,
        db: Session,
        page=None,
        per_page=None,
        lookup_id: Optional[int] = None,
        lease_id: Optional[str] = None,
        is_lease_list: Optional[bool] = None,
        lease_type: Optional[str] = None,
        medallion_number: Optional[int] = None,
        tlc_number: Optional[str] = None,
        vin_number: Optional[str] = None,
        plate_number: Optional[str] = None,
        driver_id: Optional[str] = None,
        driver_name: Optional[str] = None,
        vehicle_id: Optional[int] = None,
        lease_start_date: Optional[date] = None,
        lease_end_date: Optional[date] = None,
        status: Optional[str] = None,
        lease_amount: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        exclude_additional_drivers: Optional[bool] = None,
        multiple: bool = False,
    ) -> Union[Lease, List[Lease], None]:
        """Get a lease by ID, vehicle ID, or status"""
        try:
            query = db.query(Lease)
            joined_medallion = False
            joined_vehicle = False
            joined_vehicle_registration = False
            joined_lease_driver = False
            joined_driver = False
            joined_tlc_license = False
            joined_lease_configuration = False

            if lookup_id:
                query = query.filter(Lease.id == lookup_id)
            if lease_id:
                lease_ids = [i.strip() for i in lease_id.split(",") if i.strip()]
                query = query.filter(
                    or_(*[Lease.lease_id.ilike(f"%{i}%") for i in lease_ids])
                )
            if lease_type:
                query = query.filter(Lease.lease_type == lease_type)

            if vehicle_id:
                query = query.filter(Lease.vehicle_id == vehicle_id)
            if medallion_number:
                medallion_numbers = [
                    i.strip() for i in medallion_number.split(",") if i.strip()
                ]
                if not joined_medallion:
                    query = query.join(Medallion, Lease.medallion_id == Medallion.id)
                    joined_medallion = True
                query = query.filter(
                    or_(
                        *[
                            Medallion.medallion_number.ilike(f"%{number}%")
                            for number in medallion_numbers
                        ]
                    )
                )
            if vin_number:
                vins = [i.strip() for i in vin_number.split(",") if i.strip()]
                if not joined_vehicle:
                    query = query.join(Vehicle, Lease.vehicle_id == Vehicle.id)
                    joined_vehicle = True
                query = query.filter(
                    or_(*[Vehicle.vin.ilike(f"%{vin}%") for vin in vins])
                )
            if plate_number:
                plate_numbers = [
                    i.strip() for i in plate_number.split(",") if i.strip()
                ]
                if not joined_vehicle:
                    query = query.join(Vehicle, Lease.vehicle_id == Vehicle.id)
                    joined_vehicle = True
                if not joined_vehicle_registration:
                    query = query.join(
                        VehicleRegistration,
                        Vehicle.id == VehicleRegistration.vehicle_id,
                    )
                    joined_vehicle_registration = True
                query = query.filter(
                    or_(
                        *[
                            VehicleRegistration.plate_number.ilike(f"%{plate}%")
                            for plate in plate_numbers
                        ]
                    )
                )

            if driver_id:
                driver_ids = [i.strip() for i in str(driver_id).split(",") if i.strip()]
                if not joined_lease_driver:
                    query = query.join(LeaseDriver, Lease.id == LeaseDriver.lease_id)
                    joined_lease_driver = True
                if not joined_driver:
                    query = query.join(
                        Driver, LeaseDriver.driver_id == Driver.driver_id
                    )
                    joined_driver = True

                query = query.filter(
                    or_(*[Driver.driver_id.ilike(f"%{id}%") for id in driver_ids])
                )

            if driver_name:
                driver_names = [i.strip() for i in driver_name.split(",") if i.strip()]
                if not joined_lease_driver:
                    query = query.join(LeaseDriver, Lease.id == LeaseDriver.lease_id)
                    joined_lease_driver = True
                if not joined_driver:
                    query = query.join(
                        Driver, LeaseDriver.driver_id == Driver.driver_id
                    )
                    joined_driver = True
                query = query.filter(
                    or_(
                        *[
                            (Driver.full_name.ilike(f"%{name}%"))
                            for name in driver_names
                        ]
                    )
                )
            # ✅ NEW: Exclude additional drivers if requested
            if exclude_additional_drivers is not None:
                logger.info(
                    f"Filtering leases for tlc_number={tlc_number}: excluding additional drivers"
                )
                if not joined_lease_driver:
                    query = query.join(
                        LeaseDriver, Lease.id == LeaseDriver.lease_id
                    )
                    joined_lease_driver = True
                    
                query = query.filter(
                    or_(
                        LeaseDriver.is_additional_driver == False,
                        LeaseDriver.is_additional_driver.is_(None)
                    )
                )

            if tlc_number:
                tlc_numbers = [i.strip() for i in tlc_number.split(",") if i.strip()]
                if not joined_lease_driver:
                    query = query.join(LeaseDriver, Lease.id == LeaseDriver.lease_id)
                    joined_lease_driver = True
                if not joined_driver:
                    query = query.join(
                        Driver, LeaseDriver.driver_id == Driver.driver_id
                    )
                    joined_driver = True
                if not joined_tlc_license:
                    query = query.join(
                        TLCLicense, Driver.tlc_license_number_id == TLCLicense.id
                    )
                    joined_tlc_license = True
                    
                query = query.filter(
                    or_(
                        *[
                            TLCLicense.tlc_license_number.ilike(f"%{tlc}%")
                            for tlc in tlc_numbers
                        ]
                    )
                )

            if lease_start_date:
                query = query.filter(Lease.lease_end_date >= lease_start_date)
            if lease_end_date:
                query = query.filter(Lease.lease_end_date <= lease_end_date)
            if status:
                query = query.filter(Lease.lease_status == status)

            if lease_amount is not None:
                from sqlalchemy import Float, cast

                # Parse comma-separated lease amounts
                lease_amounts = [
                    float(amt.strip()) for amt in str(lease_amount).split(",") if amt.strip()
                ]

                if not joined_lease_configuration:
                    query = query.join(
                        LeaseConfiguration,
                        and_(
                            LeaseConfiguration.lease_id == Lease.id,
                            LeaseConfiguration.lease_breakup_type == "lease_amount",
                        ),
                    )
                    joined_lease_configuration = True

                # Filter by multiple lease amounts using OR
                query = query.filter(
                    or_(
                        *[
                            cast(LeaseConfiguration.lease_limit, Float) == amt
                            for amt in lease_amounts
                        ]
                    )
                )

            if sort_by:
                sort_attr = [
                    "lease_id",
                    "created_on",
                    "lease_type",
                    "lease_start_date",
                    "lease_end_date",
                    "lease_status",
                    "vin_no",
                    "medallion_no",
                    "plate_no",
                    "driver_id",
                    "driver_name",
                    "tlc_number",
                    "lease_amount",
                ]
                if sort_by in sort_attr:
                    if sort_by == "vin_no":
                        if not joined_vehicle:
                            query = query.join(Vehicle, Lease.vehicle_id == Vehicle.id)
                            joined_vehicle = True
                        query = query.order_by(
                            Vehicle.vin.asc()
                            if sort_order == "asc"
                            else Vehicle.vin.desc()
                        )
                    if sort_by == "medallion_no":
                        if not joined_medallion:
                            query = query.join(
                                Medallion, Lease.medallion_id == Medallion.id
                            )
                            joined_medallion = True
                        query = query.order_by(
                            Medallion.medallion_number.asc()
                            if sort_order == "asc"
                            else Medallion.medallion_number.desc()
                        )
                    if sort_by == "plate_no":
                        if not joined_vehicle:
                            query = query.join(Vehicle, Lease.vehicle_id == Vehicle.id)
                            joined_vehicle = True
                        if not joined_vehicle_registration:
                            query = query.join(
                                VehicleRegistration,
                                Vehicle.id == VehicleRegistration.vehicle_id,
                            )
                            joined_vehicle_registration = True
                        query = query.order_by(
                            VehicleRegistration.plate_number.asc()
                            if sort_order == "asc"
                            else VehicleRegistration.plate_number.desc()
                        )
                    if sort_by == "driver_id":
                        if not joined_lease_driver:
                            query = query.join(
                                LeaseDriver, Lease.id == LeaseDriver.lease_id
                            )
                            joined_lease_driver = True
                        if not joined_driver:
                            query = query.join(
                                Driver, LeaseDriver.driver_id == Driver.driver_id
                            )
                            joined_driver = True
                        query = query.order_by(
                            Driver.driver_id.asc()
                            if sort_order == "asc"
                            else Driver.driver_id.desc()
                        )
                    if sort_by == "driver_name":
                        if not joined_lease_driver:
                            query = query.join(
                                LeaseDriver, Lease.id == LeaseDriver.lease_id
                            )
                            joined_lease_driver = True
                        if not joined_driver:
                            query = query.join(
                                Driver, LeaseDriver.driver_id == Driver.driver_id
                            )
                            joined_driver = True
                        query = query.order_by(
                            Driver.last_name.asc()
                            if sort_order == "asc"
                            else Driver.last_name.desc()
                        )
                    if sort_by == "tlc_number":
                        if not joined_lease_driver:
                            query = query.join(
                                LeaseDriver, Lease.id == LeaseDriver.lease_id
                            )
                            joined_lease_driver = True
                        if not joined_driver:
                            query = query.join(
                                Driver, LeaseDriver.driver_id == Driver.driver_id
                            )
                            joined_driver = True
                        if not joined_tlc_license:
                            query = query.join(
                                TLCLicense,
                                Driver.tlc_license_number_id == TLCLicense.id,
                            )
                            joined_tlc_license = True
                        query = query.order_by(
                            TLCLicense.tlc_license_number.asc()
                            if sort_order == "asc"
                            else TLCLicense.tlc_license_number.desc()
                        )
                    if sort_by == "lease_start_date":
                        query = query.order_by(
                            Lease.lease_start_date.asc()
                            if sort_order == "asc"
                            else Lease.lease_start_date.desc()
                        )
                    if sort_by == "lease_end_date":
                        query = query.order_by(
                            Lease.lease_end_date.asc()
                            if sort_order == "asc"
                            else Lease.lease_end_date.desc()
                        )
                    if sort_by == "created_on":
                        query = query.order_by(
                            Lease.created_on.asc()
                            if sort_order == "asc"
                            else Lease.created_on.desc()
                        )
                    if sort_by == "lease_status":
                        query = query.order_by(
                            Lease.lease_status.asc()
                            if sort_order == "asc"
                            else Lease.lease_status.desc()
                        )
                    if sort_by == "lease_type":
                        query = query.order_by(
                            Lease.lease_type.asc()
                            if sort_order == "asc"
                            else Lease.lease_type.desc()
                        )
                    if sort_by == "lease_id":
                        query = query.order_by(
                            Lease.lease_id.asc()
                            if sort_order == "asc"
                            else Lease.lease_id.desc()
                        )
                    if sort_by == "lease_amount":
                        # Join with LeaseConfiguration and cast lease_limit to numeric for proper sorting
                        from sqlalchemy import Float, cast

                        if not joined_lease_configuration:
                            query = query.outerjoin(
                                LeaseConfiguration,
                                and_(
                                    LeaseConfiguration.lease_id == Lease.id,
                                    LeaseConfiguration.lease_breakup_type == "lease_amount",
                                ),
                            )
                            joined_lease_configuration = True
                        query = query.order_by(
                            cast(LeaseConfiguration.lease_limit, Float).asc()
                            if sort_order == "asc"
                            else cast(LeaseConfiguration.lease_limit, Float).desc()
                        )
            else:
                query = query.order_by(Lease.updated_on.desc(), Lease.created_on.desc())

            if multiple:
                total_count = query.distinct(Lease.id).count()
                if page and per_page:
                    query = query.offset((page - 1) * per_page).limit(per_page)
                return query.all(), total_count
            return query.first()
        except Exception as e:
            logger.error("Error getting lease: %s", str(e), exc_info=True)
            raise e

    def get_can_lease(
        self,
        db: Session,
        vin: str = None,
        medallion_number: str = None,
        plate_number: str = None,
        shift_availability: str = None,
        sort_by: str = None,
        sort_order: str = None,
        page: int = None,
        per_page: int = None,
        multiple: bool = False,
    ):
        """
        Get all vehicles that can create a lease:
        1. Vehicles with HACKED_UP status (not yet leased)
        2. Vehicles in active leases with only day shift OR only night shift selected (can add another shift lease)

        Args:
            shift_availability: Filter by shift availability
                - 'full': Both day and night shifts available (no active lease)
                - 'day': Day shift available (night shift may or may not be occupied)
                - 'night': Night shift available (day shift may or may not be occupied)
        """

        try:
            # Subquery to find vehicles with active leases that have only one shift selected
            single_shift_vehicles = (
                db.query(Lease.vehicle_id).filter(
                    Lease.lease_status == LeaseStatus.ACTIVE,
                    or_(
                        # Only day shift is selected (night shift is False or NULL)
                        and_(
                            Lease.is_day_shift == True,
                            or_(
                                Lease.is_night_shift == False,
                                Lease.is_night_shift.is_(None),
                            ),
                        ),
                        # Only night shift is selected (day shift is False or NULL)
                        and_(
                            Lease.is_night_shift == True,
                            or_(
                                Lease.is_day_shift == False,
                                Lease.is_day_shift.is_(None),
                            ),
                        ),
                    ),
                )
            ).subquery()

            query = (
                db.query(
                    Vehicle,
                    Medallion.medallion_number.label("medallion_number"),
                    VehicleRegistration.plate_number.label("plate_number"),
                )
                .outerjoin(
                    VehicleRegistration, Vehicle.id == VehicleRegistration.vehicle_id
                )
                .outerjoin(Medallion, Vehicle.medallion_id == Medallion.id)
                .filter(
                    or_(
                        # Vehicles that are hacked up and not yet leased
                        Vehicle.vehicle_status == VehicleStatus.HACKED_UP,
                        # Vehicles that have an active lease with only one shift
                        Vehicle.id.in_(single_shift_vehicles),
                    )
                )
            )
            if vin:
                query = query.filter(Vehicle.vin.ilike(f"%{vin}%"))
            if medallion_number:
                query = query.filter(
                    Medallion.medallion_number.ilike(f"%{medallion_number}%")
                )
            if plate_number:
                query = query.filter(
                    VehicleRegistration.plate_number.ilike(f"%{plate_number}%")
                )

            if sort_by and sort_order:
                sort_attr = {
                    "medallion_number": Medallion.medallion_number,
                    "plate_number": VehicleRegistration.plate_number,
                    "vin": Vehicle.vin,
                    "status": Vehicle.vehicle_status,
                    "created_on": Vehicle.created_on,
                    "updated_on": Vehicle.updated_on,
                    "make": Vehicle.make,
                    "model": Vehicle.model,
                    "year": Vehicle.year,
                }
                if sort_attr.get(sort_by):
                    query = query.order_by(
                        sort_attr.get(sort_by).asc()
                        if sort_order == "asc"
                        else sort_attr.get(sort_by).desc()
                    )

            if multiple:
                # First, fetch ALL results without pagination
                all_results = []
                for vehicle, medallion_number, plate_number in query.all():
                    # Get ALL active leases for this vehicle to check shift occupancy
                    active_leases = (
                        db.query(Lease)
                        .filter(
                            Lease.vehicle_id == vehicle.id,
                            Lease.lease_status == LeaseStatus.ACTIVE,
                        )
                        .all()
                    )

                    # Determine which shifts are occupied across ALL active leases
                    has_day_shift = False
                    has_night_shift = False

                    for lease in active_leases:
                        if lease.is_day_shift:
                            has_day_shift = True
                        if lease.is_night_shift:
                            has_night_shift = True

                    # If both shifts are occupied, skip this vehicle entirely
                    if has_day_shift and has_night_shift:
                        continue

                    # Calculate available shifts
                    available_day_shift = not has_day_shift
                    available_night_shift = not has_night_shift

                    # Apply shift availability filter if provided
                    if shift_availability:
                        shift_filter = shift_availability.lower()
                        logger.info(
                            f"Filtering by shift_availability='{shift_filter}' for vehicle {vehicle.id} "
                            f"(has_day_shift={has_day_shift}, has_night_shift={has_night_shift}, "
                            f"available_day_shift={available_day_shift}, available_night_shift={available_night_shift})"
                        )
                        if shift_filter == "full":
                            # Only show vehicles with BOTH shifts available (no active lease at all)
                            if has_day_shift or has_night_shift:
                                logger.info(
                                    f"Skipping vehicle {vehicle.id} - not fully available"
                                )
                                continue
                        elif shift_filter == "day":
                            # Only show vehicles with ONLY day shift available (night shift must be occupied)
                            if not available_day_shift or not has_night_shift:
                                logger.info(
                                    f"Skipping vehicle {vehicle.id} - not day-shift-only available"
                                )
                                continue
                        elif shift_filter == "night":
                            # Only show vehicles with ONLY night shift available (day shift must be occupied)
                            if not available_night_shift or not has_day_shift:
                                logger.info(
                                    f"Skipping vehicle {vehicle.id} - not night-shift-only available"
                                )
                                continue

                    all_results.append(
                        {
                            "id": vehicle.id,
                            "vin": vehicle.vin,
                            "medallion_number": medallion_number,
                            "plate_number": plate_number,
                            "vehicle_type": vehicle.vehicle_type,
                            "status": vehicle.vehicle_status,
                            "created_on": vehicle.created_on,
                            "updated_on": vehicle.updated_on,
                            "make": vehicle.make,
                            "model": vehicle.model,
                            "year": vehicle.year,
                            "base_price": vehicle.base_price,
                            "sales_tax": vehicle.sales_tax,
                            "vehicle_hack_up_cost": vehicle.vehicle_hack_up_cost,
                            "vehicle_true_cost": vehicle.vehicle_true_cost,
                            "vehicle_lifetime_cap": vehicle.vehicle_lifetime_cap,
                            "recoverable_base": min(
                                vehicle.vehicle_true_cost or 0,
                                vehicle.vehicle_lifetime_cap or 0,
                            ),
                            "has_active_lease": len(active_leases) > 0,
                            "current_day_shift_occupied": has_day_shift,
                            "current_night_shift_occupied": has_night_shift,
                            "available_day_shift": available_day_shift,
                            "available_night_shift": available_night_shift,
                        }
                    )

                # Get total count from filtered results
                total = len(all_results)

                # Apply pagination to the filtered results
                if page and per_page:
                    start_idx = (page - 1) * per_page
                    end_idx = start_idx + per_page
                    results = all_results[start_idx:end_idx]
                else:
                    results = all_results

                return results, total

            vehicle, medallion_number, plate_number = query.first()
            if vehicle:
                return {
                    "id": vehicle.id,
                    "vin": vehicle.vin,
                    "medallion_number": medallion_number,
                    "plate_number": plate_number,
                    "vehicle_type": vehicle.vehicle_type,
                    "status": vehicle.vehicle_status,
                    "created_on": vehicle.created_on,
                    "updated_on": vehicle.updated_on,
                    "make": vehicle.make,
                    "model": vehicle.model,
                    "year": vehicle.year,
                    "base_price": vehicle.base_price,
                    "sales_tax": vehicle.sales_tax,
                    "vehicle_hack_up_cost": vehicle.vehicle_hack_up_cost,
                    "vehicle_true_cost": vehicle.vehicle_true_cost,
                    "vehicle_lifetime_cap": vehicle.vehicle_lifetime_cap,
                    "recoverable_base": min(
                        vehicle.vehicle_true_cost or 0,
                        vehicle.vehicle_lifetime_cap or 0,
                    ),
                }
            return None
        except Exception as e:
            logger.error("Error getting all active leases: %s", str(e))
            raise e

    def get_lease_configurations(
        self,
        db: Session,
        lookup_id: Optional[int] = None,
        lease_id: Optional[str] = None,
        lease_configuration_id: Optional[int] = None,
        lease_breakup_type: Optional[str] = None,
        sort_order: Optional[str] = "desc",
        multiple: bool = False,
    ) -> Union[LeaseConfiguration, List[LeaseConfiguration], None]:
        """Get lease configurations by ID"""
        try:
            query = db.query(LeaseConfiguration)
            if lookup_id:
                query = query.filter(LeaseConfiguration.id == lookup_id)
            if lease_id:
                query = query.filter(LeaseConfiguration.lease_id == lease_id)
            if lease_configuration_id:
                query = query.filter(LeaseConfiguration.id == lease_configuration_id)
            if lease_breakup_type:
                query = query.filter(
                    LeaseConfiguration.lease_breakup_type == lease_breakup_type
                )
            if sort_order:
                if sort_order == "desc":
                    query = query.order_by(desc(LeaseConfiguration.created_on))
                else:
                    query = query.order_by(asc(LeaseConfiguration.created_on))

            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting lease configurations: %s", str(e))
            raise e

    def upsert_lease_configuration(
        self, db: Session, lease_configuration_data: dict
    ) -> LeaseConfiguration:
        """Upsert a lease configuration"""
        try:
            if lease_configuration_data.get("id"):
                lease_configuration = (
                    db.query(LeaseConfiguration)
                    .filter(LeaseConfiguration.id == lease_configuration_data.get("id"))
                    .first()
                )
                if lease_configuration:
                    for key, value in lease_configuration_data.items():
                        setattr(lease_configuration, key, value)
                    db.flush()
                    db.refresh(lease_configuration)
                    return lease_configuration
            else:
                lease_configuration = LeaseConfiguration(**lease_configuration_data)
                db.add(lease_configuration)
                db.flush()
                db.refresh(lease_configuration)
                return lease_configuration
        except Exception as e:
            logger.error("Error upserting lease configuration: %s", str(e))
            raise e

    def delete_lease_configurations(self, db: Session, config_id: int):
        """Delete lease configurations by lease ID"""
        try:
            db.query(LeaseConfiguration).filter(
                LeaseConfiguration.id == config_id
            ).delete()
            db.flush()
        except Exception as e:
            logger.error("Error deleting lease configurations: %s", str(e))
            raise e

    def upsert_lease(self, db: Session, lease_data: dict) -> Lease:
        """Upsert a lease"""
        try:
            if lease_data.get("id"):
                lease = db.query(Lease).filter(Lease.id == lease_data.get("id")).first()
                if lease:
                    for key, value in lease_data.items():
                        setattr(lease, key, value)
                    db.flush()
                    db.refresh(lease)
                    return lease
            else:
                lease = Lease(**lease_data)
                db.add(lease)
                db.flush()
                db.refresh(lease)
                return lease
        except Exception as e:
            logger.error("Error upserting lease: %s", str(e))
            raise e

    def get_lease_driver_documents(
        self,
        db: Session,
        lease_driver_id: Optional[int] = None,
        lease_id: Optional[int] = None,
        status: Optional[bool] = None,
        multiple: bool = False,
    ) -> Union[LeaseDriverDocument, List[LeaseDriverDocument], None]:
        """Get lease driver documents by ID"""
        try:
            query = db.query(LeaseDriverDocument)
            if lease_driver_id:
                query = query.filter(
                    LeaseDriverDocument.lease_driver_id == lease_driver_id
                )
            if lease_id:
                query = query.filter(LeaseDriverDocument.lease_id == lease_id)
            if status:
                query = query.filter(LeaseDriverDocument.status == status)

            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting lease driver documents: %s", str(e))
            raise e

    def get_lease_drivers(
        self,
        db: Session,
        lease_id: Optional[int] = None,
        lease_driver_id: Optional[int] = None,
        driver_id: Optional[str] = None,
        sort_order: Optional[str] = "desc",
        is_additional_driver: Optional[bool] = None,
        multiple: bool = False,
    ) -> Union[LeaseDriver, List[LeaseDriver], None]:
        """Get lease drivers by ID"""
        try:
            query = db.query(LeaseDriver).filter(LeaseDriver.is_active)
            if lease_driver_id:
                query = query.filter(LeaseDriver.id == lease_driver_id)
            if lease_id:
                query = query.filter(LeaseDriver.lease_id == lease_id)
            if driver_id:
                query = query.filter(LeaseDriver.driver_id == driver_id)
            if is_additional_driver is not None:
                query = query.filter(LeaseDriver.is_additional_driver == is_additional_driver)
            if sort_order:
                query = query.order_by(
                    desc(LeaseDriver.created_on)
                    if sort_order == "desc"
                    else asc(LeaseDriver.created_on)
                )

            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting lease drivers: %s", str(e))
            raise e

    def get_lease_payment_configuration(self, db: Session):
        """Get Lease Payment"""
        try:
            configs = db.query(LeasePaymentConfiguration).all()

            config_map = {}
            for config in configs:
                entry = {"total_amount": config.total_amount}
                if config.day_shift_amount is not None:
                    entry["day_shift_amount"] = config.day_shift_amount
                if config.night_shift_amount is not None:
                    entry["night_shift_amount"] = config.night_shift_amount
                config_map[config.config_type] = entry

            return config_map

        except Exception as e:
            logger.error("Error getting lease payment config: %s", str(e))
            raise e

    def upsert_lease_driver(self, db: Session, lease_driver_data: dict) -> LeaseDriver:
        """Upsert a lease driver"""
        try:
            if lease_driver_data.get("id"):
                lease_driver = self.get_lease_drivers(
                    db, lease_driver_id=lease_driver_data.get("id")
                )
                if lease_driver:
                    for key, value in lease_driver_data.items():
                        setattr(lease_driver, key, value)
                    db.flush()
                    db.refresh(lease_driver)
                    return lease_driver
            else:
                lease_driver = LeaseDriver(**lease_driver_data)
                db.add(lease_driver)
                db.flush()
                db.refresh(lease_driver)
                return lease_driver
        except Exception as e:
            logger.error("Error upserting lease driver: %s", str(e), exc_info=True)
            raise e

    def upsert_lease_payment_configuration(
        self, db: Session, lease_payment_config_data: dict
    ):
        """Upsert lease payment configuration"""

        try:
            config_type = lease_payment_config_data.get("config_type")
            if config_type:
                lease_config = (
                    db.query(LeasePaymentConfiguration)
                    .filter(LeasePaymentConfiguration.config_type == config_type)
                    .first()
                )

                if lease_config:
                    # Update existing fields
                    for key, value in lease_payment_config_data.items():
                        setattr(lease_config, key, value)

                    db.flush()
                    db.refresh(lease_config)
                    return lease_config

            # If not found, insert new
            lease_payment_config = LeasePaymentConfiguration(
                **lease_payment_config_data
            )
            db.add(lease_payment_config)
            db.flush()
            db.refresh(lease_payment_config)
            return lease_payment_config

        except Exception as e:
            logger.error(
                "Error upserting lease payment config: %s", str(e), exc_info=True
            )
            raise e

    def delete_lease_driver(self, db: Session, lease_driver_id: int):
        """Delete a lease driver"""
        try:
            db.query(LeaseDriver).filter(LeaseDriver.id == lease_driver_id).delete()
            db.flush()
        except Exception as e:
            logger.error("Error deleting lease driver: %s", str(e), exc_info=True)
            raise e

    def fetch_lease_information_driver(
        self, db: Session, driver_id: Optional[str] = None
    ):
        """Fetch lease information for a driver"""
        try:
            query = (
                db.query(
                    LeaseDriver.id.label("driver_lease_id"),
                    Lease.lease_id,
                    Medallion.medallion_number,
                    Driver.first_name,
                    Driver.last_name,
                    Driver.driver_id,
                    Vehicle.vin,
                    VehicleRegistration.plate_number,
                    Lease.lease_date,
                    Lease.id.label("lease_id_pk"),
                )
                .join(Driver, Driver.driver_id == LeaseDriver.driver_id)
                .join(Lease, Lease.id == LeaseDriver.lease_id)
                .join(Medallion, Medallion.id == Lease.medallion_id)
                .join(Vehicle, Vehicle.id == Lease.vehicle_id)
                .join(VehicleRegistration, VehicleRegistration.vehicle_id == Vehicle.id)
                .filter(
                    Driver.is_active == True,
                    LeaseDriver.is_active == True,
                    VehicleRegistration.is_active == True,
                )
            )

            if driver_id:
                query = query.filter(LeaseDriver.driver_id == driver_id)

            active_drivers = query.all()
            driver_ids = [ldr.driver_id for ldr in active_drivers]

            driver_lease_documents = (
                db.query(LeaseDriverDocument.lease_driver_id)
                .join(
                    LeaseDriver, LeaseDriver.id == LeaseDriverDocument.lease_driver_id
                )
                .filter(
                    LeaseDriverDocument.is_active == True,
                    LeaseDriver.driver_id.in_(driver_ids),
                )
                .all()
            )

            driver_ids_with_documents = {
                doc.lease_driver_id for doc in driver_lease_documents
            }

            lease_vehicle_info = []
            for lease_driver in active_drivers:
                if not lease_driver.lease_id:
                    continue

                lease_vehicle_info.append(
                    {
                        "driver_lease_id": lease_driver.driver_lease_id,
                        "lease_id": lease_driver.lease_id,
                        "medallion_number": lease_driver.medallion_number,
                        "driver_name": f"{lease_driver.first_name} {lease_driver.last_name}",
                        "vin_number": lease_driver.vin,
                        "vehicle_plate_number": lease_driver.plate_number,
                        "lease_date": lease_driver.lease_date,
                        "lease_id_pk": lease_driver.lease_id_pk,
                        "is_manager": lease_driver.driver_lease_id
                        in driver_ids_with_documents,
                    }
                )

            return lease_vehicle_info
        except Exception as e:
            logger.error(
                "Error fetching lease information for a driver: %s",
                str(e),
                exc_info=True,
            )
            raise e

    def handle_dov_lease(self, db: Session, lease_id: int, lease_data: DOVLease):
        """Handle DOV lease"""
        try:
            financials = lease_data.financial_information.model_dump(exclude_none=True)
            configuration_data = {}

            for key, value in financials.items():
                existing_config = self.get_lease_configurations(
                    db, lease_id=lease_id, lease_breakup_type=key
                )

                if existing_config:
                    configuration_data = {
                        "id": existing_config.id,
                        "lease_limit": value,
                    }
                else:
                    configuration_data = {
                        "lease_id": lease_id,
                        "lease_breakup_type": key,
                        "lease_limit": value,
                    }
                self.upsert_lease_configuration(db, configuration_data)
        except Exception as e:
            logger.error("Error handling DOV lease: %s", str(e))
            raise e

    def handle_long_term_lease(
        self, db: Session, lease_id: int, lease_data: LongTermLease
    ):
        """Handle Long Term Lease"""
        try:
            financials = lease_data.financialInformation.model_dump(exclude_none=True)
            configuration_data = {}

            for key, value in financials.items():
                existing_config = self.get_lease_configurations(
                    db, lease_id=lease_id, lease_breakup_type=key
                )

                if existing_config:
                    configuration_data = {
                        "id": existing_config.id,
                        "lease_limit": value,
                    }
                else:
                    configuration_data = {
                        "lease_id": lease_id,
                        "lease_breakup_type": key,
                        "lease_limit": value,
                    }
                self.upsert_lease_configuration(db, configuration_data)
        except Exception as e:
            logger.error("Error handling Long Term Lease: %s", str(e))
            raise e

    def handle_short_term_lease(
        self, db: Session, lease_id: int, short_term_data: ShortTermLease
    ):
        """Handle Short Term Lease"""
        try:
            financials = short_term_data.financialInformation
            days_of_week = ["sun", "mon", "tus", "wen", "thu", "fri", "sat"]

            for day in days_of_week:
                config_data = {}
                day_info = financials.get(day)
                if not day_info:
                    continue

                for shift_type in ["day_shift", "night_shift"]:
                    lease_breakup_type = f"{day}_{shift_type}"
                    lease_limit = day_info.get(
                        "day_shift" if shift_type == "day_shift" else "night_shift", ""
                    )
                    if lease_limit is None:
                        continue

                    existing_config = self.get_lease_configurations(
                        db, lease_id=lease_id, lease_breakup_type=lease_breakup_type
                    )

                    if existing_config:
                        config_data = {
                            "lease_limit": lease_limit,
                            "id": existing_config.id,
                        }
                    else:
                        config_data = {
                            "lease_id": lease_id,
                            "lease_breakup_type": lease_breakup_type,
                            "lease_limit": lease_limit,
                        }

                    self.upsert_lease_configuration(db, config_data)
        except Exception as e:
            logger.error("Error handling Short Term Lease: %s", str(e))
            raise e

    def handle_medallion_lease(
        self, db: Session, lease_id: int, medallion_data: MedallionOnlyLease
    ):
        """Handle Medallion Only Lease"""
        try:
            financials = medallion_data.financialInformation.model_dump(
                exclude_none=True
            )
            configuration_data = {}

            for key, value in financials.items():
                existing_config = self.get_lease_configurations(
                    db, lease_id=lease_id, lease_breakup_type=key
                )

                if existing_config:
                    configuration_data = {
                        "id": existing_config.id,
                        "lease_limit": value,
                    }
                else:
                    configuration_data = {
                        "lease_id": lease_id,
                        "lease_breakup_type": key,
                        "lease_limit": value,
                    }
                self.upsert_lease_configuration(db, configuration_data)
        except Exception as e:
            logger.error("Error handling Long Term Lease: %s", str(e))
            raise e

    def update_lease_driver_info(self, db: Session, lease_id: int, driver_info: dict):
        """Update lease driver information"""
        try:
            driver_id = driver_info.get("driver_id")
            is_day_night_shift = driver_info.get("is_day_night_shift")
            co_lease_seq = driver_info.get("co_lease_seq")

            valid_driver = driver_service.get_drivers(db, driver_id=driver_id)
            if not valid_driver:
                raise ValueError(f"Driver ID {driver_id} passed is invalid")

            if is_day_night_shift is None:
                driver_role = "L"
            elif is_day_night_shift:
                driver_role = "DL"
            else:
                driver_role = "NL"

            lease_driver = self.get_lease_drivers(
                db, lease_id=lease_id, driver_id=driver_id
            )
            lease_driver_data = {}

            if lease_driver:
                lease_driver_data = {
                    "id": lease_driver.id,
                    "is_day_night_shift": is_day_night_shift,
                    "co_lease_seq": co_lease_seq,
                    "is_active": True,
                }
            else:
                lease_driver_data = {
                    "driver_id": driver_id,
                    "lease_id": lease_id,
                    "driver_role": driver_role,
                    "is_day_night_shift": is_day_night_shift,
                    "co_lease_seq": co_lease_seq,
                    "date_added": datetime.now(timezone.utc),
                    "is_active": True,
                }
            self.upsert_lease_driver(db, lease_driver_data)
            return f"Driver {driver_id} added or updated successfully for lease with ID {lease_id}"
        except Exception as e:
            logger.error("Error updating lease driver information: %s", str(e))
            raise e

    def remove_drivers_from_lease(
        self, db: Session, lease_id: int, driver_ids: set[str]
    ):
        """Mark drivers as inactive instead of removing them from lease"""
        try:
            # Get all lease drivers matching the criteria
            lease_drivers = (
                db.query(LeaseDriver)
                .filter(
                    LeaseDriver.lease_id == lease_id,
                    LeaseDriver.driver_id.in_(driver_ids),
                )
                .all()
            )

            # Mark each driver as inactive
            count = 0
            for lease_driver in lease_drivers:
                lease_driver.is_active = False
                lease_driver.updated_on = datetime.now(timezone.utc)
                db.add(lease_driver)
                count += 1
                logger.info(
                    "%s marked as inactive in the lease table", lease_driver.driver_id
                )

            db.flush()
            return count
        except Exception as e:
            logger.error("Error removing drivers from lease: %s", str(e))
            raise e

    def fetch_latest_driver_document_status_by_lease(self, db: Session, lease: Lease):
        """Fetch the latest driver document status by lease"""
        try:
            # Fetch only active drivers associated with the lease
            lease_drivers = (
                db.query(LeaseDriver)
                .filter(LeaseDriver.lease_id == lease.id, LeaseDriver.is_active == True)
                .all()
            )

            if not lease_drivers:
                return {"message": "No drivers associated with this lease."}

            result = []

            for lease_driver in lease_drivers:
                co_lease_seq = lease_driver.co_lease_seq
                driver_id = lease_driver.driver_id

                lease_driver_documents = lease_driver.documents[:2]

                # Fetch the latest document for the driver
                latest_docs = (
                    db.query(Document)
                    .filter(
                        Document.object_lookup_id == str(lease_driver.id),
                        Document.object_type == f"co-leasee-{co_lease_seq}",
                        Document.document_type.in_(
                            [
                                "driver_vehicle_lease",
                                "driver_medallion_lease",
                                "driver_long_term_lease",
                            ]
                        ),
                    )
                    .order_by(desc(Document.created_on))
                    .all()
                )

                # signed_document_url = ""
                if not lease_driver_documents:
                    for latest_document in latest_docs[:2]:
                        result.append(
                            {
                                "document_id": latest_document.id,
                                "driver_id": lease_driver.driver_id,
                                "driver_name": lease_driver.driver.full_name,
                                "driver_email": lease_driver.driver.email_address,
                                "document_name": latest_document.document_name,
                                "envelope_id": "",
                                "is_sent_for_signature": False,
                                "has_front_desk_signed": False,
                                "has_driver_signed": False,
                                "document_envelope_id": "",
                                "document_date": latest_document.document_date,
                                "file_size": latest_document.document_actual_size
                                if latest_document.document_actual_size
                                else 0,
                                "comments": latest_document.document_note,
                                "document_type": latest_document.document_type,
                                "object_type": latest_document.object_type,
                                "presigned_url": latest_document.presigned_url,
                                "document_format": latest_document.document_format,
                                "document_created_on": latest_document.created_on,
                                "object_lookup_id": lease_driver.id,
                                "signing_type": "",
                            }
                        )
                    return result

                for lease_driver_document in lease_driver_documents:
                    if lease_driver_document.envelope:
                        document_type = (
                            f"driver_{lease_driver_document.envelope.object_type}"
                        )
                        latest_document = (
                            db.query(Document)
                            .filter(
                                Document.object_lookup_id == str(lease_driver.id),
                                Document.object_type == f"co-leasee-{co_lease_seq}",
                                Document.document_type.in_([document_type]),
                            )
                            .order_by(desc(Document.created_on))
                            .first()
                        )
                    else:
                        latest_document = (
                            db.query(Document)
                            .filter(
                                Document.id == str(lease_driver_document.document_id),
                            )
                            .order_by(desc(Document.created_on))
                            .first()
                        )
                    result.append(
                        {
                            "object_lookup_id": lease_driver.id,
                            "document_id": latest_document.id,
                            "driver_id": lease_driver.driver_id,
                            "driver_name": lease_driver.driver.full_name,
                            "driver_email": lease_driver.driver.email_address,
                            "document_name": latest_document.document_name,
                            "envelope_id": lease_driver_document.document_envelope_id
                            if lease_driver_document
                            else "",
                            "is_sent_for_signature": False
                            if lease_driver_document
                            else False,
                            "has_front_desk_signed": lease_driver_document.has_frontend_signed
                            if lease_driver_document
                            else None,
                            "has_driver_signed": lease_driver_document.has_driver_signed
                            if lease_driver_document
                            else None,
                            "document_envelope_id": lease_driver_document.document_envelope_id
                            if lease_driver_document
                            else None,
                            "document_date": latest_document.document_date,
                            "file_size": latest_document.document_actual_size
                            if latest_document.document_actual_size
                            else 0,
                            "comments": latest_document.document_note,
                            "document_type": latest_document.document_type,
                            "object_type": latest_document.object_type,
                            "presigned_url": latest_document.presigned_url,
                            "document_format": latest_document.document_format,
                            # "signed_document_url": signed_document_url,
                            "document_created_on": latest_document.created_on,
                            "signing_type": lease_driver_document.signing_type,
                        }
                    )
            return result
        except Exception as e:
            logger.error(
                "Error fetching latest driver document status by lease: %s", str(e)
            )
            raise e

    def upsert_lease_drive_document_for_wet_signature(
        self,
        db: Session,
        lease: Lease,
        signature_mode="",
        print_document_details=[],
    ):
        """Upsert lease driver documents"""
        try:
            documents = []
            for document_detail in print_document_details:
                for driver in lease.lease_driver:
                    lease_document = LeaseDriverDocument(
                        lease_driver_id=driver.id,
                        document_id=document_detail["document_id"],
                        document_envelope_id=None,
                        has_frontend_signed=document_detail["has_front_desk_signed"],
                        has_driver_signed=document_detail["has_driver_signed"],
                        frontend_signed_date=datetime.now(timezone.utc),
                        driver_signed_date=datetime.now(timezone.utc),
                        signing_type=signature_mode,
                        created_on=datetime.now(timezone.utc),
                        updated_on=datetime.now(timezone.utc),
                    )
                    db.add(lease_document)
                    db.flush()
                    documents.append(
                        {
                            "driver_id": driver.id,
                            "lease_id": lease.id,
                            "document_envelope_id": lease_document.document_envelope_id,
                            "has_frontend_signed": lease_document.has_frontend_signed,
                            "has_driver_signed": lease_document.has_driver_signed,
                        }
                    )
            return documents
        except Exception as e:
            logger.error(
                "Error upserting lease driver documents: %s", str(e), exc_info=True
            )
            raise e

    def invalidate_lease_driver_documents(self, db: Session, lease: Lease):
        for driver in lease.lease_driver:
            lease_document = (
                db.query(LeaseDriverDocument)
                .filter(
                    LeaseDriverDocument.lease_driver_id == driver.id,
                    LeaseDriverDocument.is_active,
                )
                .first()
            )

            if lease_document:
                logger.info(
                    f"Marking this lease driver's - {lease_document.lease_driver_id} document {lease_document.id} as inactive",
                )
                lease_document.is_active = False
                lease_document.updated_on = datetime.now(timezone.utc)
                db.add(lease_document)
                db.flush()
        logger.info(
            f"All documents for the lease id {lease.lease_id} have been invalidated"
        )

    def upsert_lease_driver_documents(
        self,
        db: Session,
        lease: Lease,
        signature_mode="",
        envelope_ids=[],
        document_types=[],
    ):
        """Upsert lease driver documents"""
        try:
            documents = []
            for driver in lease.lease_driver:
                lease_document = (
                    db.query(LeaseDriverDocument)
                    .filter(
                        LeaseDriverDocument.lease_driver_id == driver.id,
                        LeaseDriverDocument.is_active,
                    )
                    .first()
                )

                latest_docs = (
                    db.query(Document)
                    .filter(
                        Document.object_lookup_id == str(driver.id),
                        Document.object_type == f"co-leasee-{driver.co_lease_seq}",
                        Document.document_type.in_(
                            document_types,
                        ),
                    )
                    .order_by(desc(Document.document_date))
                    .all()
                )

                for id, latest_doc in enumerate(latest_docs[: len(document_types)]):
                    try:
                        envelope_id = envelope_ids[id]
                    except (IndexError, TypeError):
                        envelope_id = ""

                    lease_document = LeaseDriverDocument(
                        lease_driver_id=driver.id,
                        document_envelope_id=envelope_id,
                        has_frontend_signed=False,
                        has_driver_signed=False,
                        frontend_signed_date=None,
                        driver_signed_date=None,
                        signing_type=signature_mode,
                        created_on=datetime.now(timezone.utc),
                        updated_on=datetime.now(timezone.utc),
                    )
                    db.add(lease_document)
                    db.flush()
                    documents.append(
                        {
                            "driver_id": driver.id,
                            "lease_id": lease.id,
                            "document_envelope_id": lease_document.document_envelope_id,
                            "has_frontend_signed": lease_document.has_frontend_signed,
                            "has_driver_signed": lease_document.has_driver_signed,
                        }
                    )
            return documents
        except Exception as e:
            logger.error(
                "Error upserting lease driver documents: %s", str(e), exc_info=True
            )
            raise e

    def fetch_lease_information_for_driver(self, db: Session, driver_id: str = None):
        """Fetch lease information for a driver"""
        try:
            active_lease_drivers_query = (
                db.query(
                    LeaseDriver.id.label("driver_lease_id"),
                    Lease.lease_id,
                    Medallion.medallion_number,
                    Driver.first_name,
                    Driver.last_name,
                    Driver.driver_id,
                    Vehicle.vin,
                    VehicleRegistration.plate_number,
                    Lease.lease_date,
                    Lease.id.label("lease_id_pk"),
                )
                .join(Driver, Driver.driver_id == LeaseDriver.driver_id)
                .join(Lease, Lease.id == LeaseDriver.lease_id)
                .join(Medallion, Medallion.id == Lease.medallion_id)
                .join(Vehicle, Vehicle.id == Lease.vehicle_id)
                .join(VehicleRegistration, VehicleRegistration.vehicle_id == Vehicle.id)
                .filter(
                    Driver.is_active == True,
                    LeaseDriver.is_active == True,
                    VehicleRegistration.is_active == True,
                )
            )

            if driver_id:
                active_lease_drivers_query = active_lease_drivers_query.filter(
                    LeaseDriver.driver_id == driver_id
                )

            active_lease_drivers = active_lease_drivers_query.all()
            lease_vehicle_info = []
            for lease_driver in active_lease_drivers:
                driver_lease_document = (
                    db.query(LeaseDriverDocument)
                    .join(
                        LeaseDriver,
                        LeaseDriver.id == LeaseDriverDocument.lease_driver_id,
                    )
                    .filter(
                        LeaseDriverDocument.is_active == True,
                        LeaseDriver.driver_id == lease_driver.driver_id,
                    )
                    .first()
                )
                if not lease_driver.lease_id:
                    continue
                lease_vehicle_info.append(
                    {
                        "driver_lease_id": lease_driver.driver_lease_id,
                        "lease_id": lease_driver.lease_id,
                        "medallion_number": lease_driver.medallion_number,
                        "driver_name": f"{lease_driver.first_name} {lease_driver.last_name}",
                        "vin_number": lease_driver.vin,
                        "vehicle_plate_number": lease_driver.plate_number,
                        "lease_date": lease_driver.lease_date,
                        "lease_id_pk": lease_driver.lease_id_pk,
                        "is_manager": True if driver_lease_document else False,
                    }
                )
            return lease_vehicle_info
        except Exception as e:
            logger.error(
                "Error fetching lease information for driver: %s", e, exc_info=True
            )
            raise e

    def fetch_lease_payment_configuration(
        self,
        db: Session,
        config_type: Optional[str] = None,
        multiple: Optional[bool] = False,
    ) -> Union[LeasePaymentConfiguration, List[LeasePaymentConfiguration], None]:
        """Fetch lease payment configuration"""
        try:
            query = db.query(LeasePaymentConfiguration)
            if config_type:
                config_types = config_type.split(",")
                query = query.filter(
                    LeasePaymentConfiguration.config_type.in_(config_types)
                )

            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error(
                "Error fetching lease payment configuration: %s", e, exc_info=True
            )
            raise e

    # --- LEASE PRESET CRUD METHODS ---

    def get_lease_preset(self, db: Session, preset_id: int) -> Optional[LeasePreset]:
        """Gets a single lease preset by its ID."""
        stmt = select(LeasePreset).where(LeasePreset.id == preset_id)
        return db.execute(stmt).scalar_one_or_none()

    def list_lease_presets(
        self,
        db: Session,
        page: int,
        per_page: int,
        sort_by: str,
        sort_order: str,
        lease_type: Optional[str] = None,
        vehicle_make: Optional[str] = None,
        vehicle_model: Optional[str] = None,
        vehicle_year: Optional[int] = None,
    ) -> Tuple[List[LeasePreset], int]:
        """Lists lease presets with filtering, sorting, and pagination."""
        stmt = select(LeasePreset)

        # Apply filters
        if lease_type:
            stmt = stmt.where(LeasePreset.lease_type.ilike(f"%{lease_type}%"))
        if vehicle_make:
            stmt = stmt.where(LeasePreset.vehicle_make.ilike(f"%{vehicle_make}%"))
        if vehicle_model:
            stmt = stmt.where(LeasePreset.vehicle_model.ilike(f"%{vehicle_model}%"))
        if vehicle_year:
            stmt = stmt.where(LeasePreset.vehicle_year == vehicle_year)

        # Get total count before pagination
        count_stmt = select(func.count()).select_from(stmt.subquery())
        total_items = db.execute(count_stmt).scalar()

        # Apply sorting
        if hasattr(LeasePreset, sort_by):
            sort_column = getattr(LeasePreset, sort_by)
            stmt = stmt.order_by(
                sort_column.desc() if sort_order == "desc" else sort_column.asc()
            )

        # Apply pagination
        stmt = stmt.offset((page - 1) * per_page).limit(per_page)

        results = db.execute(stmt).scalars().all()
        return results, total_items

    def create_lease_preset(
        self, db: Session, preset_data: LeasePresetCreate
    ) -> LeasePreset:
        """Creates a new lease preset record."""
        new_preset = LeasePreset(**preset_data.model_dump())
        db.add(new_preset)
        db.flush()
        db.refresh(new_preset)
        return new_preset

    def update_lease_preset(
        self, db: Session, preset_id: int, preset_data: LeasePresetUpdate
    ) -> LeasePreset:
        """Updates an existing lease preset record."""
        preset = self.get_lease_preset(db, preset_id)
        if not preset:
            raise ValueError(f"LeasePreset with id {preset_id} not found.")

        update_data = preset_data.model_dump(exclude_unset=True)
        for key, value in update_data.items():
            setattr(preset, key, value)

        db.flush()
        db.refresh(preset)
        return preset

    def delete_lease_preset(self, db: Session, preset_id: int) -> bool:
        """Deletes a lease preset record."""
        preset = self.get_lease_preset(db, preset_id)
        if not preset:
            raise ValueError(f"LeasePreset with id {preset_id} not found.")

        db.delete(preset)
        db.flush()
        return True

    def _coerce_to_date(self, dt_str: Optional[str]) -> date:
        if not dt_str:
            return date.today()
        try:
            if isinstance(dt_str, datetime):
                return dt_str.date()
            s = str(dt_str)
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            return datetime.fromisoformat(s).date()
        except Exception:
            return date.today()

    def mark_bat_manager_as_signed(self, db: Session, lease: Lease):
        for lease_driver in lease.lease_driver:
            for driver_doc in lease_driver.documents:
                driver_doc.has_frontend_signed = True
                driver_doc.frontend_signed_date = date.today()
                logger.info(
                    f"Marking lease document id-{driver_doc.id}, for envelope {driver_doc.document_envelope_id} for driver {lease_driver.driver_id} as signed by manager"
                )

        db.add(lease)
        db.flush()

    def update_lease_driver_document_signoff_latest(
        self,
        db: Session,
        *,
        ctx: dict,
        signed_on: Optional[date] = None,
    ) -> dict:
        """
        Update the latest LeaseDriverDocument for ctx['envelope_id']:
        - recipient_id '1' => set driver signoff + date (has_driver_signed)
        - recipient_id '2' => set front-end signoff + date (has_frontend_signed)

        Note: For additional driver agreements with two signers:
        - recipient_id '1' = Additional Driver → has_driver_signed
        - recipient_id '2' = Primary Driver (Lessee) → has_frontend_signed

        The field name 'has_frontend_signed' is used for the primary driver in this context,
        though the field name may be semantically misleading.

        Update-only, picks ONE latest row (by id desc). Calls db.flush(); no commit/rollback.
        """
        envelope_id = (ctx or {}).get("envelope_id")
        if not envelope_id:
            raise ValueError(
                "update_lease_driver_document_signoff_latest: missing ctx['envelope_id']"
            )

        recipient_id_raw = (ctx or {}).get("recipient_id")
        if recipient_id_raw is None:
            raise ValueError(
                "update_lease_driver_document_signoff_latest: missing ctx['recipient_id']"
            )

        recipient_id = str(recipient_id_raw).strip()
        when = signed_on or self._coerce_to_date((ctx or {}).get("generated_at"))

        row = (
            db.query(LeaseDriverDocument)
            .filter(LeaseDriverDocument.document_envelope_id == envelope_id)
            .order_by(LeaseDriverDocument.id.desc())
            .first()
        )

        if not row:
            return {
                "ok": False,
                "reason": "no LeaseDriverDocument found for envelope",
                "envelope_id": envelope_id,
                "updated": False,
            }

        updated = False
        if recipient_id == "1":
            # Driver signoff
            if row.has_driver_signed is not True:
                row.has_driver_signed = True
                updated = True
            if not row.driver_signed_date:
                row.driver_signed_date = when
                updated = True
        elif recipient_id == "2":
            # Front-end signoff
            if row.has_frontend_signed is not True:
                row.has_frontend_signed = True
                updated = True
            if not row.frontend_signed_date:
                row.frontend_signed_date = when
                updated = True
        else:
            return {
                "ok": False,
                "reason": f"recipient_id '{recipient_id}' not mapped (expect '1' for driver, '2' for front-end)",
                "envelope_id": envelope_id,
                "updated": False,
            }

        # Push changes without ending the transaction
        db.flush()

        return {
            "ok": True,
            "envelope_id": envelope_id,
            "recipient_id": recipient_id,
            "lease_driver_document_id": row.id,
            "updated": updated,
            "signed_on": when.isoformat(),
        }

    def create_or_update_lease_schedule(
        self,
        db: Session,
        lease: Lease,
        vehicle_weekly_amount: float = 0.0,
        medallion_weekly_amount: float = 0.0,
        override_start_date: Optional[date] = None,
    ) -> List[LeaseSchedule]:
        """
        Create or update lease schedule entries for a lease.
        Marks existing active schedule records as inactive and creates new ones.

        Args:
            db: Database session
            lease: Lease object
            vehicle_weekly_amount: Weekly vehicle lease amount
            medallion_weekly_amount: Weekly medallion lease amount
            override_start_date: Optional override for the start date (used for renewals to start from last_renewed_date)
        """
        try:
            from datetime import datetime

            from app.leases.utils import calculate_weekly_lease_schedule

            # Mark existing active schedule records as inactive instead of deleting
            existing_schedules = (
                db.query(LeaseSchedule)
                .filter(
                    LeaseSchedule.lease_id == lease.id, LeaseSchedule.is_active == True
                )
                .all()
            )

            for schedule in existing_schedules:
                schedule.is_active = False
                schedule.updated_on = datetime.now(timezone.utc)

            db.flush()


            logger.info(
                "Weekly amounts -> Vehicle: %s (%s), Medallion: %s (%s)",
                vehicle_weekly_amount,
                type(vehicle_weekly_amount),
                medallion_weekly_amount,
                type(medallion_weekly_amount),
            )

            vehicle_weekly_amount = float(vehicle_weekly_amount or 0)
            medallion_weekly_amount = float(medallion_weekly_amount or 0)
            total_weekly = vehicle_weekly_amount + medallion_weekly_amount

            if total_weekly <= 0:
                logger.warning(
                    f"Total weekly amount is 0 for lease {lease.id}, skipping schedule creation"
                )
                return []

            # Generate schedule using existing utility function
            from app.core.config import settings

            # Use override_start_date for renewals, otherwise use lease_start_date
            schedule_start_date = (
                override_start_date if override_start_date else lease.lease_start_date
            )

            schedule_data = calculate_weekly_lease_schedule(
                lease_start_date=schedule_start_date,
                duration_weeks=lease.duration_in_weeks or 0,
                payment_due_day=lease.lease_pay_day or settings.payment_date,
                weekly_lease_amount=total_weekly,
                lease_end_date=lease.lease_end_date,
            )

            # Create LeaseSchedule entries
            lease_schedules = []
            for entry in schedule_data:
                # Parse the due date from the formatted string
                due_date_str = entry.get("due_date", "")
                try:
                    due_date = datetime.strptime(due_date_str, "%a, %B %d, %Y").date()
                except Exception as e:
                    logger.error(f"Error parsing due date '{due_date_str}': {e}")
                    continue

                # Parse period start and end dates
                period_start_str = entry.get("period_start", "")
                period_end_str = entry.get("period_end", "")
                try:
                    period_start = (
                        datetime.strptime(period_start_str, "%a, %B %d, %Y").date()
                        if period_start_str
                        else None
                    )
                    period_end = (
                        datetime.strptime(period_end_str, "%a, %B %d, %Y").date()
                        if period_end_str
                        else None
                    )
                except Exception as e:
                    logger.error(f"Error parsing period dates: {e}")
                    period_start = None
                    period_end = None

                # Parse amount from string (format: "$ 1,234.56")
                amount_str = entry.get("amount_due", "$ 0.00")
                try:
                    amount = float(amount_str.replace("$", "").replace(",", "").strip())
                except Exception as e:
                    logger.error(f"Error parsing amount '{amount_str}': {e}")
                    amount = 0.0

                # Calculate prorated amounts if needed
                if entry.get("is_prorated", False):
                    # For prorated periods, maintain the same proportion between vehicle and medallion
                    # The amount is already prorated based on days, so split it proportionally
                    if total_weekly > 0:
                        vehicle_proportion = vehicle_weekly_amount / total_weekly
                        medallion_proportion = medallion_weekly_amount / total_weekly
                        prorated_vehicle = amount * vehicle_proportion
                        prorated_medallion = amount * medallion_proportion
                    else:
                        prorated_vehicle = 0.0
                        prorated_medallion = 0.0
                else:
                    prorated_vehicle = vehicle_weekly_amount
                    prorated_medallion = medallion_weekly_amount

                schedule_entry = LeaseSchedule(
                    lease_id=lease.id,
                    installment_number=entry.get("installment_no"),
                    installment_due_date=due_date,
                    installment_amount=amount,
                    period_start_date=period_start,
                    period_end_date=period_end,
                    medallion_installment_amount=round(prorated_medallion, 2),
                    vehicle_installment_amount=round(prorated_vehicle, 2),
                    installment_status="D",  # D = Due
                    is_active=True,  # Mark new schedule entries as active
                    created_on=datetime.now(timezone.utc),
                    updated_on=datetime.now(timezone.utc),
                )
                db.add(schedule_entry)
                lease_schedules.append(schedule_entry)

            db.flush()
            logger.info(
                f"Created {len(lease_schedules)} schedule entries for lease {lease.id}"
            )
            return lease_schedules

        except Exception as e:
            logger.error(
                f"Error creating lease schedule for lease {lease.id}: {str(e)}",
                exc_info=True,
            )
            raise e

    def get_lease_schedule(
        self, db: Session, lease_id: int, multiple: bool = True, is_active: bool = True
    ) -> Union[LeaseSchedule, List[LeaseSchedule], None]:
        """Get lease schedule entries for a lease

        Args:
            db: Database session
            lease_id: The lease ID to filter by
            multiple: Whether to return multiple records or just one
            is_active: Filter by is_active status (default True to return only active schedules)
        """
        try:
            query = db.query(LeaseSchedule).filter(LeaseSchedule.lease_id == lease_id)

            # Filter by is_active status
            if is_active is not None:
                query = query.filter(LeaseSchedule.is_active == is_active)

            query = query.order_by(asc(LeaseSchedule.installment_number))

            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error(f"Error getting lease schedule: {str(e)}", exc_info=True)
            raise e
        
    def get_active_leases_with_drivers(self, db: Session) -> List[Lease]:
        """
        Efficiently fetch all active leases with their related entities loaded.
        This method is optimized for CURB import to avoid N+1 queries.
        
        Returns:
            List of Lease objects with drivers, medallions, and vehicles eagerly loaded
        """
        try:
            query = (
                db.query(Lease)
                .filter(Lease.lease_status == "Active")
                .options(
                    # Eagerly load all required relationships
                    joinedload(Lease.lease_driver).joinedload(LeaseDriver.driver).joinedload(Driver.tlc_license),
                    joinedload(Lease.medallion),
                    joinedload(Lease.vehicle)
                )
            )
            
            active_leases = query.all()
            
            logger.info(f"Retrieved {len(active_leases)} active leases with drivers for CURB import")
            return active_leases
            
        except Exception as e:
            logger.error(f"Error fetching active leases with drivers: {e}", exc_info=True)
            raise

    def get_leases_by_status_with_relationships(
        self, 
        db: Session, 
        status: str = "Active",
        include_drivers: bool = True,
        include_medallion: bool = True,
        include_vehicle: bool = True
    ) -> List[Lease]:
        """
        Generic method to fetch leases by status with configurable relationship loading.
        
        Args:
            db: Database session
            status: Lease status to filter by (default: "Active")
            include_drivers: Whether to load driver relationships
            include_medallion: Whether to load medallion relationship
            include_vehicle: Whether to load vehicle relationship
            
        Returns:
            List of Lease objects with requested relationships loaded
        """
        try:
            query = db.query(Lease).filter(Lease.lease_status == status)
            
            # Build joinedload options based on parameters
            options = []
            
            if include_drivers:
                options.append(
                    joinedload(Lease.lease_driver)
                    .joinedload(LeaseDriver.driver)
                    .joinedload(Driver.tlc_license)
                )
            
            if include_medallion:
                options.append(joinedload(Lease.medallion))
            
            if include_vehicle:
                options.append(joinedload(Lease.vehicle))
            
            if options:
                query = query.options(*options)
            
            leases = query.all()
            
            logger.info(f"Retrieved {len(leases)} leases with status '{status}'")
            return leases
            
        except Exception as e:
            logger.error(f"Error fetching leases by status with relationships: {e}", exc_info=True)
            raise

    def get_current_lease_amount(self, db: Session, lease_id: int) -> Optional[Decimal]:
        """
        Get the current or next scheduled lease amount from lease_schedule.
        This is the authoritative source for lease amounts as it accounts for proration.
        
        Args:
            db: Database session
            lease_id: Lease ID
            
        Returns:
            Decimal amount or None if no schedule exists
        """
        from datetime import datetime
        from app.leases.models import LeaseSchedule
        from sqlalchemy import and_
        
        today = datetime.now().date()
        
        # Try current period
        current_installment = db.query(LeaseSchedule).filter(
            and_(
                LeaseSchedule.lease_id == lease_id,
                LeaseSchedule.period_start_date <= today,
                LeaseSchedule.period_end_date >= today,
                LeaseSchedule.installment_status.in_(['Scheduled', 'Posted'])
            )
        ).first()
        
        # If no current, get next upcoming
        if not current_installment:
            current_installment = db.query(LeaseSchedule).filter(
                and_(
                    LeaseSchedule.lease_id == lease_id,
                    LeaseSchedule.installment_due_date >= today,
                    LeaseSchedule.installment_status == 'Scheduled'
                )
            ).order_by(LeaseSchedule.installment_due_date.asc()).first()
        
        # Fallback to most recent
        if not current_installment:
            current_installment = db.query(LeaseSchedule).filter(
                LeaseSchedule.lease_id == lease_id
            ).order_by(LeaseSchedule.installment_due_date.desc()).first()
        
        return current_installment.installment_amount if current_installment else None


lease_service = LeaseService()
