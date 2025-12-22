### app/medallions/search_service.py

# Standard library imports
from typing import List, Optional, Tuple
from datetime import date , timedelta , datetime

# Third party imports
from sqlalchemy import (
    func, or_, desc, asc , exists, not_ , and_ , case
)
from sqlalchemy.orm import Session, joinedload

# Local imports
from app.utils.logger import get_logger
from app.medallions.models import (
    Medallion, MedallionOwner, MOLease,
    MedallionStorage
)
from app.medallions.schemas import MedallionStatus, MedallionOwnerType , MedallionType
from app.medallions.utils import format_medallion_response
from app.entities.models import Individual, Corporation
from app.vehicles.models import (
    VehicleHackUp, Vehicle, VehicleRegistration
)
from app.vehicles.schemas import VehicleStatus
from app.leases.models import Lease, LeaseDriver
from app.drivers.models import Driver
from app.drivers.utils import format_driver_response
from app.vehicles.utils import format_vehicle_response
from app.audit_trail.services import audit_trail_service
from app.uploads.services import upload_service

logger = get_logger(__name__)

class MedallionSearchService:
    """Service for searching medallions"""

    def medallion_lease_report(self, db: Session, page: int, per_page: int,
                          days_in_advance: int = 30,
                          medallion_number: Optional[str] = None,
                          medallion_status: Optional[str] = None,
                          medallion_type: Optional[str] = None,
                          medallion_owner: Optional[str] = None,
                          lease_expiry_from: Optional[date] = None,
                          lease_expiry_to: Optional[date] = None,
                          sort_by: Optional[str] = "created_on",
                          sort_order: Optional[str] = "asc") -> Tuple[List[Medallion], int]:
        """Get medallion expiry date"""
        try:
            check_date = datetime.today() + timedelta(days=days_in_advance)

            query = db.query(Medallion).options(
                joinedload(Medallion.mo_lease),
                joinedload(Medallion.vehicle),
                joinedload(Medallion.owner).joinedload(MedallionOwner.individual),
                joinedload(Medallion.owner).joinedload(MedallionOwner.corporation),
            )
            query = query.filter(Medallion.mo_lease.has(
                and_(
                    MOLease.contract_end_date <= check_date.date(),
                    MOLease.contract_end_date >= datetime.today().date()
                )
            ))

            if medallion_number:
                numbers = [n.strip() for n in medallion_number.split(",") if n.strip()]
                query = query.filter(or_(
                    *[Medallion.medallion_number.ilike(f"%{n}%") for n in numbers]
                ))
            
            if medallion_status:
                try:
                    status_value = MedallionStatus[medallion_status.upper()].value
                    query = query.filter(Medallion.medallion_status == status_value)
                except KeyError as e:
                    raise e
                    
            if medallion_type:
                query = query.filter(Medallion.medallion_type == medallion_type)

            if medallion_owner:
                query = query.filter(Medallion.owner.has(
                    or_(
                        MedallionOwner.individual.has(Individual.full_name.ilike(f"%{medallion_owner}%")),
                        MedallionOwner.corporation.has(Corporation.name.ilike(f"%{medallion_owner}%"))
                    )
                ))

            if lease_expiry_from:
                query = query.join(MOLease).filter(MOLease.contract_end_date >= lease_expiry_from)
            if lease_expiry_to:
                query = query.join(MOLease).filter(MOLease.contract_end_date <= lease_expiry_to)

            if sort_by:
                sort_attr = {
                    "medallion_number": Medallion.medallion_number,
                    "medallion_status": Medallion.medallion_status,
                    "medallion_type": Medallion.medallion_type,
                    "contract_end_date": MOLease.contract_end_date,
                    "created_on": Medallion.created_on
                }.get(sort_by)

                if sort_attr:
                    query = query.order_by(desc(sort_attr) if sort_order == "desc" else asc(sort_attr))
            else:
                query = query.order_by(desc(MOLease.contract_end_date))

            total_count = query.count()
            medallions = query.offset((page - 1) * per_page).limit(per_page).distinct().all()

            return self._format_medallion_expiry_response(db, medallions, days_in_advance, check_date, total_count)

        except Exception as e:
            logger.error("Error searching medallions: %s", e)
            raise

    def _format_medallion_expiry_response(self, db: Session, medallions: List[Medallion],
                                      days_in_advance: int, check_date: datetime,
                                    total_count: int) -> dict:
        response = []
        value_to_key = {member.value: member.name for member in MedallionStatus}

        for medallion in medallions:
            owner = medallion.owner
            owner_name = None
            if owner:
                if owner.medallion_owner_type == MedallionOwnerType.INDIVIDUAL:
                    owner_name = owner.individual.full_name
                elif owner.medallion_owner_type == MedallionOwnerType.CORPORATION:
                    owner_name = owner.corporation.name

            response.append({
                "medallion_number": medallion.medallion_number,
                "renewal_date": medallion.medallion_renewal_date,
                "contract_start_date": medallion.mo_lease.contract_start_date if medallion.mo_lease else None,
                "contract_end_date": medallion.mo_lease.contract_end_date if medallion.mo_lease else None,
                "medallion_owner": owner_name,
                "medallion_status": value_to_key.get(medallion.medallion_status),
                "medallion_type": medallion.medallion_type
            })

        types = db.query(Medallion.medallion_type).distinct().all()

        return {
            "days_in_advance": days_in_advance,
            "check_date": check_date,
            "medallions": response,
            "statuses": [status.name for status in MedallionStatus if status != MedallionStatus.IN_PROGRESS],
            "medallion_type_list": [type.medallion_type for type in types],
            "filters": {
                "medallion_status": {
                    "type": "select",
                    "label": "Medallion Status",
                    "placeholder": "Select Medallion Status",
                    "options": [{"label": status.name, "value": status.value}
                                for status in MedallionStatus if status != MedallionStatus.IN_PROGRESS]
                },
                "medallion_type": {
                    "type": "select",
                    "label": "Medallion Type",
                    "placeholder": "Select Medallion Type",
                    "options": [{"label": type.medallion_type, "value": type.medallion_type} for type in types]
                },
                "medallion_owner": {
                    "type": "text",
                    "label": "Medallion Owner",
                    "placeholder": "Select Medallion Owner",
                },
                "medallion_number": {
                    "type": "text",
                    "label": "Medallion Number",
                    "placeholder": "Enter Medallion Number"
                },
                "lease_expiry_date": {
                    "type": "date",
                    "label": "Lease Expiry Date",
                    "placeholder": "Select Lease Expiry Date"
                }
            },
            "total_count": total_count
        }

    def search_medallions(
        self, db: Session, page: int, per_page: int,
        medallion_list_days: Optional[int] = None,
        medallion_created_from: Optional[date] = None,
        medallion_created_to: Optional[date] = None,
        medallion_number: Optional[str] = None,
        medallion_status: Optional[str] = None,
        medallion_type: Optional[str] = None,
        medallion_owner: Optional[str] = None,
        renewal_date_from: Optional[date] = None,
        renewal_date_to: Optional[date] = None,
        validity_end_date_from: Optional[date] = None,
        validity_end_date_to: Optional[date] = None,
        lease_expiry_from: Optional[date] = None,
        lease_expiry_to: Optional[date] = None,
        has_vehicle: Optional[bool] = None,
        in_storage: Optional[bool] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None
    ) -> Tuple[List[Medallion], int]:
        """Search for medallions"""
        try:
            query = db.query(Medallion).filter(
                Medallion.medallion_status != MedallionStatus.IN_PROGRESS,
                Medallion.medallion_status != MedallionStatus.ARCHIVED,
                Medallion.is_active == True
            )

            is_mo_lease_joined = False
            is_vehicle_joined = False
            is_owner_joined = False
            is_individual_joined = False
            is_corporation_joined = False

            # Filters
            if medallion_number:
                numbers = [n.strip() for n in medallion_number.split(",") if n.strip()]
                query = query.filter(or_(
                    *[Medallion.medallion_number.ilike(f"%{n}%") for n in numbers]
                ))
            
            if medallion_status:
                try:
                    status_value = MedallionStatus[medallion_status.upper()].value
                    query = query.filter(Medallion.medallion_status == status_value)
                except KeyError as e:
                    valid_statuses = [status.name for status in MedallionStatus]
                    raise e
            
            if medallion_type:
                query = query.filter(Medallion.medallion_type == medallion_type)
            
            if medallion_owner:
                if not is_owner_joined:
                    query = query.outerjoin(Medallion.owner)
                    is_owner_joined = True
                if not is_individual_joined:
                    query = query.outerjoin(Individual, MedallionOwner.individual)
                    is_individual_joined = True
                if not is_corporation_joined:
                    query = query.outerjoin(Corporation, MedallionOwner.corporation)
                    is_corporation_joined = True
                query = query.filter(Medallion.owner.has(
                    or_(
                        MedallionOwner.individual.has(Individual.full_name.ilike(f"%{medallion_owner}%")),
                        MedallionOwner.corporation.has(Corporation.name.ilike(f"%{medallion_owner}%"))
                    )
                ))

            if medallion_list_days:
                date_from = datetime.today() - timedelta(days=medallion_list_days)
                query = query.filter(Medallion.created_on >= date_from)
            
            if medallion_created_from:
                query = query.filter(Medallion.created_on >= medallion_created_from)
            
            if medallion_created_to:
                query = query.filter(Medallion.created_on <= medallion_created_to + timedelta(days=1))

            if renewal_date_from:
                query = query.filter(Medallion.medallion_renewal_date >= renewal_date_from)
            if renewal_date_to:
                query = query.filter(Medallion.medallion_renewal_date <= renewal_date_to)
            if validity_end_date_from:
                query = query.filter(Medallion.validity_end_date >= validity_end_date_from)
            if validity_end_date_to:
                query = query.filter(Medallion.validity_end_date <= validity_end_date_to)
            if lease_expiry_from or lease_expiry_to:
                if not is_mo_lease_joined:
                    query = query.outerjoin(MOLease)
                    is_mo_lease_joined = True
                if lease_expiry_from:
                    query = query.filter(MOLease.contract_end_date >= lease_expiry_from)
                if lease_expiry_to:
                    query = query.filter(MOLease.contract_end_date <= lease_expiry_to + timedelta(days=1))

            if has_vehicle is not None:
                if not is_vehicle_joined:
                    query = query.outerjoin(Medallion.vehicle)
                    is_vehicle_joined = True
                query = query.filter(Medallion.vehicle.any() if has_vehicle else ~Medallion.vehicle.any())

            if in_storage is not None:
                storage_exits = exists().where(MedallionStorage.medallion_number == Medallion.medallion_number)
                query = query.filter(storage_exits if in_storage else not_(storage_exits))


            # Sorting based on sort_fields dictionary of filed and order
            if sort_by and sort_order:
                if sort_by == "medallion_number":
                    query = query.order_by(desc(Medallion.medallion_number) if sort_order == "desc" else asc(Medallion.medallion_number))
                elif sort_by == "medallion_status":
                    status_order = case(
                        (Medallion.medallion_status == 'Y', 'ACTIVE'),
                        (Medallion.medallion_status == 'N', 'ARCHIVED'),
                        (Medallion.medallion_status == 'V', 'ASSIGNED_TO_VEHICLE'),
                        (Medallion.medallion_status == 'A', 'AVAILABLE'),
                        (Medallion.medallion_status == 'I', 'IN_PROGRESS')
                    )
                    query = query.order_by(desc(status_order) if sort_order == "desc" else asc(status_order))
                elif sort_by == "medallion_type":
                    query = query.order_by(desc(Medallion.medallion_type) if sort_order == "desc" else asc(Medallion.medallion_type))
                elif sort_by == "validity_end_date":
                    query = query.order_by(desc(Medallion.validity_end_date) if sort_order == "desc" else asc(Medallion.validity_end_date))
                elif sort_by == "medallion_owner":
                    if not is_owner_joined:
                        query = query.outerjoin(Medallion.owner)
                        is_owner_joined = True
                    if not is_individual_joined:
                        query = query.outerjoin(Individual, MedallionOwner.individual)
                        is_individual_joined = True
                    if not is_corporation_joined:
                        query = query.outerjoin(Corporation, MedallionOwner.corporation)
                        is_corporation_joined = True

                    owner_name = func.coalesce(Individual.full_name, Corporation.name)
                    query = query.order_by(desc(owner_name) if sort_order == "desc" else asc(owner_name))
                elif sort_by == "renewal_date":
                    query = query.order_by(desc(Medallion.medallion_renewal_date) if sort_order == "desc" else asc(Medallion.medallion_renewal_date))
                elif sort_by == "lease_expiry":
                    if not is_mo_lease_joined:
                        query = query.outerjoin(MOLease)
                        is_mo_lease_joined = True
                    query = query.order_by(desc(MOLease.contract_end_date) if sort_order == "desc" else asc(MOLease.contract_end_date))
                elif sort_by == "created_on":
                    query = query.order_by(desc(Medallion.created_on) if sort_order == "desc" else asc(Medallion.created_on))
            else:
                query = query.order_by(desc(Medallion.updated_on),desc(Medallion.created_on))

            # Pagination
            total_count = query.count()

            query = query.offset((page - 1) * per_page).limit(per_page)
            medallions = query.all()

            response = []
            for medallion in medallions:
                lease = medallion.mo_lease
                vehicle = medallion.vehicle if medallion.vehicle else None
                owner = medallion.owner

                owner_name = None
                if owner:
                    if owner.medallion_owner_type == MedallionOwnerType.INDIVIDUAL:
                        owner_name = owner.individual.full_name
                    elif owner.medallion_owner_type == MedallionOwnerType.CORPORATION:
                        owner_name = owner.corporation.name

                audit_trails = audit_trail_service.get_related_audit_trail(db, medallion_id=medallion.id)

                documents = upload_service.get_documents(db, object_type="medallion", object_id=medallion.id, multiple=True)

                vehicle_hack = db.query(VehicleHackUp).filter(
                    VehicleHackUp.vehicle_id == vehicle[0].id
                ).first() if vehicle and len(vehicle) > 0 else False

                in_storage = db.query(MedallionStorage).filter(
                    MedallionStorage.medallion_number == medallion.medallion_number
                ).order_by(
                    desc(MedallionStorage.created_on)
                ).first()

                value_to_key = {member.value: member.name for member in MedallionStatus}

                response.append({
                    "medallion_id": medallion.id,
                    "medallion_number": medallion.medallion_number if medallion.medallion_number else "",
                    "renewal_date": medallion.medallion_renewal_date if medallion.medallion_renewal_date else "",
                    "contract_start_date": lease.contract_start_date if lease and lease.contract_start_date else "",
                    "contract_end_date": lease.contract_end_date if lease and lease.contract_end_date else "",
                    "hack_indicator": bool(vehicle_hack),
                    "medallion_owner": owner_name if owner_name else "",
                    "medallion_status": value_to_key[medallion.medallion_status],
                    "medallion_type": medallion.medallion_type if medallion.medallion_type else "",
                    "validity_end_date": medallion.validity_end_date if medallion.validity_end_date else "",
                    "lease_expiry_date": lease.contract_end_date if lease and  lease.contract_end_date else "",
                    "in_storage": True if in_storage and in_storage.retrieval_date is None else False,
                    "does_medallion_have_documents": len(documents) > 0,
                    "vehicle": bool(vehicle),
                    "audit_trial": len(audit_trails) > 0,
                    "created_on": medallion.created_on
                })

            return {
                "medallions": response,
                "statuses": [status.name for status in MedallionStatus if status != MedallionStatus.IN_PROGRESS],
                "medallion_type_list": [type for type in MedallionType],
                "filters": {
                    "medallion_status": {
                        "type": "select",
                        "label": "Medallion Status",
                        "placeholder": "Select Medallion Status",
                        "options": [{"label": status.name, "value": status.value} for status in MedallionStatus if status != MedallionStatus.IN_PROGRESS]
                    },
                    "medallion_type": {
                        "type": "select",
                        "label": "Medallion Type",
                        "placeholder": "Select Medallion Type",
                        "options": [{"label": type, "value": type} for type in MedallionType]
                    },
                    "medallion_owner": {
                        "type": "text",
                        "label": "Medallion Owner",
                        "placeholder": "Select Medallion Owner",
                    },
                    "medallion_number": {
                        "type": "text",
                        "label": "Medallion Number",
                        "placeholder": "Enter Medallion Number"
                    },
                    "lease_expiry_date": {
                        "type": "date",
                        "label": "Lease Expiry Date",
                        "placeholder": "Select Lease Expiry Date"
                    },
                    "renewal_date": {
                        "type": "date",
                        "label": "Renewal Date",
                        "placeholder": "Select Renewal Date"
                    },
                },
                "total_count": total_count
            }
        except Exception as e:
            logger.error("Error searching medallions: %s", e)
            raise e
        
    def get_medallion_details(self, db: Session, medallion_number: str):
        """Get medallion details"""
        try:
            response = {
                "vehicle": {},
                "medallion": {},
                "driver": {}
            }

            medallion = db.query(Medallion).filter(
                Medallion.medallion_number == medallion_number,
                Medallion.is_active == True
            ).options(
                joinedload(Medallion.owner),
                joinedload(Medallion.lease),
                joinedload(Medallion.mo_lease),
                joinedload(Medallion.vehicle).joinedload(Vehicle.vehicle_entity),
                joinedload(Medallion.vehicle).joinedload(Vehicle.dealer),
                joinedload(Medallion.vehicle).joinedload(Vehicle.registrations),
                joinedload(Medallion.vehicle).joinedload(Vehicle.inspections),
                joinedload(Medallion.vehicle).joinedload(Vehicle.hackups)
            ).first()

            if not medallion:
                raise ValueError(f"Medallion not found with number: {medallion_number}")
            
            in_storage = db.query(MedallionStorage).filter(
                MedallionStorage.medallion_number == medallion.medallion_number
            ).first()

            medallion_documents = upload_service.get_documents(db, object_type="medallion", object_id=medallion.id, multiple=True)
            medallion_audit_trail = audit_trail_service.get_related_audit_trail(db, medallion_id=medallion.id)
            response["medallion"]["documents"] = medallion_documents
            response["medallion"]["history"] = medallion_audit_trail
            response["medallion"]["details"] = format_medallion_response(
                medallion, has_documents=len(medallion_documents) > 0, in_storage=bool(in_storage), has_audit_trail=len(medallion_audit_trail) > 0
            )
            
            if medallion.vehicle:
                vehicle = medallion.vehicle[0] if isinstance(medallion.vehicle, list) else medallion.vehicle
                driver_associated = db.query(Driver).join(LeaseDriver).join(Lease).filter(
                        Lease.vehicle_id == vehicle.id
                    ).first() is not None
                vehicle_registration = db.query(VehicleRegistration).filter_by(vehicle_id=vehicle.id, status="active").first()
                vehicle_hackup = db.query(VehicleHackUp).filter(
                    VehicleHackUp.vehicle_id == vehicle.id , VehicleHackUp.status==VehicleStatus.ACTIVE
                ).first()
                vehicle_can_rehack = True
                if vehicle.vehicle_status != VehicleStatus.AVAILABLE or vehicle.medallion_id or not vehicle_hackup:
                    vehicle_can_rehack = False
                vehicle_documents = upload_service.get_documents(db, object_type="vehicle", object_id=vehicle.id, multiple=True)
                vehicle_audit_trail = audit_trail_service.get_related_audit_trail(db, vehicle_id=vehicle.id)
                response["vehicle"] = {
                    "details": format_vehicle_response(
                        vehicle,
                        has_documents=len(vehicle_documents) > 0,
                        has_medallion=bool(vehicle.medallions),
                        is_driver_associated=driver_associated,
                        registration_details=vehicle_registration if vehicle_registration else None,
                        vehicle_hackup=vehicle_hackup if vehicle_hackup else None,
                        vehicle_can_rehack=vehicle_can_rehack,
                        has_audit_trail=len(vehicle_audit_trail) > 0
                    ),
                    "documents": vehicle_documents,
                    "history": vehicle_audit_trail
                }

            if medallion.lease:
                active_driver = None
                lease = medallion.lease[0] if isinstance(medallion.lease, list) else medallion.lease
                if lease.lease_driver:
                    active_driver = lease.lease_driver[0] if isinstance(
                        lease.lease_driver, list) else lease.lease_driver
                    driver = active_driver.driver
                    driver_documents = upload_service.get_documents(db, object_type="driver", object_id=driver.id, multiple=True)
                    driver_audit_trail = audit_trail_service.get_related_audit_trail(db, driver_id=driver.id)
                    response["driver"] = {
                        "details": format_driver_response(
                            driver,
                            has_documents=len(driver_documents) > 0,
                            has_active_lease=True,
                            has_vehicle=bool(medallion.vehicle),
                            has_audit_trail=len(driver_audit_trail) > 0
                        ),
                        "documents": driver_documents,
                        "history": driver_audit_trail
                    }
            return response
        except Exception as e:
            logger.error("Error getting medallion details: %s", e)
            raise e
        

medallion_search_service = MedallionSearchService()
