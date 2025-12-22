### app/medallions/services.py

# Standard library imports
from datetime import datetime
from typing import Optional, Union, List, Tuple , Dict, Any


# Third party imports
from sqlalchemy import or_, desc, asc, func , case
from sqlalchemy.orm import Session, selectinload , aliased
from fastapi import HTTPException, status

# Local imports
from app.utils.logger import get_logger
from app.medallions.models import (
    Medallion, MedallionOwner, MedallionStorage, MOLease,
    MedallionRenewal
)
from app.entities.models import Individual, Corporation
from app.medallions.schemas import MedallionStatus
from app.audit_trail.models import AuditTrail
from app.uploads.models import Document
from app.vehicles.models import VehicleHackUp, Vehicle
from app.medallions.utils import format_medallion_owner_response

logger = get_logger(__name__)


class MedallionService:
    """Service for medallion operations"""
    def get_medallions_by_status(
        self, db: Session,
    ) -> Union[List[Medallion], List[Medallion]]:
        """Get medallions by status"""
        try:
            status_count = db.query(Medallion.medallion_status, func.count(Medallion.id)).group_by(
                Medallion.medallion_status
            ).filter(
                (
                    Medallion.medallion_status.in_([
                    MedallionStatus.ACTIVE,
                    MedallionStatus.AVAILABLE,
                    MedallionStatus.IN_PROGRESS,
                    MedallionStatus.ASSIGNED_TO_VEHICLE
                ])
                ) &( 
                    Medallion.is_active == True
                )
            ).all()

            return dict(status_count)
        except Exception as e:
            logger.error("Error getting medallions by status: %s", str(e))
            raise e
        
    def get_medallion(
            self, db: Session,
            medallion_id: Optional[int] = None,
            medallion_number: Optional[str] = None,
            owner_id: Optional[int] = None,
            multiple: bool = False
    ) -> Union[Medallion, List[Medallion]]:
        """Get a medallion by ID, number, or multiple"""
        try:
            query = db.query(Medallion)
            if medallion_id:
                query = query.filter(Medallion.id == medallion_id)
            if medallion_number:
                query = query.filter(Medallion.medallion_number == medallion_number)
            if owner_id:
                query = query.filter(Medallion.owner_id == owner_id)

            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting medallion: %s", str(e))
            raise e        
    
    def get_medallion_owner(
            self, db: Session,
            medallion_owner_id: Optional[int] = None,
            medallion_number: Optional[str] = None,
            corporation_id : Optional[int] = None,
            individual_id: Optional[int] = None,
            address_id: Optional[int] = None,
            multiple: bool = False,
            sort_order: Optional[str] = "asc"
    ) -> Union[MedallionOwner, List[MedallionOwner]]:
        """Get a medallion owner by ID, or multiple"""
        try:
            query = db.query(MedallionOwner)
            if medallion_owner_id:
                query = query.filter(MedallionOwner.id == medallion_owner_id)
            if address_id:
                query = query.filter(MedallionOwner.primary_address_id == address_id)
            if medallion_number:
                query = query.join(Medallion, Medallion.owner_id == MedallionOwner.id).filter(
                    Medallion.medallion_number == medallion_number
                )
            if corporation_id:
                query = query.filter(MedallionOwner.corporation_id == corporation_id)
            
            if individual_id:
                query = query.filter(MedallionOwner.individual_id == individual_id)
            
            if sort_order:
                query = query.order_by(
                    desc(MedallionOwner.created_on) if sort_order == "desc" else asc(MedallionOwner.created_on)
                )

            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting medallion owner: %s", str(e))
            raise e
        
    def upsert_medallion(
            self, db: Session,
            medallion_data: dict
    ) -> Medallion:
        """Upsert a medallion"""
        try:
            if not medallion_data.get("id"):
                medallion = Medallion(**medallion_data)
                db.add(medallion)
            else:
                medallion = self.get_medallion(db, medallion_id=medallion_data["id"])
                if medallion:
                    for key, value in medallion_data.items():
                        setattr(medallion, key, value)
                else:
                    raise ValueError("Medallion not found")
                
            db.flush()
            
            return medallion
        except Exception as e:
            logger.error("Error upserting medallion: %s", str(e))
            raise e
        
    def search_medallion_owners(
    self,
    db: Session,
    medallion_owner_name: Optional[str] = None,
    ein: Optional[str] = None,
    ssn: Optional[str] = None,
    holding_entity: Optional[str] = None,
    contact_number: Optional[str] = None,
    email: Optional[str] = None,
    owner_type: Optional[str] = None,
    is_holding_entity: Optional[bool] = None,
    page: int = 1, 
    per_page: int = 10,
    sort_by: Optional[str] = None,
    sort_order: Optional[str] = None
    ) -> Dict[str, Any]:
        """Search for medallion owners"""
        try:
            query = db.query(MedallionOwner)

            # Track which joins have been applied
            joined_individual = False
            joined_corporation = False
            joined_parent_corp = False
            ParentCorp = aliased(Corporation)

            if medallion_owner_name:
                names = [name.strip() for name in medallion_owner_name.split(",") if name.strip()]
                if not joined_individual:
                    query = query.outerjoin(Individual, MedallionOwner.individual_id == Individual.id)
                    joined_individual = True
                if not joined_corporation:
                    query = query.outerjoin(Corporation, MedallionOwner.corporation_id == Corporation.id)
                    joined_corporation = True

                query = query.filter(or_(
                    *[Individual.full_name.ilike(f"%{name}%") for name in names],
                    *[Corporation.name.ilike(f"%{name}%") for name in names]
                ))

            if ein:
                eins = [e.strip() for e in ein.split(",") if e.strip()]
                if not joined_corporation:
                    query = query.outerjoin(Corporation, MedallionOwner.corporation_id == Corporation.id)
                    joined_corporation = True
                query = query.filter(or_(
                    *[Corporation.ein.ilike(f"%{e}%") for e in eins]
                ))

            if ssn:
                ssns = [s.strip() for s in ssn.split(",") if s.strip()]
                if not joined_individual:
                    query = query.outerjoin(Individual, MedallionOwner.individual_id == Individual.id)
                    joined_individual = True
                query = query.filter(or_(
                    *[Individual.masked_ssn.ilike(f"%{s}%") for s in ssns]
                ))

            if holding_entity:
                holdings = [h.strip() for h in holding_entity.split(",") if h.strip()]
                if not joined_corporation:
                    query = query.outerjoin(Corporation, MedallionOwner.corporation_id == Corporation.id)
                    joined_corporation = True
                if not joined_parent_corp:
                    joined_parent_corp = True
                    query = query.outerjoin(ParentCorp, Corporation.linked_pad_owner)
                query = query.filter(
                    or_(
                        *[ParentCorp.name.ilike(f"%{h}%") for h in holdings]
                    )
                )

            if contact_number:
                numbers = [n.strip() for n in contact_number.split(",") if n.strip()]
                query = query.filter(or_(
                    *[MedallionOwner.primary_phone.ilike(f"%{n}%") for n in numbers]
                ))

            if email:
                mails = [e.strip() for e in email.split(",") if e.strip()]
                query = query.filter(or_(
                    *[MedallionOwner.primary_email_address.ilike(f"%{e}%") for e in mails]
                ))

            if owner_type:
                query = query.filter(MedallionOwner.medallion_owner_type == owner_type)

            if is_holding_entity is not None:
                if not joined_corporation:
                    query = query.outerjoin(Corporation, MedallionOwner.corporation_id == Corporation.id)
                    joined_corporation = True
                query = query.filter(Corporation.is_holding_entity == is_holding_entity)

            # Sorting
            if sort_by == "created_on":
                sort_expr = asc(MedallionOwner.created_on) if sort_order == "asc" else desc(MedallionOwner.created_on)
                query = query.order_by(sort_expr)
            elif sort_by == "medallion_owner_name":
                if not joined_corporation:
                    query = query.outerjoin(Corporation, MedallionOwner.corporation_id == Corporation.id)
                    joined_corporation = True

                if not joined_individual:
                    query = query.outerjoin(Individual, MedallionOwner.individual_id == Individual.id)
                    joined_individual = True

                owner_name = case(
                    (Corporation.name != None, Corporation.name),
                    else_=Individual.full_name
                )

                query = query.order_by(
                    asc(owner_name) if sort_order == "asc" else desc(owner_name)
                )

            elif sort_by == "ssn":
                if not joined_individual:
                    query = query.outerjoin(Individual, MedallionOwner.individual_id == Individual.id)
                    joined_individual = True
                query = query.order_by(
                    asc(Individual.masked_ssn) if sort_order == "asc" else desc(Individual.masked_ssn)
                )
            elif sort_by == "ein":
                if not joined_corporation:
                    query = query.outerjoin(Corporation, MedallionOwner.corporation_id == Corporation.id)
                    joined_corporation = True
                query = query.order_by(
                    asc(Corporation.ein) if sort_order == "asc" else desc(Corporation.ein)
                )
            elif sort_by == "owner_type":
                query = query.order_by(
                    asc(MedallionOwner.medallion_owner_type) if sort_order == "asc" else desc(MedallionOwner.medallion_owner_type)
                )
            elif sort_by == "contact_number":
                query = query.order_by(
                    asc(MedallionOwner.primary_phone) if sort_order == "asc" else desc(MedallionOwner.primary_phone)
                )
            elif sort_by == "email":
                query = query.order_by(
                    asc(MedallionOwner.primary_email_address) if sort_order == "asc" else desc(MedallionOwner.primary_email_address)
                )
            elif sort_by == "holding_entity":
                if not joined_corporation:
                    query = query.outerjoin(Corporation, MedallionOwner.corporation_id == Corporation.id)
                    joined_corporation = True
                if not joined_parent_corp:
                    joined_parent_corp = True
                    query = query.outerjoin(ParentCorp, Corporation.linked_pad_owner)
                query = query.order_by(
                    asc(ParentCorp.name) if sort_order == "asc" else desc(ParentCorp.name)
                )
            elif sort_by == "is_holding_entity":
                if not joined_corporation:
                    query = query.outerjoin(Corporation, MedallionOwner.corporation_id == Corporation.id)
                    joined_corporation = True
                query = query.order_by(
                    asc(Corporation.is_holding_entity) if sort_order == "asc" else desc(Corporation.is_holding_entity)
                )
            else:
                latest_ts = func.coalesce(MedallionOwner.updated_on, MedallionOwner.created_on)

                query = query.order_by(
                    latest_ts.desc(),
                    MedallionOwner.id.desc()
                )

            total_count = query.count()

            medallion_owners = query.offset((page - 1) * per_page).limit(per_page).all()

            results = [format_medallion_owner_response(db , owner) for owner in medallion_owners]

            return {
                "total_items": total_count,
                "items": results,
                "page": page,
                "per_page": per_page,
                "total_pages": (total_count + per_page - 1) // per_page
            }

        except Exception as e:
            logger.error("Error searching medallion owners: %s", e)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)) from e

        
    def flatten_medallion_owner_records(self , records: list[dict]) -> list[dict]:
        """Flatten medallion owner records for easier processing"""
        flat_data = []
        for record in records:
            flat_record = record.copy()
            # Flatten medallions
            medallion_list = record.get("additional_info", {}).get("medallions", [])
            flat_record["medallion_numbers"] = ", ".join(m.get("medallion_number", "") for m in medallion_list)
            
            # Remove unhashable nested dict
            flat_record.pop("additional_info", None)

            flat_data.append(flat_record)
        return flat_data

        
    def get_medallion_renewal(
            self, db: Session,
            medallion_renewal_id: Optional[int] = None,
            medallion_number: Optional[str] = None,
            multiple: bool = False
    ) -> Union[MedallionRenewal, List[MedallionRenewal]]:
        """Get a medallion renewal by ID, or multiple"""
        try:
            query = db.query(MedallionRenewal)
            if medallion_renewal_id:
                query = query.filter(MedallionRenewal.id == medallion_renewal_id)
            if medallion_number:
                query = query.filter(MedallionRenewal.medallion_number == medallion_number)
            
            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting medallion renewal: %s", str(e))
            raise e
                
    def create_medallion_renewal(
            self, db: Session,
            medallion_renewal_data: dict
    ) -> MedallionRenewal:
        """Create a medallion renewal"""
        try:
            medallion_renewal = MedallionRenewal(**medallion_renewal_data)
            db.add(medallion_renewal)
            db.flush()
            db.refresh(medallion_renewal)
            return medallion_renewal
        except Exception as e:
            logger.error("Error creating medallion renewal: %s", str(e))
            raise e
        
    def get_medallion_storage(
            self, db: Session,
            medallion_storage_id: Optional[int] = None,
            medallion_number: Optional[str] = None,
            sort_by : Optional[str] = "created_on",
            sort_order: Optional[str] = "desc",
            multiple: bool = False,
    ) -> Union[MedallionStorage, List[MedallionStorage]]:
        """Get a medallion storage by ID, or multiple"""
        try:
            query = db.query(MedallionStorage)
            if medallion_storage_id:
                query = query.filter(MedallionStorage.id == medallion_storage_id)
            if medallion_number:
                query = query.filter(MedallionStorage.medallion_number == medallion_number).order_by(desc(MedallionStorage.created_on))
            if sort_by and sort_order:
                query = query.order_by(desc(getattr(MedallionStorage, sort_by)) if sort_order == "desc" else asc(getattr(MedallionStorage, sort_by)))      
                
            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting medallion storage: %s", str(e))
            raise e
        
    def upsert_medallion_storage(
            self, db: Session,
            medallion_storage_data: dict
    ) -> MedallionStorage:
        """Upsert a medallion storage"""
        try:
            if not medallion_storage_data.get("id"):
                medallion_storage = MedallionStorage(**medallion_storage_data)
                db.add(medallion_storage)
                db.flush()
                db.refresh(medallion_storage)
            else:
                medallion_storage = self.get_medallion_storage(db, medallion_storage_id=medallion_storage_data["id"])
                if medallion_storage:
                    for key, value in medallion_storage_data.items():
                        setattr(medallion_storage, key, value)
                    db.flush()
                    db.refresh(medallion_storage)
                else:
                    raise ValueError("Medallion storage not found")
            return medallion_storage
        except Exception as e:
            logger.error("Error upserting medallion storage: %s", str(e))
            raise e
        
    def get_mo_lease(
        self, db: Session,
        mo_lease_id: int,
        multiple: Optional[bool] = False
    ) -> Union[MOLease, List[MOLease]]:
        """Get a medallion lease by ID, or multiple"""
        try:
            query = db.query(MOLease)
            if mo_lease_id:
                query = query.filter(MOLease.id == mo_lease_id)
                
            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting medallion lease: %s", str(e))
            raise e
        
    def upsert_mo_lease(
        self, db: Session,
        mo_lease_data: dict
    ) -> MOLease:
        """Upsert a medallion lease"""
        try:
            if not mo_lease_data.get("id"):
                mo_lease = MOLease(**mo_lease_data)
                db.add(mo_lease)
                db.flush()
                db.refresh(mo_lease)
            else:
                mo_lease = self.get_mo_lease(db, mo_lease_id=mo_lease_data["id"])
                if mo_lease:
                    for key, value in mo_lease_data.items():
                        setattr(mo_lease, key, value)
                    db.flush()
                    db.refresh(mo_lease)
                else:
                    raise ValueError("Medallion storage not found")
            return mo_lease
        except Exception as e:
            logger.error("Error upserting medallion storage: %s", str(e))
            raise e
        
    def upsert_medallion_owner(
        self, db: Session,
        medallion_owner_data: dict
    ) -> MedallionOwner:
        """Upsert a medallion owner"""
        try:
            if not medallion_owner_data.get("id"):
                medallion_owner = MedallionOwner(**medallion_owner_data)
                db.add(medallion_owner)
                db.flush()
                db.refresh(medallion_owner)
            else:
                medallion_owner = self.get_medallion_owner(db, medallion_owner_id=medallion_owner_data["id"])
                if medallion_owner:
                    for key, value in medallion_owner_data.items():
                        setattr(medallion_owner, key, value)
                    db.flush()
                    db.refresh(medallion_owner)
                else:
                    raise ValueError("Medallion owner not found")
            return medallion_owner
        except Exception as e:
            logger.error("Error upserting medallion owner: %s", str(e))
            raise e


medallion_service = MedallionService()
