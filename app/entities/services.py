### app/entities/services.py

# Standard library imports
from typing import Optional, List, Union

# Third party imports
from sqlalchemy.orm import Session

# Local imports
from app.entities.models import (
    Entity, Address, BankAccount,
    Individual, Corporation , CorporationPayee , CorporationOwners
)
from app.medallions.models import Medallion, MedallionOwner
from app.utils.logger import get_logger
import re
from fastapi import HTTPException, status

logger = get_logger(__name__)


class EntityService:
    """Service for managing entities"""
    def get_address(
            self, db: Session,
            address_id: Optional[int] = None,
            address_line_1: Optional[str] = None,
            medallion_number: Optional[str] = None,
            multiple: bool = False
    ) -> Union[Address, List[Address], None]:
        """Get address"""
        try:
            query = db.query(Address)
            if address_id:
                query = query.filter(Address.id == address_id)
            if address_line_1:
                query = query.filter(Address.address_line_1 == address_line_1)
            if medallion_number:
                query = query.join(
                    MedallionOwner, Address.id == MedallionOwner.primary_address_id
                ).join(
                    Medallion, Medallion.owner_id == MedallionOwner.id
                ).filter(
                    Medallion.medallion_number == medallion_number
                )

            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting address: %s", str(e))
            raise e
        
    def upsert_address(
        self, db: Session, address_data: dict
    ) -> Address:
        """Upsert address"""
        try:
            if address_data.get("id"):
                address = self.get_address(db, address_id=address_data.get("id"))
                if address:
                    for key, value in address_data.items():
                        if hasattr(address, key):
                            # Only update existing fields
                            if value is not None:
                                # Avoid setting None values
                                setattr(address, key, value)
                    db.flush()
                    db.refresh(address)
                    return address
            else:
                allowed_fields = {c.name for c in Address.__table__.columns}
                filtered_data = {k: v for k, v in address_data.items() if k in allowed_fields}
                
                address = Address(**filtered_data)
                db.add(address)
                db.flush()
                db.refresh(address)
                return address

        except Exception as e:
            logger.error("Error upserting address: %s", str(e))
            raise e
        
    def validate_bank_account_number(self, account_number: str) -> None:
        """
        Validate that the bank account number is between 6 and 18 digits.
        Logs clean error message without HTTP status codes.
        """
        if not account_number:
            msg = "Bank account number is required."
            logger.error("ERROR: %s", msg)
            raise ValueError(msg)

        if not re.fullmatch(r"^\d{6,18}$", str(account_number)):
            msg = "Invalid bank account number. It must be 6–18 digits long."
            logger.error("ERROR: %s", msg)
            raise ValueError(msg)


    def get_bank_account(
        self, db: Session, bank_account_id: Optional[int] = None,
        bank_account_number: Optional[str] = None,
        multiple: bool = False
    ) -> Union[BankAccount, List[BankAccount], None]:
        """Get bank account"""
        try:
            query = db.query(BankAccount)
            if bank_account_id:
                query = query.filter(BankAccount.id == bank_account_id)
            if bank_account_number:
                self.validate_bank_account_number(bank_account_number)
                query = query.filter(BankAccount.bank_account_number == bank_account_number)
                
            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting bank account: %s", str(e))
            raise ValueError(str(e)) 
        
    def upsert_bank_account(
        self, db: Session, bank_account_data: dict
    ) -> BankAccount:
        """Upsert bank account"""
        try:
            if bank_account_data.get("id"):
                bank_account = self.get_bank_account(db, bank_account_id=bank_account_data.get("id"))
                if bank_account:
                    for key, value in bank_account_data.items():
                        setattr(bank_account, key, value)
                    db.flush()
                    db.refresh(bank_account)
                    return bank_account
            else:
                allowed_fields = {c.name for c in BankAccount.__table__.columns}
                filtered_data = {k: v for k, v in bank_account_data.items() if k in allowed_fields}
                bank_account = BankAccount(**filtered_data)
                db.add(bank_account)
                db.flush()
                db.refresh(bank_account)
                return bank_account
        except Exception as e:
            logger.error("Error upserting bank account: %s", str(e))
            raise e
        
    def get_corporation_payee(
        self, db: Session, 
        corporation_payee_id: Optional[int] = None,
        corporation_id: Optional[int] = None,
        payee_type: Optional[str] = None,
        sequence: Optional[int] = None,
        bank_account_number: Optional[str] = None,
        multiple: bool = False
    ) -> Union[CorporationPayee, List[CorporationPayee], None]:
        """Get corporation payee"""
        try:
            query = db.query(CorporationPayee)
            if corporation_payee_id:
                query = query.filter(CorporationPayee.id == corporation_payee_id)
            if corporation_id:
                query = query.filter(CorporationPayee.corporation_id == corporation_id)
            if payee_type:
                query = query.filter(CorporationPayee.payee_type == payee_type)
            if sequence:
                query = query.filter(CorporationPayee.sequence == sequence)
            if bank_account_number:
                query = query.join(BankAccount, CorporationPayee.bank_account_id == BankAccount.id).filter(BankAccount.bank_account_number == bank_account_number)
            
            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting corporation payee: %s", str(e))
            raise e
            
    def upsert_corporation_payee(self, db: Session, corporation_payee_data: dict):
        """Upsert corporation payee"""
        try:
            if corporation_payee_data.get("id"):
                corporation_payee = self.get_corporation_payee(db, corporation_payee_id=corporation_payee_data.get("id"))
                if corporation_payee:
                    for key, value in corporation_payee_data.items():
                        setattr(corporation_payee, key, value)
                    db.flush()
                    db.refresh(corporation_payee)
                    return corporation_payee
            else:
                corporation_payee = CorporationPayee(**corporation_payee_data)
                db.add(corporation_payee)
                db.flush()
                db.refresh(corporation_payee)
                return corporation_payee
        except Exception as e:
            logger.error("Error upserting corporation payee: %s", str(e))
            raise e      
    def delete_bank_account(
        self, db: Session, bank_account_id: int
    ) -> None:
        """Delete bank account"""
        try:
            bank_account = self.get_bank_account(db, bank_account_id=bank_account_id)
            if bank_account:
                db.delete(bank_account)
                db.flush()
        except Exception as e:
            logger.error("Error deleting bank account: %s", str(e))
            raise e
        
    def get_individual(
        self, db: Session,
        individual_id: Optional[int] = None,
        name: Optional[str] = None,
        ssn: Optional[str] = None,
        primary_email_address: Optional[str] = None,
        primary_contact_number: Optional[str] = None,
        driving_license: Optional[str] = None,
        passport_number: Optional[str] = None,
        bank_account_number: Optional[str] = None,
        sort_by: Optional[str] = "created_on",
        sort_order: Optional[str] = "desc",
        page: Optional[int] = None,
        per_page: Optional[int] = None,
        multiple: bool = False
    ) -> Union[Individual, List[Individual], None]:
        """Get individual"""
        try:
            query = db.query(Individual)
            if individual_id:
                query = query.filter(Individual.id == individual_id)
            if ssn:
                query = query.filter(Individual.masked_ssn.ilike(f"%{ssn}%"))
            if name:
                query = query.filter(Individual.full_name.ilike(f"%{name}%"))
            if primary_email_address:
                query = query.filter(Individual.primary_email_address == primary_email_address)
            if primary_contact_number:
                query = query.filter(Individual.primary_contact_number == primary_contact_number)
            if driving_license:
                query = query.filter(Individual.driving_license == driving_license)
            if passport_number:
                query = query.filter(Individual.passport == passport_number)
            if bank_account_number:
                query = query.join(BankAccount, Individual.bank_account_id == BankAccount.id).filter(BankAccount.bank_account_number == bank_account_number)

            if sort_by:
                sort_attr = {
                    "name": Individual.full_name,
                    "ssn": Individual.masked_ssn,
                    "created_on": Individual.created_on,
                    "updated_on": Individual.updated_on
                }
                if sort_attr.get(sort_by):
                    query = query.order_by(sort_attr[sort_by].desc() if sort_order == "desc" else sort_attr[sort_by].asc())
                
            if multiple:
                total_count = query.count()
                if page and per_page:
                    query = query.offset((page - 1) * per_page).limit(per_page)
                return query.all() , total_count
            return query.first()
        except Exception as e:
            logger.error("Error getting individual: %s", str(e))
            raise e
        
    def get_corporation(
        self, db: Session, 
        corporation_id: Optional[int] = None,
        is_holding_entity: Optional[bool] = None,
        ein: Optional[str] = None,
        name: Optional[str] = None,
        page: Optional[int] = None,
        per_page: Optional[int] = None,
        sort_by : Optional[str] = "created_on",
        sort_order: Optional[str] = "desc",
        multiple: bool = False
    ) -> Union[Corporation, List[Corporation], None]:
        """Get corporation"""
        try:
            query = db.query(Corporation)
            if corporation_id:
                query = query.filter(Corporation.id == corporation_id)
            if is_holding_entity is not None:
                query = query.filter(Corporation.is_holding_entity == is_holding_entity)
            if ein:
                query = query.filter(Corporation.ein.ilike(f"%{ein}%"))
            if name:
                query = query.filter(Corporation.name.ilike(f"%{name}%"))
            if sort_by:
                query = query.order_by(getattr(Corporation, sort_by).desc() if sort_order == "desc" else getattr(Corporation, sort_by).asc())
            if multiple:
                total_count = query.count()
                if page and per_page:
                    query = query.offset((page - 1) * per_page).limit(per_page)
                return query.all() , total_count
            return query.first()
        except Exception as e:
            logger.error("Error getting corporation: %s", str(e))
            raise e
            
    def upsert_individual(
        self, db: Session, individual_data: dict
    ) -> Individual:
        """Upsert individual"""
        try:
            if individual_data.get("id"):
                individual = self.get_individual(db, individual_id=individual_data.get("id"))
                if individual:
                    for key, value in individual_data.items():
                        setattr(individual, key, value)
                    db.flush()
                    db.refresh(individual)
                    return individual
            else:
                individual = Individual(**individual_data)
                db.add(individual)
                db.flush()
                db.refresh(individual)
                return individual
        except Exception as e:
            logger.error("Error upserting individual: %s", str(e))
            raise e

    def upsert_corporation(
        self, db: Session, corporation_data: dict
    ) -> Corporation:
        """Upsert corporation"""
        try:
            if corporation_data.get("id"):
                corporation = self.get_corporation(db, corporation_id=corporation_data.get("id"))
                if corporation:
                    for key, value in corporation_data.items():
                        setattr(corporation, key, value)
                    db.flush()
                    db.refresh(corporation)
                    return corporation
            else:
                corporation = Corporation(**corporation_data)
                db.add(corporation)
                db.flush()
                db.refresh(corporation)
                return corporation
        except Exception as e:
            logger.error("Error upserting corporation: %s", str(e))
            raise e
        
    def get_entities(
        self, db: Session,
        lookup_id: Optional[int] = None,
        ein: Optional[str] = None,
        entity_name: Optional[str] = None,
        page: Optional[int] = None,
        per_page: Optional[int] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        multiple: bool = False
    ) -> Union[Entity, List[Entity], None]:
        """Get entities"""
        try:
            query = db.query(Entity)
            if lookup_id:
                query = query.filter(Entity.id == lookup_id)
            if ein:
                query = query.filter(Entity.ein_ssn == ein)
            if entity_name:
                query = query.filter(Entity.entity_name == entity_name)
            if sort_by:
                sort_attr = {
                    "name": Entity.entity_name,
                    "ein": Entity.ein_ssn,
                    "created_on": Entity.created_on,
                    "updated_on": Entity.updated_on
                }
                if sort_by in sort_attr and sort_order:
                    query = query.order_by(sort_attr[sort_by].desc() if sort_order == "desc" else sort_attr[sort_by].asc())
            if multiple:
                total_count = query.count()
                if page and per_page:
                    query = query.offset((page - 1) * per_page).limit(per_page)
                    return {
                        "entities": query.all(),
                        "total_count": total_count,
                        "page": page,
                        "per_page": per_page
                    }
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting entities: %s", str(e))
            raise e
        
    def upsert_entity(
        self, db: Session, entity_data: dict
    ) -> Entity:
        """Upsert entity"""
        try:
            if entity_data.get("id"):
                entity = self.get_entities(db, lookup_id=entity_data.get("id"))
                if entity:
                    for key, value in entity_data.items():
                        setattr(entity, key, value)
                    db.flush()
                    db.refresh(entity)
                    return entity
            else:
                entity = Entity(**entity_data)
                db.add(entity)
                db.flush()
                db.refresh(entity)
                return entity
        except Exception as e:
            logger.error("Error upserting entity: %s", str(e))
            raise e
    def upsert_corporation_owner(
            self, db: Session, owner_data: dict
            ):
        """Upsert corporation owner"""
        try:
            if owner_data.get("id"):
                owner = db.query(CorporationOwners).filter(CorporationOwners.id == owner_data.get("id")).first()
                if owner:
                    for key, value in owner_data.items():
                        setattr(owner, key, value)
                    db.flush()
                    db.refresh(owner)
                    return owner
            else:
                owner = CorporationOwners(**owner_data)
                db.add(owner)
                db.flush()
                db.refresh(owner)
                return owner
        except Exception as e:
            logger.error("Error upserting corporation owner: %s", str(e))
            raise e
    def get_corporation_owner(
            self,
            db: Session,
            id: Optional[int] = None,
            corporation_id: Optional[int] = None,
            owner_id: Optional[int] = None,
            name: Optional[str] = None,
            is_authorized_signatory: Optional[bool] = None,
            is_payee: Optional[bool] = None,
            is_primary_contact: Optional[bool] = None,
            multiple: bool = False,
            sort_by: Optional[str] = None,
            sort_order: Optional[str] = None
    ):
        """Get corporation owner"""
        try:
            query = db.query(CorporationOwners)
            if id:
                query = query.filter(CorporationOwners.id == id)
            if corporation_id:
                query = query.filter(CorporationOwners.corporation_id == corporation_id)
            if owner_id:
                query = query.filter(CorporationOwners.owner_id == owner_id)
            if name:
                query = query.filter(CorporationOwners.name.ilike(f"%{name}%"))
            if is_authorized_signatory is not None:
                query = query.filter(CorporationOwners.is_authorized_signatory == is_authorized_signatory)
            if is_payee is not None:
                query = query.filter(CorporationOwners.is_payee == is_payee)
            if is_primary_contact is not None:
                query = query.filter(CorporationOwners.is_primary_contact == is_primary_contact)
            if sort_by and sort_order:
                query = query.order_by(getattr(CorporationOwners, sort_by).desc() if sort_order == "desc" else getattr(CorporationOwners, sort_by).asc())
            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting corporation owner: %s", str(e))
            raise e
        
    def delete_corporation_owners(self, db: Session, corporation_id: int , owner_id: Optional[int] = None , all: bool = False) -> None:
        """Delete corporation owners"""
        try:
            query = db.query(CorporationOwners).filter(CorporationOwners.corporation_id == corporation_id)
            if owner_id:
                query = query.filter(CorporationOwners.id == owner_id)
            if all:
                owners = query.all()
                for owner in owners:
                    db.delete(owner)
            else:
                owner = query.first()
                if owner:
                    db.delete(owner)
            db.flush()
        except Exception as e:
            logger.error("Error deleting corporation owners: %s", str(e))
            raise e
        
    def delete_corporation_payees(self, db: Session, corporation_id: int , individual_id: Optional[int] = None ,corporation_owner_id : Optional[int] = None, all: bool = False) -> None:
        """Delete corporation payees"""
        try:
            query = db.query(CorporationPayee).filter(CorporationPayee.corporation_id == corporation_id)
            if individual_id:
                query = query.filter(CorporationPayee.individual_id == individual_id)
            if corporation_owner_id:
                query = query.filter(CorporationPayee.corporation_owner_id == corporation_owner_id)
            if all:
                payees = query.all()
                for payee in payees:
                    db.delete(payee)
            else:
                payee = query.first()
                if payee:
                    db.delete(payee)
            db.flush()
        except Exception as e:
            logger.error("Error deleting corporation payees: %s", str(e))
            raise e

    def get_child_corporations(
        self, db: Session, parent_corporation_id: int
    ) -> List[Corporation]:
        """Get child corporations of a holding entity"""
        try:
            return db.query(Corporation).filter(Corporation.linked_pad_owner_id == parent_corporation_id).all()
        except Exception as e:
            logger.error("Error getting child corporations: %s", str(e))
            raise e
        
entity_service = EntityService()
