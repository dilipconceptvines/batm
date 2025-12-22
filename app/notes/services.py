## app/notes/utils.py

from sqlalchemy.orm import Session

from app.notes.models import Note, DriverNote , MedallionNote , VehicleNote , LeaseNote
from app.drivers.services import driver_service
from app.vehicles.services import vehicle_service
from app.leases.services import lease_service
from app.medallions.services import medallion_service
from app.utils.logger import get_logger

logger = get_logger(__name__)

class NoteService:
    """
    Service class for managing notes associated with drivers.
    This class provides methods to add, retrieve, update, and delete notes for drivers.
    """
    def upsert_note(
            self,
            db: Session,
            note_data: dict,
            ) -> Note:
        """
        upsert a note.
        """
        try:
            note_text = note_data.get("note", "")
            note_type = note_data.get("note_type", "")
            
            if not note_text or not note_text.strip():
                raise ValueError("Note text cannot be empty")
            if note_type and not note_type.strip():
                raise ValueError("Note type cannot be empty if provided")
            
             # Upsert logic

            
            if note_data.get("id"):
                note = db.query(Note).filter(Note.id == note_data["id"]).first()
                if not note:
                    raise ValueError(f"Note with ID {note_data['id']} not found")
                
                for key, value in note_data.items():
                    if hasattr(note, key) and value is not None:
                        setattr(note, key, value)
            else:
                note = Note(**note_data)
                db.add(note)
            db.flush()
            logger.info(f"Upserted note (ID: {note.id})")
            return note
        except Exception as e:
            logger.error(f"Error upserting note: {e}")
            raise e

    def upsert_driver_note(
            self,
            db: Session,
            driver_id: int,
            note_id: int = None,
            note_data : dict = None
            ) -> DriverNote:
        # Validate inputs
        try:
            if not driver_id:
                raise ValueError("Driver ID must be provided")
            if not note_data or not isinstance(note_data, dict):
                raise ValueError("Note data must be provided as a dictionary")
            
            if driver_id:
                driver  = driver_service.get_drivers(db=db , id=driver_id)
                if not driver:
                    raise ValueError(f"Driver with ID {driver_id} not found")

            if note_id:
                driver_note = db.query(DriverNote).filter(DriverNote.note_id == note_id , DriverNote.driver_id == driver_id).first()
                if not driver_note:
                    raise ValueError(f"Driver note with Note ID {note_id} not found")
                note = self.upsert_note(db=db, note_data={
                    "id": note_id, **note_data})
                driver_note.note_id = note.id
            else:
                note = self.upsert_note(db=db, note_data=note_data)
                driver_note = DriverNote(
                    driver_id=driver_id,
                    note_id=note.id
                )
                db.add(driver_note)
            db.flush()
            logger.info(f"Upserted note (ID: {note.id}) for driver (ID: {driver_id})")
            return driver_note
        except Exception as e:
            logger.error(f"Error upserting note: {e}")
            raise e
    
    def upsert_medallion_note(
            self,
            db: Session,
            medallion_id: int,
            note_id: int = None,
            note_data : dict = None
            ) -> MedallionNote:
        # Validate inputs
        try:
            if not medallion_id:
                raise ValueError("Medallion ID must be provided")
            if not note_data or not isinstance(note_data, dict):
                raise ValueError("Note data must be provided as a dictionary")
            
            if medallion_id:
                medallion  = medallion_service.get_medallion(db=db , medallion_id=medallion_id)
                if not medallion:
                    raise ValueError(f"Medallion with ID {medallion_id} not found")

            if note_id:
                medallion_note = db.query(MedallionNote).filter(MedallionNote.note_id == note_id , MedallionNote.medallion_id == medallion_id).first()
                if not medallion_note:
                    raise ValueError(f"Medallion note with Note ID {note_id} not found")
                note = self.upsert_note(db=db, note_data={"id": note_id , **note_data})
                medallion_note.note_id = note.id
            else:
                note = self.upsert_note(db=db, note_data=note_data)
                medallion_note = MedallionNote(
                    medallion_id=medallion_id,
                    note_id=note.id
                )
                db.add(medallion_note)

            db.flush()
            logger.info(f"Upserted note (ID: {note.id}) for medallion (ID: {medallion_id})")
            return medallion_note
        except Exception as e:
            logger.error(f"Error upserting note: {e}")
            raise e
        
    def upsert_vehicle_note(
            self,
            db: Session,
            vehicle_id: int,
            note_id: int = None,
            note_data : dict = None
            ) -> VehicleNote:
        # Validate inputs
        try:
            if not vehicle_id:
                raise ValueError("Vehicle ID must be provided")
            if not note_data or not isinstance(note_data, dict):
                raise ValueError("Note data must be provided as a dictionary")
            if vehicle_id:
                vehicle  = vehicle_service.get_vehicles(db=db , vehicle_id=vehicle_id)
                if not vehicle:
                    raise ValueError(f"Vehicle with ID {vehicle_id} not found")
                
            if note_id:
                vehicle_note = db.query(VehicleNote).filter(VehicleNote.note_id == note_id , VehicleNote.vehicle_id == vehicle_id).first()
                if not vehicle_note:
                    raise ValueError(f"Vehicle note with Note ID {note_id} not found")
                note = self.upsert_note(db=db, note_data={"id": note_id , **note_data})
                vehicle_note.note_id = note.id
            else:
                note = self.upsert_note(db=db, note_data=note_data)
                vehicle_note = VehicleNote(
                    vehicle_id=vehicle_id,
                    note_id=note.id
                )
                db.add(vehicle_note)
                
            db.flush()
            logger.info(f"Upserted note (ID: {note.id}) for vehicle (ID: {vehicle_id})")
            return vehicle_note
        except Exception as e:
            logger.error(f"Error upserting note: {e}")
            raise e
        
    def upsert_lease_note(
            self,
            db: Session,
            lease_id: int,
            note_id: int = None,
            note_data : dict = None
            ) -> LeaseNote:
        # Validate inputs
        try:
            if not lease_id:
                raise ValueError("Lease ID must be provided")
            if not note_data or not isinstance(note_data, dict):
                raise ValueError("Note data must be provided as a dictionary")
            
            if lease_id:
                lease  = lease_service.get_lease(db=db , lookup_id=lease_id)
                if not lease:
                    raise ValueError(f"Lease with ID {lease_id} not found")
                
            if note_id:
                lease_note = db.query(LeaseNote).filter(LeaseNote.note_id == note_id , LeaseNote.lease_id == lease_id).first()
                if not lease_note:
                    raise ValueError(f"Lease note with Note ID {note_id} not found")
                note = self.upsert_note(db=db, note_data={"id": note_id , **note_data})
                lease_note.note_id = note.id
            else:
                note = self.upsert_note(db=db, note_data=note_data)
                lease_note = LeaseNote(
                    lease_id=lease_id,
                    note_id=note.id
                )
                db.add(lease_note)
            db.flush()
            logger.info(f"Upserted note (ID: {note.id}) for lease (ID: {lease_id})")
            return lease_note
        except Exception as e:
            logger.error(f"Error upserting note: {e}")
            raise e
    
    def get_notes(
            self,
            db: Session,
            entity_type: str,
            entity_id: int,
            page: int = None, 
            per_page: int = None,
            note_type: str = None,
            note_id: int = None,
            note: str = None,
            multiple: bool = False
            ) -> list:
        """
        Get all notes for a specific entity (driver, medallion, vehicle, lease).

        Args:
            db: Database session
            entity_type: Type of the entity ("driver", "medallion", "vehicle", "lease")
            entity_id: ID of the entity
            note_type: Optional filter by note type

        Returns:
            List of notes for the entity
        Raises:
            ValueError: If entity not found
        """
        try:
            if entity_type == "driver":
                driver = driver_service.get_drivers(db=db , id=entity_id)
                if not driver:
                    raise ValueError(f"Driver with ID {entity_id} not found")
                query = db.query(DriverNote).filter(
                    DriverNote.driver_id == entity_id,
                    DriverNote.is_active
                )
            elif entity_type == "medallion":
                medallion = medallion_service.get_medallion(db=db , medallion_id=entity_id)
                if not medallion:
                    raise ValueError(f"Medallion with ID {entity_id} not found")
                query = db.query(MedallionNote).filter(
                    MedallionNote.medallion_id == entity_id,
                    MedallionNote.is_active
                )
            elif entity_type == "vehicle":
                vehicle = vehicle_service.get_vehicles(db=db , vehicle_id=entity_id)
                if not vehicle:
                    raise ValueError(f"Vehicle with ID {entity_id} not found")
                query = db.query(VehicleNote).filter(
                    VehicleNote.vehicle_id == entity_id,
                    VehicleNote.is_active
                )
            elif entity_type == "lease":
                lease = lease_service.get_lease(db=db , lookup_id=entity_id)
                if not lease:
                    raise ValueError(f"Lease with ID {entity_id} not found")
                query = db.query(LeaseNote).filter(
                    LeaseNote.lease_id == entity_id,
                    LeaseNote.is_active
                )
            else:
                raise ValueError("Invalid entity type. Must be one of: driver, medallion, vehicle, lease")
            
            # Filter by note type if provided
            query = query.join(Note)

            if note_type:
                query = query.filter(Note.note_type == note_type)
            if note_id:
                query = query.filter(Note.id == note_id)
            if note:
                query = query.filter(Note.note.ilike(f"%{note}%"))

            total_notes = query.count()
            query = query.order_by(Note.created_on.desc())

            if page and per_page:
                query = query.offset((page - 1) * per_page).limit(per_page)
            
            if multiple:
                notes = query.all()

                logger.info(f"Retrieved {len(notes)} notes for entity (ID: {entity_id})")
                return {
                "entity_id": entity_id,
                "entity_type": entity_type,
                "total_items": total_notes,
                "items": [n.to_dict() for n in notes],
                "page": page or 1,
                "per_page": per_page or total_notes,
                "total_pages": (total_notes // per_page + (1 if total_notes % per_page else 0)) if page and per_page else 1,
                }
            else:
                note = query.first()
                logger.info(f"Retrieved note (ID: {note.id if note else 'N/A'}) for entity (ID: {entity_id})")
                return note.to_dict() if note else None
        except Exception as e:
            logger.error(f"Error getting notes: {e}")
            raise e
        
    def delete_note(
            self,
            db: Session,
            entity_type: str,
            entity_id: int,
            note_id: int
            ) -> bool:
        """
        Delete a note for a specific entity (driver, medallion, vehicle, lease).

        Args:
            db: Database session
            entity_type: Type of the entity ("driver", "medallion", "vehicle", "lease")
            entity_id: ID of the entity
            note_id: ID of the note to delete

        Returns:
            bool: True if deleted successfully
        Raises:
            ValueError: If entity not found
        """
        try:
            if entity_type == "driver":
                driver = driver_service.get_drivers(db=db , id=entity_id)
                if not driver:
                    raise ValueError(f"Driver with ID {entity_id} not found")
                association = db.query(DriverNote).filter(
                    DriverNote.driver_id == entity_id,
                    DriverNote.note_id == note_id
                ).first()
            elif entity_type == "medallion":
                medallion = medallion_service.get_medallion(db=db , medallion_id=entity_id)
                if not medallion:
                    raise ValueError(f"Medallion with ID {entity_id} not found")
                association = db.query(MedallionNote).filter(
                    MedallionNote.medallion_id == entity_id,
                    MedallionNote.note_id == note_id
                ).first()
            elif entity_type == "vehicle":
                vehicle = vehicle_service.get_vehicles(db=db , vehicle_id=entity_id)
                if not vehicle:
                    raise ValueError(f"Vehicle with ID {entity_id} not found")
                association = db.query(VehicleNote).filter(
                    VehicleNote.vehicle_id == entity_id,
                    VehicleNote.note_id == note_id
                ).first()
            elif entity_type == "lease":
                lease = lease_service.get_lease(db=db , lookup_id=entity_id)
                if not lease:
                    raise ValueError(f"Lease with ID {entity_id} not found")
                association = db.query(LeaseNote).filter(
                    LeaseNote.lease_id == entity_id,
                    LeaseNote.note_id == note_id
                ).first()
            else:
                raise ValueError("Invalid entity type. Must be one of: driver, medallion, vehicle, lease")
            if not association:
                raise ValueError(f"Note with ID {note_id} not found for the specified entity")
                # Soft delete the association
            association.is_active = False
            db.flush()
            return True
        except Exception as e:
            logger.error(f"Error deleting note: {e}")
            raise e
    
note_service = NoteService()