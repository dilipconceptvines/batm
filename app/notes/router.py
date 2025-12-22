from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session

# Local imports
from app.utils.logger import get_logger
from app.core.db import get_db
from app.users.models import User
from app.users.utils import get_current_user
from app.core.dependencies import get_db_with_current_user
from app.notes.services import note_service

logger = get_logger(__name__)
router = APIRouter(tags=["Notes"], prefix="/notes")

@router.get("/list", summary="Get all notes for the current user")
def list_notes(
    entity_type: str = Query(..., description="Type of entity: driver, vehicle, lease, medallion"),
    entity_id: int = Query(..., description="ID of the entity"),
    note_type: str = Query(None, description="Type of note to filter by"),
    note_id: int = Query(None, description="Specific note ID to retrieve"),
    note: str = Query(None, description="Content of the note to filter by"),
    page: int = Query(1, ge=1, description="Page number for pagination"),
    per_page: int = Query(10, ge=1, le=100, description="Number of items per page"),
    db: Session = Depends(get_db_with_current_user),
    logged_in_user: User = Depends(get_current_user)
):
    """
    Get all notes for the current user.
    """
    try:
        return note_service.get_notes(db=db , 
                                      entity_type=entity_type , entity_id=entity_id,
                                      note_type=note_type , note_id=note_id , note=note,
                                      page=page , per_page=per_page , multiple=True)
    except Exception as e:
        logger.error("Error fetching notes: %s", str(e))
        raise e
@router.post("/add", summary="Add a new note")
def add_note(
    note_data: dict,
    entity_type: str = Query(..., description="Type of entity: driver, vehicle, lease, medallion"),
    entity_id: int = Query(..., description="ID of the entity"),
    db: Session = Depends(get_db_with_current_user),
    logged_in_user: User = Depends(get_current_user)
):
    """
    Add a new note.
    """
    try:
        if entity_type == "driver":
            return note_service.upsert_driver_note(db=db , note_data=note_data , driver_id=entity_id)
        elif entity_type == "vehicle":
            return note_service.upsert_vehicle_note(db=db , note_data=note_data , vehicle_id=entity_id)
        elif entity_type == "lease":
            return note_service.upsert_lease_note(db=db , note_data=note_data , lease_id=entity_id)
        elif entity_type == "medallion":
            return note_service.upsert_medallion_note(db=db , note_data=note_data , medallion_id=entity_id)
        else:
            raise HTTPException(status_code=400, detail="Invalid entity type")
    except Exception as e:
        logger.error("Error adding note: %s", str(e))
        raise e
    
@router.put("/update", summary="Update an existing note")
def update_note(
    note_data: dict,
    entity_type: str = Query(..., description="Type of entity: driver, vehicle, lease, medallion"),
    entity_id: int = Query(..., description="ID of the entity"),
    note_id: int = Query(..., description="ID of the note to update"),
    db: Session = Depends(get_db_with_current_user),
    logged_in_user: User = Depends(get_current_user)
):
    """
    Update an existing note.
    """
    try:

        if entity_type == "driver":
            return note_service.upsert_driver_note(db=db , note_data=note_data , driver_id=entity_id, note_id=note_id)
        elif entity_type == "vehicle":
            return note_service.upsert_vehicle_note(db=db , note_data=note_data , vehicle_id=entity_id, note_id=note_id)
        elif entity_type == "lease":
            return note_service.upsert_lease_note(db=db , note_data=note_data , lease_id=entity_id, note_id=note_id)
        elif entity_type == "medallion":
            return note_service.upsert_medallion_note(db=db , note_data=note_data , medallion_id=entity_id, note_id=note_id)
        else:
            raise HTTPException(status_code=400, detail="Invalid entity type")
    except Exception as e:
        logger.error("Error updating note: %s", str(e))
        raise e

@router.delete("/delete", summary="Delete a note")
def delete_note(
    entity_type: str = Query(..., description="Type of entity: driver, vehicle, lease, medallion"),
    entity_id: int = Query(..., description="ID of the entity"),
    note_id: int = Query(..., description="ID of the note to delete"),
    db: Session = Depends(get_db_with_current_user),
    logged_in_user: User = Depends(get_current_user)
):
    """
    Delete a note.
    """
    try:
        if entity_type == "driver":
            return note_service.delete_note(db=db , entity_type= entity_type , entity_id=entity_id, note_id=note_id)
        elif entity_type == "vehicle":
            return note_service.delete_note(db=db , entity_type= entity_type , entity_id=entity_id, note_id=note_id)
        elif entity_type == "lease":
            return note_service.delete_note(db=db , entity_type= entity_type , entity_id=entity_id, note_id=note_id)
        elif entity_type == "medallion":
            return note_service.delete_note(db=db , entity_type= entity_type , entity_id=entity_id, note_id=note_id)
        else:
            raise HTTPException(status_code=400, detail="Invalid entity type")
    except Exception as e:
        logger.error("Error deleting note: %s", str(e))
        raise e