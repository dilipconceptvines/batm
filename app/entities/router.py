### app/entities/router.py

# Standard library imports
from typing import List
import requests

# Third party imports
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from fastapi.responses import JSONResponse

# Local imports
from app.core.db import get_db
from app.utils.logger import get_logger
from app.entities.services import entity_service
from app.users.utils import get_current_user
from app.users.models import User

logger = get_logger(__name__)
router = APIRouter(tags=["Entities"])


@router.get("/entities", summary="Search Entities associated with Vehicles")
def search_entities(
    ein: str = Query(None, description="EIN of the entity"),
    entity_name: str = Query(None, description="Name of the entity"),
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1, description="Page number, starts from 1"),
    limit: int = Query(
        10, ge=1, le=100, description="Number of results per page, maximum 100"),
    sort_by: str = Query("created_on", description="Sort by field"),
    sort_order: str = Query("desc", description="Sort order"),
    logged_in_user: User = Depends(get_current_user)
):
    """
    Searches for entities associated with vehicles based on EIN or entity name.
    """
    try:
        results = entity_service.get_entities(
            db, ein=ein, entity_name=entity_name, page=page, per_page=limit, multiple=True , sort_by=sort_by, sort_order=sort_order
        )

        return {
            "page": page,
            "per_page": limit,
            "total_items": results["total_count"],
            "items": [{
                "entity_id": entity.id,
                "entity_name": entity.entity_name,
                "ein": entity.ein_ssn,
                "contact_number": entity.contact_person.primary_contact_number if entity.contact_person else "",
                "contact_email": entity.contact_person.primary_email_address if entity.contact_person else ""
            } for entity in results["entities"]]
        }
    except Exception as e:
        logger.error("Error searching entities: %s", str(e))
        raise HTTPException(status_code=500, detail="Error searching entities") from e
    
@router.get("/bank/{router_number}" , summary = "Get Bank Details by Router Number")
def get_bank_details(
    router_number : str,
    db : Session = Depends(get_db)
):
    """Get bank details by router number"""
    try:
        if not router_number:
            raise HTTPException(status_code=400, detail="Router number is required")
        
        resp = requests.get(
            f"https://routing-number-bank-lookup.p.rapidapi.com/api/v1/{router_number}?format=json&paymentType=ach",
            headers={
                "x-rapidapi-host": "routing-number-bank-lookup.p.rapidapi.com",
                "x-rapidapi-key": "17efb65c09mshca3ee3f2dceeccbp162dccjsn8c0b5c2a5d30"
            }
        )

        if resp.status_code == 200:
            json_data = resp.json()
            if isinstance(json_data, list) and json_data:
                data = json_data[0].get("data", {})
                if data:
                    return data
                else:
                    logger.warning("WARNING: Routing Number not found.")
                    return JSONResponse(
                        {"message": "WARNING: Routing Number not found."},
                        status_code=400
                    )
            else:
                logger.warning("WARNING: Routing Number not found.")
                return JSONResponse(
                    {"message": "WARNING: Routing Number not found."},
                    status_code=400
                )
        else:
            logger.warning("WARNING: Routing Number not found.")
            return JSONResponse(
                {"message": "WARNING: Routing Number not found."},
                status_code=400
            )

    except Exception as e:
        logger.error("Error getting bank details: %s", str(e))
        raise HTTPException(status_code=500, detail="Error getting bank details") from e

@router.get("/individual/list", summary="Get List of Individuals")
def get_individual_list(
    db: Session = Depends(get_db),
    individual_id : int = Query(None, description="ID of the individual"),
    name : str = Query(None, description="Name of the individual"),
    ssn : str = Query(None, description="SSN of the individual"),
    page : int = Query(1, description="Page number, starts from 1"),
    per_page : int = Query(10, description="Number of results per page, maximum 100"),
    sort_by : str = Query("created_on", description="Sort by field"),
    sort_order : str = Query("desc", description="Sort order"),
    logged_in_user: User = Depends(get_current_user)
):
    """
    Get list of individuals
    """
    try:
        individuals , total_count = entity_service.get_individual(db,individual_id=individual_id,
                                                    name=name,ssn=ssn,
                                                    page=page,per_page=per_page,
                                                    sort_by=sort_by,sort_order=sort_order,
                                                    multiple=True)
        
        return {
            "page": page,
            "per_page": per_page,
            "total_items": total_count,
            "items": individuals,
            "total_pages" : total_count // per_page + 1 if total_count % per_page != 0 else total_count // per_page
        }
    except Exception as e:
        logger.error("Error getting individual list: %s", str(e))
        raise HTTPException(status_code=500, detail="Error getting individual list") from e
    
@router.get("/corporation/list", summary="Get List of Corporations")
def get_corporation_list(
    db: Session = Depends(get_db),
    corporation_id : int = Query(None, description="ID of the corporation"),
    is_holding_entity : bool = Query(None, description="Indicates if the Corporation is a Holding Entity"),
    ein : str = Query(None, description="EIN of the corporation"),
    name : str = Query(None, description="Name of the corporation"),
    page : int = Query(1, description="Page number, starts from 1"),
    per_page : int = Query(10, description="Number of results per page, maximum 100"),
    sort_by : str = Query("created_on", description="Sort by field"),
    sort_order : str = Query("desc", description="Sort order"),
    logged_in_user: User = Depends(get_current_user)
    ):
    """
    Get list of corporations
    """
    try:
        corporations , total_count = entity_service.get_corporation(db=db,
                                                      corporation_id=corporation_id,
                                                      is_holding_entity=is_holding_entity,
                                                      ein=ein,name=name,
                                                      page=page,per_page=per_page,
                                                      sort_by=sort_by,sort_order=sort_order,
                                                      multiple=True
                                                      )
        return {
            "page": page,
            "per_page": per_page,
            "total_items": total_count,
            "items": corporations,
            "total_pages" : total_count // per_page + 1 if total_count % per_page != 0 else total_count // per_page
        }
    except Exception as e:
        logger.error("Error getting corporation list: %s", str(e))
        raise HTTPException(status_code=500, detail="Error getting corporation list") from e
