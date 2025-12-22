## app/bpm/router.py

# Standard Library Imports
import json
import os
from datetime import date

# Third party imports
from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse
from jsonschema import ValidationError, validate
from sqlalchemy.orm import Session

from app.audit_trail.schemas import AuditTrailType
from app.audit_trail.services import audit_trail_service
from app.bpm.exception import CaseStopException
from app.bpm.schemas import CreateCaseRequest, StepDataRequest
from app.bpm.services import CaseReassignService, bpm_service
from app.bpm.step_info import STEP_REGISTRY
from app.bpm.utils import calculate_time_due

# Local imports
from app.core.config import settings
from app.core.db import get_db
from app.core.dependencies import get_db_with_current_user
from app.users.models import User
from app.users.utils import get_current_user
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils

router = APIRouter(tags=["BPM"])
logger = get_logger(__name__)


@router.post("/case", tags=["BPM"])
async def create_new_case(
    request: Request,
    case_request: CreateCaseRequest,
    db: Session = Depends(get_db_with_current_user),
    logged_in_user: User = Depends(get_current_user),
):
    """
    Create a new case with the given case type prefix.

    Args:
        request: The FastAPI request object.
        case_request: The case creation request data.
        db: The database session.
        logged_in_user: The currently logged-in user.
    Returns:
        A dictionary containing case information and step details.
    """
    try:
        new_case = bpm_service.create_case(
            db, prefix=case_request.case_type, user=logged_in_user
        )
        logger.info("New case created: %s", new_case.case_no)

        audit_trail_service.create_audit_trail(
            db,
            case=new_case,
            user=logged_in_user,
            description=f"Created new case with case number: {new_case.case_no}",
            audit_type=AuditTrailType.AUTOMATED,
        )
        logger.info("Audit trail created for case creation")

        grouped_steps = bpm_service.get_case_step_information(
            db, case_request.case_type, logged_in_user, new_case
        )
        case_information = {
            "case_no": new_case.case_no,
            "created_by": logged_in_user.first_name,
            "case_created_on": new_case.created_on,
            "case_status": new_case.case_status.name,
            "steps": list(grouped_steps.values()),
        }
        return case_information
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/case/{case_no}", tags=["BPM"])
async def process_case_step(
    request: Request,
    case_no: str = Path(..., description="The case number"),
    step_data: StepDataRequest = Body(..., description="The JSON data to validate"),
    db: Session = Depends(get_db_with_current_user),
    logged_in_user: User = Depends(get_current_user),
):
    """Validate JSON data with the schema configured for this step id. Then process the step."""

    # Step 1: Validate the case_no
    result = bpm_service.get_cases(db, case_no=case_no, sort_order="desc")

    if not result:
        raise HTTPException(
            status_code=404, detail="Valid case not found for the provided case number"
        )

    # Step 2: Validate the step_id and retrieve the function from step_registry
    if f"{step_data.step_id}-process" not in STEP_REGISTRY:
        raise HTTPException(
            status_code=400, detail="Step ID not present in the registry"
        )

    # Step 3: Retrieve and load JSON schema for the given step_id
    case_step_config = bpm_service.get_case_step_config(db, step_id=step_data.step_id)
    if not case_step_config:
        raise HTTPException(
            status_code=404,
            detail="Step configuration not found for the provided step ID",
        )

    # Step 4: Validate if the role is valid for this step.
    if not bpm_service.has_required_role(logged_in_user, case_step_config):
        raise HTTPException(
            status_code=404,
            detail="User does not have the valid role to access this case",
        )
    else:
        logger.info("User has the valid roles to access this step")

    config_path_entry = bpm_service.get_case_step_config_path(
        db, case_step_config_id=case_step_config.id
    )

    if not config_path_entry:
        raise HTTPException(
            status_code=404, detail="JSON schema path not found for the specified step"
        )

    # Record is there but schema is blank because the case step may only need submission.
    if not config_path_entry.path:
        logger.info(
            "Config path record is present but is empty, probably because the step does not need form inputs"
        )

    if config_path_entry.path:
        try:
            s3_key = settings.json_config + "/" + config_path_entry.path
            logger.info("Fetching schema from S3: %s", s3_key)
            schema_content = s3_utils.download_file(s3_key)
            if not schema_content:
                raise HTTPException(
                    status_code=404, detail=f"Schema file not found in S3: {s3_key}"
                )

            schema = json.loads(schema_content)
            logger.info("Successfully loaded schema from S3")
        except FileNotFoundError as exc:
            raise HTTPException(
                status_code=500, detail="Schema file not found on the server"
            ) from exc
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=500, detail="Invalid JSON schema format"
            ) from exc

        # Step 5: Validate the incoming JSON data against the schema
        try:
            validate(instance=step_data.data, schema=schema)
            logger.info("Incoming Schema Validated")
        except ValidationError as e:
            raise HTTPException(
                status_code=400, detail=f"JSON validation error: {e.message}"
            ) from e

    # Step 6: Call the step-specific function from the registry
    try:
        step_function = STEP_REGISTRY[f"{step_data.step_id}-process"]
        import inspect

        func = step_function["function"]
        if inspect.iscoroutinefunction(func):
            function_result = await func(db, case_no, step_data.data)
        else:
            function_result = func(db, case_no, step_data.data)
    except ValueError as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error("****************** %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e

    # Create audit trail for the step
    audit_trail_service.create_audit_trail(
        db,
        case=result,
        user=logged_in_user,
        description=f"Processed step {step_data.step_id} for case {case_no}",
        audit_type=AuditTrailType.AUTOMATED,
    )
    # Return the result of the function
    return JSONResponse(content={"message": "OK", "data": function_result}, status_code=200)


@router.post("/case/{case_no}/move", tags=["BPM"])
async def move_case_to_next_step(
    request: Request,
    case_no: str,
    step_id: str = "",
    db: Session = Depends(get_db_with_current_user),
    logged_in_user: User = Depends(get_current_user),
):
    """Move the case to the next step or a specific step."""
    logger.info("Check if the case no is a valid open or in progress case")
    result = bpm_service.get_cases(db, case_no=case_no, sort_order="desc")

    if not result:
        raise HTTPException(
            status_code=404, detail="Open case not found for the provided case number"
        )

    logger.info("Move case no %s to the next step or step id %s", case_no, step_id)

    # Check if any results match the criteria
    result = bpm_service.get_case_info(db, case_no=case_no, step_id=step_id)

    if not result:
        raise ValueError("Case number invalid or combination is invalid")

    # TODO: Check if the current logged in user is the right person to do this operation

    try:
        result = None
        if not step_id:
            result = bpm_service.move_task_to_next_step(db, case_no, logged_in_user)
        else:
            result = bpm_service.move_task_to_step(db, case_no, step_id)

        # Create an audit trail
        audit_trail_service.create_audit_trail(
            db,
            case=result["case"],
            user=logged_in_user,
            description=f"Case moved to step {result['step_config'].step_id} for case number {case_no}",
            audit_type=AuditTrailType.AUTOMATED,
        )
    except CaseStopException:
        logger.info("Marking status as closed now")
        case = bpm_service.mark_case_as_closed(db, case_no)
        # Create an audit trail
        audit_trail_service.create_audit_trail(
            db,
            case=case,
            user=logged_in_user,
            description=f"Case with case number {case_no} closed",
            audit_type=AuditTrailType.AUTOMATED,
        )
    except Exception as e:
        logger.info(e)
        db.rollback()
        raise HTTPException(status_code=500, detail="Case could not be moved") from e

    return "Ok"


@router.get("/case-history/{case_no}", tags=["BPM"])
async def get_case_history(
    case_no: str,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    """
    Retrieve the history of all Case objects with the specified case_no.

    Args:
        case_no: The case number to retrieve history for.
        db: The database session.
    Returns:
        A list of formatted text representations of each Case object.
    """

    # Query all cases with the given case_no
    cases = bpm_service.get_cases(db, case_no=case_no, sort_order="desc", multiple=True)

    # Check if any cases were found
    if not cases:
        raise HTTPException(
            status_code=404, detail=f"No cases found for case number '{case_no}'"
        )

    # Format each case using the __str__ method
    case_history = [json.loads(str(case)) for case in cases]

    return case_history


@router.get("/cases/by-type/{case_type}", tags=["BPM"])
async def get_cases_by_type(
    case_type: str,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    """
    Retrieve all Case objects with the specified case_type.

    Args:
        case_type: The case type to retrieve.
        db: The database session.
    Returns:
        A list of formatted text representations of each Case object.
    """

    # Query all cases with the given case_type
    cases = bpm_service.get_cases(
        db, case_type_name=case_type, sort_order="desc", multiple=True
    )

    # Check if any cases were found
    if not cases:
        raise HTTPException(
            status_code=404, detail=f"No cases found for case type '{case_type}'"
        )

    # Format each case using the __str__ method
    case_list = [json.loads(str(case)) for case in cases]

    return case_list


@router.get("/case/{case_no}", tags=["BPM"])
async def get_case_steps(
    request: Request,
    case_no: str,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    """Get the case steps for a given case number."""
    case_params = dict(request.query_params)

    # Step 1: Fetch all the schema configs from all the steps
    case_obj = bpm_service.get_cases(db, case_no=case_no, sort_order="desc")

    calculate_due = {"due_date": "", "time_left": ""}
    if case_obj.sla:
        calculate_due = calculate_time_due(case_obj.created_on, case_obj.sla.time_limit)
    case_step_information = {
        "case_info": {
            "case_no": case_obj.case_no,
            "created_by": logged_in_user.first_name,
            "case_created_on": case_obj.created_on,
            "case_status": case_obj.case_status.name,
            "action_due_on": calculate_due["due_date"],
            "to_be_completed_in": calculate_due["time_left"],
        },
        "steps": [],
    }

    grouped_steps = bpm_service.get_case_step_information(
        db, case_obj.case_type.prefix, logged_in_user, case_obj
    )

    for step_info in list(grouped_steps.values()):
        for sub_step in step_info["sub_steps"]:
            step_function = STEP_REGISTRY[f"{sub_step['step_id']}-fetch"]
            logger.debug("Calling function with %s-fetch", sub_step["step_id"])
            case_step_data = step_function["function"](db, case_no, case_params)
            case_step_config = bpm_service.get_case_step_config(
                db, step_id=sub_step["step_id"]
            )

            cases = bpm_service.get_cases(
                db,
                case_no=case_no,
                case_status="Open,In Progress",
                case_step_config_id=case_step_config.id,
                multiple=True,
            )
            sub_step["step_data"] = case_step_data
            sub_step["is_current_step"] = (
                case_obj.case_step_config_id == case_step_config.id
            )
            sub_step["has_already_been_used"] = len(cases) > 0

    case_step_information["steps"] = list(grouped_steps.values())
    logger.info(case_step_information)
    return case_step_information


@router.get("/case/{case_no}/{step_id}", tags=["BPM"])
async def get_case_step_information(
    request: Request,
    case_no: str,
    step_id: str,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    """Get the case step information for a given case number and step id."""
    case_params = dict(request.query_params)
    case_step_information = {}
    result = bpm_service.get_case_info(db, case_no=case_no, step_id=step_id)

    if not result:
        raise HTTPException(
            status_code=400,
            detail=f"case_no '{case_no}' and step_id '{step_id}' combination not found",
        )
    _, case_step_config = result

    try:
        step_function = STEP_REGISTRY[f"{case_step_config.step_id}-fetch"]
        logger.debug("Calling function with %s-fetch", case_step_config.step_id)
        case_step_information = step_function["function"](db, case_no, case_params)
    except ValueError as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=500, detail="Error while fetching details"
        ) from e

    return case_step_information


@router.get("/cases/workbasket/", tags=["BPM"])
async def get_workbasket(
    from_date: date = None,
    to_date: date = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    """Retrieve all the cases that are associated with the logged in user."""
    try:
        logged_in_user_role_ids = [role.id for role in logged_in_user.roles]

        # Create a subquery to find case numbers that have any Closed status
        closed_cases = bpm_service.get_cases(
            db, case_status="Closed", multiple=True, unique=True
        )
        closed_case_nos = [case.case_no for case in closed_cases]
        all_cases = bpm_service.get_cases_info(db, closed_case_nos, from_date, to_date)

        filtered_cases = []

        unique_case = set()
        for case in all_cases:
            process_case_details = False
            # Check if user is assigned to case
            if case.current_user_id and case.current_user_id == logged_in_user.id:
                process_case_details = True

            # Check if role is assigned to case
            if case.role_id and case.role_id in logged_in_user_role_ids:
                process_case_details = True

            if process_case_details and case.case_no not in unique_case:
                filtered_cases.append(case)
                unique_case.add(case.case_no)

        total_count = len(filtered_cases)

        paginated_cases = filtered_cases[(page - 1) * per_page : page * per_page]

        # Process and format cases
        detailed_cases = []
        for case in paginated_cases:
            try:
                case_details = bpm_service.get_case_details(db, case)
                detailed_cases.append(case_details)
            except Exception as e:
                logger.error(e)
                raise HTTPException(
                    status_code=500, detail="Error while fetching details"
                ) from e

        return {
            "total_cases": total_count,
            "page": page,
            "per_page": per_page,
            "total_pages": (total_count // per_page)
            + (1 if total_count % per_page > 0 else 0),
            "cases": detailed_cases,
        }
    except Exception as e:
        logger.error("****************** %s", e)
        raise HTTPException(
            status_code=500, detail="Error while fetching details"
        ) from e


@router.put("/reassign-case", tags=["BPM"])
async def reassign_case(
    case_no: str,
    role_id: int = None,
    user_id: int = None,
    current_step_only: bool = False,
    db: Session = Depends(get_db),
    logged_in_user: User = Depends(get_current_user),
):
    """Reassign a case to a different user or role."""
    response = CaseReassignService.assign_user_to_case(
        db, logged_in_user, case_no, role_id, user_id, current_step_only
    )

    return {"message": response}
