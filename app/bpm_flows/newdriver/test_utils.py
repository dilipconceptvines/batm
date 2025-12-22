## app/bpm_flows/newdriver/test_utils.py

from test.test_db import test_step
from app.utils.logger import get_logger
from app.bat import utils as bat_utils
from app.bpm import utils

logger = get_logger(__name__)

@test_step(step_id="551", name = "Add driver_id to payload before processing details", operation= "process")
def update_driver_id(db, payload, case_no):
    case_entity = None
    case_entity = utils.fetch_case_entity(db, case_no)
    driver = None
    if case_entity:
        driver = bat_utils.fetch_driver_by_id(db, case_entity.identifier_value)
        driver_id = driver.driver_id
        if not driver:
            raise ValueError(f"Driver with ID {driver_id} not found in DB")
        # Update driver_id in the payload if it's empty
        if payload["data"]["driver_details"]["driver_id"] is 0:
            payload["data"]["driver_details"]["driver_id"] = driver.driver_id
        logger.info(f"Updated payload with driver_id: {driver.driver_id}")

    return payload


@test_step(step_id="553", name = "Add driver_id to payload before approval", operation= "process")
def update_driver_id(db, payload, case_no):
    case_entity = None
    case_entity = utils.fetch_case_entity(db, case_no)
    if case_entity:
        driver = None
        driver = bat_utils.fetch_driver_by_id(db, case_entity.identifier_value)
        driver_id = driver.driver_id
        if not driver:
            raise ValueError(f"Driver with ID {driver_id} not found in DB")
        # Update driver_id in the payload if it's empty
        if payload["data"]["driver_details"]["driver_id"] is "":
            payload["data"]["driver_details"]["driver_id"] = driver.driver_id
            logger.info(f"Updated payload with driver_id: {driver.driver_id}")

    return payload
