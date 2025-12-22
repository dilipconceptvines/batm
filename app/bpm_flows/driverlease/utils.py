## app/bpm_flows/driverlease/utils.py

# Standard imports
import random
import string
import json
from datetime import datetime

# Third party imports
import requests
from sqlalchemy.orm import Session
from sqlalchemy import delete, desc

# Local imports
from app.vehicles.models import Vehicle, VehicleHackUp, VehicleRegistration
from app.medallions.models import Medallion
from app.leases.models import Lease, LeaseConfiguration, LeaseDriver, LeaseDriverDocument
from app.drivers.models import Driver, TLCLicense, DMVLicense
from app.vehicles.schemas import VehicleStatus
from app.leases.schemas import LeaseStatus, LeaseType, LongTermLease, ShiftLease, ShortTermLease, MedallionOnlyLease
from app.drivers.schemas import DOVLease
from app.bat import utils as bat_utils
from app.vehicles.services import vehicle_service
from app.medallions.services import medallion_service
from app.drivers.services import driver_service
from app.core.config import settings
from app.uploads.models import Document
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.utils.lambda_utils import invoke_lambda_function
from app.medallions.utils import format_medallion_response
from app.utils.email import send_email

logger = get_logger(__name__)


def choose_vehicle_search_details(
        db: Session,
        medallion_number: str,
        vin: str,
        plate_number: str
):
    """
    Choose the vehicle search details for the driver lease step
    """
    query = db.query(Vehicle)

    # Filter by medallion_number using the medallion_id relationship
    if medallion_number:
        query = query.join(Medallion).filter(
            Medallion.medallion_number == medallion_number,
            Vehicle.medallion_id == Medallion.id
        )

    # Filter by VIN
    if vin:
        query = query.filter(Vehicle.vin == vin)

    # Filter by plate_number
    if plate_number:
        query = query.join(VehicleRegistration).filter(
            VehicleRegistration.plate_number == plate_number,
            VehicleRegistration.vehicle_id == Vehicle.id
        )

    vehicle = query.first()

    if not vehicle:
        return {}

    if vehicle.vehicle_status != VehicleStatus.HACKED_UP :
        return {}

    # Check if the vehicle has an active or in-progress hackup
    hackup_exists = (
        db.query(VehicleHackUp)
        .filter(VehicleHackUp.vehicle_id == vehicle.id)
        .filter(VehicleHackUp.status.in_(["Active", "In Progress"]))
        .first() is not None
    )

    print("Vehicle hackup found: ", hackup_exists)

    return {
        "vin": vehicle.vin,
        "make": vehicle.make,
        "model": vehicle.model,
        "year": vehicle.year,
        "vehicle_type": vehicle.vehicle_type,
        # "registration_state": vehicle.registrations.registration_state if vehicle.registrations else "",
        "plate_number": vehicle.registrations[0].plate_number if vehicle.registrations else "",
        "medallion_number": vehicle.medallions.medallion_number if vehicle.medallions else "",
        "entity_name": vehicle.vehicle_entity.entity_name if vehicle.vehicle_entity else None,
        "is_hacked_up": True if vehicle.hackups else False,
    }


def create_or_update_empty_lease(db: Session, vehicle: Vehicle) -> Lease:
    """
    Creates or updates an empty Lease object in the database.

    Args:
        db (Session): The database session.
        vehicle_vin (str): The vehicle vin number.

    Returns:
        Lease: The created or updated Lease object.
    """

    # Create a new vehicle
    new_lease = Lease()
    
    new_lease.lease_id = generate_16_digit_mix()
    new_lease.vehicle = vehicle
    new_lease.medallion_id = vehicle.medallion_id
    new_lease.is_active = False
    new_lease.lease_status = LeaseStatus.IN_PROGRESS
    db.add(new_lease)
    db.flush()
    db.refresh(new_lease)
    return new_lease

def get_lease_by_lease_id(db: Session, lease_id: str) -> Lease:
    """
    Fetches a lease object by its lease ID.

    Args:
        db (Session): SQLAlchemy database session.
        lease_id (str): The ID of the lease to fetch.

    Returns:
        Lease: The Lease object if found.

    Raises:
        HTTPException: If the lease is not found.
    """
    lease = db.query(Lease).filter(
        Lease.lease_id == lease_id).first()

    if not lease:
        raise ValueError(f"Lease with ID {lease_id} not found.")

    return lease


def generate_16_digit_mix():
    """Generate a 14-character alphanumeric string with 2 to 3 uppercase letters."""
    num_letters = random.choice([2, 3])  # Choose 2 or 3 letters
    num_digits = 16 - num_letters  # Remaining characters will be digits
    
    letters = random.choices(string.ascii_uppercase, k=num_letters)
    digits = random.choices(string.digits, k=num_digits)
    
    result = letters + digits
    random.shuffle(result)  # Shuffle to mix letters and digits
    
    return ''.join(result)

def get_lease_by_id(db: Session, lease_id: int) -> Lease:
    """
    Fetches a lease object by its ID.

    Args:
        db (Session): SQLAlchemy database session.
        lease_id (int): The ID of the lease to fetch.

    Returns:
        Lease: The Lease object if found.

    Raises:
        HTTPException: If the lease is not found.
    """
    lease = db.query(Lease).filter(
        Lease.id == lease_id).first()

    if not lease:
        raise ValueError(f"Lease with ID {lease_id} not found.")

    return lease


def get_driver_ids_by_lease(db: Session, lease_id: str):
    """
    Fetch all driver_ids associated with a given lease_id.

    Args:
        lease_id (int): The ID of the lease to search for.
        db (Session): The database session.

    Returns:
        List[int]: A list of driver IDs associated with the lease.
    """
    # Querying the LeaseDriver table for driver IDs associated with the lease ID
    driver_ids = db.query(LeaseDriver.driver_id).filter(
        LeaseDriver.lease_id == lease_id).all()

    return [str(driver_id[0]) for driver_id in driver_ids if driver_id[0] is not None]


def remove_drivers_from_lease(db: Session, lease_id: int, driver_ids: set[str]):
    """
    Removes drivers from the LeaseDriver table for a given lease ID.

    Args:
        db (Session): The database session.
        lease_id (int): The lease ID to filter driver records.
        driver_ids (set[str]): A set of driver IDs to be removed.

    Returns:
        int: The number of records deleted.
    """
    # Execute delete query
    delete_query = (
        delete(LeaseDriver)
        .where(LeaseDriver.lease_id == lease_id)
        .where(LeaseDriver.driver_id.in_(driver_ids))
    )
    result = db.execute(delete_query)

    for d_id in driver_ids:
        logger.info(f"{d_id} removed from the lease table")
    return result.rowcount


def create_or_update_lease(db: Session, lease: Lease, lease_data: dict):
    """
    Creates or updates lease information based on the provided data.
    """

    # Update the Lease object with the provided data
    lease.lease_id = lease_data.get("lease_id", lease.lease_id)

    lease_id_check= db.query(Lease).filter(Lease.lease_id==lease_data.get("lease_id")).first()

    if lease_id_check and lease_id_check.id != lease.id:
        raise ValueError("Lease ID already exists in the system")

    # If type changes then all the configurations need to be removed
    if lease_data.get("lease_type") != lease.lease_type:
        existing_configs = db.query(LeaseConfiguration).filter(
            LeaseConfiguration.lease_id == lease.id).all()

        if existing_configs:
            for config in existing_configs:
                db.delete(config)
            db.flush()

    lease.lease_type = lease_data.get("lease_type", lease.lease_type)

    lease.duration_in_weeks = lease_data.get(
        "total_weeks", lease.duration_in_weeks)
    lease.lease_start_date = (
        datetime.strptime(lease_data["lease_start_date"], "%Y-%m-%d").date()
        if lease_data.get("lease_start_date") else lease.lease_start_date
    )
    lease.lease_end_date = (
        datetime.strptime(lease_data["lease_end_date"], "%Y-%m-%d").date()
        if lease_data.get("lease_end_date") else lease.lease_end_date
    )
    lease.lease_pay_day = lease_data.get("pay_day", lease.lease_pay_day)
    lease.lease_payments_type = lease_data.get(
        "payments", lease.lease_payments_type)
    lease.is_auto_renewed = lease_data.get(
        "is_auto_renewal", lease.is_auto_renewed)
    lease.is_day_shift = lease_data.get(
        "is_day_shift", lease.is_day_shift)
    lease.cancellation_fee = lease_data.get(
        "cancellation_fee", lease.cancellation_fee)
    lease.lease_remark = lease_data.get(
        "lease_remark", lease.lease_remark)
    if "lease_status" in lease_data:
        lease.lease_status = lease_data.get("lease_status")
    if "is_active" in lease_data:
        lease.is_active = lease_data.get("is_active")
    db.flush()

    return lease


def get_lease_details(db: Session, lease: Lease):
    """
    Get the lease details for the driver lease step
    """
    medallion_details = format_medallion_response(
        db, lease.medallion)
    vehicle_vin = lease.vehicle.vin if lease.vehicle else None
    plate_number = ""  # lease.vehicle.registrations.plate_number if lease.vehicle and lease.vehicle.registrations else None
    vehicle_type = lease.vehicle.vehicle_type if lease.vehicle else None
    lease_type = lease.lease_type
    return {
        "medallion_number": medallion_details["medallion_number"] if medallion_details else None,
        "medallion_owner": medallion_details['medallion_owner_name'] if medallion_details else None,
        "vehicle_vin": vehicle_vin if vehicle_vin else None,
        "plate_number": plate_number if plate_number else None,
        "vehicle_type": vehicle_type if vehicle_type else None,
        "lease_type": lease_type if lease_type else None,
    }


def handle_dov_lease(db, lease_id: int, dov_data: DOVLease):
    """
    Handle the DOV lease
    """
    financials = dov_data.financialInformation.dict(exclude_none=True)

    for key, value in financials.items():
        existing_config = db.query(LeaseConfiguration).filter_by(
            lease_id=lease_id, lease_breakup_type=key
        ).first()

        if existing_config:
            existing_config.lease_limit = value
        else:
            new_config = LeaseConfiguration(
                lease_id=lease_id,
                lease_breakup_type=key,
                lease_limit=value,
            )
            db.add(new_config)


def handle_long_term_lease(db, lease_id: int, long_term_data: LongTermLease):
    """
    Handle the long term lease
    """
    financials = long_term_data.financialInformation.dict(exclude_none=True)

    for key, value in financials.items():
        existing_config = db.query(LeaseConfiguration).filter_by(
            lease_id=lease_id, lease_breakup_type=key
        ).first()

        if existing_config:
            existing_config.lease_limit = value
        else:
            new_config = LeaseConfiguration(
                lease_id=lease_id,
                lease_breakup_type=key,
                lease_limit=value,
            )
            db.add(new_config)


def handle_shift_lease(db, lease_id: int, shift_lease_data: ShiftLease):
    """
    Handle the shift lease - follows the same pattern as long term lease
    """
    financials = shift_lease_data.financialInformation.dict(exclude_none=True)

    for key, value in financials.items():
        existing_config = db.query(LeaseConfiguration).filter_by(
            lease_id=lease_id, lease_breakup_type=key
        ).first()

        if existing_config:
            existing_config.lease_limit = value
        else:
            new_config = LeaseConfiguration(
                lease_id=lease_id,
                lease_breakup_type=key,
                lease_limit=value,
            )
            db.add(new_config)


def handle_short_term_lease(db, lease_id: int, short_term_data: ShortTermLease):
    """
    Handle the short term lease
    """
    financials = short_term_data.financialInformation

    days_of_week = ["sun", "mon", "tus", "wen", "thu", "fri", "sat"]

    for day in days_of_week:
        day_info = financials.get(day)
        if not day_info:
            continue

        for shift_type in ["day_shift", "night_shift"]:
            lease_breakup_type = f"{day}_{shift_type}"
            lease_limit = day_info.get(
                "day_shift" if shift_type == "day_shift" else "night_shift", "")

            if lease_limit is None:
                continue

            existing_config = db.query(LeaseConfiguration).filter_by(
                lease_id=lease_id, lease_breakup_type=lease_breakup_type
            ).first()

            if existing_config:
                existing_config.lease_limit = lease_limit
            else:
                new_config = LeaseConfiguration(
                    lease_id=lease_id,
                    lease_breakup_type=lease_breakup_type,
                    lease_limit=lease_limit,
                )
                db.add(new_config)


def handle_medallion_only_lease(
        db,
        lease_id: int,
        medallion_data: MedallionOnlyLease
):
    """
    Handle the medallion only lease
    """
    financials = medallion_data.financialInformation.dict(exclude_none=True)

    for key, value in financials.items():
        existing_config = db.query(LeaseConfiguration).filter_by(
            lease_id=lease_id, lease_breakup_type=key
        ).first()

        if existing_config:
            existing_config.lease_limit = value
        else:
            new_config = LeaseConfiguration(
                lease_id=lease_id,
                lease_breakup_type=key,
                lease_limit=value,
            )
            db.add(new_config)


def create_or_update_lease_configurations(db, lease_id: int, lease_data: dict):
    """
    Create or update lease configurations based on lease data.

    Args:
        db: SQLAlchemy database session.
        lease_id: ID of the lease associated.
        lease_data: Parsed lease data dictionary containing `leaseType` and `financialInformation`.

    Raises:
        ValueError: If the lease type is not recognized.
    """
    lease_type = lease_data.get("leaseType")

    lease = db.query(Lease).filter(
        Lease.id == lease_id).first()

    if lease.lease_type != lease_type:
        raise ValueError("Lease type does not match with this lease id")

    if lease_type == LeaseType.DOV.value:
        dov_data = DOVLease(**lease_data)
        handle_dov_lease(db, lease_id, dov_data)

    elif lease_type == LeaseType.LONG_TERM.value:
        long_term_data = LongTermLease(**lease_data)
        handle_long_term_lease(db, lease_id, long_term_data)

    elif lease_type == LeaseType.SHIFT.value:
        shift_lease_data = ShiftLease(**lease_data)
        handle_shift_lease(db, lease_id, shift_lease_data)

    elif lease_type == LeaseType.SHORT_TERM.value:
        short_term_data = ShortTermLease(**lease_data)
        handle_short_term_lease(db, lease_id, short_term_data)

    elif lease_type == LeaseType.MEDALLION.value:
        medallion_data = MedallionOnlyLease(**lease_data)
        handle_medallion_only_lease(db, lease_id, medallion_data)

    else:
        raise ValueError(f"Invalid lease type: {lease_type}")

    db.flush()


def get_lease_configurations(db, lease_id: int):
    """
    Retrieve lease configurations for a given lease ID.

    Args:
        db: SQLAlchemy database session.
        lease_id: ID of the lease.

    Returns:
        A dictionary containing lease details and configurations.
    """
    # Fetch the lease
    lease = db.query(Lease).filter(
        Lease.id == lease_id).first()
    if not lease:
        raise ValueError(f"No lease found with ID: {lease_id}")

    # Fetch lease configurations
    configurations = db.query(LeaseConfiguration).filter(
        LeaseConfiguration.lease_id == lease_id).all()

    # Prepare the response
    response = {
        "lease_id": lease.lease_id,
        "lease_type": lease.lease_type if lease.lease_type else "",
        'total_weeks': lease.duration_in_weeks,
        "medallion_id": lease.medallion.medallion_number if lease.medallion else "",
        "vehicle_id": lease.vehicle.vin if lease.vehicle else "",
        "lease_start_date": lease.lease_start_date.isoformat() if lease.lease_start_date else "",
        "lease_end_date": lease.lease_end_date.isoformat() if lease.lease_end_date else "",
        "is_auto_renewed": lease.is_auto_renewed,
        "is_day_shift": lease.is_day_shift,
        "lease_remark": lease.lease_remark,
        "configurations": [
            {
                "lease_breakup_type": config.lease_breakup_type,
                "lease_limit": config.lease_limit,
            }
            for config in configurations
        ],
    }

    return response


def search_driver(db, ssn=None, tlc_license_number=None, dmv_license_number=None):
    """
    Search for a driver
    """
    query = db.query(Driver).join(TLCLicense,
                                             TLCLicense.id == Driver.tlc_license_number_id).join(DMVLicense, DMVLicense.id == Driver.dmv_license_number_id)

    match_criteria = []

    if ssn:
        query = query.filter(Driver.ssn == ssn)
        match_criteria.append("SSN")

    if tlc_license_number:
        query = query.filter(
            TLCLicense.tlc_license_number == tlc_license_number)
        match_criteria.append("TLC License Number")

    if dmv_license_number:
        query = query.filter(
            DMVLicense.dmv_license_number == dmv_license_number)
        match_criteria.append("DMV License Number")

    # print(query.statement.compile(dialect=sqlite.dialect()))
    driver = query.first()
    return driver


def update_lease_driver_info(
    db: Session,
    lease: Lease,
    driver_update_info: dict
):
    """
    Add or remove drivers associated with a lease in the LeaseDriver table.

    Args:
        db (Session): The database session.
        lease (Lease): The lease object to associate or disassociate the driver with.
        driver_update_info (dict): Dictionary containing:
            - driver_id (int): ID of the driver.
            - is_day_shift (bool): Indicates if day-night shift applies (only for add).
            - operation (str): "add" or "remove".

    Returns:
        str: Success message indicating the operation performed.
    """
    driver_id = driver_update_info.get("driver_id")
    is_day_night_shift = driver_update_info.get("is_day_night_shift")
    co_lease_seq = driver_update_info.get("co_lease_seq")

    valid_driver = db.query(Driver).filter(
        Driver.driver_id == driver_id).first()

    if not valid_driver:
        raise ValueError(f"Driver ID {driver_id} passed is invalid")

    if is_day_night_shift is None:
        driver_role = "L"
    elif is_day_night_shift:
        driver_role = "DL"
    else:
        driver_role = "NL"

    # Check if the driver already exists in the LeaseDriver table
    lease_driver = db.query(LeaseDriver).filter(
        LeaseDriver.driver_id == driver_id,
        LeaseDriver.lease_id == lease.id
    ).first()

    if lease_driver:
        # Update existing record
        lease_driver.is_day_night_shift = is_day_night_shift
        lease_driver.co_lease_seq = co_lease_seq
    else:
        # Create a new record
        lease_driver = LeaseDriver(
            driver_id=driver_id,
            lease_id=lease.id,
            driver_role=driver_role,
            is_day_shift=is_day_night_shift,
            co_lease_seq=co_lease_seq,
            date_added=datetime.utcnow()
        )
        db.add(lease_driver)
    db.flush()
    return f"Driver {driver_id} added or updated successfully for lease {lease.lease_id}."


def generate_medallion_lease_document(db: Session, lease: Lease):

    for driver in lease.lease_driver:
        # Prepare payload for Lambda function
        payload = {
            "data": prepare_medallion_lease_document(db),
            "identifier": f"form_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            "template_id": settings.medallion_lease_template_id,
            "bucket": settings.s3_bucket_name
        }

        logger.info("Calling Lambda function with payload: %s", payload)

        response = invoke_lambda_function(
            function_name="pdf_filler",
            payload=payload
        )


        if not response or 'statusCode' in response and response['statusCode'] != 200:
            error_message = response.get('body', str(
                response)) if response else "No response from Lambda"
            logger.error("Invalid response from Lambda: %s", error_message)
            raise ValueError(
                f"Failed to process PDF document: {error_message}")

        # Extract s3_key from response
        logger.info("Response from Lambda: %s", response)
        response_body = json.loads(response["body"])
        s3_key = response_body.get("s3_key")  # Use the output key we specified

        file = ("driver_medallion_lease.pdf",s3_utils.download_file(s3_key))

        medallion = medallion_service.get_medallion(db , medallion_id=lease.medallion_id)

        medallion_owner= format_medallion_response(db , medallion)
        email = getattr(medallion_owner, "primary_email_address", "") if medallion_owner else ""

        send_email(db=db,attachments=[file] ,to_email=email, subject="driver_medallion_lease",body="Please find the attached storage receipt for your records.",medallion_number=medallion.medallion_number)

        # Create document records
        medallion_document = Document(
            document_date=datetime.now(),
            document_upload_date=datetime.now(),
            document_name=f"Medallion Lease Document for Lease ID {lease.lease_id} for Driver ID {driver.driver_id}",
            document_format="PDF",
            document_path=s3_key,
            document_type="driver_medallion_lease",
            object_type=f"co-leasee-{driver.co_lease_seq}",
            object_lookup_id=str(driver.id),
            document_note="Medallion lease document created"
        )
        db.add(medallion_document)
        db.flush()

        logger.info(
            f"Create the medallion lease document {medallion_document.id} for driver {driver.driver_id}")


def generate_vehicle_lease_document(db: Session, lease: Lease):

    for driver in lease.lease_driver:
        # Prepare payload for Lambda function
        payload = {
            "data": prepare_vehicle_lease_document(db),
            "identifier": f"form_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            "template_id": settings.vehicle_lease_template_id,
            "bucket": settings.s3_bucket_name
        }

        logger.info("Calling Lambda function with payload: %s", payload)

        response = invoke_lambda_function(
            function_name="pdf_filler",
            payload=payload
        )

        if not response or 'statusCode' in response and response['statusCode'] != 200:
            error_message = response.get('body', str(
                response)) if response else "No response from Lambda"
            logger.error("Invalid response from Lambda: %s", error_message)
            raise ValueError(
                f"Failed to process PDF document: {error_message}")

        # Extract s3_key from response
        logger.info("Response from Lambda: %s", response)
        response_body = json.loads(response["body"])
        s3_key = response_body.get("s3_key")  # Use the output key we specified

        file = ("driver_vehicle_lease.pdf",s3_utils.download_file(s3_key))
        
        driver = driver_service.get_drivers(db=db , driver_id=driver.driver_id)
        email = getattr(driver, "email_address", "") if driver else ""

        send_email(db=db,attachments=[file] ,to_email=email, subject="driver_medallion_lease",body="Please find the attached storage receipt for your records.",driver_id=driver.id)
        vehicle_document = Document(
            document_date=datetime.now(),
            document_upload_date=datetime.now(),
            document_name=f"Vehicle Lease Document for Lease ID {lease.lease_id} for Driver ID {driver.driver_id}",
            document_format="PDF",
            document_path=s3_key,
            document_type="driver_vehicle_lease",
            object_type=f"co-leasee-{driver.co_lease_seq}",
            object_lookup_id=str(driver.id),
            document_note="Vehicle lease document created."
        )
        db.add(vehicle_document)
        db.flush()
        logger.info(
            f"Create the vehicle lease document {vehicle_document.id} for driver {driver.driver_id}")


# def fetch_latest_driver_document_by_lease(db: Session, lease: Lease):
#     """
#     Fetches the latest driver document for each driver associated with the lease,
#     filtering by document type ('driver_medallion_lease' or 'driver_vehicle_lease')
#     and using the 'co_leasee-{co_lease_seq}' object type.

#     Parameters:
#         lease: The Lease object for which driver documents need to be fetched.
#         db: SQLAlchemy session object.

#     Returns:
#         A list of dictionaries containing:
#             - document_name
#             - has_front_desk_signed
#             - has_driver_signed
#             - document_type
#             - document_date
#             - file_size
#             - comments
#     """
#     # Fetch drivers associated with the lease
#     lease_drivers = db.query(LeaseDriver).filter(
#         LeaseDriver.lease_id == lease.id).all()

#     if not lease_drivers:
#         return {"message": "No drivers associated with this lease."}

#     result = []

#     for lease_driver in lease_drivers:
#         co_lease_seq = lease_driver.co_lease_seq
#         driver_id = lease_driver.driver_id

#         # Fetch the latest document for the driver
#         latest_document = (
#             db.query(Document)
#             .filter(
#                 Document.object_lookup_id == str(driver_id),
#                 Document.object_type == f"co-leasee-{co_lease_seq}",
#                 Document.document_type.in_(
#                     ["driver_medallion_lease", "driver_vehicle_lease"]),
#             )
#             .order_by(desc(Document.document_date))
#             .first()  # Get only the latest document
#         )

#         if latest_document:
#             result.append({
#                 "document_id": latest_document.id,
#                 "driver_id": lease_driver.driver_id,
#                 "document_name": latest_document.document_name,
#                 "has_front_desk_signed": None,
#                 "has_driver_signed": None,
#                 "document_type": latest_document.document_type,
#                 "document_date": latest_document.document_date,
#                 "file_size": latest_document.document_actual_size,
#                 "comments": latest_document.document_note,
#             })

#     return result


def create_or_update_lease_driver_documents(db: Session, lease: Lease):
    """
    Creates or updates records in the LeaseDriverDocuments table for the given driver IDs.

    Parameters:
        driver_ids (list): List of driver IDs.
        lease_id (int): The ID of the associated lease.
        db (Session): The SQLAlchemy database session.

    Returns:
        list: List of successfully created or updated documents with their details.
    """
    created_documents = []

    for lease_driver in lease.lease_driver:
        # Check if the document already exists for the driver and lease
        lease_document = (
            db.query(LeaseDriverDocument)
            .filter(
                LeaseDriverDocument.lease_driver_id == lease_driver.id,
                LeaseDriverDocument.is_active,
            )
            .first()
        )

        # If the lease document is there, it should be removed first and a new one added
        if lease_document:
            logger.info(
                "Marking this lease driver document %s as in active", lease_document.id)
            lease_document.is_active = False
            db.add(lease_document)
            db.flush()

        latest_docs = (
            db.query(Document)
            .filter(
                Document.object_lookup_id == str(lease_driver.id),
                Document.object_type == f"co-leasee-{lease_driver.co_lease_seq}",
                Document.document_type.in_(
                    ["driver_medallion_lease", "driver_vehicle_lease"]),
            )
            .order_by(desc(Document.document_date))
            .all()
        )

        for latest_document in latest_docs[:2]:

            # Send document for signature
            signature_details = send_document_for_signature(
                db, latest_document)

            envelope_id = signature_details['envelope_id']
            # Create a new document record
            lease_document = LeaseDriverDocument(
                lease_driver_id=lease_driver.id,
                document_envelope_id=envelope_id,
                has_frontend_signed=True, #TODO: This is true since we are not able to capture docusign status, change this to false when we are able to capture docusign status
                has_driver_signed=True, #TODO: This is true since we are not able to capture docusign status, change this to false when we are able to capture docusign status
                frontend_signed_date=None,
                driver_signed_date=None,
                created_on=datetime.utcnow(),
                updated_on=datetime.utcnow()
            )
            db.add(lease_document)

            # Add or update the document in the database
            db.flush()

            # Append details to the result
            created_documents.append({
                "driver_id": lease_driver.id,
                "lease_id": lease.id,
                "document_envelope_id": lease_document.document_envelope_id,
                "has_frontend_signed": lease_document.has_frontend_signed,
                "has_driver_signed": lease_document.has_driver_signed,
            })

    return created_documents


def update_lease_active(db:Session , lease:Lease):
    """
    Update the lease status to active
    """

    driver_lease= db.query(LeaseDriver).filter(
        LeaseDriver.lease_id==lease.id,
             LeaseDriver.is_active==True
                                                ).first()

    if not driver_lease:
        raise ValueError("No driver associated with this lease")
    
    driver= db.query(Driver).filter(Driver.driver_id==driver_lease.driver_id).first()

    if not driver:
        raise ValueError("No driver found with this lease")
    
    return driver
    

def fetch_latest_driver_document_status_by_lease(db: Session, lease: Lease):
    """
    """

    # Fetch drivers associated with the lease
    lease_drivers = db.query(LeaseDriver).filter(
        LeaseDriver.lease_id == lease.id).all()

    if not lease_drivers:
        return {"message": "No drivers associated with this lease."}

    result = []

    for lease_driver in lease_drivers:
        co_lease_seq = lease_driver.co_lease_seq
        driver_id = lease_driver.driver_id

        lease_driver_document = (
            db.query(LeaseDriverDocument)
            .filter(
                LeaseDriverDocument.lease_driver_id == lease_driver.id,
                LeaseDriverDocument.is_active,
            )
            .first()
        )

        # Fetch the latest document for the driver
        latest_docs = (
            db.query(Document)
            .filter(
                Document.object_lookup_id == str(lease_driver.id),
                Document.object_type == f"co-leasee-{co_lease_seq}",
                Document.document_type.in_(
                    ["driver_medallion_lease", "driver_vehicle_lease"]),
            )
            .order_by(desc(Document.document_date))
            .all()
        )

        signed_document_url = ""
        if lease_driver_document and lease_driver_document.document_envelope_id:

            # TODO: Call the signed document corresponding to this envelope id
            signed_document_url = "singed_document_url"

        for latest_document in latest_docs[:2]:
            result.append({
                "document_id": latest_document.id,
                "driver_id": lease_driver.driver_id,
                "document_name": latest_document.document_name,
                "is_sent_for_signature": True if lease_driver_document else False,
                "has_front_desk_signed": lease_driver_document.has_frontend_signed if lease_driver_document else None,
                "has_driver_signed": lease_driver_document.has_driver_signed if lease_driver_document else None,
                "document_envelope_id": lease_driver_document.document_envelope_id if lease_driver_document else None,
                "document_date": latest_document.document_date,
                "file_size": latest_document.document_actual_size if latest_document.document_actual_size else 0,
                "comments": latest_document.document_note,
                "document_type": latest_document.document_type.replace('_', ' '),
                "object-type": latest_document.object_type,
                "presigned_url": bat_utils.get_presigned_url(latest_document),
                "document_format": latest_document.document_format,
                "signed_document_url": signed_document_url,
                "document_created_on": latest_document.created_on
            })

    return result


def send_document_for_signature(db: Session, document: Document):
    """
    Send a document for signature
    """

    signature_packet = {
        "document_path": document.document_path,
        "recipients": [
            {
                "name": "Alkema",
                "sequence": 1,
                "markers": [
                    {
                        "anchorString": "authorized_agent_signature",
                        "anchorXOffset": "0",
                        "anchorYOffset": "0",
                        "anchorUnits": "pixels",
                        "anchorCaseSensitive": True,
                        "optional": False
                    }
                ]
            },
            {
                "name": "Michael Rahman",
                "sequence": 2,
                "markers": [
                    {
                        "anchorString": "driver_signature",
                        "anchorXOffset": "0",
                        "anchorYOffset": "0",
                        "anchorUnits": "pixels",
                        "anchorCaseSensitive": True,
                        "optional": False
                    }
                ]
            }
        ],
        "identifier": "string"
    }
    signature_url = settings.esign_envelope_creation_url
    try:
        signature_response = requests.post(
            signature_url, data=json.dumps(signature_packet), timeout=int(settings.docusign_envelope_timeout))
    except requests.exceptions.Timeout:
        logger.error("The request timed out")

    if signature_response.status_code == 200:
        return signature_response.json()
    else:
        if 'detail' in signature_response.json():
            logger.error(signature_response.json())
            raise ValueError("Signature could not be sent")
        raise ValueError("Error while send to signature")


def prepare_vehicle_lease_document(db: Session, medallion=None):

    vehicle_lease_document_info = {
        "date_of_lease_agreement": "2024-01-01",
        "manager": "Jane Smith",
        "driver_name": "John Doe",
        "address": "123 Main St, Apartment 4B, New York, NY 10001",
        "telephone": "+1-555-123-4567",
        "social_security": "123-45-6789",
        "dmv_license": "D1234567",
        "dmv_license_expiration": "2025-05-01",
        "hack_license": "H123456",
        "nyc_medallion_and_roof_light": "MR123456",
        "plate_number": "XYZ1234",
        "vehicle_make": "Toyota",
        "vin": "1HGBH41JXMN109186",
        "vehicle_year": "2023",
        "serial_number": "SN1234567890",
        "meter_make": "Wayne",
        "lease_term_commencement": "2024-01-01",
        "expiration": "2025-01-01",
        "vehicle_lease_payment": "800.00",
        "sales_tax_on_vehicle_purchase": "1500.00",
        "tlc_inspection_fee": "200.00",
        "tax_stamps": "50.00",
        "vehicle_registration": "V1234567890",
        "payment_due_day": "15th",
        "total_weekly_lease_payment": "200.00",
        "total_payment_for_lease_term": "20000.00",
        "vehicle_sales_price": "25000.00",
        "vehicle_lease_amount": "18000.00",
        "vehicle_lease_weeks": "52",
        "additional_balance_due": "500.00",
        "lease_id_number": "LN789012",
        "security_deposit": "1500.00",
        "security_deposit_held_at": "Bank of America",
        "security_deposit_location": "New York, NY",
        "security_deposit_account_number": "1234567890",
        "drivers_email": "john.doe@example.com",
        "authorized_agent_sign_date": "2024-01-02",
        "driver_sign_date": "2024-01-03"
    }
    return vehicle_lease_document_info


def prepare_medallion_lease_document(db: Session, medallion=None):

    medallion_lease_document_info = {
        "agent_sign_date": "2024-01-15",
        "driver_sign_date": "2024-01-16",
        "sign_lease_id_number": "L123456",
        "sign_driver_name": "John Doe",
        "sign_driver_email": "john.doe@example.com",
        "lease_id_number": "LN789012",
        "additional_balance_due": "500.00",
        "security_deposit": "1500.00",
        "payment_due_day": "15th",
        "total_payment_for_lease_term": "20000.00",
        "medallion_lease_payment": "800.00",
        "lease_expiration": "2025-01-15",
        "lease_term_commencement": "2024-01-01",
        "serial_number": "SN1234567890",
        "meter_make": "Wayne",
        "vehicle_year": "2023",
        "vehicle_model": "Camry",
        "vehicle_make": "Toyota",
        "nyc_medallion_and_roof_light": "MR123456",
        "plate_number": "XYZ1234",
        "hack_license": "H123456",
        "hack_license_expiration": "2025-01-01",
        "dmv_license": "D1234567",
        "dmv_license_expiration": "2025-05-01",
        "telephone": "+1-555-123-4567",
        "social_security": "123-45-6789",
        "address": "123 Main St, Apartment 4B, New York, NY 10001",
        "driver_name": "John Doe",
        "manager": "Jane Smith",
        "date_of_agreement": "2024-01-01"
    }
    return medallion_lease_document_info