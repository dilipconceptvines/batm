# Third party imports
# import pdb
import pandas as pd
from sqlalchemy.orm import Session

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.audit_trail.models import AuditTrail

# Import models to ensure they're available for SQLAlchemy relationship resolution
from app.dtr.models import DTR  # Import DTR to resolve Vehicle.dtrs relationship
from app.driver_payments.models import *
from app.deposits.models import *

from app.seeder.bat.parse_address import parse_address
from app.seeder.bat.parse_bank_accounts import parse_bank_accounts
from app.seeder.bat.parse_individuals import parse_individuals
from app.seeder.bat.parse_entity import parse_entity
from app.seeder.bat.parse_corporation import parse_corporation
from app.seeder.bat.parse_dealers import parse_dealers
from app.seeder.bat.parse_medallions import parse_medallions
from app.seeder.bat.parse_mo_lease import parse_mo_lease
from app.seeder.bat.parse_medallion_owner import parse_medallion_owner
from app.seeder.bat.parse_vehicles import parse_vehicles
from app.seeder.bat.parse_vehicle_hackups import parse_vehicle_hackup_information
from app.seeder.bat.parse_vehicle_registration import parse_vehicle_registration_information
from app.seeder.bat.parse_vehicle_inspections import parse_vehicle_inspection_information
from app.seeder.bat.parse_drivers import parse_drivers
from app.seeder.bat.parse_leases import parse_lease
from app.seeder.bat.parse_lease_driver import parse_lease_driver
from app.seeder.bat.parse_vehicle_entity import parse_vehicle_entity
from app.seeder.bat.parse_vehicle_expenses import parse_vehicle_expenses_and_compliance

logger = get_logger(__name__)

# Ordered list of sheet parsers
SHEET_PARSERS = {
    "address": parse_address,
    "bank_accounts": parse_bank_accounts,
    "Individual": parse_individuals,
    # "entity": parse_entity,
    "vehicle_entity": parse_vehicle_entity,
    "corporation": parse_corporation,
    # "medallion_owner": parse_medallion_owner,
    "dealers": parse_dealers,
    "medallion": parse_medallions,
    "drivers": parse_drivers,
    "vehicles": parse_vehicles,
    "vehicle_hackups": parse_vehicle_hackup_information,
    "vehicle_registration": parse_vehicle_registration_information,
    # "vehicle_inspections": parse_vehicle_inspection_information,
    "vehicle_expenses": parse_vehicle_expenses_and_compliance,
    "leases": parse_lease,
    "lease_driver": parse_lease_driver,
    # "mo_lease": parse_mo_lease,
}

def load_and_process_data(
        db: Session, key: str = settings.bat_file_key
) -> pd.ExcelFile:
    """Load data from S3"""
    try:
        # pdb.set_trace()
        data = s3_utils.download_file(key)
        excel_data = pd.ExcelFile(data)

        # Iterate over required sheets in the defined order
        for sheet_name, parser_func in SHEET_PARSERS.items():
            if sheet_name in excel_data.sheet_names:
                logger.info("Processing sheet: %s", sheet_name)
                sheet_df = excel_data.parse(sheet_name)
                parser_func(db, sheet_df)
            else:
                logger.warning("Sheet not found: %s", sheet_name)

        logger.info("All sheets processed successfully")
    except Exception as e:
        logger.error("Error loading data from S3: %s", e)
        raise e
    

if __name__ == "__main__":
    logger.info("Starting BAT Seeder")
    db_session = SessionLocal()
    load_and_process_data(db=db_session)
