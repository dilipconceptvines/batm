# Standard library imports
from datetime import datetime

# Third party imports
import pandas as pd
from sqlalchemy.orm import Session

# Local imports
from app.core.db import SessionLocal
from app.core.config import settings
from app.utils.s3_utils import s3_utils
from app.utils.logger import get_logger
from app.vehicles.models import Vehicle , VehicleExpensesAndCompliance , VehicleInspection , VehicleInsurance
from app.vehicles.schemas import ExpensesAndComplianceSubType
from app.utils.general import get_safe_value


logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def parse_vehicle_expenses_and_compliance(db: Session, df: pd.DataFrame):
    """
    Parses the vehicle hackup information from the excel file and upserts the data into the database.
    """

    try:
        for _, row in df.iterrows():
            vehicle_vin = get_safe_value(row , "vin")
            category = get_safe_value(row , "category")
            sub_type = get_safe_value(row , "sub_type")
            amount = get_safe_value(row , "amount")
            issue_date = get_safe_value(row , "issue_date")
            expiry_date = get_safe_value(row , "expiry_date")
            specific_info = get_safe_value(row , "specific_info")
            note = get_safe_value(row , "note")
            status = get_safe_value(row , "status")

            #get vehicle info
            vehicle = db.query(Vehicle).filter_by(vin=vehicle_vin).first()

            if not vehicle:
                logger.warning("No vehicle found for VIN: %s. Skipping.", vehicle_vin)
                continue

            if not category or not sub_type:
                logger.info("category and sub type both are required")
                continue

            vehicle_id = vehicle.id

            vehicle_expense = VehicleExpensesAndCompliance(
                vehicle_id=vehicle_id,
                category=category,
                sub_type=sub_type,
                amount=amount,
                issue_date=issue_date,
                expiry_date=expiry_date,
                specific_info=specific_info,
                note=note
            )
            db.add(vehicle_expense)

            if category == "inspections_and_compliance":
                allowed_inspection_types = [
                    ExpensesAndComplianceSubType.MILE_RUN_INSPECTION.value,
                    ExpensesAndComplianceSubType.TLC_INSPECTION.value,
                    ExpensesAndComplianceSubType.DMV_INSPECTION.value
                    ]
                allowed_insurance_type = [
                    ExpensesAndComplianceSubType.Worker_Compensation_Insurance.value,
                    ExpensesAndComplianceSubType.Liability_Insurance.value
                ]
                if sub_type in allowed_inspection_types:

                    inspection = VehicleInspection(
                        vehicle_id=vehicle_id,
                        mile_run= True if sub_type == ExpensesAndComplianceSubType.MILE_RUN_INSPECTION.value else False,
                        inspection_type=sub_type if sub_type else ExpensesAndComplianceSubType.MILE_RUN_INSPECTION.value,
                        inspection_date=issue_date,
                        next_inspection_due_date=expiry_date,
                        inspection_fee=amount,
                        status="Active"
                    )
                    db.add(inspection)
                    
                if sub_type in allowed_insurance_type:
                    insurance_number = note.split(":", 1)[1].strip() if note and ":" in note else None
                    insurance = VehicleInsurance(
                        vehicle_id=vehicle_id,
                        insurance_type=sub_type,
                        insurance_number=insurance_number,
                        insurance_start_date=issue_date,
                        insurance_end_date=expiry_date,
                        amount=amount,
                        status="Active"
                    )
                    db.add(insurance)

            logger.info(f'importing vehicle expenses and compliance for {vehicle_vin} with category {category} and sub type {sub_type}')
            db.flush()

        db.commit()    
        logger.info("✅ Data successfully processed.")
    except Exception as e:
        db.rollback()
        logger.error("Error parsing data: %s", e)
        raise
    finally:
        db.close()

if __name__ == "__main__":
    logger.info("Loading Entity information")
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bat_file_key))
    entity_df = pd.read_excel(excel_file, 'vehicle_expenses')
    parse_vehicle_expenses_and_compliance(db_session, entity_df)

