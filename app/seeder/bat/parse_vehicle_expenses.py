# Standard library imports
from datetime import datetime

# Third party imports
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Local imports
from app.core.db import SessionLocal
from app.core.config import settings
from app.core.data_loader_config import data_loader_settings
from app.utils.s3_utils import s3_utils
from app.utils.logger import get_logger
from app.vehicles.models import Vehicle , VehicleExpensesAndCompliance , VehicleInspection , VehicleInsurance
from app.vehicles.schemas import ExpensesAndComplianceSubType
from app.utils.general import get_safe_value
from app.seeder.parsing_result import ParseResult, apply_parse_result_to_df
from app.seeder_loader.parser_registry import parser
import tempfile
import os

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

@parser(
    name="vehicle_expenses",
    sheet_names=[data_loader_settings.parser_vehicle_expenses_sheet],
    version="1.0",
    deprecated=False,
    description="Process vehicle expenses from Excel sheet"
)
def parse_vehicle_expenses_and_compliance(db: Session, df: pd.DataFrame) -> ParseResult:
    """
    Parses the vehicle hackup information from the excel file and upserts the data into the database.
    """
    result = ParseResult(sheet_name="vehicle_expenses")
    created_expenses = 0
    updated_expenses = 0
    
    try:
        for idx, row in df.iterrows():
            try:
                vehicle_vin = get_safe_value(row , "vin")
                category = get_safe_value(row , "category")
                sub_type = get_safe_value(row , "sub_type")
                amount = get_safe_value(row , "amount")
                issue_date = get_safe_value(row , "issue_date")
                expiry_date = get_safe_value(row , "expiry_date")
                specific_info = get_safe_value(row , "specific_info")
                note = get_safe_value(row , "note")
                status = get_safe_value(row , "status")

                if not vehicle_vin:
                    logger.warning("Skipping row with missing VIN")
                    result.record_failed(idx, "Missing VIN")
                    continue

                #get vehicle info
                vehicle = db.query(Vehicle).filter_by(vin=vehicle_vin).first()

                if not vehicle:
                    logger.warning("No vehicle found for VIN: %s. Skipping.", vehicle_vin)
                    result.record_failed(idx, "Vehicle not found")
                    continue

                if not category or not sub_type:
                    logger.info("category and sub type both are required")
                    result.record_failed(idx, "Missing category or sub_type")
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
                created_expenses += 1
                result.record_inserted(idx)

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
            except Exception as row_error:
                logger.exception("Error parsing vehicle expense row %s: %s", idx, row_error)
                result.record_failed(idx, str(row_error))

        logger.info("✅ Data successfully processed.")
        
        return result
    except Exception as e:
        logger.exception("Critical failure in parser vehicle_expenses: %s", e)
        raise RuntimeError(f"Parser vehicle_expenses failed: {e}") from e

if __name__ == "__main__":
    logger.info("Loading vehicle expenses configuration")
    db_session = SessionLocal()

    tmp_file_path = None
    try:
        # Download file to temp
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
            tmp_file_path = tmp_file.name
            
        file_bytes = s3_utils.download_file(settings.bat_file_key)
        if not file_bytes:
             raise Exception("Failed to download file from S3")
        
        with open(tmp_file_path, 'wb') as f:
            f.write(file_bytes)
            
        excel_file = pd.ExcelFile(tmp_file_path)
        data_df = pd.read_excel(excel_file, "vehicle_expenses")

        result = parse_vehicle_expenses_and_compliance(db_session, data_df)
        
        # Apply results
        updated_df = apply_parse_result_to_df(data_df, result)

        # Write back to temp file
        with pd.ExcelWriter(
            tmp_file_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            updated_df.to_excel(writer, sheet_name="vehicle_expenses", index=False)
            
        # Upload back to S3
        with open(tmp_file_path, 'rb') as f:
            s3_utils.upload_file(f, settings.bat_file_key)

        db_session.commit()
        logger.info("Vehicle expenses committed successfully")
    except IntegrityError:
        db_session.rollback()
        logger.error("Session could not be committed due to integrity error")
    except Exception as e:
        db_session.rollback()
        logger.error("Error processing vehicle expenses: %s", e)
        raise
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        db_session.close()

