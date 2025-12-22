# Standard library imports
from datetime import datetime, timezone

# Third party imports
import pandas as pd
from sqlalchemy.orm import Session

# Local imports
from app.core.config import settings
from app.core.db import SessionLocal
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from app.vehicles.models import Dealer

logger = get_logger(__name__)
SUPERADMIN_USER_ID = 1

def parse_date(date_str):
    """
    Parses a date string into a datetime object.
    If the string is invalid or empty, returns None.

    Args:
        date_str: The date string to parse

    Returns:
        The datetime object or None if the string is invalid or empty
    """
    try:
        return datetime.fromisoformat(date_str) if pd.notnull(date_str) else None
    except ValueError:
        return None

def parse_dealers(db: Session, df: pd.DataFrame):
    """Parse dealers"""
    try:
        for _, row in df.iterrows():
            # Check if the dealer already exists
            dealer = db.query(Dealer).filter(
                Dealer.dealer_name == row["dealer_name"]).first()

            new_dealer = None
            if dealer:
                # Update existing dealer record
                dealer.dealer_bank_name = row["dealer_bank_name"]
                dealer.dealer_bank_account_number = row["dealer_bank_account_number"]
            else:
                # Create new dealer record
                new_dealer = Dealer(
                    dealer_name=row["dealer_name"],
                    dealer_bank_name=row["dealer_bank_name"],
                    dealer_bank_account_number=row["dealer_bank_account_number"]
                )
                db.add(new_dealer)
        db.commit()
    except Exception as e:
        logger.error("Error parsing dealers: %s", e)
        db.rollback()
        raise


if __name__ == "__main__":
    db_session = SessionLocal()
    excel_file = pd.ExcelFile(s3_utils.download_file(settings.bat_file_key))
    dealers_df = pd.read_excel(excel_file, 'dealers')
    parse_dealers(db_session, dealers_df)
    db_session.close()

