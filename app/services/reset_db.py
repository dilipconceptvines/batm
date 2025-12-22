### app/services/reset_db.py

# Standard library imports
import subprocess
import sys
from typing import Dict

# Third party library imports
from sqlalchemy import text

# Local imports
from app.utils.logger import get_logger
from app.core.db import engine

logger = get_logger(__name__)

def reset_all_tables():
    """Drop all the tables"""
    with engine.begin() as conn:
        # Disable foreign key checks temporarily
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))

        # Fetch all tables
        result = conn.execute(text("SHOW TABLES;"))
        tables = [row[0] for row in result.fetchall()]

        # Drop all tables
        for table in tables:
            conn.execute(text(f"DROP TABLE IF EXISTS `{table}`;"))
        
        # Re-enable FK checks
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

        print(f"✅ Successfully dropped {len(tables)} tables.")

def reset_db() -> Dict[str, str]:
    """
    Reset the database to a clean state using Alembic.
    """
    logger.info("🔧 Starting database reset...")
    
    # Run alembic downgrade to base (this will drop all tables)
    logger.info("🗑️ Dropping all tables...")
    reset_all_tables()
    
    # Run alembic upgrade to head (this will recreate all tables)
    logger.info("🔄 Recreating all tables...")
    subprocess.run(["alembic", "upgrade", "head"], check=True)
    
    # Run seeders
    logger.info("------------  Seeding the BPM data ...  --------------------")
    subprocess.run([sys.executable, "-m", "app.seeder.bpm.seeder"], check=True)
    logger.info("------------  Seeding the BAT data ...  --------------------")
    subprocess.run([sys.executable, "-m", "app.seeder.bat.seeder"], check=True)
    logger.info("--------------------------------")
    
    # logger.info("✅ Database reset successfully")
    return {"status": "success", "message": "Database reset successfully"}

if __name__ == "__main__":
    reset_db()