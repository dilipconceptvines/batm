from sqlalchemy import text
from app.core.db import engine
from app.utils.logger import get_logger
from app.core.data_loader_config import data_loader_settings
import subprocess

logger = get_logger(__name__)

def truncate_tables(type: str):
    with engine.begin() as conn:
        # Disable FK checks
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))

        if type == "bat":
            tables = data_loader_settings.bat_tables

        elif type == "bpm":
            tables = data_loader_settings.bpm_tables

        elif type == "all":
            result = conn.execute(text("SHOW TABLES;"))
            tables = [row[0] for row in result.fetchall()]

        else:
            logger.error("❌ Invalid type. Use 'bat', 'bpm', or 'all'.")
            return False

        for table in tables:
            logger.info(f"Truncating table: {table}")
            conn.execute(text(f"TRUNCATE TABLE `{table}`;"))

        # Re-enable FK checks
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

        logger.info(f"✅ Successfully truncated {len(tables)} tables ({type}).")

    return True





if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Drop database tables")
    parser.add_argument(
        "--type",
        required=True,
        choices=["bpm", "bat", "all"],
        help="Which tables to drop"
    )

    parser.add_argument(
    "--recreate",
    action="store_true",
    help="Recreate tables using alembic"
    )


    args = parser.parse_args()

    if args.type == "all":
        confirm = input("⚠️ This will DROP ALL TABLES. Type YES: ")
        if confirm != "YES":
            print("Aborted.")
            exit(1)

    truncate_tables(args.type)

    if args.recreate:
        logger.info("🔄 Recreating all tables...")
        subprocess.run(["alembic", "upgrade", "head"], check=True)
        logger.info("✅ Successfully recreated tables using alembic.")

        