from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict
import pandas as pd
import tempfile
import os

from app.core.data_loader_config import data_loader_settings
from app.utils.s3_utils import s3_utils
from app.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class RowResult:
    """Represents processing status for a single spreadsheet row."""
    row_index: int
    result: str  # inserted | updated | failed
    failed_reason: str = ""


@dataclass
class ParseResult:
    """Aggregate counters and row level outcomes for a parser."""
    sheet_name: str
    inserted_count: int = 0
    updated_count: int = 0
    failed_count: int = 0
    details: Dict[str, int] = field(default_factory=dict)
    row_results: List[RowResult] = field(default_factory=list)

    def record_inserted(self, row_index: int) -> None:
        self.inserted_count += 1
        self.row_results.append(RowResult(row_index=row_index, result="inserted"))

    def record_updated(self, row_index: int) -> None:
        self.updated_count += 1
        self.row_results.append(RowResult(row_index=row_index, result="updated"))

    def record_failed(self, row_index: int, reason: str) -> None:
        self.failed_count += 1
        self.row_results.append(
            RowResult(row_index=row_index, result="failed", failed_reason=reason)
        )

    def summary(self) -> Dict[str, int]:
        return {
            "inserted": self.inserted_count,
            "updated": self.updated_count,
            "failed": self.failed_count,
        }

    def result(self) -> Dict[str, int]:
        return {
            "sheet_name": self.sheet_name,
            "inserted": self.inserted_count,
            "updated": self.updated_count,
            "failed": self.failed_count,
            "details": self.details,
            "row_results": self.row_results,
        }

    def add_detail(self, key: str, value: int) -> None:
        self.details[key] = value

    @property
    def inserted(self) -> set[int]:
        return {r.row_index for r in self.row_results if r.result == "inserted"}

    @property
    def updated(self) -> set[int]:
        return {r.row_index for r in self.row_results if r.result == "updated"}

    @property
    def failed(self) -> Dict[int, str]:
        return {
            r.row_index: r.failed_reason
            for r in self.row_results
            if r.result == "failed"
        }


def apply_parse_result_to_df(df: pd.DataFrame, result: ParseResult) -> pd.DataFrame:
    """
    Applies the ParseResult to the DataFrame by adding 'result' and 'failed_reason' columns.
    """

    # Ensure columns exist
    if "result" not in df.columns:
        df["result"] = ""
    if "failed_reason" not in df.columns:
        df["failed_reason"] = ""

    inserted_indices = set(result.inserted)
    updated_indices = set(result.updated)
    failed_indices = result.failed  # dict: row_index -> reason

    # Default state
    df["result"] = "skipped"
    df["failed_reason"] = ""

    index_series = df.index.to_series()

    is_inserted = index_series.isin(inserted_indices)
    is_updated = index_series.isin(updated_indices)
    is_failed = index_series.isin(failed_indices.keys())

    df.loc[is_inserted, "result"] = "inserted"
    df.loc[is_updated, "result"] = "updated"
    df.loc[is_failed, "result"] = "failed"

    # ✅ SAFE mapping (no hashing issues)
    df.loc[is_failed, "failed_reason"] = index_series.map(
        lambda i: failed_indices.get(i, "")
    )

    return df

def generate_summary(df: pd.DataFrame, result: dict, parser_name: str) -> pd.DataFrame:
    """
    Update or insert summary row for a parser.

    Columns:
    sheet | inserted | updated | failed
    """

    try:
        # Ensure required columns exist
        required_columns = ["sheet", "inserted", "updated", "failed"]
        for col in required_columns:
            if col not in df.columns:
                df[col] = pd.Series(dtype="object")

        # Check if model already exists
        if parser_name in df["sheet"].values:
            # Update existing row
            df.loc[df["sheet"] == parser_name, ["inserted", "updated", "failed"]] = [
                result.get("inserted", 0),
                result.get("updated", 0),
                result.get("failed", 0)
            ]
        else:
            # Insert new row
            df.loc[len(df)] = {
                "sheet": parser_name,
                "inserted": result.get("inserted", 0),
                "updated": result.get("updated", 0),
                "failed": result.get("failed", 0)
            }

        return df

    except Exception as e:
        logger.error("Error generating summary: %s", e, exc_info=True)
        raise


def generate_report_key(key):

    dir_path = os.path.dirname(key)

    original_filename = os.path.basename(key)
    base_name, ext = os.path.splitext(original_filename)
    new_filename = f"{base_name}_result{ext}"           # test_parse_result.xlsx

    result_key = os.path.join(dir_path, new_filename)

    try:
         # 1️⃣ Create temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
            tmp_file_path = tmp_file.name

        # 2️⃣ Download from S3
        file_bytes = s3_utils.download_file(key)
        if not file_bytes:
            raise Exception("Failed to download file from S3")

        with open(tmp_file_path, "wb") as f:
            f.write(file_bytes)

        with open(tmp_file_path, "rb") as f:
            s3_utils.upload_file(f, result_key)

        return result_key
    except Exception as e:
        logger.error("Error creating report: %s", e)
        return e
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)

def create_report(parse: str, key: str, result: ParseResult):
    tmp_file_path = None
    try:
        # 1️⃣ Create temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
            tmp_file_path = tmp_file.name

        # 2️⃣ Download from S3
        file_bytes = s3_utils.download_file(key)
        if not file_bytes:
            raise Exception("Failed to download file from S3")

        with open(tmp_file_path, "wb") as f:
            f.write(file_bytes)

        # 3️⃣ READ SHEET (important: no pd.ExcelFile)
        data_df = pd.read_excel(tmp_file_path, sheet_name=parse)

        # 4️⃣ Apply parse result
        updated_df = apply_parse_result_to_df(data_df, result)

        # 5️⃣ WRITE BACK (replace sheet)
        with pd.ExcelWriter(
            tmp_file_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            updated_df.to_excel(writer, sheet_name=parse, index=False)

        with open(tmp_file_path, "rb") as f:
            s3_utils.upload_file(f, key)

        return True

    except Exception as e:
        logger.error("Error creating report: %s", e)
        return False

    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)


def write_summary_to_excel(key: str, df: pd.DataFrame):
    tmp_file_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp_file_path = tmp.name

        file_bytes = s3_utils.download_file(key)
        with open(tmp_file_path, "wb") as f:
            f.write(file_bytes)

        with pd.ExcelWriter(
            tmp_file_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            df.to_excel(writer, sheet_name="summary", index=False)

        with open(tmp_file_path, "rb") as f:
            s3_utils.upload_file(f, key)

    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
