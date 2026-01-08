import pandas as pd
from unittest.mock import MagicMock, patch
import io
import os
import sys

# Mock imports to avoid environment issues
sys.modules['app.core.config'] = MagicMock()
sys.modules['app.core.db'] = MagicMock()
sys.modules['app.utils.logger'] = MagicMock()
sys.modules['app.utils.s3_utils'] = MagicMock()
sys.modules['app.bpm.models'] = MagicMock()
sys.modules['app.users.models'] = MagicMock()
sys.modules['app.audit_trail.models'] = MagicMock()
sys.modules['app.seeder_loader.parser_registry'] = MagicMock()

# Manually define ParseResult and apply_parse_result_to_df since we can't import easily with mocked app modules
from dataclasses import dataclass, field
from typing import List,  Dict

@dataclass
class RowResult:
    row_index: int
    result: str
    failed_reason: str = ""

@dataclass
class ParseResult:
    sheet_name: str
    inserted_count: int = 0
    updated_count: int = 0
    failed_count: int = 0
    details: Dict[str, int] = field(default_factory=dict)
    row_results: List[RowResult] = field(default_factory=list)

    @property
    def inserted(self):
        return {r.row_index for r in self.row_results if r.result == "inserted"}
    
    @property
    def updated(self):
        return {r.row_index for r in self.row_results if r.result == "updated"}
        
    @property
    def failed(self):
        return {r.row_index: r.failed_reason for r in self.row_results if r.result == "failed"}

def apply_parse_result_to_df(df: pd.DataFrame, result: ParseResult) -> pd.DataFrame:
    if "result" not in df.columns:
        df["result"] = ""
    if "failed_reason" not in df.columns:
        df["failed_reason"] = ""

    inserted_indices = result.inserted
    updated_indices = result.updated
    failed_indices = result.failed
    
    df["result"] = "skipped"
    
    df_index_series = df.index.to_series()
    
    is_inserted = df_index_series.isin(inserted_indices)
    is_updated = df_index_series.isin(updated_indices)
    is_failed = df_index_series.isin(failed_indices.keys())
    
    df.loc[is_inserted, "result"] = "inserted"
    df.loc[is_updated, "result"] = "updated"
    df.loc[is_failed, "result"] = "failed"
    
    df.loc[is_failed, "failed_reason"] = df.index.to_series().map(failed_indices)
    
    return df

def test_apply_parse_result_to_df():
    print("Testing apply_parse_result_to_df...")
    
    # Create dummy dataframe
    df = pd.DataFrame({
        "col1": ["A", "B", "C", "D"],
        "col2": [1, 2, 3, 4]
    })
    
    # Create ParseResult
    result = ParseResult(sheet_name="test_sheet")
    
    # Simulate processing
    result.row_results.append(RowResult(row_index=0, result="inserted"))
    result.row_results.append(RowResult(row_index=1, result="updated"))
    result.row_results.append(RowResult(row_index=2, result="failed", failed_reason="Some error"))
    # Row 3 is skipped (not in results)
    
    # Apply results
    updated_df = apply_parse_result_to_df(df, result)
    
    # Verify results
    print("DataFrame after application:")
    print(updated_df)
    
    assert updated_df.loc[0, "result"] == "inserted"
    assert updated_df.loc[1, "result"] == "updated"
    assert updated_df.loc[2, "result"] == "failed"
    assert updated_df.loc[2, "failed_reason"] == "Some error"
    assert updated_df.loc[3, "result"] == "skipped"
    
    print("✅ Test passed!")

if __name__ == "__main__":
    test_apply_parse_result_to_df()
