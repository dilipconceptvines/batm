### app/utils/exporter/excel_exporter.py

# Standard library imports
import uuid
from io import BytesIO

# Third party imports
import pandas as pd

# Local imports
from app.utils.exporter.base import DataExportBase


class ExcelExporter(DataExportBase):
    """Export data to Excel file"""

    def export(self) -> BytesIO:
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            sheet_name = f"Report-{str(uuid.uuid4())[:8]}"
            self.df.to_excel(writer, index=False, sheet_name=sheet_name)
            ws = writer.book[sheet_name]
            ws.auto_filter.ref = ws.dimensions
        output.seek(0)
        return output