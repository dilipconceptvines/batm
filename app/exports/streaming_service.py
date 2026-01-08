### app/exports/streaming_service.py

"""
Streaming Export Service - Memory-Efficient Export Generation

This service uses SQLAlchemy streaming queries and incremental file writing
to handle large datasets without memory exhaustion.

CRITICAL: Uses MySQL-specific streaming patterns with yield_per() and stream_results.
"""

import csv
import json
from datetime import datetime
from decimal import Decimal
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, Side
from openpyxl.writer.excel import save_workbook
from sqlalchemy.orm import Query

from app.utils.logger import get_logger

logger = get_logger(__name__)


class StreamingExportService:
    """
    Service for streaming large query results directly to files.
    
    Uses memory-efficient patterns:
    - SQLAlchemy yield_per() for batched streaming
    - Write-only mode for Excel (openpyxl)
    - Incremental CSV writing
    - No accumulation of data in memory
    """
    
    def __init__(self, output_dir: str = "/tmp/exports"):
        """
        Initialize streaming export service.
        
        Args:
            output_dir: Directory to write export files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"StreamingExportService initialized with output_dir: {output_dir}")
    
    def stream_query_to_file(
        self,
        query: Query,
        filename: str,
        export_format: str,
        row_transformer: callable,
        headers: Optional[List[str]] = None,
        batch_size: int = 1000
    ) -> tuple[str, int]:
        """
        Stream query results to file in specified format.
        
        Args:
            query: SQLAlchemy query object (not executed)
            filename: Base filename (extension added automatically)
            export_format: Format (excel, csv, pdf, json)
            row_transformer: Function to transform ORM object to dict
            headers: Column headers (inferred from first row if None)
            batch_size: Number of rows to process per batch
            
        Returns:
            Tuple of (file_path, record_count)
        """
        logger.info(f"Starting streaming export: {filename}.{export_format}")
        
        # Enable MySQL streaming on query
        query = query.execution_options(stream_results=True)
        
        # Route to appropriate format handler
        if export_format == "excel":
            return self._stream_to_excel(query, filename, row_transformer, headers, batch_size)
        elif export_format == "csv":
            return self._stream_to_csv(query, filename, row_transformer, headers, batch_size)
        elif export_format == "json":
            return self._stream_to_json(query, filename, row_transformer, batch_size)
        elif export_format == "pdf":
            # PDF requires all data in memory, so we'll limit and warn
            return self._stream_to_pdf(query, filename, row_transformer, headers, batch_size)
        else:
            raise ValueError(f"Unsupported export format: {export_format}")
    
    def _stream_to_excel(
        self,
        query: Query,
        filename: str,
        row_transformer: callable,
        headers: Optional[List[str]],
        batch_size: int
    ) -> tuple[str, int]:
        """
        Stream query results to Excel file using write-only mode.
        
        Uses openpyxl write-only mode to avoid loading entire sheet in memory.
        """
        filepath = self.output_dir / f"{filename}.xlsx"
        logger.info(f"Creating Excel file: {filepath}")
        
        # Create workbook in write-only mode
        workbook = Workbook(write_only=True)
        sheet = workbook.create_sheet()
        
        # Style for header
        header_font = Font(bold=True)
        header_alignment = Alignment(horizontal="center", vertical="center")
        thin_border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        
        record_count = 0
        headers_written = False
        
        try:
            # Stream results in batches
            for batch in self._batch_query_results(query, batch_size):
                for row_obj in batch:
                    # Transform ORM object to dict
                    row_dict = row_transformer(row_obj)
                    
                    # Write headers from first row if not provided
                    if not headers_written:
                        if headers is None:
                            headers = list(row_dict.keys())
                        
                        # Write header row with styling
                        header_cells = []
                        for header in headers:
                            cell = self._create_styled_cell(header, header_font, header_alignment, thin_border)
                            header_cells.append(header)  # Write-only mode doesn't support cell objects
                        sheet.append(header_cells)
                        headers_written = True
                    
                    # Write data row
                    data_row = [self._serialize_value(row_dict.get(h, "")) for h in headers]
                    sheet.append(data_row)
                    record_count += 1
                    
                    # Log progress
                    if record_count % 10000 == 0:
                        logger.info(f"Processed {record_count} records...")
            
            # Save workbook to file
            workbook.save(filepath)
            logger.info(f"Excel export completed: {record_count} records written to {filepath}")
            
            return str(filepath), record_count
            
        except Exception as e:
            logger.error(f"Error during Excel export: {e}", exc_info=True)
            # Clean up partial file
            if filepath.exists():
                filepath.unlink()
            raise
    
    def _stream_to_csv(
        self,
        query: Query,
        filename: str,
        row_transformer: callable,
        headers: Optional[List[str]],
        batch_size: int
    ) -> tuple[str, int]:
        """
        Stream query results to CSV file.
        
        Uses csv.DictWriter with incremental writing and periodic flushing.
        """
        filepath = self.output_dir / f"{filename}.csv"
        logger.info(f"Creating CSV file: {filepath}")
        
        record_count = 0
        headers_written = False
        
        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = None
                
                # Stream results in batches
                for batch in self._batch_query_results(query, batch_size):
                    for row_obj in batch:
                        # Transform ORM object to dict
                        row_dict = row_transformer(row_obj)
                        
                        # Initialize writer with headers from first row
                        if writer is None:
                            if headers is None:
                                headers = list(row_dict.keys())
                            writer = csv.DictWriter(csvfile, fieldnames=headers)
                            writer.writeheader()
                            headers_written = True
                        
                        # Write data row (serialize values)
                        serialized_row = {
                            k: self._serialize_value(v) for k, v in row_dict.items()
                        }
                        writer.writerow(serialized_row)
                        record_count += 1
                        
                        # Log progress
                        if record_count % 10000 == 0:
                            logger.info(f"Processed {record_count} records...")
                            csvfile.flush()  # Flush to disk periodically
                
                csvfile.flush()  # Final flush
            
            logger.info(f"CSV export completed: {record_count} records written to {filepath}")
            return str(filepath), record_count
            
        except Exception as e:
            logger.error(f"Error during CSV export: {e}", exc_info=True)
            # Clean up partial file
            if filepath.exists():
                filepath.unlink()
            raise
    
    def _stream_to_json(
        self,
        query: Query,
        filename: str,
        row_transformer: callable,
        batch_size: int
    ) -> tuple[str, int]:
        """
        Stream query results to JSON file.
        
        Writes array incrementally to avoid loading all data in memory.
        """
        filepath = self.output_dir / f"{filename}.json"
        logger.info(f"Creating JSON file: {filepath}")
        
        record_count = 0
        
        try:
            with open(filepath, 'w', encoding='utf-8') as jsonfile:
                # Start JSON array
                jsonfile.write('[')
                
                first_record = True
                
                # Stream results in batches
                for batch in self._batch_query_results(query, batch_size):
                    for row_obj in batch:
                        # Transform ORM object to dict
                        row_dict = row_transformer(row_obj)
                        
                        # Serialize values
                        serialized_row = {
                            k: self._serialize_value(v) for k, v in row_dict.items()
                        }
                        
                        # Add comma separator (except for first record)
                        if not first_record:
                            jsonfile.write(',\n')
                        else:
                            jsonfile.write('\n')
                            first_record = False
                        
                        # Write JSON object
                        json.dump(serialized_row, jsonfile, indent=2)
                        record_count += 1
                        
                        # Log progress
                        if record_count % 10000 == 0:
                            logger.info(f"Processed {record_count} records...")
                
                # Close JSON array
                jsonfile.write('\n]')
            
            logger.info(f"JSON export completed: {record_count} records written to {filepath}")
            return str(filepath), record_count
            
        except Exception as e:
            logger.error(f"Error during JSON export: {e}", exc_info=True)
            # Clean up partial file
            if filepath.exists():
                filepath.unlink()
            raise
    
    def _stream_to_pdf(
        self,
        query: Query,
        filename: str,
        row_transformer: callable,
        headers: Optional[List[str]],
        batch_size: int
    ) -> tuple[str, int]:
        """
        Export to PDF (limited support).
        
        WARNING: PDF generation requires all data in memory.
        Limited to 10,000 records to prevent memory issues.
        """
        logger.warning("PDF export requires all data in memory. Limiting to 10,000 records.")
        
        # Import PDF dependencies only when needed
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import landscape, letter
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
        
        filepath = self.output_dir / f"{filename}.pdf"
        
        # Collect data (with limit)
        data_rows = []
        record_count = 0
        max_records = 10000
        
        for batch in self._batch_query_results(query, batch_size):
            for row_obj in batch:
                if record_count >= max_records:
                    logger.warning(f"PDF export limited to {max_records} records")
                    break
                
                row_dict = row_transformer(row_obj)
                
                # Get headers from first row
                if not headers and record_count == 0:
                    headers = list(row_dict.keys())
                
                # Add data row
                data_row = [self._serialize_value(row_dict.get(h, "")) for h in headers]
                data_rows.append(data_row)
                record_count += 1
            
            if record_count >= max_records:
                break
        
        # Build PDF
        try:
            doc = SimpleDocTemplate(str(filepath), pagesize=landscape(letter))
            
            # Prepare table data
            table_data = [headers] + data_rows
            
            # Create and style table
            table = Table(table_data)
            style = TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 8),
            ])
            table.setStyle(style)
            
            # Build PDF
            doc.build([table])
            
            logger.info(f"PDF export completed: {record_count} records written to {filepath}")
            return str(filepath), record_count
            
        except Exception as e:
            logger.error(f"Error during PDF export: {e}", exc_info=True)
            if filepath.exists():
                filepath.unlink()
            raise
    
    def _batch_query_results(self, query: Query, batch_size: int):
        """
        Generator that yields batches of query results using MySQL streaming.
        
        This is the CRITICAL optimization for memory efficiency.
        Uses SQLAlchemy's yield_per() which works with stream_results=True.
        """
        logger.debug(f"Starting streaming query with batch_size={batch_size}")
        
        # Yield results in batches
        # yield_per() with stream_results=True uses server-side cursor
        batch = []
        for row in query.yield_per(batch_size):
            batch.append(row)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        
        # Yield remaining rows
        if batch:
            yield batch
    
    def _serialize_value(self, value: Any) -> str:
        """
        Serialize value for export (handle dates, decimals, None, etc.)
        """
        if value is None:
            return ""
        elif isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(value, Decimal):
            return str(float(value))
        elif isinstance(value, (list, dict)):
            return json.dumps(value)
        else:
            return str(value)
    
    def _create_styled_cell(self, value, font=None, alignment=None, border=None):
        """
        Create a styled cell for Excel export.
        
        Note: In write-only mode, cell styling is limited.
        This is kept for reference but may not apply all styles.
        """
        # In write-only mode, we can't apply full styling
        # This is a simplified version
        return value
