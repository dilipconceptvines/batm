### app/reports/utils.py

# Standard library imports
import re
from io import BytesIO

# Third party imports
import pandas as pd
import pdfkit
from jinja2 import Template

# Local imports
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)

def parse_sql_query(query: str):
    """Parse a SQL query into base query, where conditions, and order by fields"""
    # Normalize spaces
    query = ' '.join(query.strip().split())

    # Regex patterns
    where_pattern = re.compile(r"\bWHERE\b(.*?)\b(ORDER BY|$)", re.IGNORECASE)
    order_by_pattern = re.compile(r"\bORDER BY\b(.*)", re.IGNORECASE)

    # Extract base query
    base_query = re.split(r"\bWHERE\b|\bORDER BY\b", query, flags=re.IGNORECASE)[0].strip()

    # Extract WHERE clause into dict
    where_clause = where_pattern.search(query)
    where_conditions = {}
    if where_clause:
        conditions_str = where_clause.group(1).strip()
        conditions = re.split(r"\s+AND\s+", conditions_str, flags=re.IGNORECASE)
        for cond in conditions:
            if '=' in cond:
                key, val = cond.split('=', 1)
                where_conditions[key.strip()] = val.strip()
            elif '>' in cond:
                key, val = cond.split('>', 1)
                where_conditions[key.strip()] = f"> {val.strip()}"
            elif '<' in cond:
                key, val = cond.split('<', 1)
                where_conditions[key.strip()] = f"< {val.strip()}"
            # Add other operators if needed

    # Extract ORDER BY clause into list
    order_by_clause = order_by_pattern.search(query)
    order_by_fields = []
    if order_by_clause:
        fields = order_by_clause.group(1).strip()
        order_by_fields = [field.strip() for field in fields.split(',') if field.strip()]

    return base_query, where_conditions, order_by_fields

def apply_filters(query: str, filters: dict, sort_by: str = None, sort_order: str = "asc") -> str:
    """Apply filters to a SQL query and handle sorting properly.
    
    Args:
        query (str): The base SQL query
        filters (dict): Dictionary of column names and their filter values
        sort_by (str, optional): Column name to sort by
        sort_order (str, optional): Sort order ('asc' or 'desc'). Defaults to 'asc'
    
    Returns:
        str: Modified SQL query with filters and sorting applied
    """
    try:
        if not filters:
            return query

        # Normalize the query for consistent processing
        query = query.strip()
        base, where_list, order_by = parse_sql_query(query)

        if where_list:
            filters = {
                **where_list,
                **filters
            }

        # Build WHERE clause from filters
        clauses = []
        for col, val in filters.items():
            if isinstance(val, list):
                clauses.append(f"{col} IN ({', '.join(f'{repr(v)}' for v in val)})")
            else:
                clauses.append(f"{col} = {repr(val)}")
        
        where_clause = " AND ".join(clauses)

        final_query = base + " WHERE " + where_clause

        if order_by:
            final_query += " ORDER BY " + ", ".join(order_by)

        return final_query

    except Exception as e:
        logger.error("Error applying filters: %s", e)
        raise e
    
def generate_export_key(query_id: int, ext: str) -> str:
    """Generate a unique export key for a query"""
    return f"reports/query_{query_id}_{pd.Timestamp.now().isoformat()}.{ext}"

def export_to_xls(df: pd.DataFrame, filters: dict, query_id: int) -> str:
    """Export a DataFrame to an Excel file"""
    try:
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Results")
            pd.DataFrame.from_dict(filters, orient="index").to_excel(writer, sheet_name="Filters")

        key = generate_export_key(query_id, "xlsx")
        s3_utils.upload_file(buffer, key, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        return key
    except Exception as e:
        logger.error("Error exporting to Excel: %s", e)
        raise e
    
def export_to_pdf(df: pd.DataFrame, filters: dict, query_id: int) -> str:
    """Export a DataFrame to a PDF file"""
    try:
        template = Template("""
            <html>
            <head><style>
                table { border-collapse: collapse; width: 100%; font-size: 12px; }
                th, td { border: 1px solid #ccc; padding: 4px; text-align: left; }
                h2 { font-family: sans-serif; }
            </style></head>
            <body>
                <h2>Report Filters</h2>
                <ul>{% for k, v in filters.items() %}<li><strong>{{ k }}</strong>: {{ v }}</li>{% endfor %}</ul>
                <h2>Results</h2>
                {{ table | safe }}
            </body>
            </html>
        """)
        html = template.render(table=df.to_html(index=False), filters=filters)

        # 2. Save to PDF
        pdf_buffer = BytesIO()
        pdfkit.from_string(html, pdf_buffer, options={"quiet": ""})
        pdf_data = pdf_buffer.getvalue()

        # 3. Upload
        key = generate_export_key(query_id, "pdf")
        s3_utils.upload_file(pdf_data, key, "application/pdf")
        return key
    except Exception as e:
        logger.error("Error exporting to PDF: %s", e)
        raise e