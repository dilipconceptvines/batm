### app/utils/sql_filter.py

import re
from typing import Dict, Any, Optional
from sqlalchemy import text

def append_sql_filters(original_query: str, filters: Dict[str, Any]) -> str:
    """
    Append filters to an existing SQL query's WHERE clause or create a new WHERE clause.
    
    Args:
        original_query (str): The original SQL query
        filters (Dict[str, Any]): Dictionary of filters to apply
        
    Returns:
        str: Modified SQL query with filters applied
        
    Example:
        original_query = "SELECT * FROM users WHERE status = 'active'"
        filters = {"age": {"$gt": 18}, "name": {"$like": "John%"}}
        result = append_sql_filters(original_query, filters)
    """
    # Clean and normalize the query
    query = original_query.strip()
    
    # Convert filters to SQL conditions
    conditions = []
    for field, value in filters.items():
        if isinstance(value, dict):
            for op, op_value in value.items():
                condition = _build_condition(field, op, op_value)
                if condition:
                    conditions.append(condition)
        else:
            conditions.append(f"{field} = {_format_value(value)}")
    
    if not conditions:
        return query
    
    # Check if query already has WHERE clause
    where_pattern = r'\bWHERE\b'
    order_by_pattern = r'\bORDER\s+BY\b'
    
    has_where = bool(re.search(where_pattern, query, re.IGNORECASE))
    order_by_match = re.search(order_by_pattern, query, re.IGNORECASE)
    
    # Combine all conditions
    combined_conditions = ' AND '.join(conditions)
    
    if has_where:
        # Insert new conditions before ORDER BY if it exists
        if order_by_match:
            order_by_pos = order_by_match.start()
            query = (
                query[:order_by_pos].rstrip() + 
                f" AND {combined_conditions} " + 
                query[order_by_pos:]
            )
        else:
            # Add to existing WHERE clause
            query = query.rstrip() + f" AND {combined_conditions}"
    else:
        # Add new WHERE clause before ORDER BY if it exists
        if order_by_match:
            order_by_pos = order_by_match.start()
            query = (
                query[:order_by_pos].rstrip() + 
                f" WHERE {combined_conditions} " + 
                query[order_by_pos:]
            )
        else:
            # Add WHERE clause at the end
            query = query.rstrip() + f" WHERE {combined_conditions}"
    
    return query

def _build_condition(field: str, operator: str, value: Any) -> Optional[str]:
    """Build a SQL condition based on the operator."""
    operators = {
        '$eq': '=',
        '$ne': '!=',
        '$gt': '>',
        '$lt': '<',
        '$gte': '>=',
        '$lte': '<=',
        '$like': 'LIKE',
        '$in': 'IN',
        '$nin': 'NOT IN'
    }
    
    sql_op = operators.get(operator)
    if not sql_op:
        return None
        
    if operator in ['$in', '$nin']:
        if not isinstance(value, (list, tuple)):
            return None
        values = [_format_value(v) for v in value]
        return f"{field} {sql_op} ({', '.join(values)})"
    else:
        return f"{field} {sql_op} {_format_value(value)}"

def _format_value(value: Any) -> str:
    """Format a value for SQL query."""
    if value is None:
        return 'NULL'
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    else:
        # Escape single quotes and wrap in quotes
        return f"'{str(value).replace("'", "''")}'"

def validate_sql_query(query: str) -> bool:
    """
    Basic validation of SQL query structure.
    
    Args:
        query (str): SQL query to validate
        
    Returns:
        bool: True if query appears valid, False otherwise
    """
    # Check for balanced parentheses
    if query.count('(') != query.count(')'):
        return False
        
    # Check for basic SQL syntax
    required_keywords = ['SELECT', 'FROM']
    for keyword in required_keywords:
        if keyword not in query.upper():
            return False
            
    # Check for proper WHERE clause placement
    where_pos = query.upper().find('WHERE')
    order_by_pos = query.upper().find('ORDER BY')
    if where_pos != -1 and order_by_pos != -1 and where_pos > order_by_pos:
        return False
        
    return True 