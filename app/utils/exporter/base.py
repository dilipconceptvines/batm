### app/utils/exporter/base.py

# Standard library imports
from abc import ABC, abstractmethod
from typing import Union, List, Dict, Any

# Third party imports
import pandas as pd


class DataExportBase(ABC):
    """Base class for data exporters"""
    def __init__(self, data: Union[Dict[str, List[Any]], List[Dict[str, Any]]]):
        self.df = self._validate_and_parse(data)

    def _validate_and_parse(self, data) -> pd.DataFrame:
        """Validate and parse the data"""
        if isinstance(data, dict):
            df = pd.DataFrame.from_dict(data)
        elif isinstance(data, list) and all(isinstance(row, dict) for row in data):
            df = pd.DataFrame(data)
        else:
            raise ValueError("Input must be a dictionary of lists or list of dictionaries")
        
        if df.empty:
            raise ValueError("Provided data results in an empty dataframe")
        
        return df.dropna(how="all").drop_duplicates()
    
    @abstractmethod
    def export(self) -> bytes:
        """Return BytesIO object containing the exported data"""
        pass