### api/app/esign/storage.py

# Standard library imports
import os
import json
from typing import Dict, List, Optional
from datetime import datetime, timezone


class EnvelopeStorage:
    """
    This class is used to store the envelope data in a JSON file.
    """
    def __init__(self, storage_path: str = "envelopes.json"):
        self.storage_path = storage_path
        self.envelopes: Dict = self._load_storage()

    def _load_storage(self) -> Dict:
        """Load envelope data from JSON file"""
        if os.path.exists(self.storage_path):
            with open(self.storage_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}
    
    def save_storage(self) -> None:
        """Save envelope data to JSON file."""
        with open(self.storage_path, "w", encoding="utf-8") as f:
            json.dump(self.envelopes, f, indent=4)

    def add_envelope(self, envelope_id: str, data: Dict) -> None:
        """Add or update envelope data"""
        data["last_updated"] = datetime.now(timezone.utc).isoformat()
        self.envelopes[envelope_id] = data
        self.save_storage()

    def get_envelope(self, envelope_id: str) -> Optional[Dict]:
        """Get the envelope data by ID"""
        return self.envelopes.get(envelope_id)
    
    def update_envelope_status(self, envelope_id: str, status: str) -> None:
        """Update envelope status"""
        if envelope_id in self.envelopes:
            self.envelopes[envelope_id]["status"] = status
            self.envelopes[envelope_id]["last_updated"] = datetime.now(timezone.utc).isoformat()
            self.save_storage()

    def list_envelopes(self, status: Optional[str] = None) -> List[Dict]:
        """List all envelopes, optionally filtered by status"""
        if status:
            return [env for env in self.envelopes.values() if env.get("status") == status]
        return list(self.envelopes.values())