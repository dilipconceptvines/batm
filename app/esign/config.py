### api/app/esign/config.py

# Standard library imports
import os
import json
import logging
from typing import Dict, Optional
from pathlib import Path

logger = logging.getLogger("ESign configuration -*-*-*-")


class Config:
    """
    This class is used to load and save the configuration for the e-signature client.
    """
    def __init__(self, config_path: str = "config.json"):
        """Initialize the configuration"""
        # Get the directory where this script is located
        self.script_dir = Path(__file__).parent.absolute()
        # Join the script directory with the config path
        self.config_path = Path.joinpath(self.script_dir, config_path)
        self.config: Dict = self._load_config()

    def _load_config(self) -> Dict:
        """Load the configuration from the file"""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            return {
                "client_id": "",
                "client_secret": "",
                "account_id": "",
                "auth_server": "account-d.docusign.com",
                "private_key_file": "private.key",
                "user_id": "",
                "base_path": "https://demo.docusign.net/restapi"
            }
        except Exception as e:
            logger.error("Error loading config: %s", e, exc_info=True)
            raise e
    
    def save_config(self) -> None:
        """Save the configuration to the file"""
        try:
            with open(self.config_path, "w", encoding="utf-8") as f:
                json.dump(self.config, f, indent=4)
        except Exception as e:
            logger.error("Error saving config: %s", e, exc_info=True)
            raise e

    def update_config(self, **kwargs) -> None:
        """Update the configuration with the given key-value pairs"""
        try:
            self.config.update(kwargs)
            self.save_config()
        except Exception as e:
            logger.error("Error updating config: %s", e, exc_info=True)
            raise e

    def get_value(self, key: str) -> Optional[str]:
        """Get the value of a key from the configuration"""
        return self.config.get(key)
    
    def load_private_key(self) -> Optional[str]:
        """Load the private key from the file"""
        try:
            key_path = Path.joinpath(self.script_dir, self.config.get("private_key_file"))
            if key_path and os.path.exists(key_path):
                with open(key_path, "r", encoding="utf-8") as f:
                    return f.read()
            return None
        except Exception as e:
            logger.error("Error loading private key: %s", e, exc_info=True)
            raise e
    