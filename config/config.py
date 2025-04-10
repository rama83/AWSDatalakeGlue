"""
Configuration management for the AWS Data Lake Framework.
"""
import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()


class Config:
    """Configuration manager for the data lake framework."""

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the configuration manager.

        Args:
            config_path: Path to the configuration file. If None, uses the default.
        """
        # Set default config path if not provided
        if config_path is None:
            base_dir = Path(__file__).parent
            config_path = os.path.join(base_dir, "settings.yaml")

        # Load configuration from file
        self.config_dict = self._load_config(config_path)

        # Override with environment variables
        self._override_from_env()

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Load configuration from a YAML file.

        Args:
            config_path: Path to the configuration file.

        Returns:
            Dict containing configuration values.
        """
        try:
            with open(config_path, "r") as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            print(f"Warning: Configuration file not found at {config_path}")
            return {}
        except yaml.YAMLError as e:
            print(f"Error parsing configuration file: {e}")
            return {}

    def _override_from_env(self) -> None:
        """Override configuration values with environment variables."""
        # Environment variables take precedence over config file
        # Format: DATALAKE_SECTION_KEY (e.g., DATALAKE_S3_BUCKET)
        for key, value in os.environ.items():
            if key.startswith("DATALAKE_"):
                parts = key.lower().split("_")
                if len(parts) >= 3:
                    section = parts[1]
                    subkey = "_".join(parts[2:])
                    
                    # Create section if it doesn't exist
                    if section not in self.config_dict:
                        self.config_dict[section] = {}
                    
                    # Set the value
                    self.config_dict[section][subkey] = value

    def get(self, section: str, key: str, default: Any = None) -> Any:
        """
        Get a configuration value.

        Args:
            section: Configuration section.
            key: Configuration key.
            default: Default value if the key is not found.

        Returns:
            Configuration value or default.
        """
        try:
            return self.config_dict.get(section, {}).get(key, default)
        except (KeyError, AttributeError):
            return default

    def get_section(self, section: str) -> Dict[str, Any]:
        """
        Get an entire configuration section.

        Args:
            section: Configuration section.

        Returns:
            Dict containing the section's configuration values.
        """
        return self.config_dict.get(section, {})


# Create a singleton instance
config = Config()
