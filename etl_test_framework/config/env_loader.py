"""
config/env_loader.py
--------------------
Loads configuration from config.yaml with environment variable
substitution and .env file support.
"""

import os
import re
import yaml
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()  # Load .env file if present

CONFIG_PATH = Path(__file__).parent / "config.yaml"


def _substitute_env_vars(value: str) -> str:
    """Replace ${VAR_NAME} placeholders with actual env variables."""
    if isinstance(value, str):
        pattern = r"\$\{(\w+)\}"
        matches = re.findall(pattern, value)
        for match in matches:
            env_val = os.getenv(match, "")
            if not env_val:
                raise EnvironmentError(
                    f"Required environment variable '{match}' is not set."
                )
            value = value.replace(f"${{{match}}}", env_val)
    return value


def _resolve_config(obj):
    """Recursively resolve env vars in config dict."""
    if isinstance(obj, dict):
        return {k: _resolve_config(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_resolve_config(i) for i in obj]
    elif isinstance(obj, str):
        return _substitute_env_vars(obj)
    return obj


def load_config() -> dict:
    """Load and return the resolved configuration for the active environment."""
    with open(CONFIG_PATH, "r") as f:
        raw_config = yaml.safe_load(f)

    active_env = os.getenv("ETL_ENV", raw_config.get("active_env", "dev"))
    env_config = raw_config["environments"].get(active_env)

    if not env_config:
        raise ValueError(f"Environment '{active_env}' not found in config.yaml")

    resolved = _resolve_config(env_config)
    resolved["csv_sources"] = _resolve_config(raw_config.get("csv_sources", {}))
    resolved["thresholds"] = raw_config.get("thresholds", {})
    resolved["table_mappings"] = raw_config.get("table_mappings", [])
    resolved["active_env"] = active_env

    return resolved


# Singleton config instance
CONFIG = load_config()
