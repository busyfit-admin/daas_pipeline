"""
Shared pipeline config loader.

  from config import CONFIG, resolve

The config file defaults to config/weather.yaml and can be switched with the
PIPELINE_CONFIG environment variable (path relative to the project root, or absolute).
"""

import os
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = "config/weather.yaml"


def resolve(path: str | Path) -> Path:
	"""Resolve a config path relative to the project root (absolute paths are kept)."""
	path = Path(path)
	return path if path.is_absolute() else PROJECT_ROOT / path


def load_config(path: str | Path | None = None) -> dict:
	config_path = resolve(path or os.environ.get("PIPELINE_CONFIG") or DEFAULT_CONFIG)
	if not config_path.is_file():
		raise FileNotFoundError(f"Pipeline config not found: {config_path}")
	with open(config_path) as f:
		config = yaml.safe_load(f) or {}
	config["_path"] = str(config_path)
	return config


CONFIG = load_config()
