"""
Shared pipeline config loader.

  from config import CONFIG, resolve

The dataset config defaults to config/weather.yaml and can be switched with the
PIPELINE_CONFIG environment variable (path relative to the project root, or absolute).
It is deep-merged on top of config/base.yaml, which holds settings shared by all datasets.
"""

import os
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASE_CONFIG = "config/base.yaml"
DEFAULT_CONFIG = "config/weather.yaml"


def resolve(path: str | Path) -> Path:
	"""Resolve a config path relative to the project root (absolute paths are kept)."""
	path = Path(path)
	return path if path.is_absolute() else PROJECT_ROOT / path


def _read_yaml(path: Path) -> dict:
	with open(path) as f:
		return yaml.safe_load(f) or {}


def deep_merge(base: dict, override: dict) -> dict:
	merged = dict(base)
	for key, value in override.items():
		if isinstance(value, dict) and isinstance(merged.get(key), dict):
			merged[key] = deep_merge(merged[key], value)
		else:
			merged[key] = value
	return merged


def load_config(path: str | Path | None = None) -> dict:
	config_path = resolve(path or os.environ.get("PIPELINE_CONFIG") or DEFAULT_CONFIG)
	if not config_path.is_file():
		raise FileNotFoundError(f"Pipeline config not found: {config_path}")
	base_path = resolve(BASE_CONFIG)
	base = _read_yaml(base_path) if base_path.is_file() else {}
	config = deep_merge(base, _read_yaml(config_path))
	config["_path"] = str(config_path)
	return config


CONFIG = load_config()
