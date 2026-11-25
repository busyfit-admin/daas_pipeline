"""
Run the raw (10_ingest) script of the selected dataset.

Each dataset config names its ingest script in raw.script, so `make raw` works for any
dataset: PIPELINE_CONFIG=config/plant_watering.yaml python pipeline_layers/10_ingest/run_raw.py
"""

import runpy
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from config import CONFIG, resolve  # noqa: E402

script = resolve(CONFIG["raw"]["script"])
print(f"Dataset {CONFIG.get('dataset')}: running {script.relative_to(resolve('.'))}")
runpy.run_path(str(script), run_name = "__main__")
