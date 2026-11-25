import sys
from pathlib import Path

# Pipeline layer folders start with digits (e.g. 20_staging), so they can't be imported
# as packages; put them on sys.path and import the modules directly.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
for layer_dir in sorted((PROJECT_ROOT / "pipeline_layers").iterdir()):
	if layer_dir.is_dir():
		sys.path.insert(0, str(layer_dir))
