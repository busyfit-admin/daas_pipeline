"""
Raw layer: pull daily weather from Open-Meteo for a grid of points and land one CSV per point.

All settings (API, region, variables, folders) come from config/weather.yaml.

  <raw.dir>/<batch_id>/<raw.file_prefix>_<lat>_<lon>.csv    (one folder per run = one batch)
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from config import CONFIG, PROJECT_ROOT, resolve  # noqa: E402

# ---------------------------------------------------------------------------
# Settings from config
# ---------------------------------------------------------------------------
API = CONFIG["api"]
REGION = CONFIG["region"]
DAILY_VARIABLES = CONFIG["daily_variables"]  # name -> "timestamp" | "float", in request order
RAW_DIR = resolve(CONFIG["raw"]["dir"])
RAW_FILE_PREFIX = CONFIG["raw"]["file_prefix"]
BATCH_ID_FORMAT = CONFIG["batch_id_format"]
CACHE_FILE = resolve(API["cache"]["file"])

# ---------------------------------------------------------------------------
# Open-Meteo client with cache and retry on error
# ---------------------------------------------------------------------------
CACHE_FILE.parent.mkdir(parents = True, exist_ok = True)
cache_session = requests_cache.CachedSession(str(CACHE_FILE), expire_after = API["cache"]["expire_after_seconds"])
retry_session = retry(cache_session, retries = API["retry"]["retries"], backoff_factor = API["retry"]["backoff_factor"])
openmeteo = openmeteo_requests.Client(session = retry_session)

# Grid of points inside the region (bounding-box calls aren't supported by the "best_match" model)
step = REGION["grid_step_degrees"]
lats = np.round(np.arange(REGION["south"], REGION["north"] + 1e-9, step), 4)
lons = np.round(np.arange(REGION["west"], REGION["east"] + 1e-9, step), 4)
grid = [(lat, lon) for lat in lats for lon in lons]

params = {
	"latitude": [float(lat) for lat, _ in grid],
	"longitude": [float(lon) for _, lon in grid],
	"daily": list(DAILY_VARIABLES),
	"past_days": API["past_days"],
	**(API.get("extra_params") or {}),
}
print(f"Config: {CONFIG['_path']}")
print(f"Requesting {len(grid)} points in {REGION['name']} with {len(DAILY_VARIABLES)} daily variables")
responses = openmeteo.weather_api(API["url"], params = params)

# Raw landing zone: one folder per run
LOAD_TS = datetime.now(timezone.utc).strftime(BATCH_ID_FORMAT)
batch_dir = RAW_DIR / LOAD_TS
batch_dir.mkdir(parents = True, exist_ok = True)

seen, written = set(), 0
for response in responses:
	# Nearby grid points can snap to the same API grid cell; keep one file per cell
	cell = (round(response.Latitude(), 4), round(response.Longitude(), 4))
	if cell in seen:
		print(f"Skipped duplicate grid cell {cell[0]:.4f}, {cell[1]:.4f} (requested points snapped to the same cell)")
		continue
	seen.add(cell)

	daily = response.Daily()
	daily_data = {
		"date": pd.date_range(
			start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
			end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
			freq = pd.Timedelta(seconds = daily.Interval()),
			inclusive = "left",
		)
	}

	# Location columns so each raw file is self-describing
	daily_data["latitude"] = response.Latitude()
	daily_data["longitude"] = response.Longitude()
	daily_data["elevation"] = response.Elevation()

	# Read each variable by its position in the request
	for i, (name, kind) in enumerate(DAILY_VARIABLES.items()):
		variable = daily.Variables(i)
		if kind == "timestamp":
			daily_data[name] = pd.to_datetime(variable.ValuesInt64AsNumpy(), unit = "s", utc = True)
		else:
			daily_data[name] = variable.ValuesAsNumpy()

	df = pd.DataFrame(data = daily_data)
	csv_path = batch_dir / f"{RAW_FILE_PREFIX}_{response.Latitude():.2f}_{response.Longitude():.2f}.csv"
	df.to_csv(csv_path, index = False)
	written += 1
	print(f"Saved {csv_path.relative_to(PROJECT_ROOT)}  ({len(df)} rows, elevation {response.Elevation()} m)")

print(f"\nBatch {LOAD_TS}: {written} files in {batch_dir.relative_to(PROJECT_ROOT)}")
