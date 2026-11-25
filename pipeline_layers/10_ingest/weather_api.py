"""
Raw layer: pull daily weather from Open-Meteo for a grid of points and land one CSV per point.

  data/10_raw/weather/<YYYYmmddTHHMMSSZ>/weather_<lat>_<lon>.csv    (one folder per run = one batch)
"""

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

# ---------------------------------------------------------------------------
# Settings (candidates to move into config/weather.yaml later)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]

API_URL = "https://api.open-meteo.com/v1/forecast"
PAST_DAYS = 31
CACHE_FILE = PROJECT_ROOT / "data" / "_cache" / "openmeteo"
CACHE_EXPIRE_SECONDS = 3600
RETRIES = 5
BACKOFF_FACTOR = 0.2

REGION = {
	"name": "melbourne",
	"south": -38.06,
	"west": 144.71,
	"north": -37.56,
	"east": 145.21,
	"grid_step_degrees": 0.25,
}

# Daily variables, requested and read back in this order. "timestamp" values are
# returned as int64 epoch seconds; everything else as floats.
DAILY_VARIABLES = {
	"sunrise": "timestamp",
	"sunset": "timestamp",
	"rain_sum": "float",
	"temperature_2m_max": "float",
	"temperature_2m_min": "float",
}

RAW_DIR = PROJECT_ROOT / "data" / "10_raw" / "weather"
RAW_FILE_PREFIX = "weather"

# ---------------------------------------------------------------------------
# Open-Meteo client with cache and retry on error
# ---------------------------------------------------------------------------
CACHE_FILE.parent.mkdir(parents = True, exist_ok = True)
cache_session = requests_cache.CachedSession(str(CACHE_FILE), expire_after = CACHE_EXPIRE_SECONDS)
retry_session = retry(cache_session, retries = RETRIES, backoff_factor = BACKOFF_FACTOR)
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
	"past_days": PAST_DAYS,
}
print(f"Requesting {len(grid)} points in {REGION['name']} with {len(DAILY_VARIABLES)} daily variables")
responses = openmeteo.weather_api(API_URL, params = params)

# Raw landing zone: one folder per run
LOAD_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
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
