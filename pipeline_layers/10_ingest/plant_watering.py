"""
Raw layer: simulate how much water each backyard plant got today, and land one CSV per plant.

Plants, watering ranges and folders come from config/plant_watering.yaml. Every run draws
new random values (unless source.seed is set), so re-running on the same day produces a
changed version of each (plant_id, date) record for staging's SCD Type 2 history.

  <raw.dir>/<batch_id>/<raw.file_prefix>_<plant_id>.csv    (one folder per run = one batch)
"""

import random
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from config import CONFIG, PROJECT_ROOT, resolve  # noqa: E402


def generate(plants: list[dict], day: date, rng: random.Random, moisture_range = (10, 60)) -> pd.DataFrame:
	"""One random watering per plant on `day`."""
	day_start = datetime.combine(day, time(6, 0), tzinfo = timezone.utc)
	rows = []
	for plant in plants:
		rows.append({
			"plant_id": plant["id"],
			"date": day.isoformat(),
			"plant_name": plant["name"],
			"plant_type": plant["type"],
			"location": plant["location"],
			# a random moment between 06:00 and 20:00 UTC
			"watered_at": (day_start + timedelta(seconds = rng.randint(0, 14 * 3600))).isoformat(),
			"water_ml": round(rng.uniform(plant["min_ml"], plant["max_ml"]), 1),
			"soil_moisture_pct": round(rng.uniform(*moisture_range), 1),
		})
	return pd.DataFrame(rows)


def main() -> None:
	source, raw = CONFIG["source"], CONFIG["raw"]
	now = datetime.now(timezone.utc)
	rng = random.Random(source.get("seed"))  # seed None -> new values every run

	df = generate(source["plants"], now.date(), rng, tuple(source.get("soil_moisture_pct", (10, 60))))

	batch_id = now.strftime(CONFIG["batch_id_format"])
	batch_dir = resolve(raw["dir"]) / batch_id
	batch_dir.mkdir(parents = True, exist_ok = True)

	print(f"Config: {CONFIG['_path']}")
	print(f"Generating waterings for {len(df)} plants on {now.date()}")
	for _, row in df.iterrows():
		csv_path = batch_dir / f"{raw['file_prefix']}_{row['plant_id']}.csv"
		row.to_frame().T.to_csv(csv_path, index = False)
		print(f"Saved {csv_path.relative_to(PROJECT_ROOT)}  ({row['plant_name']}: {row['water_ml']} ml)")
	print(f"\nBatch {batch_id}: {len(df)} files in {batch_dir.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
	main()
