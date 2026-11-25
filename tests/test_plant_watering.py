"""Basic data checks for pipeline_layers/10_ingest/plant_watering.py."""

import random
from datetime import date

import pandas as pd

import plant_watering as pw
from config import load_config

PLANT_CONFIG = load_config("config/plant_watering.yaml")
PLANTS = PLANT_CONFIG["source"]["plants"]
DAY = date(2026, 11, 26)


def test_one_row_per_plant_with_staging_columns():
	df = pw.generate(PLANTS, DAY, random.Random())
	assert 1 <= len(PLANTS) <= 7
	assert len(df) == len(PLANTS)
	assert df["plant_id"].is_unique
	assert list(df.columns) == list(PLANT_CONFIG["staging"]["columns"])
	assert (df["date"] == DAY.isoformat()).all()


def test_values_within_configured_ranges():
	df = pw.generate(PLANTS, DAY, random.Random())
	limits = pd.DataFrame(PLANTS).set_index("id")
	for _, row in df.iterrows():
		assert limits.loc[row["plant_id"], "min_ml"] <= row["water_ml"] <= limits.loc[row["plant_id"], "max_ml"]
	assert df["soil_moisture_pct"].between(10, 60).all()
	assert pd.to_datetime(df["watered_at"]).dt.date.eq(DAY).all()


def test_each_run_generates_new_values():
	first = pw.generate(PLANTS, DAY, random.Random())
	second = pw.generate(PLANTS, DAY, random.Random())
	assert not first["water_ml"].equals(second["water_ml"])
