"""Basic data checks for pipeline_layers/40_gold/build_gold.py (uses the real seeds and models)."""

from datetime import date, datetime, timezone

import pyarrow as pa
import pytest

import build_gold as bg
import lakehouse

UTC = timezone.utc
TS = pa.timestamp("us", tz = "UTC")
NORMAL, HOT, RAINY = date(2026, 11, 1), date(2026, 11, 2), date(2026, 11, 3)
BUILT_AT = datetime(2026, 11, 5, tzinfo = UTC)


def at(day: date, hour: int) -> datetime:
	return datetime(day.year, day.month, day.day, hour, tzinfo = UTC)


def weather_rows():
	# two grid points per day; regional averages: NORMAL 20C/0mm, HOT 31C/0mm, RAINY 18C/8mm
	rows = []
	for day, tmax, rain in [(NORMAL, (19, 21), 0.0), (HOT, (30, 32), 0.0), (RAINY, (17, 19), 8.0)]:
		for i, t in enumerate(tmax):
			rows.append({
				"latitude": -37.8 + i, "longitude": 145.0, "date": day,
				"sunrise": at(day, 6), "sunset": at(day, 20),
				"rain_sum": rain, "temperature_2m_max": float(t), "temperature_2m_min": 10.0,
				"last_updated": at(RAINY, 23),
			})
	return rows


def plant_row(plant_id, day, water_ml, soil = 40.0, name = "x"):
	return {"plant_id": plant_id, "date": day, "plant_name": name, "plant_type": "t", "location": "l",
	        "watered_at": at(day, 8), "water_ml": float(water_ml), "soil_moisture_pct": soil}


PLANTS = [
	plant_row("P01", NORMAL, 400),            # Tomato needs 500-800        -> under_watered
	plant_row("P05", NORMAL, 350),            # Lavender needs 150-300      -> over_watered
	plant_row("P03", NORMAL, 800, soil = 30), # Rose needs 600-1000, 25-55% -> ok
	plant_row("P01", HOT, 900),               # hot day: Tomato +25% -> 625-1000 -> ok
	plant_row("P01", RAINY, 600),             # 8mm >= Tomato's 5mm skip    -> watered_on_rain_day
	plant_row("P99", NORMAL, 100),            # not in the seed             -> no_water_needs
]


@pytest.fixture
def gold(tmp_path, monkeypatch):
	"""Temp catalog with small current tables; builds gold and returns a reader."""
	monkeypatch.setattr(lakehouse, "CATALOG_DB", tmp_path / "catalog.db")
	monkeypatch.setattr(lakehouse, "LAKEHOUSE_ROOT", tmp_path / "data")
	catalog = lakehouse.get_catalog()
	for name, rows in [("weather_daily", weather_rows()), ("plant_watering_daily", PLANTS)]:
		data = pa.Table.from_pylist(rows)
		layer = {"dir": "30_current", "namespace": "current", "table": name}
		lakehouse.replace_table_contents(lakehouse.load_or_create_table(catalog, layer, data.schema), data)

	bg.build_gold(catalog, BUILT_AT)
	return lambda name: catalog.load_table(("gold", name)).scan().to_pandas()


def test_all_models_and_seed_are_published(gold):
	for name in ["seed_plant_water_needs", *bg.GOLD["models"]]:
		df = gold(name)
		assert len(df) > 0, name
		assert (df["built_at"] == BUILT_AT).all(), name


def test_weather_daily_one_row_per_date_with_regional_averages(gold):
	df = gold("weather_daily").set_index("date")
	assert sorted(df.index) == [NORMAL, HOT, RAINY]
	assert df.loc[NORMAL, "locations"] == 2
	assert df.loc[NORMAL, "avg_temp_max_c"] == 20.0
	assert df.loc[RAINY, "avg_rain_mm"] == 8.0
	assert df.loc[NORMAL, "avg_daylight_hours"] == 14.0


def test_plant_summary_one_row_per_plant(gold):
	df = gold("plant_summary").set_index("plant_id")
	assert sorted(df.index) == ["P01", "P03", "P05", "P99"]
	assert df.loc["P01", "days_recorded"] == 3
	assert df.loc["P01", "total_water_ml"] == 1900.0
	assert df.loc["P01", "need_min_ml"] == 500 and df.loc["P01", "need_max_ml"] == 800


def test_garden_daily_totals(gold):
	df = gold("garden_daily").set_index("date")
	assert df.loc[NORMAL, "plants_watered"] == 4
	assert df.loc[NORMAL, "total_water_ml"] == 400 + 350 + 800 + 100


def test_fact_one_row_per_plant_per_day(gold):
	df = gold("weather_and_plant_watering_daily_checks")
	assert len(df) == len(PLANTS)
	assert not df.duplicated(["plant_id", "date"]).any()


@pytest.mark.parametrize("plant_id, day, expected", [
	("P01", NORMAL, "under_watered"),
	("P05", NORMAL, "over_watered"),
	("P03", NORMAL, "ok"),
	("P01", HOT, "ok"),
	("P01", RAINY, "watered_on_rain_day"),
	("P99", NORMAL, "no_water_needs"),
])
def test_fact_watering_checks(gold, plant_id, day, expected):
	df = gold("weather_and_plant_watering_daily_checks").set_index(["plant_id", "date"])
	assert df.loc[(plant_id, day), "watering_check"] == expected


def test_fact_hot_day_raises_targets(gold):
	df = gold("weather_and_plant_watering_daily_checks").set_index(["plant_id", "date"])
	hot = df.loc[("P01", HOT)]
	assert hot["is_hot_day"]
	assert (hot["target_min_ml"], hot["target_max_ml"]) == (625.0, 1000.0)
	assert df.loc[("P03", NORMAL), "all_checks_ok"]
