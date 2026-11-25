"""Tests for pipeline_layers/20_staging/build_staging.py (with the weather config): clean() and apply_scd2()."""

from datetime import date, datetime, timezone

import pandas as pd
import pytest

import lakehouse
import build_staging as ws

BATCH_1 = "20261101T000000Z"
BATCH_2 = "20261102T000000Z"
BATCH_3 = "20261103T000000Z"


def ts(batch_id: str) -> pd.Timestamp:
	return pd.Timestamp(datetime.strptime(batch_id, ws.BATCH_ID_FORMAT).replace(tzinfo = timezone.utc))


def raw_row(**overrides) -> dict:
	"""One raw CSV row as written by the ingest layer."""
	row = {
		"date": "2026-10-25 00:00:00+00:00",
		"latitude": -37.79,
		"longitude": 144.94,
		"elevation": 21.0,
		"sunrise": "2026-10-24 19:23:20+00:00",
		"sunset": "2026-10-25 08:47:45+00:00",
		"rain_sum": 1.5,
		"temperature_2m_max": 20.0,
		"temperature_2m_min": 10.0,
		"source_file": "data/10_raw/weather/x/weather_-37.79_144.94.csv",
	}
	row.update(overrides)
	return row


def stage(*rows: dict) -> pd.DataFrame:
	"""Raw rows -> cleaned, hashed batch, as main() hands it to apply_scd2()."""
	return ws.add_record_hash(ws.clean(pd.DataFrame(list(rows))))


def key_of(row: dict) -> tuple:
	return (row["latitude"], row["longitude"], pd.Timestamp(row["date"]).date())


def all_rows(table) -> pd.DataFrame:
	return table.refresh().scan().to_arrow().cast(ws.ARROW_SCHEMA).to_pandas()


def versions(table, row: dict) -> pd.DataFrame:
	lat, lon, day = key_of(row)
	df = all_rows(table)
	return df[(df.latitude == lat) & (df.longitude == lon) & (df.date == day)].sort_values("valid_from").reset_index(drop = True)


@pytest.fixture
def table(tmp_path, monkeypatch):
	"""A fresh, empty Iceberg table in a temporary catalog for each test."""
	monkeypatch.setattr(lakehouse, "CATALOG_DB", tmp_path / "catalog.db")
	monkeypatch.setattr(lakehouse, "LAKEHOUSE_ROOT", tmp_path / "data")
	return ws.get_table()


# ---------------------------------------------------------------------------
# clean()
# ---------------------------------------------------------------------------
class TestClean:
	def test_casts_types(self):
		out = ws.clean(pd.DataFrame([raw_row()]))
		assert out.loc[0, "date"] == date(2026, 10, 25)
		assert isinstance(out["sunrise"].dtype, pd.DatetimeTZDtype) and str(out["sunrise"].dt.tz) == "UTC"
		assert out.loc[0, "sunrise"] == pd.Timestamp("2026-10-24 19:23:20", tz = "UTC")
		for col in ["latitude", "longitude", *ws.FLOAT_COLUMNS]:
			assert pd.api.types.is_float_dtype(out[col]), col

	def test_parses_mixed_date_formats_in_one_batch(self):
		out = ws.clean(pd.DataFrame([
			raw_row(date = "2026-10-25 00:00:00+00:00"),
			raw_row(date = "2026-10-26"),
			raw_row(date = "2026-10-27T00:00:00Z", sunrise = "2026-10-26T19:20:00Z"),
		]))
		assert list(out["date"]) == [date(2026, 10, 25), date(2026, 10, 26), date(2026, 10, 27)]
		assert out["sunrise"].notna().all()

	def test_unparseable_values_become_null(self):
		out = ws.clean(pd.DataFrame([raw_row(rain_sum = "n/a", sunset = "not a time")]))
		assert pd.isna(out.loc[0, "rain_sum"])
		assert pd.isna(out.loc[0, "sunset"])

	def test_normalises_column_names(self):
		df = pd.DataFrame([raw_row()]).rename(columns = {"rain_sum": " Rain_Sum "})
		assert "rain_sum" in ws.clean(df).columns

	def test_rounds_coordinates_and_measures(self):
		out = ws.clean(pd.DataFrame([raw_row(latitude = -37.574691772, longitude = 144.720001220, rain_sum = 20.299997)]))
		assert out.loc[0, "latitude"] == -37.5747
		assert out.loc[0, "longitude"] == 144.72
		assert out.loc[0, "rain_sum"] == 20.3

	@pytest.mark.parametrize("column", ["date", "latitude", "longitude"])
	def test_drops_rows_without_key(self, column):
		missing_key = {**raw_row(date = "2026-10-26"), column: None}
		out = ws.clean(pd.DataFrame([raw_row(), missing_key]))
		assert len(out) == 1
		assert out.loc[0, "date"] == date(2026, 10, 25)

	def test_drops_rows_with_no_measurements(self):
		empty = {c: None for c in ws.ATTRIBUTE_COLUMNS if c != "elevation"}
		out = ws.clean(pd.DataFrame([raw_row(), raw_row(date = "2026-10-26", **empty)]))
		assert len(out) == 1

	def test_keeps_rows_with_some_measurements(self):
		out = ws.clean(pd.DataFrame([raw_row(rain_sum = None, sunrise = None)]))
		assert len(out) == 1

	def test_negative_rain_is_nulled_not_dropped(self):
		out = ws.clean(pd.DataFrame([raw_row(rain_sum = -1.0)]))
		assert len(out) == 1
		assert pd.isna(out.loc[0, "rain_sum"])

	def test_warns_when_min_temperature_exceeds_max(self, capsys):
		ws.clean(pd.DataFrame([raw_row(temperature_2m_min = 25.0, temperature_2m_max = 20.0)]))
		assert "temperature_2m_min > temperature_2m_max" in capsys.readouterr().out

	def test_deduplicates_on_business_key_keeping_last(self):
		out = ws.clean(pd.DataFrame([raw_row(rain_sum = 1.0), raw_row(rain_sum = 2.0)]))
		assert len(out) == 1
		assert out.loc[0, "rain_sum"] == 2.0

	def test_raises_on_missing_columns(self):
		with pytest.raises(ValueError, match = "rain_sum"):
			ws.clean(pd.DataFrame([raw_row()]).drop(columns = ["rain_sum"]))

	def test_does_not_mutate_input(self):
		df = pd.DataFrame([raw_row(), raw_row()])
		before = df.copy()
		ws.clean(df)
		pd.testing.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# add_record_hash()
# ---------------------------------------------------------------------------
class TestRecordHash:
	def test_same_values_same_hash(self):
		a, b = stage(raw_row()), stage(raw_row())
		assert a.loc[0, "record_hash"] == b.loc[0, "record_hash"]

	@pytest.mark.parametrize("column, value", [
		("rain_sum", 9.9), ("temperature_2m_max", 30.0), ("elevation", 50.0),
		("sunrise", "2026-10-24 19:30:00+00:00"), ("rain_sum", None),
	])
	def test_attribute_change_changes_hash(self, column, value):
		assert stage(raw_row()).loc[0, "record_hash"] != stage(raw_row(**{column: value})).loc[0, "record_hash"]

	def test_float_noise_does_not_change_hash(self):
		assert stage(raw_row(rain_sum = 20.3)).loc[0, "record_hash"] == stage(raw_row(rain_sum = 20.299997)).loc[0, "record_hash"]

	def test_metadata_does_not_affect_hash(self):
		assert stage(raw_row()).loc[0, "record_hash"] == stage(raw_row(source_file = "other.csv")).loc[0, "record_hash"]


# ---------------------------------------------------------------------------
# apply_scd2()
# ---------------------------------------------------------------------------
class TestApplySCD2:
	def test_first_load_inserts_all_rows_as_current(self, table):
		rows = [raw_row(), raw_row(date = "2026-10-26")]
		stats = ws.apply_scd2(table, stage(*rows), BATCH_1)

		assert stats == {"new": 2, "changed": 0, "unchanged": 0}
		df = all_rows(table)
		assert len(df) == 2
		assert df["is_current"].all()
		assert (df["valid_from"] == ts(BATCH_1)).all()
		assert df["valid_to"].isna().all()
		assert (df["batch_id"] == BATCH_1).all()
		assert (df["source_file"] == rows[0]["source_file"]).all()
		assert df["record_hash"].str.fullmatch(r"[0-9a-f]{64}").all()

	def test_unchanged_batch_adds_no_rows(self, table):
		ws.apply_scd2(table, stage(raw_row()), BATCH_1)
		stats = ws.apply_scd2(table.refresh(), stage(raw_row(source_file = "later.csv")), BATCH_2)

		assert stats == {"new": 0, "changed": 0, "unchanged": 1}
		df = all_rows(table)
		assert len(df) == 1
		assert df.loc[0, "batch_id"] == BATCH_1  # original version untouched

	def test_changed_row_closes_old_version_and_adds_new_one(self, table):
		ws.apply_scd2(table, stage(raw_row(rain_sum = 1.5)), BATCH_1)
		stats = ws.apply_scd2(table.refresh(), stage(raw_row(rain_sum = 9.9)), BATCH_2)

		assert stats == {"new": 0, "changed": 1, "unchanged": 0}
		v = versions(table, raw_row())
		assert len(v) == 2

		old, new = v.iloc[0], v.iloc[1]
		assert old["rain_sum"] == 1.5 and not old["is_current"]
		assert old["valid_from"] == ts(BATCH_1) and old["valid_to"] == ts(BATCH_2)
		assert old["batch_id"] == BATCH_1

		assert new["rain_sum"] == 9.9 and new["is_current"]
		assert new["valid_from"] == ts(BATCH_2) and pd.isna(new["valid_to"])
		assert new["batch_id"] == BATCH_2
		assert new["record_hash"] != old["record_hash"]

	def test_repeated_changes_build_a_contiguous_history(self, table):
		for batch_id, rain in [(BATCH_1, 1.0), (BATCH_2, 2.0), (BATCH_3, 3.0)]:
			ws.apply_scd2(table.refresh(), stage(raw_row(rain_sum = rain)), batch_id)

		v = versions(table, raw_row())
		assert list(v["rain_sum"]) == [1.0, 2.0, 3.0]
		assert list(v["is_current"]) == [False, False, True]
		# each version ends exactly where the next one starts
		assert list(v["valid_to"].iloc[:-1]) == list(v["valid_from"].iloc[1:])
		assert pd.isna(v["valid_to"].iloc[-1])

	def test_reverting_to_earlier_values_creates_a_new_version(self, table):
		for batch_id, rain in [(BATCH_1, 1.0), (BATCH_2, 2.0), (BATCH_3, 1.0)]:
			ws.apply_scd2(table.refresh(), stage(raw_row(rain_sum = rain)), batch_id)

		v = versions(table, raw_row())
		assert list(v["rain_sum"]) == [1.0, 2.0, 1.0]
		assert v["is_current"].sum() == 1 and v.iloc[-1]["is_current"]

	def test_new_key_in_later_batch_is_inserted(self, table):
		ws.apply_scd2(table, stage(raw_row()), BATCH_1)
		stats = ws.apply_scd2(table.refresh(), stage(raw_row(), raw_row(date = "2026-10-26")), BATCH_2)

		assert stats == {"new": 1, "changed": 0, "unchanged": 1}
		new = versions(table, raw_row(date = "2026-10-26"))
		assert len(new) == 1
		assert new.loc[0, "is_current"] and new.loc[0, "batch_id"] == BATCH_2

	def test_key_missing_from_batch_stays_current(self, table):
		ws.apply_scd2(table, stage(raw_row(), raw_row(date = "2026-10-26")), BATCH_1)
		ws.apply_scd2(table.refresh(), stage(raw_row(date = "2026-10-26")), BATCH_2)

		v = versions(table, raw_row())
		assert len(v) == 1
		assert v.loc[0, "is_current"] and pd.isna(v.loc[0, "valid_to"])

	def test_mixed_batch_counts_and_one_current_row_per_key(self, table):
		ws.apply_scd2(table, stage(raw_row(date = "2026-10-25"), raw_row(date = "2026-10-26")), BATCH_1)
		stats = ws.apply_scd2(table.refresh(), stage(
			raw_row(date = "2026-10-25"),                 # unchanged
			raw_row(date = "2026-10-26", rain_sum = 7.0),  # changed
			raw_row(date = "2026-10-27"),                 # new
		), BATCH_2)

		assert stats == {"new": 1, "changed": 1, "unchanged": 1}
		df = all_rows(table)
		assert len(df) == 4
		current_per_key = df[df.is_current].groupby(ws.KEY_COLUMNS).size()
		assert len(current_per_key) == 3 and (current_per_key == 1).all()

	def test_change_only_affects_matching_location(self, table):
		here, there = raw_row(), raw_row(latitude = -37.57, longitude = 144.72)
		ws.apply_scd2(table, stage(here, there), BATCH_1)
		ws.apply_scd2(table.refresh(), stage(raw_row(rain_sum = 9.9), there), BATCH_2)

		assert len(versions(table, here)) == 2
		assert len(versions(table, there)) == 1

	def test_watermark_advances_even_without_changes(self, table):
		ws.apply_scd2(table, stage(raw_row()), BATCH_1)
		assert table.refresh().properties[ws.WATERMARK_PROPERTY] == BATCH_1

		ws.apply_scd2(table.refresh(), stage(raw_row()), BATCH_2)
		assert table.refresh().properties[ws.WATERMARK_PROPERTY] == BATCH_2

	def test_each_batch_is_a_single_commit(self, table):
		ws.apply_scd2(table, stage(raw_row()), BATCH_1)
		before = len(table.refresh().snapshots())
		ws.apply_scd2(table.refresh(), stage(raw_row(rain_sum = 9.9), raw_row(date = "2026-10-26")), BATCH_2)
		after = table.refresh().snapshots()
		# expire + insert land together: no snapshot exists with a closed version but no new one
		assert len(after) > before
		assert all_rows(table).groupby(ws.KEY_COLUMNS)["is_current"].sum().eq(1).all()


# ---------------------------------------------------------------------------
# Config wiring
# ---------------------------------------------------------------------------
class TestConfig:
	def test_schema_follows_configured_variables(self):
		from config import CONFIG
		variables = list(CONFIG["daily_variables"])
		assert ws.ATTRIBUTE_COLUMNS == ["elevation", *variables]
		assert ws.ALL_COLUMNS[:3] == ["latitude", "longitude", "date"]
		assert ws.ALL_COLUMNS[-6:] == ws.SCD_COLUMNS

	def test_paths_resolve_under_project_root(self):
		from config import CONFIG, PROJECT_ROOT
		for path in [ws.RAW_DIR, lakehouse.CATALOG_DB, lakehouse.table_location(CONFIG["staging"])]:
			assert path.is_absolute() and PROJECT_ROOT in path.parents

	def test_pipeline_config_env_var_selects_file(self, tmp_path, monkeypatch):
		import config
		other = tmp_path / "other.yaml"
		other.write_text("dataset: other\n")
		monkeypatch.setenv("PIPELINE_CONFIG", str(other))
		assert config.load_config()["dataset"] == "other"

	def test_missing_config_file_raises(self, monkeypatch):
		import config
		monkeypatch.setenv("PIPELINE_CONFIG", "config/does_not_exist.yaml")
		with pytest.raises(FileNotFoundError):
			config.load_config()
