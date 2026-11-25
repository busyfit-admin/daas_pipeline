"""Basic data checks for pipeline_layers/30_current/build_current.py."""

from datetime import datetime, timezone

import pandas as pd
import pytest

import build_current as bc
import lakehouse
import weather_staging as ws
from config import CONFIG
from test_weather_staging import BATCH_1, BATCH_2, raw_row, stage, ts

BUILT_AT = datetime(2026, 11, 5, 12, 0, tzinfo = timezone.utc)


@pytest.fixture
def catalog(tmp_path, monkeypatch):
	monkeypatch.setattr(lakehouse, "CATALOG_DB", tmp_path / "catalog.db")
	monkeypatch.setattr(lakehouse, "LAKEHOUSE_ROOT", tmp_path / "data")
	return lakehouse.get_catalog()


def load_staging(*batches):
	"""Run apply_scd2 for each (batch_id, raw rows) pair; returns the staging table."""
	table = ws.get_table()
	for batch_id, rows in batches:
		ws.apply_scd2(table.refresh(), stage(*rows), batch_id)
	return table.refresh()


def current_df(catalog) -> pd.DataFrame:
	table = catalog.load_table(lakehouse.table_identifier(CONFIG["current"]))
	return table.scan().to_pandas().sort_values(["date"]).reset_index(drop = True)


def test_one_row_per_record_with_latest_values(catalog):
	load_staging(
		(BATCH_1, [raw_row(rain_sum = 1.0), raw_row(date = "2026-10-26", rain_sum = 5.0)]),
		(BATCH_2, [raw_row(rain_sum = 2.0), raw_row(date = "2026-10-26", rain_sum = 5.0)]),
	)
	bc.build_current(catalog, BUILT_AT)

	df = current_df(catalog)
	assert len(df) == 2
	assert not df.duplicated(CONFIG["staging"]["key_columns"]).any()
	assert list(df["rain_sum"]) == [2.0, 5.0]


def test_columns_are_data_plus_lineage(catalog):
	load_staging((BATCH_1, [raw_row()]))
	bc.build_current(catalog, BUILT_AT)

	data_columns = [c for c in ws.ALL_COLUMNS if c not in lakehouse.SCD_COLUMNS]
	assert list(current_df(catalog).columns) == data_columns + bc.LINEAGE_COLUMNS


def test_lineage_values(catalog):
	staging = load_staging(
		(BATCH_1, [raw_row(rain_sum = 1.0), raw_row(date = "2026-10-26")]),
		(BATCH_2, [raw_row(rain_sum = 2.0), raw_row(date = "2026-10-26")]),
	)
	bc.build_current(catalog, BUILT_AT)
	df = current_df(catalog)

	changed, unchanged = df.iloc[0], df.iloc[1]
	assert changed["last_updated"] == ts(BATCH_2) and changed["source_batch"] == BATCH_2
	assert unchanged["last_updated"] == ts(BATCH_1) and unchanged["source_batch"] == BATCH_1
	assert (df["source_snapshot_id"] == staging.current_snapshot().snapshot_id).all()
	assert (df["built_at"] == pd.Timestamp(BUILT_AT)).all()


def test_rebuild_replaces_rows(catalog):
	load_staging((BATCH_1, [raw_row(rain_sum = 1.0)]))
	bc.build_current(catalog, BUILT_AT)
	load_staging((BATCH_2, [raw_row(rain_sum = 2.0), raw_row(date = "2026-10-26")]))
	bc.build_current(catalog, BUILT_AT)

	df = current_df(catalog)
	assert len(df) == 2
	assert list(df["rain_sum"]) == [2.0, 1.5]
