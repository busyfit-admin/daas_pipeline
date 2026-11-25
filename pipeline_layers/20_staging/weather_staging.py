"""
Staging layer: load raw weather batches into an Iceberg table with SCD Type 2 history.

  data/10_raw/weather/<batch_id>/*.csv  ->  Iceberg table staging.weather_daily

For each new raw batch (oldest first):
  1. read all CSVs in the batch folder
  2. clean them (types, rounding, nulls, duplicates, sanity checks)
  3. compare with the current rows in the table, by business key + record_hash
  4. apply SCD Type 2 in a single Iceberg transaction:
       - new key         -> insert as current version
       - changed values  -> close the current version, insert a new current version
       - unchanged       -> nothing
     Keys missing from a batch are NOT closed: each raw batch only covers a rolling
     window of days, so absence does not mean the record was deleted.

SCD columns:
  record_hash  sha256 of the tracked attribute values (detects changes)
  valid_from   load time of the batch that introduced this version (UTC)
  valid_to     load time of the batch that replaced it; null while current
  is_current   true for the latest version of each key
  batch_id     raw batch folder that produced this version
  source_file  raw CSV the version came from (relative to the project root)

All settings (folders, catalog, table, keys, variables) come from config/weather.yaml.

Processed batches are tracked with the table property "staging.last_batch_id", so
re-runs only pick up newer batches and old batches are never replayed over newer data.
"""

import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
from pyiceberg.expressions import EqualTo

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from config import CONFIG, PROJECT_ROOT, resolve  # noqa: E402
from lakehouse import SCD_COLUMNS, get_catalog, load_or_create_table, table_name  # noqa: E402

# ---------------------------------------------------------------------------
# Settings from config
# ---------------------------------------------------------------------------
_staging = CONFIG["staging"]

RAW_DIR = resolve(CONFIG["raw"]["dir"])
RAW_FILE_GLOB = f"{CONFIG['raw']['file_prefix']}_*.csv"
BATCH_ID_FORMAT = CONFIG["batch_id_format"]

TABLE_NAME = table_name(_staging)
WATERMARK_PROPERTY = _staging["watermark_property"]

# Business key, e.g. one row per location per day
KEY_COLUMNS = list(_staging["key_columns"])

# Tracked attributes: a change in any of these creates a new SCD version.
# elevation comes from the ingest layer's location columns; the rest are the API variables.
DAILY_VARIABLES = CONFIG["daily_variables"]
ATTRIBUTE_COLUMNS = ["elevation", *DAILY_VARIABLES]
TIMESTAMP_COLUMNS = [name for name, kind in DAILY_VARIABLES.items() if kind == "timestamp"]
MEASURE_COLUMNS = [c for c in ATTRIBUTE_COLUMNS if c not in TIMESTAMP_COLUMNS]

COORD_DECIMALS = _staging["coord_decimals"]
MEASURE_DECIMALS = _staging["measure_decimals"]

TS = pa.timestamp("us", tz = "UTC")
ARROW_SCHEMA = pa.schema(
	[("latitude", pa.float64()), ("longitude", pa.float64()), ("date", pa.date32())]
	+ [(c, TS if c in TIMESTAMP_COLUMNS else pa.float64()) for c in ATTRIBUTE_COLUMNS]
	+ [
		("record_hash", pa.string()),
		("valid_from", TS),
		("valid_to", TS),
		("is_current", pa.bool_()),
		("batch_id", pa.string()),
		("source_file", pa.string()),
	]
)
ALL_COLUMNS = ARROW_SCHEMA.names


# ---------------------------------------------------------------------------
# Iceberg
# ---------------------------------------------------------------------------
def get_table():
	"""Open (or create) the staging table in the shared lakehouse catalog."""
	return load_or_create_table(get_catalog(), _staging, ARROW_SCHEMA)


def to_arrow(df: pd.DataFrame) -> pa.Table:
	return pa.Table.from_pandas(df[ALL_COLUMNS], schema = ARROW_SCHEMA, preserve_index = False)


def load_current_rows(table) -> pd.DataFrame:
	current = table.scan(row_filter = EqualTo("is_current", True)).to_arrow()
	# Empty or not, go through the Arrow schema so column dtypes always match the batch
	return current.cast(ARROW_SCHEMA).to_pandas() if current.num_rows else ARROW_SCHEMA.empty_table().to_pandas()


# ---------------------------------------------------------------------------
# Raw batches
# ---------------------------------------------------------------------------
def find_new_batches(last_batch_id: str | None) -> list[Path]:
	"""Raw batch folders newer than the watermark, oldest first."""
	if not RAW_DIR.exists():
		return []
	batches = sorted(p for p in RAW_DIR.iterdir() if p.is_dir() and is_batch_id(p.name))
	return [b for b in batches if last_batch_id is None or b.name > last_batch_id]


def is_batch_id(name: str) -> bool:
	try:
		datetime.strptime(name, BATCH_ID_FORMAT)
		return True
	except ValueError:
		return False


def read_batch(batch_dir: Path) -> pd.DataFrame:
	frames = []
	for csv_path in sorted(batch_dir.glob(RAW_FILE_GLOB)):
		df = pd.read_csv(csv_path)
		df["source_file"] = str(csv_path.relative_to(PROJECT_ROOT))
		frames.append(df)
	return pd.concat(frames, ignore_index = True) if frames else pd.DataFrame()


# ---------------------------------------------------------------------------
# Clean
# ---------------------------------------------------------------------------
def clean(df: pd.DataFrame) -> pd.DataFrame:
	"""Boilerplate cleaning rules; extend as the dataset's quirks become known."""
	start = len(df)
	df = df.copy()
	df.columns = [c.strip().lower() for c in df.columns]

	missing = set(KEY_COLUMNS + ATTRIBUTE_COLUMNS) - set(df.columns)
	if missing:
		raise ValueError(f"Raw batch is missing expected columns: {sorted(missing)}")

	# Types. format="ISO8601" accepts both "2026-10-25" and "2026-10-25 00:00:00+00:00";
	# without it pandas infers one format from the first row and nulls the others.
	df["date"] = pd.to_datetime(df["date"], utc = True, errors = "coerce", format = "ISO8601").dt.date
	for col in TIMESTAMP_COLUMNS:
		df[col] = pd.to_datetime(df[col], utc = True, errors = "coerce", format = "ISO8601")
	for col in ["latitude", "longitude"] + MEASURE_COLUMNS:
		df[col] = pd.to_numeric(df[col], errors = "coerce")

	# Rounding
	df[["latitude", "longitude"]] = df[["latitude", "longitude"]].round(COORD_DECIMALS)
	df[MEASURE_COLUMNS] = df[MEASURE_COLUMNS].round(MEASURE_DECIMALS)

	# Drop rows without a usable key, or with no measurements at all
	df = df.dropna(subset = KEY_COLUMNS)
	df = df.dropna(subset = [c for c in ATTRIBUTE_COLUMNS if c != "elevation"], how = "all")

	# Sanity checks: invalidate impossible values rather than dropping the whole row
	if "rain_sum" in df:
		df.loc[df["rain_sum"] < 0, "rain_sum"] = None
	if {"temperature_2m_min", "temperature_2m_max"} <= set(df.columns):
		swapped = df["temperature_2m_min"] > df["temperature_2m_max"]
		if swapped.any():
			print(f"  WARNING: {swapped.sum()} rows with temperature_2m_min > temperature_2m_max")

	# One row per business key (keep the last occurrence within the batch)
	df = df.drop_duplicates(subset = KEY_COLUMNS, keep = "last")

	dropped = start - len(df)
	if dropped:
		print(f"  Cleaning dropped {dropped} of {start} rows")
	return df.reset_index(drop = True)


def add_record_hash(df: pd.DataFrame) -> pd.DataFrame:
	def row_hash(row) -> str:
		parts = []
		for col in ATTRIBUTE_COLUMNS:
			value = row[col]
			if pd.isna(value):
				parts.append("")
			elif col in TIMESTAMP_COLUMNS:
				parts.append(value.isoformat())
			else:
				parts.append(repr(float(value)))
		return hashlib.sha256("|".join(parts).encode()).hexdigest()

	df = df.copy()
	df["record_hash"] = df.apply(row_hash, axis = 1) if not df.empty else pd.Series(dtype = str)
	return df


# ---------------------------------------------------------------------------
# SCD Type 2
# ---------------------------------------------------------------------------
def apply_scd2(table, batch: pd.DataFrame, batch_id: str) -> dict:
	load_ts = datetime.strptime(batch_id, BATCH_ID_FORMAT).replace(tzinfo = timezone.utc)
	current = load_current_rows(table)

	# Compare incoming rows with current versions on the business key
	merged = batch.merge(
		current[KEY_COLUMNS + ["record_hash"]].rename(columns = {"record_hash": "current_hash"}),
		on = KEY_COLUMNS,
		how = "left",
	)
	is_new = merged["current_hash"].isna()
	is_changed = ~is_new & (merged["record_hash"] != merged["current_hash"])

	# New versions for new and changed keys
	inserts = merged.loc[is_new | is_changed].drop(columns = ["current_hash"]).copy()
	inserts["valid_from"] = load_ts
	inserts["valid_to"] = pd.NaT
	inserts["is_current"] = True
	inserts["batch_id"] = batch_id

	# Close the current versions of changed keys
	changed_keys = merged.loc[is_changed, KEY_COLUMNS]
	expired = current.merge(changed_keys, on = KEY_COLUMNS, how = "inner")
	expired["valid_to"] = load_ts
	expired["is_current"] = False

	stats = {"new": int(is_new.sum()), "changed": int(is_changed.sum()),
	         "unchanged": int(len(merged) - is_new.sum() - is_changed.sum())}

	# One atomic commit per batch: rows + watermark. Versions are identified by
	# business key + valid_from, so upsert updates expired rows and inserts new ones.
	with table.transaction() as txn:
		changes = pd.concat([expired, inserts], ignore_index = True)
		if not changes.empty:
			txn.upsert(to_arrow(changes), join_cols = KEY_COLUMNS + ["valid_from"])
		txn.set_properties(**{WATERMARK_PROPERTY: batch_id})
	return stats


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
	table = get_table()
	last_batch_id = table.properties.get(WATERMARK_PROPERTY)
	batches = find_new_batches(last_batch_id)
	print(f"Config: {CONFIG['_path']}")
	print(f"Watermark: {last_batch_id or '(none)'} -> {len(batches)} new raw batch(es) in {RAW_DIR.relative_to(PROJECT_ROOT)}")

	for batch_dir in batches:
		batch_id = batch_dir.name
		print(f"\nBatch {batch_id}")
		raw = read_batch(batch_dir)
		if raw.empty:
			print("  No raw files; skipping (watermark not advanced)")
			continue
		print(f"  Read {len(raw)} rows from {raw['source_file'].nunique()} files")

		staged = add_record_hash(clean(raw))
		stats = apply_scd2(table, staged, batch_id)
		table = table.refresh()
		print(f"  SCD2: {stats['new']} new, {stats['changed']} changed, {stats['unchanged']} unchanged")

	total = table.scan().to_arrow().num_rows
	current = len(load_current_rows(table))
	print(f"\nTable {TABLE_NAME}: {total} rows, {current} current")


if __name__ == "__main__":
	main()
