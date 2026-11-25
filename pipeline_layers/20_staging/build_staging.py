"""
Staging layer: load raw batches of any dataset into an Iceberg table with SCD Type 2 history.

Dataset-agnostic: folders, table, business key, column types and checks all come from
the pipeline config (raw + staging sections), selected with PIPELINE_CONFIG.

  <raw.dir>/<batch_id>/<raw.file_prefix>_*.csv  ->  Iceberg table <staging.namespace>.<staging.table>

For each new raw batch (oldest first):
  1. read all CSVs in the batch folder
  2. clean them (types, rounding, nulls, duplicates, configured checks)
  3. compare with the current rows in the table, by business key + record_hash
  4. apply SCD Type 2 in a single Iceberg transaction:
       - new key         -> insert as current version
       - changed values  -> close the current version, insert a new current version
       - unchanged       -> nothing
     Keys missing from a batch are NOT closed: a raw batch may only cover part of the
     data (e.g. a rolling window of days), so absence does not mean the record was deleted.

SCD columns:
  record_hash  sha256 of the tracked attribute values (detects changes)
  valid_from   load time of the batch that introduced this version (UTC)
  valid_to     load time of the batch that replaced it; null while current
  is_current   true for the latest version of each key
  batch_id     raw batch folder that produced this version
  source_file  raw CSV the version came from (relative to the project root)

Processed batches are tracked with the table property in staging.watermark_property, so
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

TS = pa.timestamp("us", tz = "UTC")
ARROW_TYPES = {
	"string": pa.string(),
	"float": pa.float64(),
	"int": pa.int64(),
	"bool": pa.bool_(),
	"date": pa.date32(),
	"timestamp": TS,
}

# Data columns (name -> type) in table order, and the business key
COLUMN_TYPES = dict(_staging["columns"])
unknown = {c: t for c, t in COLUMN_TYPES.items() if t not in ARROW_TYPES}
if unknown:
	raise ValueError(f"Unknown column types in config: {unknown}; use one of {sorted(ARROW_TYPES)}")
KEY_COLUMNS = list(_staging["key_columns"])
if set(KEY_COLUMNS) - set(COLUMN_TYPES):
	raise ValueError(f"key_columns {sorted(set(KEY_COLUMNS) - set(COLUMN_TYPES))} are not in staging.columns")

# Tracked attributes: a change in any of these creates a new SCD version
ATTRIBUTE_COLUMNS = [c for c in COLUMN_TYPES if c not in KEY_COLUMNS]

def columns_of(kind: str) -> list[str]:
	return [c for c, t in COLUMN_TYPES.items() if t == kind]

TIMESTAMP_COLUMNS = columns_of("timestamp")
FLOAT_COLUMNS = columns_of("float")

FLOAT_DECIMALS = _staging.get("float_decimals")  # strip float noise so hashes are stable
DROP_IF_ALL_NULL = _staging.get("drop_if_all_null") or ATTRIBUTE_COLUMNS
CHECKS = _staging.get("checks") or {}

ARROW_SCHEMA = pa.schema(
	[(c, ARROW_TYPES[t]) for c, t in COLUMN_TYPES.items()]
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
assert ALL_COLUMNS[len(COLUMN_TYPES):] == SCD_COLUMNS


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
		# Read everything as text; clean() casts each column to its configured type
		df = pd.read_csv(csv_path, dtype = str)
		df["source_file"] = str(csv_path.relative_to(PROJECT_ROOT))
		frames.append(df)
	return pd.concat(frames, ignore_index = True) if frames else pd.DataFrame()


# ---------------------------------------------------------------------------
# Clean
# ---------------------------------------------------------------------------
def cast_column(series: pd.Series, kind: str) -> pd.Series:
	"""Cast one raw column to its configured type; unparseable values become null."""
	if kind == "date":
		# format="ISO8601" accepts both "2026-10-25" and "2026-10-25 00:00:00+00:00";
		# without it pandas infers one format from the first row and nulls the others.
		return pd.to_datetime(series, utc = True, errors = "coerce", format = "ISO8601").dt.date
	if kind == "timestamp":
		return pd.to_datetime(series, utc = True, errors = "coerce", format = "ISO8601")
	if kind == "float":
		return pd.to_numeric(series, errors = "coerce").astype("float64")
	if kind == "int":
		return pd.to_numeric(series, errors = "coerce").round().astype("Int64")
	if kind == "bool":
		return series.map(lambda v: str(v).strip().lower() in ("true", "1", "yes") if pd.notna(v) else None).astype("boolean")
	# string: trim whitespace, empty -> null
	text = series.astype("object").where(series.notna())
	text = text.map(lambda v: str(v).strip() if pd.notna(v) else None)
	return text.where(text != "")


def clean(df: pd.DataFrame) -> pd.DataFrame:
	"""Generic cleaning driven by staging.columns / key_columns / checks."""
	start = len(df)
	df = df.copy()
	df.columns = [c.strip().lower() for c in df.columns]

	missing = set(COLUMN_TYPES) - set(df.columns)
	if missing:
		raise ValueError(f"Raw batch is missing expected columns: {sorted(missing)}")

	for col, kind in COLUMN_TYPES.items():
		df[col] = cast_column(df[col], kind)
	if FLOAT_DECIMALS is not None and FLOAT_COLUMNS:
		df[FLOAT_COLUMNS] = df[FLOAT_COLUMNS].round(FLOAT_DECIMALS)

	# Drop rows without a usable key, or with nothing in the configured value columns
	df = df.dropna(subset = KEY_COLUMNS)
	df = df.dropna(subset = DROP_IF_ALL_NULL, how = "all")

	# Configured checks: invalidate impossible values rather than dropping the whole row
	for col in CHECKS.get("non_negative", []):
		df.loc[df[col] < 0, col] = None
	for low, high in CHECKS.get("less_or_equal", []):
		violations = df[low] > df[high]
		if violations.any():
			print(f"  WARNING: {violations.sum()} rows with {low} > {high}")

	# One row per business key (keep the last occurrence within the batch)
	df = df.drop_duplicates(subset = KEY_COLUMNS, keep = "last")

	dropped = start - len(df)
	if dropped:
		print(f"  Cleaning dropped {dropped} of {start} rows")
	return df.reset_index(drop = True)


def hash_value(value, kind: str) -> str:
	if pd.isna(value):
		return ""
	if kind in ("timestamp", "date"):
		return value.isoformat()
	if kind == "float":
		return repr(float(value))
	if kind == "int":
		return repr(int(value))
	return str(value)


def add_record_hash(df: pd.DataFrame) -> pd.DataFrame:
	def row_hash(row) -> str:
		parts = [hash_value(row[col], COLUMN_TYPES[col]) for col in ATTRIBUTE_COLUMNS]
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
	print(f"Config: {CONFIG['_path']} (dataset: {CONFIG.get('dataset')})")
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
