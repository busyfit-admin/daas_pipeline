"""
Current layer: the latest version of every record in the staging layer.

Dataset-agnostic: the source and target tables come from the pipeline config
(`staging` and `current` sections), and the data columns are whatever the staging
table holds minus its SCD bookkeeping columns.

  staging.<table> (is_current = true)  ->  current.<table>   (full overwrite, one snapshot per run)

Each run:
  1. reads the current rows from staging (is_current = true), pinned to one staging snapshot
  2. keeps the data columns and adds lineage columns
  3. overwrites the current table in a single Iceberg snapshot
  4. prints a summary

Lineage columns:
  last_updated        when this version was loaded (staging valid_from)
  source_batch        raw batch the version came from (staging batch_id)
  source_snapshot_id  staging snapshot the table was built from
  built_at            when this table was built (UTC)
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.expressions import EqualTo

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from config import CONFIG  # noqa: E402
from lakehouse import (  # noqa: E402
	SCD_COLUMNS,
	get_catalog,
	load_or_create_table,
	load_table,
	replace_table_contents,
	table_name,
)

LINEAGE_COLUMNS = ["last_updated", "source_batch", "source_snapshot_id", "built_at"]
TS = pa.timestamp("us", tz = "UTC")


def read_current_staging_rows(staging) -> tuple[pa.Table, int]:
	"""Rows with is_current = true, read from one pinned staging snapshot."""
	snapshot = staging.current_snapshot()
	if snapshot is None:
		raise SystemExit(f"Staging table {staging.name()} has no data yet; run the staging layer first")
	rows = staging.scan(row_filter = EqualTo("is_current", True), snapshot_id = snapshot.snapshot_id).to_arrow()
	return rows, snapshot.snapshot_id


def check_unique_keys(rows: pa.Table, key_columns: list[str]) -> None:
	"""Staging guarantees one current row per key; fail loudly if that's ever broken."""
	if not key_columns or rows.num_rows == 0:
		return
	counts = rows.group_by(key_columns).aggregate([([], "count_all")])
	duplicates = counts.filter(pc.greater(counts["count_all"], 1))
	if duplicates.num_rows:
		raise ValueError(f"{duplicates.num_rows} keys have more than one current row in staging, e.g. {duplicates.slice(0, 3).to_pylist()}")


def to_current(rows: pa.Table, source_snapshot_id: int, built_at: datetime) -> pa.Table:
	"""Keep the data columns and add the lineage columns."""
	data_columns = [c for c in rows.column_names if c not in SCD_COLUMNS]
	n = rows.num_rows
	out = rows.select(data_columns)
	out = out.append_column(pa.field("last_updated", TS), rows["valid_from"].cast(TS))
	out = out.append_column(pa.field("source_batch", pa.string()), rows["batch_id"].cast(pa.string()))
	out = out.append_column(pa.field("source_snapshot_id", pa.int64()), pa.array([source_snapshot_id] * n, pa.int64()))
	out = out.append_column(pa.field("built_at", TS), pa.array([built_at] * n, TS))
	return out


def sync_schema(table, schema: pa.Schema):
	"""Add any new columns from staging (e.g. a newly configured variable) to the current table."""
	if set(schema.names) - set(table.schema().column_names):
		with table.update_schema() as update:
			update.union_by_name(schema)
		table = table.refresh()
	return table


def build_current(catalog = None, built_at: datetime | None = None) -> dict:
	catalog = catalog or get_catalog()
	built_at = built_at or datetime.now(timezone.utc)
	staging_cfg, current_cfg = CONFIG["staging"], CONFIG["current"]

	staging = load_table(catalog, staging_cfg)
	rows, source_snapshot_id = read_current_staging_rows(staging)
	check_unique_keys(rows, staging_cfg.get("key_columns", []))

	current_rows = to_current(rows, source_snapshot_id, built_at)
	current = load_or_create_table(catalog, current_cfg, current_rows.schema)
	current = sync_schema(current, current_rows.schema)
	current = replace_table_contents(current, current_rows)

	snapshot = current.current_snapshot()
	last_updated = current_rows["last_updated"]
	batches = current_rows.group_by("source_batch").aggregate([([], "count_all")]).sort_by("source_batch")
	return {
		"source": table_name(staging_cfg),
		"target": table_name(current_cfg),
		"rows": current_rows.num_rows,
		"data_columns": [c for c in current_rows.column_names if c not in LINEAGE_COLUMNS],
		"source_snapshot_id": source_snapshot_id,
		"snapshot_id": snapshot.snapshot_id,
		"operation": snapshot.summary.operation.value,
		"built_at": built_at,
		"last_updated_min": pc.min(last_updated).as_py(),
		"last_updated_max": pc.max(last_updated).as_py(),
		"rows_per_batch": dict(zip(batches["source_batch"].to_pylist(), batches["count_all"].to_pylist())),
	}


def print_summary(s: dict) -> None:
	print(f"Config: {CONFIG['_path']}")
	print(f"Built {s['target']} from {s['source']} (snapshot {s['source_snapshot_id']})")
	print(f"  rows:          {s['rows']}")
	print(f"  data columns:  {len(s['data_columns'])} ({', '.join(s['data_columns'])})")
	print(f"  last_updated:  {s['last_updated_min']} -> {s['last_updated_max']}")
	print(f"  built_at:      {s['built_at']:%Y-%m-%d %H:%M:%S %Z}")
	print(f"  snapshot:      {s['snapshot_id']} ({s['operation']})")
	print("  rows per source batch:")
	for batch, count in s["rows_per_batch"].items():
		print(f"    {batch}: {count}")
	if not s["rows"]:
		print("  WARNING: staging has no current rows; the current table is now empty")


if __name__ == "__main__":
	print_summary(build_current())
