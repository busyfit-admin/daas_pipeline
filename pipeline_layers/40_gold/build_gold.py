"""
Gold layer: run SQL models over the current layer of every dataset and publish them as Iceberg tables.

Settings come from the `gold` section of config/base.yaml. Each run:
  1. loads every current-layer table into DuckDB as   current.<table>
  2. loads every seed CSV (seeds_dir/*.csv) as         seeds.<seed>   and publishes it as gold.seed_<seed>
  3. runs the models (models_dir/<model>.sql) in the configured order; each result becomes
     gold.<model> in DuckDB for later models, and is written to the Iceberg table
     <gold.namespace>.<model> with a built_at column, replacing its contents in one snapshot
  4. prints a summary

Gold tables are fully derived, so if a model's columns change the table is recreated.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from config import CONFIG, resolve  # noqa: E402
from lakehouse import get_catalog, load_or_create_table, replace_table_contents  # noqa: E402

GOLD = CONFIG["gold"]
MODELS_DIR = resolve(GOLD["models_dir"])
SEEDS_DIR = resolve(GOLD["seeds_dir"])
TS = pa.timestamp("us", tz = "UTC")


def connect() -> duckdb.DuckDBPyConnection:
	con = duckdb.connect()
	con.execute("SET TimeZone = 'UTC'")
	for schema in ("current", "seeds", "gold"):
		con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
	return con


def register(con: duckdb.DuckDBPyConnection, schema: str, name: str, data: pa.Table) -> None:
	"""Expose an Arrow table to SQL as <schema>.<name>."""
	alias = f"__{schema}_{name}"
	con.register(alias, data)
	con.execute(f'CREATE OR REPLACE VIEW "{schema}"."{name}" AS SELECT * FROM "{alias}"')


def load_sources(con, catalog) -> dict[str, int]:
	namespace = GOLD["source_namespace"]
	counts = {}
	for identifier in catalog.list_tables(namespace):
		data = catalog.load_table(identifier).scan().to_arrow()
		register(con, "current", identifier[-1], data)
		counts[identifier[-1]] = data.num_rows
	if not counts:
		raise SystemExit(f"No tables in namespace '{namespace}'; run the pipelines up to the current layer first")
	return counts


def load_seeds(con) -> dict[str, pa.Table]:
	seeds = {}
	for csv_path in sorted(SEEDS_DIR.glob("*.csv")):
		data = con.sql(f"SELECT * FROM read_csv('{csv_path}', header = true)").to_arrow_table()
		register(con, "seeds", csv_path.stem, data)
		seeds[csv_path.stem] = data
	return seeds


def normalize(data: pa.Table) -> pa.Table:
	"""DuckDB returns SUM(int) as decimal(38,0); store whole-number decimals as int64, others as float64."""
	fields = []
	for field in data.schema:
		if pa.types.is_decimal(field.type):
			field = field.with_type(pa.int64() if field.type.scale == 0 else pa.float64())
		elif pa.types.is_large_string(field.type) or pa.types.is_string_view(field.type):
			field = field.with_type(pa.string())
		fields.append(field)
	return data.cast(pa.schema(fields))


def _columns(schema: pa.Schema) -> list[tuple[str, pa.DataType]]:
	"""Column names + types, treating string/large_string as equal (both are Iceberg 'string')."""
	return [(f.name, pa.string() if pa.types.is_large_string(f.type) else f.type) for f in schema]


def publish(catalog, name: str, data: pa.Table, built_at: datetime):
	"""Write `data` + built_at to <gold namespace>.<name> in one snapshot."""
	data = normalize(data).append_column(pa.field("built_at", TS), pa.array([built_at] * data.num_rows, TS))
	layer = {"dir": GOLD["dir"], "namespace": GOLD["namespace"], "table": name}
	table = load_or_create_table(catalog, layer, data.schema)
	if _columns(table.schema().as_arrow()) != _columns(data.schema):
		print(f"  {name}: columns changed, recreating table")
		catalog.drop_table((GOLD["namespace"], name))
		table = load_or_create_table(catalog, layer, data.schema)
	return replace_table_contents(table, data).current_snapshot()


def build_gold(catalog = None, built_at: datetime | None = None) -> list[dict]:
	catalog = catalog or get_catalog()
	built_at = built_at or datetime.now(timezone.utc)
	con = connect()

	sources = load_sources(con, catalog)
	print(f"Sources: {', '.join(f'current.{t} ({n} rows)' for t, n in sources.items())}")

	results = []
	for name, data in load_seeds(con).items():
		snapshot = publish(catalog, f"seed_{name}", data, built_at)
		results.append({"model": f"seed_{name}", "kind": "seed", "rows": data.num_rows,
		                "columns": data.num_columns, "snapshot_id": snapshot.snapshot_id})

	for name in GOLD["models"]:
		sql = (MODELS_DIR / f"{name}.sql").read_text()
		try:
			data = con.sql(sql).to_arrow_table()
		except duckdb.Error as e:
			raise SystemExit(f"Model {name} failed: {e}") from None
		register(con, "gold", name, data)
		snapshot = publish(catalog, name, data, built_at)
		results.append({"model": name, "kind": "model", "rows": data.num_rows,
		                "columns": data.num_columns, "snapshot_id": snapshot.snapshot_id})
	return results


def print_summary(results: list[dict]) -> None:
	print(f"\nBuilt {len(results)} gold tables in namespace '{GOLD['namespace']}':")
	for r in results:
		print(f"  {r['kind']:<5}  {r['model']:<42} {r['rows']:>6} rows  {r['columns']:>3} cols  snapshot {r['snapshot_id']}")


if __name__ == "__main__":
	print(f"Config: {CONFIG['_path']}")
	print_summary(build_gold())
