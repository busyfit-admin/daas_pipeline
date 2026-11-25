"""
Shared Iceberg helpers for the table layers (20_staging, 30_current, ...).

All layers register their tables in one SQL-backed catalog (lakehouse.catalog_db).
Each table's files live at <lakehouse.root>/<layer dir>/<table>, e.g.
data/20_staging/weather_daily, so the data folders mirror the pipeline layers.
"""

from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io.pyarrow import _dataframe_to_data_files
from pyiceberg.table import Table

from config import CONFIG, resolve

_lakehouse = CONFIG["lakehouse"]
CATALOG_NAME = _lakehouse["catalog_name"]
CATALOG_DB = resolve(_lakehouse["catalog_db"])
LAKEHOUSE_ROOT = resolve(_lakehouse["root"])

# SCD Type 2 bookkeeping columns written by the staging layer
SCD_COLUMNS = ["record_hash", "valid_from", "valid_to", "is_current", "batch_id", "source_file"]


def get_catalog() -> SqlCatalog:
	CATALOG_DB.parent.mkdir(parents = True, exist_ok = True)
	LAKEHOUSE_ROOT.mkdir(parents = True, exist_ok = True)
	return SqlCatalog(CATALOG_NAME, uri = f"sqlite:///{CATALOG_DB}", warehouse = LAKEHOUSE_ROOT.as_uri())


def table_identifier(layer: dict) -> tuple[str, str]:
	"""(namespace, table) for a layer section of the config, e.g. CONFIG["staging"]."""
	return (layer["namespace"], layer["table"])


def table_name(layer: dict) -> str:
	return ".".join(table_identifier(layer))


def table_location(layer: dict) -> Path:
	return LAKEHOUSE_ROOT / layer["dir"] / layer["table"]


def load_table(catalog: SqlCatalog, layer: dict) -> Table:
	try:
		return catalog.load_table(table_identifier(layer))
	except NoSuchTableError:
		raise SystemExit(f"Table {table_name(layer)} does not exist yet; run the layer that builds it first") from None


def load_or_create_table(catalog: SqlCatalog, layer: dict, schema: pa.Schema) -> Table:
	identifier = table_identifier(layer)
	catalog.create_namespace_if_not_exists(identifier[0])
	try:
		return catalog.load_table(identifier)
	except NoSuchTableError:
		print(f"Creating Iceberg table {table_name(layer)} at {table_location(layer)}")
		return catalog.create_table(identifier, schema = schema, location = table_location(layer).as_uri())


def replace_table_contents(table: Table, data: pa.Table) -> Table:
	"""
	Replace all rows of `table` with `data` in a single Iceberg snapshot.

	Table.overwrite() commits two snapshots on a non-empty table (delete + append);
	this swaps the data files in one "overwrite" snapshot instead.
	"""
	with table.transaction() as txn:
		with txn.update_snapshot().overwrite() as op:
			for task in table.scan().plan_files():
				op.delete_data_file(task.file)
			if data.num_rows:
				for data_file in _dataframe_to_data_files(
					table_metadata = txn.table_metadata, df = data, io = table.io, write_uuid = op.commit_uuid,
				):
					op.append_data_file(data_file)
	return table.refresh()
