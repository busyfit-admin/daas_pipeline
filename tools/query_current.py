"""
Query the current layer with DuckDB from the command line.

Every Iceberg table in the current layer's namespace (config: current.namespace) is
loaded through pyiceberg and exposed to DuckDB as a view named after the table,
e.g. current.weather_daily -> weather_daily.

  python tools/query_current.py                          interactive SQL prompt
  python tools/query_current.py -c "select count(*) from weather_daily"
  python tools/query_current.py -f queries/top20_current.sql   run every statement in a file
  python tools/query_current.py --layer staging          query another layer's tables

In the prompt, end a statement with ';'. Commands: .tables, .schema <table>, .help, .quit
"""

import argparse
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipeline_layers" / "common"))
from config import CONFIG  # noqa: E402
from lakehouse import get_catalog  # noqa: E402

try:
	import readline  # noqa: F401  (arrow keys + history in the prompt)
except ImportError:
	pass

MAX_ROWS = 50


def connect(layer: str) -> tuple[duckdb.DuckDBPyConnection, list[str]]:
	"""In-memory DuckDB with one view per Iceberg table in the layer's namespace."""
	namespace = CONFIG[layer]["namespace"]
	catalog = get_catalog()
	con = duckdb.connect()
	names = []
	for identifier in catalog.list_tables(namespace):
		name = identifier[-1]
		catalog.load_table(identifier).scan().to_duckdb(table_name = name, connection = con)
		names.append(name)
	if not names:
		raise SystemExit(f"No tables in namespace '{namespace}'; run `make {layer}` first")
	return con, names


def run(con: duckdb.DuckDBPyConnection, sql: str) -> None:
	try:
		result = con.sql(sql)
	except duckdb.Error as e:
		print(f"Error: {e}")
		return
	if result is not None:  # statements like CREATE return nothing
		result.show(max_rows = MAX_ROWS, max_width = 1000)  # wide tables: one line per row


def run_file(con: duckdb.DuckDBPyConnection, path: str) -> None:
	"""Run each ';'-separated statement in a SQL file, printing a header per statement."""
	sql = Path(path).read_text()
	statements = [s.strip() for s in sql.split(";")]
	for statement in statements:
		code = "\n".join(l for l in statement.splitlines() if not l.strip().startswith("--")).strip()
		if not code:
			continue
		print(f"-- {' '.join(code.split())[:100]}")
		run(con, code)
		print()


def describe(con: duckdb.DuckDBPyConnection, table: str) -> None:
	run(con, f'DESCRIBE "{table}"')


def repl(con: duckdb.DuckDBPyConnection, tables: list[str], layer: str) -> None:
	print(f"DuckDB {duckdb.__version__} | layer: {layer} | tables: {', '.join(tables)}")
	print("End statements with ';'. Commands: .tables  .schema <table>  .help  .quit")
	buffer = []
	while True:
		try:
			line = input("...> " if buffer else "sql> ")
		except (EOFError, KeyboardInterrupt):
			print()
			return
		stripped = line.strip()

		if not buffer and stripped.startswith("."):
			command, *args = stripped.split()
			if command in (".quit", ".exit", ".q"):
				return
			elif command == ".tables":
				print("\n".join(tables))
			elif command == ".schema":
				for table in args or tables:
					print(f"-- {table}")
					describe(con, table)
			elif command == ".help":
				print(__doc__)
			else:
				print(f"Unknown command {command}; try .help")
			continue

		if stripped:
			buffer.append(line)
		if stripped.endswith(";"):
			run(con, "\n".join(buffer))
			buffer = []


def main() -> None:
	parser = argparse.ArgumentParser(description = "Query pipeline tables with DuckDB")
	parser.add_argument("-c", "--command", help = "run one SQL statement and exit")
	parser.add_argument("-f", "--file", help = "run the statements in a .sql file and exit")
	parser.add_argument("--layer", default = "current", help = "config section whose namespace to load (default: current)")
	args = parser.parse_args()

	con, tables = connect(args.layer)
	if args.command:
		run(con, args.command)
	elif args.file:
		run_file(con, args.file)
	else:
		repl(con, tables, args.layer)


if __name__ == "__main__":
	main()
