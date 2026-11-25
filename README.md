# daas_pipeline

Data as a Service Pipeline: a local lakehouse that runs two datasets through four layers and stores the results as Iceberg tables.

| Layer | What it does |
|---|---|
| `10_raw` | Pulls the source data into CSV files |
| `20_staging` | Cleans the data and keeps the full change history (SCD Type 2) |
| `30_current` | Keeps the latest version of every record |
| `40_gold` | Runs SQL models that combine both datasets |

The two datasets are `weather` (from the Open-Meteo API) and `plant_watering` (randomly generated for 7 backyard plants).

## Requirements

- Docker (Docker Desktop on Mac and Windows)
- `make`

Everything runs inside Docker, so you don't need to install Python.

## 1. Build the Docker image

```bash
make build
```

Run this again whenever `docker/requirements.txt` changes.

## 2. Run the pipelines

Run everything (both datasets, then gold):

```bash
make all
```

Or run one dataset at a time:

```bash
make pipeline DATASET=weather
make pipeline DATASET=plant_watering
```

Each pipeline runs raw → staging → current.

## 3. Build the gold layer

```bash
make gold
```

`make all` already includes this step. Run it on its own after re-running a single dataset.

## 4. Run the sample queries

```bash
make sample        # top 20 rows of each current table
make sample-gold   # data quality checks and the garden report from the gold layer
```

To write your own SQL:

```bash
make query                                              # interactive prompt (end each statement with ;)
make query SQL="select * from weather_daily limit 5"    # run one query
make query LAYER=gold                                   # query the gold tables instead
```

## Other commands

```bash
make         # list all commands
make test    # run the tests
```

## Where things are

| Path | Contents |
|---|---|
| `config/` | All settings: `base.yaml` holds the shared settings, plus one file per dataset |
| `pipeline_layers/` | The code for each layer |
| `pipeline_layers/40_gold/models/` | Gold SQL models |
| `pipeline_layers/40_gold/seeds/` | Reference data (the water needs of each plant) |
| `queries/` | Sample queries |
| `data/` | Output (git ignores it) |
