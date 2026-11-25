# Pipeline entry point. Everything runs inside the Docker image (docker/).
#
#   make pipeline                                   weather: raw -> staging -> current
#   make pipeline DATASET=plant_watering            another dataset (config/<dataset>.yaml)
#   make all                                        every dataset in DATASETS
#   make raw | make staging | make current          one layer (also takes DATASET=...)
#   make query                                      DuckDB SQL prompt on the current layer
#   make query SQL="select count(*) from weather_daily"
#   make pipeline PIPELINE_CONFIG=config/other.yaml use a config file directly

DATASET         ?= weather
DATASETS        ?= weather plant_watering
PIPELINE_CONFIG ?= config/$(DATASET).yaml

COMPOSE := docker compose -f docker/docker-compose.yml
RUN     := $(COMPOSE) run --rm -e PIPELINE_CONFIG=$(PIPELINE_CONFIG) app

RAW_SCRIPT     := pipeline_layers/10_ingest/run_raw.py
STAGING_SCRIPT := pipeline_layers/20_staging/build_staging.py
CURRENT_SCRIPT := pipeline_layers/30_current/build_current.py
QUERY_SCRIPT   := tools/query_current.py

LAYER ?= current

.DEFAULT_GOAL := help
.PHONY: help build raw staging current pipeline all query test shell bash

help: ## Show this help
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo "\n  Datasets: $(DATASETS)  (default DATASET=$(DATASET))"

build: ## Build the Docker image (run after changing docker/requirements.txt)
	$(COMPOSE) build

raw: ## 10_raw: run the dataset's ingest script -> data/10_raw/<dataset>/<batch_id>/
	$(RUN) $(RAW_SCRIPT)

staging: ## 20_staging: load new raw batches into the Iceberg table (SCD Type 2)
	$(RUN) $(STAGING_SCRIPT)

current: ## 30_current: rebuild the latest version of every staging record (one snapshot)
	$(RUN) $(CURRENT_SCRIPT)

pipeline: ## Run raw -> staging -> current for DATASET
	$(MAKE) raw
	$(MAKE) staging
	$(MAKE) current

all: ## Run the pipeline for every dataset in DATASETS
	@for d in $(DATASETS); do $(MAKE) pipeline DATASET=$$d PIPELINE_CONFIG=config/$$d.yaml || exit 1; done

query: ## DuckDB SQL on the current layer (SQL="..." to run one query, LAYER=staging for staging)
	$(RUN) $(QUERY_SCRIPT) --layer $(LAYER) $(if $(SQL),-c "$(SQL)")

test: ## Run the test suite
	$(RUN) -m pytest

shell: ## Interactive Python shell in the container
	$(RUN)

bash: ## Bash shell in the container
	$(COMPOSE) run --rm --entrypoint bash app
