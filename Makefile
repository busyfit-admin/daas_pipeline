# Pipeline entry point. Everything runs inside the Docker image (docker/).
#
#   make pipeline                                   raw -> staging -> current
#   make raw | make staging | make current          one layer
#   make pipeline PIPELINE_CONFIG=config/other.yaml use another config file

PIPELINE_CONFIG ?= config/weather.yaml

COMPOSE := docker compose -f docker/docker-compose.yml
RUN     := $(COMPOSE) run --rm -e PIPELINE_CONFIG=$(PIPELINE_CONFIG) app

RAW_SCRIPT     := pipeline_layers/10_ingest/weather_api.py
STAGING_SCRIPT := pipeline_layers/20_staging/weather_staging.py
CURRENT_SCRIPT := pipeline_layers/30_current/build_current.py

.DEFAULT_GOAL := help
.PHONY: help build raw staging current pipeline test shell bash

help: ## Show this help
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the Docker image (run after changing docker/requirements.txt)
	$(COMPOSE) build

raw: ## 10_raw: pull Open-Meteo data into data/10_raw/<dataset>/<batch_id>/
	$(RUN) $(RAW_SCRIPT)

staging: ## 20_staging: load new raw batches into the Iceberg table (SCD Type 2)
	$(RUN) $(STAGING_SCRIPT)

current: ## 30_current: rebuild the latest version of every staging record (one snapshot)
	$(RUN) $(CURRENT_SCRIPT)

pipeline: ## Run raw -> staging -> current
	$(MAKE) raw
	$(MAKE) staging
	$(MAKE) current

test: ## Run the test suite
	$(RUN) -m pytest

shell: ## Interactive Python shell in the container
	$(RUN)

bash: ## Bash shell in the container
	$(COMPOSE) run --rm --entrypoint bash app
