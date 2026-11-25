#!/usr/bin/env bash
# Run anything inside the pipeline container (the Makefile is the main entry point; this is the raw helper).
#
# Usage (from the project root):
#   docker/run.sh pipeline                                    -> all steps of config/weather.yaml
#   docker/run.sh pipeline_layers/10_ingest/weather_api.py    -> run a single script
#   docker/run.sh build                                       -> (re)build the Docker image
#   docker/run.sh                                             -> interactive Python shell
#
# Use another pipeline config:  PIPELINE_CONFIG=config/plant_watering.yaml docker/run.sh pipeline
set -euo pipefail
cd "$(dirname "$0")/.."          # project root, so paths work from anywhere
COMPOSE=(docker compose -f docker/docker-compose.yml)
ENV_ARGS=()
[ -n "${PIPELINE_CONFIG:-}" ] && ENV_ARGS=(-e "PIPELINE_CONFIG=$PIPELINE_CONFIG")

case "${1:-}" in
  build)    exec "${COMPOSE[@]}" build ;;
  pipeline) shift; exec "${COMPOSE[@]}" run --rm ${ENV_ARGS[@]+"${ENV_ARGS[@]}"} py pipeline_layers/run_pipeline.py "$@" ;;
  *)        exec "${COMPOSE[@]}" run --rm ${ENV_ARGS[@]+"${ENV_ARGS[@]}"} py "$@" ;;
esac
