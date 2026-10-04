#!/usr/bin/env bash
set -euo pipefail

PROJECT="duckdb-20-poc"
RESULTS_DIR="${BENCHMARK_RESULTS_DIR:-benchmark-results}"
mkdir -p "$RESULTS_DIR"

sampler_pid=""

start_sampler() {
  local output=$1
  printf 'timestamp,container,cpu_percent,memory_usage,memory_percent,net_io,block_io,pids\n' >"$output"
  (
    while true; do
      containers=$(docker ps -q --filter "label=com.docker.compose.project=${PROJECT}")
      if [[ -n "$containers" ]]; then
        # Intentional word splitting: docker stats accepts a list of container IDs.
        docker stats --no-stream \
          --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}' \
          $containers |
          while IFS= read -r metrics; do
            printf '%s,%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$metrics"
          done >>"$output"
      fi
    done
  ) &
  sampler_pid=$!
}

stop_sampler() {
  if [[ -n "$sampler_pid" ]]; then
    kill "$sampler_pid" 2>/dev/null || true
    wait "$sampler_pid" 2>/dev/null || true
    sampler_pid=""
  fi
}

trap stop_sampler EXIT

echo "Starting PostgreSQL and RustFS..."
docker compose up -d --wait postgres rustfs
docker compose up --no-log-prefix rustfs-init

echo "Building benchmark image..."
docker compose --profile benchmark build duckdb-loader duckdb-server benchmark-client

echo "Loading/resuming the 50M-row energy dataset..."
docker compose --profile benchmark stop duckdb-server >/dev/null 2>&1 || true
start_sampler "$RESULTS_DIR/container-stats-load.csv"
docker compose --profile benchmark up --no-log-prefix duckdb-loader
stop_sampler

echo "Starting the Quack server after the loader completed..."
docker compose --profile benchmark up -d --wait --no-deps duckdb-server

for concurrency in 1 4; do
  echo "Running benchmark at concurrency=${concurrency}..."
  start_sampler "$RESULTS_DIR/container-stats-c${concurrency}.csv"
  BENCHMARK_CONCURRENCY=$concurrency \
    docker compose --profile benchmark run --rm --no-deps benchmark-client
  stop_sampler
done

uv run python summarize_metrics.py

echo "Results written to $RESULTS_DIR"
