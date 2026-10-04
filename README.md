# DuckDB 2.0 + Quack + DuckLake POC

A minimal local POC using:

- DuckDB 2.0 alpha from the Python prerelease channel
- DuckLake with PostgreSQL as the multi-client metadata catalog
- RustFS as S3-compatible Parquet storage
- Quack as the DuckDB client/server protocol
- `uv` for dependency management and execution

The repository contains two reproducible workloads:

- A 50M-row, three-table energy-market benchmark with concurrency profiling
- A 450M-row, 32.5 GB append-table workload written by concurrent, idiomatic DuckLake transactions

Observed results are recorded in [`BENCHMARK_RESULTS.md`](BENCHMARK_RESULTS.md) and [`SCALE_RESULTS.md`](SCALE_RESULTS.md). Run the `benchmark` and `scale` profiles separately; their Quack servers intentionally share host port 9494.

## Architecture

```text
DuckDB client --Quack/HTTP--> DuckDB 2.0 server
                                    |       |
                                    |       +--> RustFS (Parquet data)
                                    +----------> PostgreSQL (DuckLake metadata)
```

`DATA_INLINING_ROW_LIMIT 0` is intentional: data is written to RustFS instead of being inlined into PostgreSQL.

## Start infrastructure

```bash
docker compose up -d --wait postgres rustfs
docker compose up rustfs-init
```

Endpoints and development credentials:

| Service | Endpoint | Credentials |
| --- | --- | --- |
| PostgreSQL | `localhost:5432` | `ducklake` / `ducklake` |
| RustFS S3 | `http://localhost:9000` | `rustfsadmin` / `rustfsadmin` |
| RustFS console | `http://localhost:9001` | `rustfsadmin` / `rustfsadmin` |

Compose creates the `ducklake-poc` bucket automatically.

## Initialize and verify DuckLake

```bash
uv run python poc.py init
uv run python poc.py check
```

The output should include rows from DuckLake and at least one Parquet object under `s3://ducklake-poc/data`.

## Serve DuckLake over Quack

Terminal 1:

```bash
uv run python quack_server.py
```

Terminal 2:

```bash
uv run python quack_client.py
```

The client attaches the remote Quack catalog and reads `remote.events`. The server exposes that name as a small gateway view over `lake.main.events`; DuckLake metadata remains in PostgreSQL and the table data remains in RustFS.

## Energy-market throughput benchmark

The benchmark profile runs every workload component in a container:

- `duckdb-loader`: resumable loader for 50 million fact rows
- `duckdb-server`: one Quack server exposing the DuckLake
- `benchmark-client`: asynchronous orchestration of concurrent Quack requests
- PostgreSQL: DuckLake metadata catalog
- RustFS: Parquet object storage

The generated operational dataset contains:

| Table | Rows | Time series |
| --- | ---: | --- |
| `site_load` | 20,000,000 | Site load, forecast, weather, voltage, frequency, and quality |
| `generation_telemetry` | 20,000,000 | Available capacity, generation, ramp, outages, and curtailment |
| `locational_prices` | 10,000,000 | Energy, congestion, loss, reserve, and total LMP components |

All benchmark writes use the persisted DuckLake options:

```sql
CALL lake.set_option('parquet_compression', 'zstd');
CALL lake.set_option('parquet_compression_level', '22');
```

Run the loader, a single-client benchmark, a four-client benchmark, and one-second container resource sampling:

```bash
./profile_benchmark.sh
```

The latest observed run is summarized in [`BENCHMARK_RESULTS.md`](BENCHMARK_RESULTS.md). Raw results are written under `benchmark-results/`:

- `load-report.json`
- `queries-c1.csv` and `summary-c1.json`
- `queries-c4.csv` and `summary-c4.json`
- `container-stats-load.csv`, `container-stats-c1.csv`, and `container-stats-c4.csv`
- `resource-summary.json` with mean/peak CPU and memory by container and phase

The workload covers selective site history, regional dashboards, generation capacity factors, LMP volatility, a supply/demand/price join, and a full scan aggregation.

The loader is resumable. To rebuild benchmark tables, run:

```bash
ENERGY_RESET=true ./profile_benchmark.sh
```

For a quick smoke test, reduce the row counts:

```bash
ENERGY_LOAD_ROWS=200000 \
ENERGY_GENERATION_ROWS=200000 \
ENERGY_LMP_ROWS=100000 \
ENERGY_LOAD_BATCH_ROWS=100000 \
./profile_benchmark.sh
```

The Python client uses `asyncio.TaskGroup` for orchestration and `asyncio.to_thread` around each blocking DuckDB call. DuckDB's 2.0 Python API and Quack extension do not currently provide a native awaitable query API. Each concurrent request owns a separate DuckDB connection.

## Large append-table workload

The `scale` profile creates one wide `operational_measurements` table and loads until DuckLake reports approximately 32 GB of Parquet data. Allow at least 40 GB of free Docker storage; the observed run used a 12-CPU/16-GiB Docker environment. Ten independent ETL workers each own a DuckDB connection and issue 5M-row transactions through normal DuckLake SQL:

```sql
INSERT INTO lake.main.operational_measurements
SELECT ...;
```

DuckLake writes the files to RustFS and commits their metadata to PostgreSQL. There are no direct bucket writes or post-hoc file registration. Each successful transaction is queryable through Quack immediately.

Start a fresh load:

```bash
docker compose --profile benchmark --profile scale down -v --remove-orphans
docker compose --profile scale up -d --build duckdb-append-loader
docker compose --profile scale logs -f duckdb-append-loader
```

After the loader exits successfully, start Quack and the CLI container:

```bash
docker compose --profile scale up -d --build duckdb-scale-server duckdb-scale-shell
docker compose exec duckdb-scale-shell connect-quack
```

Example queries:

```sql
SELECT count(*) FROM remote.operational_measurements;
SELECT max(temperature_c)
FROM remote.operational_measurements
WHERE market = 'ERCOT';
```

Progress is written to `benchmark-results/parallel-append-report.json`. Defaults can be overridden with the `APPEND_*` variables listed in [`.env.example`](.env.example). The observed 450M-row run and resource profile are documented in [`SCALE_RESULTS.md`](SCALE_RESULTS.md).

The server sets `parquet_prefetch_column_gap=0` globally. For this physical schema, the predicate and projected columns can be separated by large high-entropy byte ranges; disabling cross-column range coalescing reduced one selective scan from roughly 18-29 GB of object traffic to approximately 1.72 GB. This is configurable with `PARQUET_PREFETCH_COLUMN_GAP` and should be retested for other schemas or remote object stores.

## Interactive containerized DuckDB CLI

Start the persistent shell container. Its image includes the DuckDB 2.0 alpha CLI and Quack extension; it does not modify the host DuckDB installation.

```bash
docker compose --profile benchmark up -d --build duckdb-shell
docker compose exec duckdb-shell bash
```

For the scale profile, use `duckdb-scale-shell` instead. Inside either container, start a CLI session already attached to its Quack server:

```bash
connect-quack
```

The launcher sets one-hour HTTP and Quack heartbeat timeouts so scans longer than `httpfs`'s 30-second default can complete. The remote catalog is named `remote`:

```sql
FROM remote.site_load LIMIT 10;
SELECT count(*) FROM remote.generation_telemetry;
SELECT market, avg(lmp_total_usd_mwh) FROM remote.locational_prices GROUP BY market;
```

You can also run a non-interactive command without opening Bash:

```bash
docker compose exec duckdb-shell connect-quack -c "FROM remote.site_load LIMIT 10"
```

## Reset

Include both profiles so Compose removes stopped workload containers before deleting the named PostgreSQL, RustFS, and extension volumes:

```bash
docker compose --profile benchmark --profile scale down -v --remove-orphans
```

Then run the desired initialization or workload steps again. OrbStack can briefly create ephemeral helper containers while mounted volumes are being released; verify cleanup with `docker compose ls` and `docker volume ls` before assuming a failed `down -v` removed the data.

## Configuration

The defaults in `poc.py` match `compose.yaml`. Copy `.env.example` values into your shell or task runner to override them. Python does not implicitly load `.env` files.

## POC caveats and practices

- DuckDB 2.0 and Quack are prerelease software. The exact Python development build is pinned in `pyproject.toml`/`uv.lock`; upgrade deliberately and rerun the POC.
- This alpha's Quack attached-catalog shorthand resolves unqualified names in the server's primary catalog, so the server creates a local view over the attached DuckLake table. One-shot `quack_query` calls can query `lake.main.events` directly.
- RustFS and PostgreSQL images are pinned, avoiding moving `latest` tags.
- Quack uses plaintext HTTP only on localhost for this POC. Use TLS and certificate pinning for any non-local deployment.
- Credentials are development-only and committed for reproducibility. Use Docker secrets or an external secret manager outside a POC.
- PostgreSQL is preferable to DuckDB/SQLite as the DuckLake catalog when multiple or remote clients may write concurrently.
- Object storage and catalog backups must be coordinated. Restoring only one side can leave catalog metadata and Parquet objects inconsistent.
- Quack serves an existing DuckDB session; `quack_server.py` must remain running.
- The scale loader retries transaction failures within a wave, but it is a POC rather than a durable workflow engine and has no external crash-recovery ledger.
- The scale dataset exceeds DuckDB's configured memory, but host and VM filesystem caches may still contain object data; results should not be described as guaranteed cold-cache measurements.
- `parquet_prefetch_column_gap=0` minimizes read amplification for this schema at the cost of more range requests. Retune it for higher-latency object stores and different column layouts.
- ZSTD level 22 favors storage reduction over load throughput and CPU efficiency. This benchmark intentionally measures that tradeoff.

## Sources consulted

- DuckDB 2.0 alpha announcement: <https://duckdb.org/2026/09/02/try-duckdb-20-alpha>
- Quack source and usage: <https://github.com/duckdb/duckdb-quack>
- DuckLake source, tests, and RustFS-compatible S3 patterns: <https://github.com/duckdb/ducklake>
- DuckLake catalog guidance: <https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database>
- RustFS source and Docker quickstart: <https://github.com/rustfs/rustfs>
