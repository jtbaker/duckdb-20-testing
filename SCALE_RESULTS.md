# Large Append-Table Results

Observed on 2026-10-04 using DuckDB `v2.0.0-alpha43763`, an Apple M4 Pro Mac with 12 CPU cores and 64 GiB host memory, and an OrbStack Docker environment configured with 12 CPUs and 16 GiB memory.

These are proof-of-concept observations, not controlled cloud or bare-metal benchmarks.

## Architecture

- Ten independent DuckDB connections issue concurrent `INSERT INTO lake.main.operational_measurements ...` transactions.
- DuckLake commits metadata to PostgreSQL and writes every committed batch as Parquet in RustFS.
- No files are written directly to the bucket and no files are registered after the fact.
- Each 5M-row transaction becomes visible through Quack as soon as it commits.
- Parquet uses ZSTD level 22, 122,880-row groups, disabled row inlining, and a 1 GB target file size.

## Final load

| Measurement | Result |
| --- | ---: |
| Rows | 450,000,000 |
| DuckLake data size | 32,509,944,576 bytes (32.51 GB) |
| Parquet files | 91 |
| Stored bytes/row | 72.2443 |
| Parquet column compressed bytes | 32,429,991,270 |
| Parquet column uncompressed bytes | 47,375,532,359 |
| Compression ratio | 1.4609x |
| Overall throughput | 349,883 rows/s |
| Loader exit status | 0 |

The wider table intentionally contains high-entropy floating-point measurements and hash/bitmap fields. Its compression ratio is therefore much lower than the highly repetitive 50M-row baseline dataset even though both use ZSTD level 22.

Ten ETL workers sustained approximately 970-1005% Docker CPU. Observed loader memory was approximately 5.3-5.9 GiB of its 10 GiB limit. PostgreSQL remained lightly utilized because row data flowed directly between DuckDB and RustFS while PostgreSQL handled DuckLake metadata transactions.

## Selective aggregate through Quack

Test query:

```sql
SELECT max(temperature_c)
FROM remote.operational_measurements
WHERE market = 'ERCOT';
```

DuckDB's physical plan projected `temperature_c`, pushed down the `market = 'ERCOT'` filter, performed the aggregate on the Quack server, and returned one value to the client.

The two required Parquet columns contain approximately 1.65 GB of compressed data. With DuckDB's adaptive Parquet range coalescing, observed server ingress was approximately 18-29 GB because the columns are separated by several high-entropy columns in the physical schema. Configuring the server with:

```sql
SET GLOBAL parquet_prefetch_column_gap = 0;
```

reduced observed server ingress to approximately 1.72 GB. The final observed query completed in 14.88 seconds, compared with approximately 76-80 seconds in earlier default-prefetch runs. These runs were not controlled cold-cache comparisons, but the byte reduction was repeatable and consistent with the required column sizes.

The containerized client also raises `http_timeout` and the Quack heartbeat lease to 3,600 seconds so legitimate scans are not terminated by the `httpfs` 30-second default.

## Caveats

- The table is larger than DuckDB's 6 GB server memory limit, but the 64 GiB host and OrbStack/Linux filesystem can still cache object data. These are not guaranteed cold-storage measurements.
- RustFS runs locally, so it does not model cloud object-store round-trip latency or request pricing.
- `parquet_prefetch_column_gap=0` favors minimal transferred bytes over fewer range requests. A small nonzero threshold may be better for remote object storage.
- Ten concurrent inserts produce more, smaller files than one serial writer. Each 5M-row transaction produced roughly one 360 MB file.
- The scale loader retries failed DuckLake transactions during a running wave. It is not a production workflow engine and does not provide a durable external batch ledger for recovery from a process or host crash in the middle of a wave.
