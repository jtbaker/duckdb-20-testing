# Benchmark Results

Run on 2026-10-04 using DuckDB `v2.0.0-alpha43763` in an OrbStack Docker environment with 12 CPUs and 16 GiB allocated to Docker.

## Dataset and storage

- 50,000,000 total operational energy-market rows
- 20,000,000 site-load rows
- 20,000,000 generation telemetry rows
- 10,000,000 locational price rows
- DuckLake metadata in PostgreSQL
- Parquet data in RustFS
- DuckLake global compression: ZSTD level 22
- 24 S3 objects totaling 424,444,270 bytes (approximately 404.8 MiB or 8.49 bytes/fact row)
- PostgreSQL catalog database size: approximately 8.5 MiB

A sampled Parquet file reported `ZSTD` compression, and DuckLake reported persisted global options `parquet_compression=zstd` and `parquet_compression_level=22`.

## Load throughput

The full run resumed after a 50,000-row smoke test, loading the remaining 49,950,000 rows.

| Measurement | Result |
| --- | ---: |
| Elapsed | 106.689 s |
| Effective throughput | 468,183 rows/s |
| Loader mean CPU | 437.8% |
| Loader maximum CPU | 599.4% |
| Loader peak memory | 2,401 MiB |
| RustFS peak memory | 281 MiB |
| PostgreSQL peak memory | 36.7 MiB |

CPU percentages use Docker semantics, where 100% represents one fully utilized CPU core.

## Query latency through one Quack server

Each query received one warmup followed by three measured repetitions. Concurrent runs issued four simultaneous requests, each with its own client-side DuckDB connection, against one Quack server.

| Query | C1 median | C1 p95 | C4 median | C4 p95 |
| --- | ---: | ---: | ---: | ---: |
| Point site/day history | 28.6 ms | 101.3 ms | 321.7 ms | 390.2 ms |
| Regional load dashboard | 55.0 ms | 130.7 ms | 297.6 ms | 438.2 ms |
| Generation capacity factor | 69.4 ms | 104.7 ms | 286.6 ms | 399.0 ms |
| LMP volatility/top nodes | 67.0 ms | 133.4 ms | 349.7 ms | 430.3 ms |
| Supply-demand-price join | 160.7 ms | 161.9 ms | 485.5 ms | 588.4 ms |
| Full 20M-row load scan | 179.1 ms | 208.4 ms | 432.6 ms | 536.1 ms |

## Query-phase container resources

| Phase/container | Mean CPU | Max CPU | Peak memory |
| --- | ---: | ---: | ---: |
| C1 Quack server | 93.2% | 184.5% | 316.8 MiB |
| C1 benchmark client | 186.0% | 186.0% | 66.6 MiB |
| C1 RustFS | 2.2% | 4.4% | 227.7 MiB |
| C1 PostgreSQL | 2.7% | 2.8% | 29.7 MiB |
| C4 Quack server | 107.0% | 242.6% | 676.4 MiB |
| C4 benchmark client | 172.3% | 199.5% | 124.2 MiB |
| C4 RustFS | 0.1% | 0.3% | 286.8 MiB |
| C4 PostgreSQL | 1.7% | 3.4% | 36.5 MiB |

## Initial observations

- Four concurrent requests increased server peak memory from about 317 MiB to 676 MiB.
- Concurrent latency increased by roughly 2.4–11.3x depending on selectivity; the highly selective point query showed the largest relative increase.
- The full scan remained below 0.54 seconds at p95 with four concurrent requests.
- PostgreSQL resource use stayed low because it served catalog metadata, not fact data.
- RustFS CPU stayed low during warm query runs; the DuckDB server performed most scan, decompression, and aggregation work.
- Client CPU is non-trivial because the DuckDB Quack client performs protocol handling and result materialization locally.

## Caveats

- This is synthetic, deterministic, highly compressible data; real telemetry cardinality and entropy may produce larger files and different scan behavior.
- Query runs were warm-cache tests with only three measured repetitions.
- Short query phases produced relatively few Docker statistics samples. The profiling loop has since been tightened for future runs.
- ZSTD level 22 intentionally trades write CPU for storage reduction. A fair compression study should compare levels such as 3, 9, and 22 using fresh volumes.
- OrbStack/Docker adds Linux virtualization and filesystem/network overhead, so these results should not be treated as bare-metal capacity numbers.
