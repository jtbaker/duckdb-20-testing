from __future__ import annotations

import json
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import replace
from pathlib import Path

import duckdb

from load_append_energy import (
    MAX_ROWS,
    MAX_STORAGE_BYTES,
    TABLE,
    TARGET_STORAGE_BYTES,
    configure_lake,
    create_table,
    generated_select_sql,
    parquet_compression,
    table_storage,
)
from poc import Config, connect_lake

WORKERS = int(os.getenv("APPEND_WORKERS", "10"))
ROWS_PER_INSERT = int(os.getenv("APPEND_ROWS_PER_INSERT", "5000000"))
MAX_RETRIES = int(os.getenv("APPEND_MAX_RETRIES", "5"))
RESULTS_DIR = Path(os.getenv("BENCHMARK_RESULTS_DIR", "benchmark-results"))
REPORT_PATH = RESULTS_DIR / "parallel-append-report.json"


def insert_batch(start: int, end: int) -> dict[str, int | float]:
    # One DuckDB connection and one DuckLake transaction per ETL worker. ZSTD's
    # level-22 writer is effectively single-core, so parallelism comes from
    # independent transactions rather than oversubscribing each connection.
    config = replace(Config(), duckdb_threads=1, duckdb_memory_limit="768MB")
    connection = connect_lake(config)
    started = time.perf_counter()
    try:
        connection.execute(
            f"INSERT INTO lake.main.{TABLE} " + generated_select_sql(start, end)
        )
        return {
            "start": start,
            "end": end,
            "rows": end - start,
            "seconds": round(time.perf_counter() - started, 3),
        }
    finally:
        connection.close()


def run_wave(jobs: list[tuple[int, int]]) -> list[dict[str, int | float]]:
    pending = jobs
    completed: list[dict[str, int | float]] = []
    for attempt in range(1, MAX_RETRIES + 1):
        failures: list[tuple[int, int]] = []
        with ProcessPoolExecutor(max_workers=min(WORKERS, len(pending))) as pool:
            future_jobs = {pool.submit(insert_batch, *job): job for job in pending}
            for future in as_completed(future_jobs):
                job = future_jobs[future]
                try:
                    result = future.result()
                except duckdb.Error as error:
                    print(
                        f"insert {job[0]:,}..{job[1]:,} failed on attempt "
                        f"{attempt}: {error}",
                        flush=True,
                    )
                    failures.append(job)
                else:
                    completed.append(result)
                    print(
                        f"committed {result['rows']:,} rows "
                        f"({result['start']:,}..{result['end']:,}) through DuckLake "
                        f"in {result['seconds']}s",
                        flush=True,
                    )
        if not failures:
            return completed
        pending = failures
        time.sleep(min(2**attempt, 15))
    raise RuntimeError(
        f"{len(pending)} DuckLake inserts failed after {MAX_RETRIES} attempts"
    )


def write_report(connection, started: float, wave_seconds: float) -> dict[str, object]:
    rows = int(
        connection.execute(f"SELECT count(*) FROM lake.main.{TABLE}").fetchone()[0]
    )
    file_count, size = table_storage(connection)
    compressed, uncompressed, compression_ratio = parquet_compression(connection)
    elapsed = time.perf_counter() - started
    bytes_per_row = size / rows if rows else 0.0
    run_rate = rows / elapsed if elapsed else 0.0
    estimated_rows = round(TARGET_STORAGE_BYTES / bytes_per_row) if bytes_per_row else 0
    report: dict[str, object] = {
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "write_path": "concurrent DuckLake INSERT transactions",
        "workers": WORKERS,
        "rows_per_insert": ROWS_PER_INSERT,
        "target_storage_bytes": TARGET_STORAGE_BYTES,
        "current_rows": rows,
        "current_storage_bytes": size,
        "storage_progress_percent": round(
            min(100.0, size / TARGET_STORAGE_BYTES * 100), 4
        ),
        "file_count": file_count,
        "bytes_per_row": round(bytes_per_row, 4),
        "parquet_column_compressed_bytes": compressed,
        "parquet_column_uncompressed_bytes": uncompressed,
        "parquet_compression_ratio": round(compression_ratio, 4),
        "run_rows_per_second": round(run_rate, 3),
        "last_wave_seconds": round(wave_seconds, 3),
        "estimated_rows_at_target": estimated_rows,
        "estimated_remaining_seconds": round(
            max(0, estimated_rows - rows) / run_rate if run_rate else 0.0, 3
        ),
        "parquet_compression": "zstd",
        "parquet_compression_level": 22,
        "registered_in_ducklake": True,
    }
    REPORT_PATH.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2), flush=True)
    return report


def main() -> None:
    if min(WORKERS, ROWS_PER_INSERT, TARGET_STORAGE_BYTES, MAX_ROWS) <= 0:
        raise ValueError("Parallel loader settings must be positive")
    if TARGET_STORAGE_BYTES > MAX_STORAGE_BYTES:
        raise ValueError("Target storage exceeds safety ceiling")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    coordinator_config = replace(Config(), duckdb_threads=1, duckdb_memory_limit="1GB")
    coordinator = connect_lake(coordinator_config)
    started = time.perf_counter()
    try:
        configure_lake(coordinator)
        create_table(coordinator, partitioned=True)
        current_rows = int(
            coordinator.execute(f"SELECT count(*) FROM lake.main.{TABLE}").fetchone()[0]
        )
        _, current_size = table_storage(coordinator)
        print(
            f"Concurrent DuckLake ETL starting at {current_rows:,} rows and "
            f"{current_size / 1e9:.3f} GB",
            flush=True,
        )

        while current_size < TARGET_STORAGE_BYTES:
            if current_rows >= MAX_ROWS:
                raise RuntimeError("Maximum row guard reached before storage target")
            jobs: list[tuple[int, int]] = []
            for worker_index in range(WORKERS):
                start = current_rows + worker_index * ROWS_PER_INSERT
                if start >= MAX_ROWS:
                    break
                jobs.append((start, min(start + ROWS_PER_INSERT, MAX_ROWS)))

            wave_started = time.perf_counter()
            run_wave(jobs)
            report = write_report(
                coordinator, started, time.perf_counter() - wave_started
            )
            current_rows = int(report["current_rows"])
            current_size = int(report["current_storage_bytes"])
            if current_size > MAX_STORAGE_BYTES:
                raise RuntimeError("Storage safety ceiling exceeded")

        print(
            f"DuckLake ETL complete: {current_rows:,} rows, "
            f"{current_size / 1e9:.3f} GB",
            flush=True,
        )
    finally:
        coordinator.close()


if __name__ == "__main__":
    main()
