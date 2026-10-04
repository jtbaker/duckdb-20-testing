from __future__ import annotations

import csv
import json
import os
import time
from pathlib import Path

from poc import Config, connect_lake

MAX_ROWS = int(os.getenv("APPEND_MAX_ROWS", "2000000000"))
TARGET_STORAGE_BYTES = int(os.getenv("APPEND_TARGET_STORAGE_BYTES", "32000000000"))
BATCH_ROWS = int(os.getenv("APPEND_BATCH_ROWS", "25000000"))
ASSET_COUNT = int(os.getenv("APPEND_ASSET_COUNT", "50000"))
MAX_STORAGE_BYTES = int(os.getenv("APPEND_MAX_STORAGE_BYTES", "220000000000"))
RESULTS_DIR = Path(os.getenv("BENCHMARK_RESULTS_DIR", "benchmark-results"))
REPORT_PATH = RESULTS_DIR / "append-load-report.json"
PROGRESS_PATH = RESULTS_DIR / "append-progress.csv"
TABLE = "operational_measurements"


def configure_lake(connection) -> None:
    connection.execute("CALL lake.set_option('parquet_compression', 'zstd')")
    connection.execute("CALL lake.set_option('parquet_compression_level', '22')")
    connection.execute("CALL lake.set_option('target_file_size', '1GB')")
    connection.execute("CALL lake.set_option('parquet_row_group_size', '122880')")
    connection.execute("CALL lake.set_option('data_inlining_row_limit', '0')")
    connection.execute("CALL lake.set_option('auto_compact', 'false')")
    connection.execute("CALL lake.set_option('per_thread_output', 'true')")


def create_table(connection, *, partitioned: bool = True) -> None:
    partition_clause = (
        "PARTITIONED BY (year(event_time), month(event_time))" if partitioned else ""
    )
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS lake.main.{TABLE} (
            sequence_id BIGINT,
            event_time TIMESTAMPTZ,
            ingested_at TIMESTAMPTZ,
            asset_id INTEGER,
            site_id INTEGER,
            market VARCHAR,
            zone VARCHAR,
            asset_type VARCHAR,
            metric_name VARCHAR,
            metric_value DOUBLE,
            active_power_mw DOUBLE,
            reactive_power_mvar DOUBLE,
            voltage_kv DOUBLE,
            current_amps DOUBLE,
            power_factor REAL,
            frequency_hz REAL,
            temperature_c REAL,
            pressure_kpa REAL,
            unit VARCHAR,
            quality_code VARCHAR,
            operating_status VARCHAR,
            is_estimated BOOLEAN,
            source_system VARCHAR,
            source_sequence BIGINT,
            alarm_bitmap UBIGINT,
            sample_hash UBIGINT,
            correlation_hash UBIGINT
        ) {partition_clause}
        """
    )


def generated_select_sql(start: int, end: int) -> str:
    return f"""
        WITH generated AS (
            SELECT
                i,
                (i % {ASSET_COUNT})::INTEGER + 1 AS asset_id,
                (i // {ASSET_COUNT})::BIGINT AS interval_number,
                (hash(i) % 1000000) / 1000000.0 AS noise_1,
                (hash(i + 101) % 1000000) / 1000000.0 AS noise_2,
                (hash(i + 211) % 1000000) / 1000000.0 AS noise_3,
                (hash(i + 307) % 1000000) / 1000000.0 AS noise_4
            FROM range({start}, {end}) AS t(i)
        ), typed AS (
            SELECT *, asset_id % 6 AS metric_type
            FROM generated
        )
        SELECT
            i AS sequence_id,
            TIMESTAMPTZ '2024-01-01 00:00:00+00' + interval_number * INTERVAL 5 MINUTE AS event_time,
            TIMESTAMPTZ '2024-01-01 00:00:00+00' + interval_number * INTERVAL 5 MINUTE
                + ((hash(i + 11) % 180)::BIGINT * INTERVAL 1 SECOND) AS ingested_at,
            asset_id,
            1 + ((asset_id - 1) // 5) AS site_id,
            ['PJM', 'CAISO', 'ERCOT', 'MISO', 'NYISO', 'SPP', 'ISONE'][1 + (asset_id % 7)] AS market,
            'ZONE-' || lpad((asset_id % 100)::VARCHAR, 3, '0') AS zone,
            ['LOAD', 'GENERATOR', 'GENERATOR', 'PRICE_NODE', 'SUBSTATION', 'WEATHER'][1 + metric_type] AS asset_type,
            ['LOAD_MW', 'GENERATION_MW', 'AVAILABLE_CAPACITY_MW', 'LMP_TOTAL', 'FREQUENCY_HZ', 'TEMPERATURE_C'][1 + metric_type] AS metric_name,
            CASE metric_type
                WHEN 0 THEN 15.0 + (asset_id % 300) * 0.7
                    + 30.0 * sin(2 * pi() * (interval_number % 288) / 288.0)
                    + noise_1 * 12.0
                WHEN 1 THEN (40.0 + (asset_id % 800) * 1.4) * (0.2 + 0.75 * noise_1)
                WHEN 2 THEN (50.0 + (asset_id % 800) * 1.5)
                    * CASE WHEN i % 100003 = 0 THEN 0.0 ELSE 0.88 + noise_1 * 0.11 END
                WHEN 3 THEN 18.0
                    + 14.0 * sin(2 * pi() * (interval_number % 288) / 288.0)
                    + noise_1 * 55.0
                    + CASE WHEN i % 999983 = 0 THEN 500.0 ELSE 0.0 END
                WHEN 4 THEN 59.94 + noise_1 * 0.12
                ELSE -12.0
                    + 24.0 * sin(2 * pi() * (interval_number % 105120) / 105120.0)
                    + noise_1 * 8.0
            END AS metric_value,
            (20.0 + (asset_id % 900) * 1.1) * (0.15 + noise_2 * 0.9) AS active_power_mw,
            (-25.0 + noise_3 * 50.0) * (1.0 + (asset_id % 20) / 10.0) AS reactive_power_mvar,
            [4.16, 12.47, 34.5, 69.0, 115.0, 230.0, 345.0][1 + (asset_id % 7)]
                * (0.98 + noise_4 * 0.04) AS voltage_kv,
            10.0 + noise_1 * 2490.0 AS current_amps,
            (0.75 + noise_2 * 0.25)::REAL AS power_factor,
            (59.94 + noise_3 * 0.12)::REAL AS frequency_hz,
            (-25.0 + noise_4 * 70.0)::REAL AS temperature_c,
            (95.0 + noise_1 * 20.0)::REAL AS pressure_kpa,
            ['MW', 'MW', 'MW', 'USD/MWh', 'Hz', 'degC'][1 + metric_type] AS unit,
            CASE
                WHEN i % 10007 = 0 THEN 'BAD'
                WHEN i % 503 = 0 THEN 'SUSPECT'
                WHEN i % 97 = 0 THEN 'ESTIMATED'
                ELSE 'GOOD'
            END AS quality_code,
            CASE
                WHEN metric_type IN (1, 2) AND i % 100003 = 0 THEN 'OUTAGE'
                WHEN metric_type IN (1, 2) AND i % 997 = 0 THEN 'CURTAILED'
                ELSE 'NORMAL'
            END AS operating_status,
            i % 97 = 0 AS is_estimated,
            ['SCADA', 'EMS', 'AMI', 'MARKET_FEED', 'HISTORIAN'][1 + (asset_id % 5)] AS source_system,
            interval_number * {ASSET_COUNT} + asset_id AS source_sequence,
            hash(i + 401) AS alarm_bitmap,
            hash(i + 503) AS sample_hash,
            hash(i + 601) AS correlation_hash
        FROM typed
    """


def table_storage(connection) -> tuple[int, int]:
    row = connection.execute(
        f"""
        SELECT file_count, file_size_bytes
        FROM lake.table_info()
        WHERE table_name = '{TABLE}'
        """
    ).fetchone()
    return (0, 0) if row is None else (int(row[0]), int(row[1]))


def parquet_compression(connection) -> tuple[int, int, float]:
    paths = [
        row[0]
        for row in connection.execute(
            f"""
            SELECT data_file
            FROM ducklake_list_files('lake', '{TABLE}', schema => 'main')
            ORDER BY data_file
            """
        ).fetchall()
    ]
    row = connection.execute(
        """
        SELECT
            coalesce(sum(total_compressed_size), 0)::UBIGINT,
            coalesce(sum(total_uncompressed_size), 0)::UBIGINT
        FROM parquet_metadata(?)
        """,
        [paths],
    ).fetchone()
    compressed = int(row[0])
    uncompressed = int(row[1])
    ratio = uncompressed / compressed if compressed else 0.0
    return compressed, uncompressed, ratio


def write_progress(report: dict[str, int | float | str]) -> None:
    REPORT_PATH.write_text(json.dumps(report, indent=2) + "\n")
    exists = PROGRESS_PATH.exists()
    with PROGRESS_PATH.open("a", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(report.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(report)


def main() -> None:
    if min(MAX_ROWS, TARGET_STORAGE_BYTES, BATCH_ROWS, ASSET_COUNT) <= 0:
        raise ValueError("Row, storage, batch, and asset targets must be positive")
    if TARGET_STORAGE_BYTES > MAX_STORAGE_BYTES:
        raise ValueError("Target storage exceeds the configured safety ceiling")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    connection = connect_lake(Config())
    process_started = time.perf_counter()
    try:
        configure_lake(connection)
        create_table(connection)
        current_rows = int(
            connection.execute(f"SELECT count(*) FROM lake.main.{TABLE}").fetchone()[0]
        )
        file_count, file_size_bytes = table_storage(connection)
        initial_rows = current_rows
        print(
            f"Resuming {TABLE} at {current_rows:,} rows and "
            f"{file_size_bytes / 1e9:.3f} GB; target {TARGET_STORAGE_BYTES / 1e9:.1f} GB",
            flush=True,
        )

        while file_size_bytes < TARGET_STORAGE_BYTES and current_rows < MAX_ROWS:
            batch_start = current_rows
            batch_end = min(batch_start + BATCH_ROWS, MAX_ROWS)
            started = time.perf_counter()
            connection.execute(
                f"INSERT INTO lake.main.{TABLE} "
                + generated_select_sql(batch_start, batch_end)
            )
            batch_seconds = time.perf_counter() - started
            current_rows = batch_end

            file_count, file_size_bytes = table_storage(connection)
            bytes_per_row = file_size_bytes / current_rows
            estimated_rows_at_target = round(TARGET_STORAGE_BYTES / bytes_per_row)
            elapsed = time.perf_counter() - process_started
            loaded_this_run = current_rows - initial_rows
            rows_per_second = loaded_this_run / elapsed if elapsed else 0.0
            remaining_rows = max(0, estimated_rows_at_target - current_rows)
            remaining_seconds = (
                remaining_rows / rows_per_second if rows_per_second else 0.0
            )
            compressed, uncompressed, compression_ratio = parquet_compression(
                connection
            )
            report: dict[str, int | float | str] = {
                "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "target_storage_bytes": TARGET_STORAGE_BYTES,
                "current_rows": current_rows,
                "current_storage_bytes": file_size_bytes,
                "storage_progress_percent": round(
                    min(100.0, file_size_bytes / TARGET_STORAGE_BYTES * 100), 4
                ),
                "batch_rows": batch_end - batch_start,
                "batch_seconds": round(batch_seconds, 3),
                "batch_rows_per_second": round(
                    (batch_end - batch_start) / batch_seconds, 3
                ),
                "run_rows_per_second": round(rows_per_second, 3),
                "estimated_rows_at_target": estimated_rows_at_target,
                "estimated_remaining_seconds": round(remaining_seconds, 3),
                "file_count": file_count,
                "bytes_per_row": round(bytes_per_row, 4),
                "parquet_column_compressed_bytes": compressed,
                "parquet_column_uncompressed_bytes": uncompressed,
                "parquet_compression_ratio": round(compression_ratio, 4),
                "parquet_compression": "zstd",
                "parquet_compression_level": 22,
            }
            write_progress(report)
            print(json.dumps(report, indent=2), flush=True)

            if file_size_bytes > MAX_STORAGE_BYTES:
                raise RuntimeError(
                    f"Storage {file_size_bytes / 1e9:.1f} GB exceeds safety ceiling "
                    f"{MAX_STORAGE_BYTES / 1e9:.1f} GB"
                )

        if file_size_bytes < TARGET_STORAGE_BYTES:
            raise RuntimeError(
                f"Reached maximum {MAX_ROWS:,} rows before storage target; "
                f"only {file_size_bytes / 1e9:.1f} GB written"
            )
    finally:
        connection.close()


if __name__ == "__main__":
    main()
