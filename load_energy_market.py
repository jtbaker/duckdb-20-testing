from __future__ import annotations

import json
import os
import time
from pathlib import Path

from poc import Config, connect_lake

LOAD_ROWS = int(os.getenv("ENERGY_LOAD_ROWS", "20000000"))
GENERATION_ROWS = int(os.getenv("ENERGY_GENERATION_ROWS", "20000000"))
LMP_ROWS = int(os.getenv("ENERGY_LMP_ROWS", "10000000"))
BATCH_ROWS = int(os.getenv("ENERGY_LOAD_BATCH_ROWS", "5000000"))
RESET = os.getenv("ENERGY_RESET", "false").lower() in {"1", "true", "yes"}
RESULTS_DIR = Path(os.getenv("BENCHMARK_RESULTS_DIR", "benchmark-results"))

FACT_TABLES = (
    ("site_load", LOAD_ROWS),
    ("generation_telemetry", GENERATION_ROWS),
    ("locational_prices", LMP_ROWS),
)


def configure_lake(connection) -> None:
    connection.execute("CALL lake.set_option('parquet_compression', 'zstd')")
    connection.execute("CALL lake.set_option('parquet_compression_level', '22')")
    connection.execute("CALL lake.set_option('target_file_size', '256MB')")
    connection.execute("CALL lake.set_option('parquet_row_group_size', '122880')")
    connection.execute("CALL lake.set_option('data_inlining_row_limit', '0')")


def reset_tables(connection) -> None:
    for table in (
        "site_load",
        "generation_telemetry",
        "locational_prices",
        "sites",
        "pricing_nodes",
    ):
        connection.execute(f"DROP TABLE IF EXISTS lake.main.{table}")


def create_dimensions(connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS lake.main.sites AS
        SELECT
            i::INTEGER AS site_id,
            'SITE-' || lpad(i::VARCHAR, 5, '0') AS site_code,
            CASE WHEN i <= 1000 THEN 'LOAD' ELSE 'GENERATION' END AS site_kind,
            ['PJM', 'CAISO', 'ERCOT', 'MISO', 'NYISO', 'SPP', 'ISONE'][1 + (i % 7)] AS market,
            'ZONE-' || lpad((i % 40)::VARCHAR, 2, '0') AS zone,
            ['PA', 'CA', 'TX', 'IL', 'NY', 'KS', 'MA', 'OH'][1 + (i % 8)] AS state,
            25.0 + (i % 240) * 0.2 AS latitude,
            -124.0 + (i % 570) * 0.1 AS longitude,
            CASE WHEN i <= 1000 THEN NULL ELSE 50.0 + (i % 500) * 2.5 END AS nameplate_capacity_mw,
            CASE WHEN i <= 1000 THEN NULL
                 ELSE ['SOLAR', 'WIND', 'GAS', 'NUCLEAR', 'HYDRO', 'COAL', 'BATTERY'][1 + (i % 7)]
            END AS fuel_type,
            DATE '1980-01-01' + ((i * 37) % 16000)::INTEGER AS commissioned_date,
            (i % 17) <> 0 AS active
        FROM range(1, 2001) AS t(i)
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS lake.main.pricing_nodes AS
        SELECT
            i::INTEGER AS node_id,
            'NODE-' || lpad(i::VARCHAR, 5, '0') AS node_code,
            ['PJM', 'CAISO', 'ERCOT', 'MISO', 'NYISO', 'SPP', 'ISONE'][1 + (i % 7)] AS market,
            'ZONE-' || lpad((i % 40)::VARCHAR, 2, '0') AS zone,
            CASE WHEN i % 5 = 0 THEN 'HUB' ELSE 'NODE' END AS node_type,
            1 + (i % 1000) AS nearest_load_site_id
        FROM range(1, 501) AS t(i)
        """
    )


def create_fact_tables(connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS lake.main.site_load (
            interval_id BIGINT,
            interval_start TIMESTAMPTZ,
            site_id INTEGER,
            market VARCHAR,
            zone VARCHAR,
            load_mw DOUBLE,
            forecast_mw DOUBLE,
            temperature_c REAL,
            frequency_hz REAL,
            voltage_kv REAL,
            quality_code VARCHAR,
            is_estimated BOOLEAN,
            source_system VARCHAR
        ) PARTITIONED BY (month(interval_start)) SORTED BY (interval_start, site_id)
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS lake.main.generation_telemetry (
            interval_id BIGINT,
            interval_start TIMESTAMPTZ,
            site_id INTEGER,
            market VARCHAR,
            zone VARCHAR,
            fuel_type VARCHAR,
            available_capacity_mw DOUBLE,
            generation_mw DOUBLE,
            ramp_mw_per_minute REAL,
            heat_rate_mmbtu_mwh REAL,
            outage_flag BOOLEAN,
            curtailment_mw DOUBLE,
            dispatch_status VARCHAR
        ) PARTITIONED BY (month(interval_start)) SORTED BY (interval_start, site_id)
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS lake.main.locational_prices (
            interval_id BIGINT,
            interval_start TIMESTAMPTZ,
            node_id INTEGER,
            market VARCHAR,
            zone VARCHAR,
            lmp_energy_usd_mwh DECIMAL(12, 4),
            lmp_congestion_usd_mwh DECIMAL(12, 4),
            lmp_loss_usd_mwh DECIMAL(12, 4),
            lmp_total_usd_mwh DECIMAL(12, 4),
            reserve_price_usd_mw DECIMAL(12, 4),
            scarcity_flag BOOLEAN,
            settlement_type VARCHAR
        ) PARTITIONED BY (month(interval_start)) SORTED BY (interval_start, node_id)
        """
    )


def load_sql(table: str, start: int, end: int) -> str:
    if table == "site_load":
        return f"""
            INSERT INTO lake.main.site_load
            WITH generated AS (
                SELECT
                    i,
                    (i % 1000)::INTEGER + 1 AS site_id,
                    (i // 1000)::BIGINT AS interval_number
                FROM range({start}, {end}) AS t(i)
            )
            SELECT
                i AS interval_id,
                TIMESTAMPTZ '2026-01-01 00:00:00+00' + interval_number * INTERVAL 5 MINUTE,
                site_id,
                ['PJM', 'CAISO', 'ERCOT', 'MISO', 'NYISO', 'SPP', 'ISONE'][1 + (site_id % 7)],
                'ZONE-' || lpad((site_id % 40)::VARCHAR, 2, '0'),
                round(greatest(1.0,
                    35.0 + (site_id % 120) * 0.55
                    + 18.0 * sin(2 * pi() * (interval_number % 288) / 288.0)
                    + 7.0 * sin(2 * pi() * (interval_number % 2016) / 2016.0)
                    + (hash(i) % 1000) / 125.0
                ), 3),
                round(greatest(1.0,
                    36.5 + (site_id % 120) * 0.54
                    + 17.5 * sin(2 * pi() * (interval_number % 288) / 288.0)
                    + (hash(i + 17) % 1000) / 150.0
                ), 3),
                (12.0 + 14.0 * sin(2 * pi() * (interval_number % 105120) / 105120.0)
                    + (site_id % 9) * 0.3)::REAL,
                (59.97 + (hash(i + 31) % 60) / 1000.0)::REAL,
                ([4.16, 12.47, 34.5, 69.0, 115.0][1 + (site_id % 5)])::REAL,
                CASE WHEN i % 997 = 0 THEN 'BAD' WHEN i % 101 = 0 THEN 'SUSPECT' ELSE 'GOOD' END,
                i % 101 = 0,
                ['SCADA', 'AMI', 'EMS'][1 + (site_id % 3)]
            FROM generated
        """
    if table == "generation_telemetry":
        return f"""
            INSERT INTO lake.main.generation_telemetry
            WITH generated AS (
                SELECT
                    i,
                    (i % 1000)::INTEGER + 1001 AS site_id,
                    (i // 1000)::BIGINT AS interval_number
                FROM range({start}, {end}) AS t(i)
            ), calculated AS (
                SELECT *,
                    50.0 + (site_id % 500) * 2.5 AS nameplate,
                    ['SOLAR', 'WIND', 'GAS', 'NUCLEAR', 'HYDRO', 'COAL', 'BATTERY'][1 + (site_id % 7)] AS fuel
                FROM generated
            )
            SELECT
                i AS interval_id,
                TIMESTAMPTZ '2026-01-01 00:00:00+00' + interval_number * INTERVAL 5 MINUTE,
                site_id,
                ['PJM', 'CAISO', 'ERCOT', 'MISO', 'NYISO', 'SPP', 'ISONE'][1 + (site_id % 7)],
                'ZONE-' || lpad((site_id % 40)::VARCHAR, 2, '0'),
                fuel,
                round(nameplate * CASE WHEN i % 4999 = 0 THEN 0.0 ELSE 0.88 + (hash(i) % 120) / 1000.0 END, 3),
                round(nameplate * CASE
                    WHEN fuel = 'SOLAR' THEN greatest(0.0, sin(pi() * (interval_number % 288) / 288.0)) * 0.86
                    WHEN fuel = 'WIND' THEN 0.25 + (hash(i + 7) % 550) / 1000.0
                    WHEN fuel = 'BATTERY' THEN 0.10 + (hash(i + 11) % 700) / 1000.0
                    ELSE 0.55 + (hash(i + 13) % 380) / 1000.0
                END, 3),
                (-4.0 + (hash(i + 19) % 800) / 100.0)::REAL,
                CASE WHEN fuel IN ('GAS', 'COAL') THEN (6.5 + (site_id % 35) / 10.0)::REAL ELSE NULL END,
                i % 4999 = 0,
                round(CASE WHEN i % 4999 = 0 THEN nameplate * 0.9 ELSE (hash(i + 23) % 200) / 100.0 END, 3),
                CASE WHEN i % 4999 = 0 THEN 'OUTAGE' WHEN i % 97 = 0 THEN 'CURTAILED' ELSE 'DISPATCHED' END
            FROM calculated
        """
    if table == "locational_prices":
        return f"""
            INSERT INTO lake.main.locational_prices
            WITH generated AS (
                SELECT
                    i,
                    (i % 500)::INTEGER + 1 AS node_id,
                    (i // 500)::BIGINT AS interval_number
                FROM range({start}, {end}) AS t(i)
            ), components AS (
                SELECT *,
                    24.0 + 11.0 * sin(2 * pi() * (interval_number % 288) / 288.0)
                        + (hash(i) % 2400) / 100.0 AS energy,
                    -4.0 + (hash(i + 29) % 1600) / 100.0
                        + CASE WHEN i % 7919 = 0 THEN 250.0 ELSE 0.0 END AS congestion,
                    -1.5 + (hash(i + 37) % 500) / 100.0 AS loss
                FROM generated
            )
            SELECT
                i AS interval_id,
                TIMESTAMPTZ '2026-01-01 00:00:00+00' + interval_number * INTERVAL 5 MINUTE,
                node_id,
                ['PJM', 'CAISO', 'ERCOT', 'MISO', 'NYISO', 'SPP', 'ISONE'][1 + (node_id % 7)],
                'ZONE-' || lpad((node_id % 40)::VARCHAR, 2, '0'),
                energy::DECIMAL(12, 4),
                congestion::DECIMAL(12, 4),
                loss::DECIMAL(12, 4),
                (energy + congestion + loss)::DECIMAL(12, 4),
                (greatest(0.0, congestion * 0.12))::DECIMAL(12, 4),
                i % 7919 = 0,
                CASE WHEN interval_number % 12 = 0 THEN 'EX_POST' ELSE 'REAL_TIME' END
            FROM components
        """
    raise ValueError(f"Unknown table: {table}")


def load_table(
    connection, table: str, target_rows: int
) -> dict[str, float | int | str]:
    current_rows = connection.execute(
        f"SELECT count(*) FROM lake.main.{table}"
    ).fetchone()[0]
    initial_rows = current_rows
    if current_rows > target_rows:
        raise RuntimeError(
            f"{table} already has {current_rows:,} rows, above target {target_rows:,}; "
            "set ENERGY_RESET=true to rebuild"
        )

    started = time.perf_counter()
    while current_rows < target_rows:
        batch_start = current_rows
        end = min(batch_start + BATCH_ROWS, target_rows)
        batch_started = time.perf_counter()
        connection.execute(load_sql(table, batch_start, end))
        elapsed = time.perf_counter() - batch_started
        current_rows = end
        print(
            f"{table}: {current_rows:,}/{target_rows:,} rows "
            f"({end - batch_start:,} batch rows, {elapsed:.2f}s)",
            flush=True,
        )

    total_seconds = time.perf_counter() - started
    loaded_this_run = target_rows - initial_rows
    return {
        "table": table,
        "rows": target_rows,
        "existing_rows_before_run": initial_rows,
        "loaded_rows_this_run": loaded_this_run,
        "load_seconds_this_run": round(total_seconds, 3),
        "rows_per_second": round(loaded_this_run / total_seconds, 3)
        if total_seconds > 0
        else 0,
    }


def main() -> None:
    if min(LOAD_ROWS, GENERATION_ROWS, LMP_ROWS, BATCH_ROWS) <= 0:
        raise ValueError("Row and batch sizes must be positive")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    connection = connect_lake(Config())
    started = time.perf_counter()
    try:
        configure_lake(connection)
        if RESET:
            reset_tables(connection)
        create_dimensions(connection)
        create_fact_tables(connection)

        reports = [load_table(connection, table, rows) for table, rows in FACT_TABLES]
        counts = {
            table: connection.execute(
                f"SELECT count(*) FROM lake.main.{table}"
            ).fetchone()[0]
            for table, _ in FACT_TABLES
        }
        options = connection.execute(
            """
            SELECT option_name, value, scope
            FROM lake.options()
            WHERE option_name IN ('parquet_compression', 'parquet_compression_level', 'target_file_size')
            ORDER BY option_name
            """
        ).fetchall()
        elapsed_seconds = time.perf_counter() - started
        loaded_rows = sum(int(report["loaded_rows_this_run"]) for report in reports)
        report = {
            "total_rows": sum(counts.values()),
            "loaded_rows_this_run": loaded_rows,
            "counts": counts,
            "elapsed_seconds": round(elapsed_seconds, 3),
            "rows_per_second": round(loaded_rows / elapsed_seconds, 3)
            if elapsed_seconds > 0
            else 0,
            "tables": reports,
            "ducklake_options": options,
        }
        report_path = RESULTS_DIR / "load-report.json"
        if loaded_rows > 0 or not report_path.exists():
            report_path.write_text(json.dumps(report, indent=2, default=str) + "\n")
        else:
            print(f"No new rows loaded; preserving {report_path}", flush=True)
        print(json.dumps(report, indent=2, default=str), flush=True)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
