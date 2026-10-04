from __future__ import annotations

import asyncio
import csv
import json
import math
import os
import statistics
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb

from poc import Config, load_extensions

CONCURRENCY = int(os.getenv("BENCHMARK_CONCURRENCY", "1"))
REPETITIONS = int(os.getenv("BENCHMARK_REPETITIONS", "3"))
WARMUPS = int(os.getenv("BENCHMARK_WARMUPS", "1"))
RESULTS_DIR = Path(os.getenv("BENCHMARK_RESULTS_DIR", "benchmark-results"))

QUERIES = {
    "point_site_day": """
        SELECT interval_start, load_mw, forecast_mw, quality_code
        FROM lake.main.site_load
        WHERE site_id = 417
          AND interval_start >= TIMESTAMPTZ '2026-02-01 00:00:00+00'
          AND interval_start < TIMESTAMPTZ '2026-02-02 00:00:00+00'
        ORDER BY interval_start
    """,
    "regional_load_dashboard": """
        SELECT market, date_trunc('hour', interval_start) AS hour,
               round(sum(load_mw), 2) AS load_mw,
               round(sum(forecast_mw), 2) AS forecast_mw,
               round(avg(temperature_c), 2) AS temperature_c
        FROM lake.main.site_load
        WHERE interval_start >= TIMESTAMPTZ '2026-02-01 00:00:00+00'
          AND interval_start < TIMESTAMPTZ '2026-02-08 00:00:00+00'
        GROUP BY market, hour
        ORDER BY hour, market
    """,
    "generation_capacity_factor": """
        SELECT market, fuel_type,
               round(sum(generation_mw), 2) AS generation_mwh_intervals,
               round(sum(available_capacity_mw), 2) AS available_mwh_intervals,
               round(sum(generation_mw) / nullif(sum(available_capacity_mw), 0), 4) AS capacity_factor,
               count_if(outage_flag) AS outage_intervals
        FROM lake.main.generation_telemetry
        WHERE interval_start >= TIMESTAMPTZ '2026-01-15 00:00:00+00'
          AND interval_start < TIMESTAMPTZ '2026-02-15 00:00:00+00'
        GROUP BY market, fuel_type
        ORDER BY market, fuel_type
    """,
    "lmp_volatility_top_nodes": """
        SELECT node_id, market,
               round(avg(lmp_total_usd_mwh), 4) AS avg_lmp,
               round(stddev_samp(lmp_total_usd_mwh), 4) AS lmp_volatility,
               max(lmp_total_usd_mwh) AS max_lmp,
               count_if(scarcity_flag) AS scarcity_intervals
        FROM lake.main.locational_prices
        WHERE interval_start >= TIMESTAMPTZ '2026-01-01 00:00:00+00'
          AND interval_start < TIMESTAMPTZ '2026-03-01 00:00:00+00'
        GROUP BY node_id, market
        ORDER BY lmp_volatility DESC
        LIMIT 25
    """,
    "supply_demand_price_join": """
        WITH load AS (
            SELECT market, date_trunc('hour', interval_start) AS hour,
                   sum(load_mw) AS load_mw
            FROM lake.main.site_load
            WHERE interval_start >= TIMESTAMPTZ '2026-02-01 00:00:00+00'
              AND interval_start < TIMESTAMPTZ '2026-02-08 00:00:00+00'
            GROUP BY market, hour
        ), generation AS (
            SELECT market, date_trunc('hour', interval_start) AS hour,
                   sum(generation_mw) AS generation_mw,
                   sum(available_capacity_mw) AS available_capacity_mw
            FROM lake.main.generation_telemetry
            WHERE interval_start >= TIMESTAMPTZ '2026-02-01 00:00:00+00'
              AND interval_start < TIMESTAMPTZ '2026-02-08 00:00:00+00'
            GROUP BY market, hour
        ), prices AS (
            SELECT market, date_trunc('hour', interval_start) AS hour,
                   avg(lmp_total_usd_mwh) AS avg_lmp
            FROM lake.main.locational_prices
            WHERE interval_start >= TIMESTAMPTZ '2026-02-01 00:00:00+00'
              AND interval_start < TIMESTAMPTZ '2026-02-08 00:00:00+00'
            GROUP BY market, hour
        )
        SELECT load.market, load.hour,
               round(load.load_mw, 2) AS load_mw,
               round(generation.generation_mw, 2) AS generation_mw,
               round(generation.available_capacity_mw, 2) AS available_capacity_mw,
               round(prices.avg_lmp, 4) AS avg_lmp
        FROM load
        JOIN generation USING (market, hour)
        JOIN prices USING (market, hour)
        ORDER BY load.hour, load.market
    """,
    "full_load_scan": """
        SELECT market,
               count(*) AS intervals,
               round(sum(load_mw), 2) AS total_load,
               round(avg(load_mw), 4) AS avg_load,
               round(quantile_cont(load_mw, 0.95), 4) AS p95_load,
               count_if(quality_code <> 'GOOD') AS degraded_intervals
        FROM lake.main.site_load
        GROUP BY market
        ORDER BY market
    """,
}


@dataclass
class QueryResult:
    query: str
    repetition: int
    request: int
    elapsed_seconds: float
    rows_returned: int


def prepare_extension() -> None:
    connection = duckdb.connect()
    try:
        load_extensions(connection, "quack")
    finally:
        connection.close()


def execute_remote_query(config: Config, sql: str) -> tuple[float, int]:
    connection = duckdb.connect()
    connection.execute("SET TimeZone = 'UTC'")
    connection.execute("LOAD quack")
    try:
        started = time.perf_counter()
        rows = connection.execute(
            """
            SELECT *
            FROM quack_query(?, ?, disable_ssl => true, token => ?)
            """,
            [config.quack_client_uri, sql, config.quack_token],
        ).fetchall()
        return time.perf_counter() - started, len(rows)
    finally:
        connection.close()


async def execute_batch(
    config: Config, query_name: str, sql: str, repetition: int
) -> list[QueryResult]:
    async def request(request_number: int) -> QueryResult:
        elapsed, row_count = await asyncio.to_thread(execute_remote_query, config, sql)
        return QueryResult(
            query=query_name,
            repetition=repetition,
            request=request_number,
            elapsed_seconds=elapsed,
            rows_returned=row_count,
        )

    tasks: list[asyncio.Task[QueryResult]] = []
    async with asyncio.TaskGroup() as group:
        for request_number in range(CONCURRENCY):
            tasks.append(group.create_task(request(request_number)))
    return [task.result() for task in tasks]


def percentile(values: list[float], percentile_value: float) -> float:
    if not values:
        return math.nan
    ordered = sorted(values)
    index = min(len(ordered) - 1, math.ceil(percentile_value * len(ordered)) - 1)
    return ordered[index]


async def main_async() -> None:
    if min(CONCURRENCY, REPETITIONS) <= 0 or WARMUPS < 0:
        raise ValueError(
            "Concurrency/repetitions must be positive and warmups non-negative"
        )

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    config = Config()
    prepare_extension()
    results: list[QueryResult] = []

    for query_name, sql in QUERIES.items():
        for warmup in range(WARMUPS):
            elapsed, rows = await asyncio.to_thread(execute_remote_query, config, sql)
            print(
                f"warmup {query_name} {warmup + 1}/{WARMUPS}: "
                f"{elapsed:.3f}s, {rows} rows",
                flush=True,
            )

        for repetition in range(REPETITIONS):
            batch_started = time.perf_counter()
            batch = await execute_batch(config, query_name, sql, repetition)
            wall_seconds = time.perf_counter() - batch_started
            results.extend(batch)
            print(
                f"{query_name} repetition {repetition + 1}/{REPETITIONS}: "
                f"{CONCURRENCY} requests in {wall_seconds:.3f}s "
                f"({CONCURRENCY / wall_seconds:.2f} req/s)",
                flush=True,
            )

    suffix = f"c{CONCURRENCY}"
    csv_path = RESULTS_DIR / f"queries-{suffix}.csv"
    with csv_path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(asdict(results[0]).keys()))
        writer.writeheader()
        writer.writerows(asdict(result) for result in results)

    summary = {}
    for query_name in QUERIES:
        values = [
            result.elapsed_seconds for result in results if result.query == query_name
        ]
        summary[query_name] = {
            "requests": len(values),
            "mean_seconds": round(statistics.mean(values), 6),
            "median_seconds": round(statistics.median(values), 6),
            "p95_seconds": round(percentile(values, 0.95), 6),
            "min_seconds": round(min(values), 6),
            "max_seconds": round(max(values), 6),
        }

    report = {
        "concurrency": CONCURRENCY,
        "repetitions": REPETITIONS,
        "warmups": WARMUPS,
        "query_summary": summary,
    }
    (RESULTS_DIR / f"summary-{suffix}.json").write_text(
        json.dumps(report, indent=2) + "\n"
    )
    print(json.dumps(report, indent=2), flush=True)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
