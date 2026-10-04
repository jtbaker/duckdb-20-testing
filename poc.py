from __future__ import annotations

import argparse
import os
import signal
import threading
from dataclasses import dataclass

import duckdb


@dataclass(frozen=True)
class Config:
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "ducklake")
    postgres_user: str = os.getenv("POSTGRES_USER", "ducklake")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "ducklake")
    s3_endpoint: str = os.getenv("S3_ENDPOINT", "localhost:9000")
    s3_access_key: str = os.getenv("S3_ACCESS_KEY", "rustfsadmin")
    s3_secret_key: str = os.getenv("S3_SECRET_KEY", "rustfsadmin")
    s3_region: str = os.getenv("S3_REGION", "us-east-1")
    s3_bucket: str = os.getenv("S3_BUCKET", "ducklake-poc")
    quack_host: str = os.getenv("QUACK_HOST", "0.0.0.0")
    quack_port: int = int(os.getenv("QUACK_PORT", "9494"))
    quack_client_host: str = os.getenv("QUACK_CLIENT_HOST", "localhost")
    quack_token: str = os.getenv("QUACK_TOKEN", "duckdb-20-poc-token")
    duckdb_threads: int = int(os.getenv("DUCKDB_THREADS", "6"))
    duckdb_memory_limit: str = os.getenv("DUCKDB_MEMORY_LIMIT", "6GB")
    parquet_prefetch_column_gap: int = int(
        os.getenv("PARQUET_PREFETCH_COLUMN_GAP", "0")
    )

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={self.postgres_password}"
        )

    @property
    def data_path(self) -> str:
        return f"s3://{self.s3_bucket}/data"

    @property
    def quack_listen_uri(self) -> str:
        return f"quack://{self.quack_host}:{self.quack_port}"

    @property
    def quack_client_uri(self) -> str:
        return f"quack://{self.quack_client_host}:{self.quack_port}"


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def load_extensions(connection: duckdb.DuckDBPyConnection, *names: str) -> None:
    for name in names:
        connection.execute(f"INSTALL {name}")
        connection.execute(f"LOAD {name}")


def connect_lake(config: Config) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("SET TimeZone = 'UTC'")
    connection.execute(f"SET threads = {config.duckdb_threads}")
    connection.execute(f"SET memory_limit = {sql_string(config.duckdb_memory_limit)}")
    connection.execute("SET enable_progress_bar = false")
    load_extensions(connection, "httpfs", "postgres", "ducklake")
    connection.execute(
        f"""
        CREATE OR REPLACE SECRET ducklake_rustfs (
            TYPE s3,
            KEY_ID {sql_string(config.s3_access_key)},
            SECRET {sql_string(config.s3_secret_key)},
            REGION {sql_string(config.s3_region)},
            ENDPOINT {sql_string(config.s3_endpoint)},
            USE_SSL false,
            URL_STYLE 'path',
            SCOPE {sql_string(f"s3://{config.s3_bucket}")}
        )
        """
    )
    connection.execute(
        f"""
        ATTACH {sql_string("ducklake:postgres:" + config.postgres_dsn)} AS lake (
            DATA_PATH {sql_string(config.data_path)},
            DATA_INLINING_ROW_LIMIT 0
        )
        """
    )
    return connection


def initialize(config: Config) -> None:
    connection = connect_lake(config)
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS lake.main.events (
                event_id BIGINT,
                event_type VARCHAR,
                occurred_at TIMESTAMPTZ
            )
            """
        )
        connection.execute(
            """
            INSERT INTO lake.main.events
            SELECT * FROM (
                VALUES
                    (1, 'ducklake-created', TIMESTAMPTZ '2026-10-01 09:00:00+00'),
                    (2, 'rustfs-written', TIMESTAMPTZ '2026-10-01 09:01:00+00'),
                    (3, 'quack-ready', TIMESTAMPTZ '2026-10-01 09:02:00+00')
            ) AS seed(event_id, event_type, occurred_at)
            WHERE NOT EXISTS (SELECT 1 FROM lake.main.events)
            """
        )
        count = connection.execute("SELECT count(*) FROM lake.main.events").fetchone()[
            0
        ]
        files = connection.execute(
            f"SELECT file FROM glob({sql_string(config.data_path + '/**')}) ORDER BY file"
        ).fetchall()
        print(f"Initialized DuckLake with {count} rows")
        print("RustFS objects:")
        for (path,) in files:
            print(f"  {path}")
    finally:
        connection.close()


def serve(config: Config) -> None:
    connection = connect_lake(config)
    # Avoid adaptive range coalescing pulling intervening high-entropy columns
    # for selective Parquet scans (for example, market + temperature_c).
    connection.execute(
        f"SET GLOBAL parquet_prefetch_column_gap = {config.parquet_prefetch_column_gap}"
    )
    load_extensions(connection, "quack")
    # Quack's attached-catalog shorthand resolves unqualified names in the
    # server's primary catalog. Expose gateway views while keeping the
    # authoritative tables in DuckLake.
    tables = connection.execute(
        """
        SELECT table_name
        FROM duckdb_tables()
        WHERE database_name = 'lake' AND schema_name = 'main'
        ORDER BY table_name
        """
    ).fetchall()
    for (table_name,) in tables:
        identifier = sql_identifier(table_name)
        connection.execute(
            f"CREATE OR REPLACE VIEW memory.main.{identifier} "
            f"AS SELECT * FROM lake.main.{identifier}"
        )
    connection.execute("USE memory")
    result = connection.execute(
        f"""
        FROM quack_serve(
            listen_uri => {sql_string(config.quack_listen_uri)},
            disable_ssl => true,
            allow_other_hostname => true,
            token => {sql_string(config.quack_token)}
        )
        """
    ).fetchone()
    print(f"DuckDB {connection.execute('SELECT version()').fetchone()[0]}")
    print(f"Quack listening at {result[0]} ({result[1]})")
    print("Serving the DuckLake catalog; press Ctrl-C to stop")

    stopped = threading.Event()

    def stop(_signum: int, _frame: object) -> None:
        stopped.set()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        stopped.wait()
    finally:
        connection.execute(f"FROM quack_stop({sql_string(config.quack_listen_uri)})")
        connection.close()
        print("Quack stopped")


def client(config: Config) -> None:
    connection = duckdb.connect()
    connection.execute("SET TimeZone = 'UTC'")
    load_extensions(connection, "quack")
    try:
        connection.execute(
            f"""
            ATTACH {sql_string(config.quack_client_uri)} AS remote (
                TYPE quack,
                TOKEN {sql_string(config.quack_token)},
                DISABLE_SSL true
            )
            """
        )
        rows = connection.execute(
            "SELECT event_id, event_type, occurred_at FROM remote.events ORDER BY event_id"
        ).fetchall()
        print(f"Read {len(rows)} rows through Quack:")
        for row in rows:
            print(row)
    finally:
        connection.close()


def check(config: Config) -> None:
    connection = connect_lake(config)
    try:
        version = connection.execute("SELECT version()").fetchone()[0]
        rows = connection.execute(
            "SELECT event_id, event_type, occurred_at FROM lake.main.events ORDER BY event_id"
        ).fetchall()
        files = connection.execute(
            f"SELECT file FROM glob({sql_string(config.data_path + '/**')}) ORDER BY file"
        ).fetchall()
        print(f"DuckDB engine: {version}")
        print(f"DuckLake rows: {len(rows)}")
        print(f"RustFS objects: {len(files)}")
        for row in rows:
            print(row)
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="DuckDB 2.0 + DuckLake + Quack POC")
    parser.add_argument("command", choices=("init", "serve", "client", "check"))
    args = parser.parse_args()
    config = Config()
    {
        "init": initialize,
        "serve": serve,
        "client": client,
        "check": check,
    }[args.command](config)


if __name__ == "__main__":
    main()
