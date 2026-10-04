#!/usr/bin/env bash
set -euo pipefail

quack_uri="quack://${QUACK_CLIENT_HOST:-duckdb-server}:${QUACK_PORT:-9494}"
quack_token="${QUACK_TOKEN:-duckdb-20-poc-token}"

exec duckdb -unsigned \
  -cmd "LOAD httpfs;" \
  -cmd "SET http_timeout = 3600;" \
  -cmd "LOAD quack;" \
  -cmd "SET quack_default_heartbeat_timeout = 3600;" \
  -cmd "ATTACH '${quack_uri}' AS remote (TYPE quack, TOKEN '${quack_token}', DISABLE_SSL true, HEARTBEAT_TIMEOUT 3600);" \
  -cmd ".timer on" \
  -cmd "SELECT version() AS client_version;" \
  -cmd "SELECT table_name, estimated_size FROM remote.duckdb_tables() ORDER BY table_name;" \
  "$@"
