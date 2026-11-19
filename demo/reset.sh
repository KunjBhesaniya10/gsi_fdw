#!/usr/bin/env bash
# Undo the demo settings without wiping the data (end of demo/demo.md).
# Run from the repo root:  bash demo/reset.sh   (or wipe: cd docker && docker compose down -v)
set -euo pipefail
cd "$(dirname "$0")/../docker"

docker compose exec -T coordinator psql -U postgres \
  -c "ALTER SYSTEM RESET gsi.resolver_grace" -c "SELECT pg_reload_conf()"
for s in shard1 shard2 shard3 shard4 shard5; do
  docker compose exec -T $s psql -U postgres \
    -c "ALTER SYSTEM RESET log_statement" -c "SELECT pg_reload_conf()"
done
