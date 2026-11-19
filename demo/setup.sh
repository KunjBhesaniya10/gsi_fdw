#!/usr/bin/env bash
# Section 0 of demo/demo.md in one go: fresh 5-node cluster, 1M rows with GSIs
# on email (unique) and name, shard-local indexes, statement logging on the
# data nodes, and the resolver held back so leftovers stay visible.
# Run from the repo root in Git Bash:  bash demo/setup.sh
set -euo pipefail
cd "$(dirname "$0")/../docker"

docker compose down -v
docker compose up -d --build --wait
docker compose ps

(cd .. && bash bench/load_users.sh)

for s in shard1 shard2 shard3 shard4 shard5; do
  docker compose exec -T $s psql -U postgres \
    -c "CREATE INDEX IF NOT EXISTS users_shard_email ON users_shard (email)" \
    -c "CREATE INDEX IF NOT EXISTS users_shard_name ON users_shard (name)" \
    -c "ANALYZE users_shard" \
    -c "ALTER SYSTEM SET log_statement = 'all'" \
    -c "SELECT pg_reload_conf()"
done

docker compose exec -T coordinator psql -U postgres \
  -c "ALTER SYSTEM SET gsi.resolver_grace = '10min'" -c "SELECT pg_reload_conf()"

echo "Ready. Open the four terminals of demo/demo.md section 1."
