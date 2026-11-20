#!/usr/bin/env bash
# Section 0 of demo/demo.md in one go: fresh 5-node cluster, 1M rows with GSIs
# on email (unique) and name, statement logging on the data nodes, and the
# resolver held back so leftovers stay visible.  The shards get no local index
# on email/name (the report's setup), so broadcast scans every shard.
# Run from the repo root in Git Bash:  bash demo/setup.sh
set -euo pipefail
cd "$(dirname "$0")/../docker"

docker compose down -v
docker compose up -d --build --wait
docker compose ps

(cd .. && bash bench/load_users.sh)

# VACUUM + pg_prewarm: stable lookup times from the first query on;
# use_remote_estimate off: no per-shard EXPLAIN while planning (same plans,
# since the shards have no local index on email/name).
for s in shard1 shard2 shard3 shard4 shard5; do
  docker compose exec -T $s psql -U postgres \
    -c "VACUUM (ANALYZE) users_shard, gsi_users_email, gsi_users_name" \
    -c "CREATE EXTENSION IF NOT EXISTS pg_prewarm" \
    -c "SELECT count(pg_prewarm(c.oid::regclass)) FROM pg_class c WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r','i') AND (c.relname LIKE 'users_shard%' OR c.relname LIKE 'gsi_users_%')" \
    -c "ALTER SYSTEM SET log_statement = 'all'" \
    -c "SELECT pg_reload_conf()"
  docker compose exec -T coordinator psql -U postgres \
    -c "ALTER SERVER $s OPTIONS (SET use_remote_estimate 'false')"
done

docker compose exec -T coordinator psql -U postgres \
  -c "ALTER SYSTEM SET gsi.resolver_grace = '10min'" -c "SELECT pg_reload_conf()"

echo "Ready. Open the four terminals of demo/demo.md (Terminals), then warm up C1 with the queries of part 2."
