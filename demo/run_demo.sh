#!/usr/bin/env bash
# Live demo of gsi_fdw.  Run after demo/setup.sh, from the repo root (Git Bash):
#
#   bash demo/run_demo.sh            # waits for Enter between sections
#   NOPAUSE=1 bash demo/run_demo.sh  # runs straight through
#
# The output is also saved to demo/demo_output.txt.
set -uo pipefail
cd "$(dirname "$0")/../docker"

OUT=../demo/demo_output.txt
exec > >(tee "$OUT") 2>&1

# ---------------------------------------------------------------- helpers

# run SQL on the coordinator, echoing every statement before its result
coord() { docker compose exec -T coordinator psql -U postgres -X -e "$@"; }

# run SQL from stdin twice in one session: first silently (warm-up: opens the
# shard connections, fills caches), then echoed with its results
coord_warm() {
  local sql; sql=$(cat)
  coord <<SQL
\set ECHO none
\o /dev/null
$sql
RESET ALL;
\o
\set ECHO queries
$sql
SQL
}

# run SQL on one data node:  shard 3 "SELECT ..."
shard() { docker compose exec -T "shard$1" psql -U postgres -X -c "$2"; }

# show where the email index entry of a key lives, read on that data node
index_entry() {
  local n
  n=$(docker compose exec -T coordinator psql -U postgres -X -At \
        -c "SELECT gsi_node_for('$1'::text) + 1")
  echo "-- email index entry of '$1' on shard$n:"
  shard "$n" "SELECT * FROM gsi_users_email WHERE key = '$1'"
}

# leftover GSI prepared transactions on every data node
prepared() {
  local rows
  rows=$(for i in 1 2 3 4 5; do
    docker compose exec -T "shard$i" psql -U postgres -X -At \
      -c "SELECT 'shard$i  ' || gid FROM pg_prepared_xacts"
  done)
  echo "-- prepared transactions on the data nodes:"
  echo "${rows:-(none)}"
}

# wait until the coordinator accepts connections again after a crash
wait_coordinator() {
  until docker compose exec -T coordinator psql -U postgres -X -c "SELECT 1" >/dev/null 2>&1; do
    sleep 1
  done
}

section() {
  [ "${NOPAUSE:-}" = 1 ] || read -rp $'\n[Enter] next section ' </dev/tty
  printf '\n==================================================================\n'
  printf ' %s\n' "$1"
  printf '==================================================================\n\n'
}

# The whole demo is one function, called on the last line: bash then reads the
# entire file before running it, so editing the script can't break a run.
main() {

# start clean: finish leftovers of an interrupted run, remove its rows (silently)
docker compose exec -T coordinator psql -U postgres -X -q \
  -c "SELECT gsi_resolve(0)" -c "DELETE FROM users WHERE id >= 9000000" >/dev/null

# ---------------------------------------------------------------- 1
section "1. Read path: lookup by email through the GSI"

coord_warm <<'SQL'
SELECT * FROM users WHERE email = 'user123456@mail.org';

EXPLAIN ANALYZE SELECT * FROM users WHERE email = 'user123456@mail.org';

-- same query without the GSI: every shard is asked
SET gsi.enable_lookup = off;
EXPLAIN ANALYZE SELECT * FROM users WHERE email = 'user123456@mail.org';
SQL

# ---------------------------------------------------------------- 2
section "2. Writes keep the GSI up to date: insert, update, delete"

coord -c "INSERT INTO users VALUES (9000001, 'demo1@x', 'Demo One')" \
      -c "SELECT * FROM users WHERE email = 'demo1@x'"
index_entry 'demo1@x'

coord -c "UPDATE users SET email = 'demo1-new@x' WHERE id = 9000001" \
      -c "SELECT * FROM users WHERE email = 'demo1@x'" \
      -c "SELECT * FROM users WHERE email = 'demo1-new@x'"
index_entry 'demo1@x'
index_entry 'demo1-new@x'

coord -c "DELETE FROM users WHERE id = 9000001" \
      -c "SELECT * FROM users WHERE email = 'demo1-new@x'"
index_entry 'demo1-new@x'

# ---------------------------------------------------------------- 3
section "3. One insert = one distributed transaction (2PC)"

since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
coord <<'SQL'
SET gsi.trace = on;
INSERT INTO users VALUES (9000002, 'twopc@x', 'Two Phase');
SQL

echo
echo "-- what the data nodes received:"
docker compose logs --since "$since" shard1 shard2 shard3 shard4 shard5 \
  | grep "PREPARE" | sort -k3,4    # sort by timestamp

# ---------------------------------------------------------------- 4
section "4a. Coordinator crashes AFTER the commit decision -> roll forward"

coord <<'SQL'
SET gsi.fail_point = 'after_local_commit';
INSERT INTO users VALUES (9000003, 'crash@x', 'Crash');
SQL
wait_coordinator

echo
prepared
coord -c "SELECT * FROM users WHERE id = 9000003" \
      -c "SELECT gsi_resolve(0)" \
      -c "SELECT * FROM users WHERE id = 9000003"
prepared

section "4b. Coordinator crashes BEFORE the commit decision -> roll back"

coord <<'SQL'
SET gsi.fail_point = 'after_prepare';
INSERT INTO users VALUES (9000004, 'lost@x', 'Lost');
SQL
wait_coordinator

echo
prepared
coord -c "SELECT gsi_resolve(0)" \
      -c "SELECT * FROM users WHERE id = 9000004"
prepared

echo
echo "Done. Output saved to demo/demo_output.txt"
}

main "$@"
