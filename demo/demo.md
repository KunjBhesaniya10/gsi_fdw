# Live Demo (final presentation)

A manual demo: every step is a query or command typed live. No scripts run during the demo. Duration ~15 min (parts 1-6), plus the appendix for questions.

Uses the phase 2 demo additions `gsi.trace` (2PC steps as NOTICEs) and `gsi.pause_point` (stop between PREPARE and COMMIT so a node can be killed by hand). **Dry-run the whole demo on the presentation laptop the day before.**

All shard numbers below are fixed by the hash functions, so they are the same on every fresh cluster (checked on 2026-11-20).

---

## Setup: before the audience arrives (~10 min)

**Shortcut:** `bash demo/setup.sh` from the repo root (Git Bash) runs every step of this section except the C1 warm-up; `bash demo/reset.sh` undoes the settings afterwards. The steps by hand:

From the repo root, Git Bash:
```
cd docker
docker compose down -v && docker compose up -d --build
docker compose ps                         # 6 containers, all (healthy)
cd .. && bash bench/load_users.sh         # 1M rows, GSIs on email (unique) and name (~80 s)
```
The shards get **no** local index on `email` or `name`, as in the report's results: `users_shard` has only its primary key on `id`, so a broadcast lookup scans every shard.

Prepare every data node for stable timings, turn on statement logging there (so the 2PC commands appear in the log terminal), and hold back the background resolver so leftovers stay visible until we resolve them by hand:
```
cd docker
for s in shard1 shard2 shard3 shard4 shard5; do
  docker compose exec -T $s psql -U postgres \
    -c "VACUUM (ANALYZE) users_shard, gsi_users_email, gsi_users_name" \
    -c "CREATE EXTENSION IF NOT EXISTS pg_prewarm" \
    -c "SELECT count(pg_prewarm(c.oid::regclass)) FROM pg_class c WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r','i') AND (c.relname LIKE 'users_shard%' OR c.relname LIKE 'gsi_users_%')" \
    -c "ALTER SYSTEM SET log_statement = 'all'" -c "SELECT pg_reload_conf()"
  docker compose exec -T coordinator psql -U postgres -c "ALTER SERVER $s OPTIONS (SET use_remote_estimate 'false')"
done
docker compose exec -T coordinator psql -U postgres -c "ALTER SYSTEM SET gsi.resolver_grace = '10min'" -c "SELECT pg_reload_conf()"
```
*Why each step:*
- `VACUUM`: the load leaves rows that autovacuum has only partly processed, so the first read of a row checks its transaction status (first lookups 30-60 ms, the first broadcast ~1 s) and autovacuum could start mid-demo. After VACUUM every lookup costs the same.
- `pg_prewarm`: puts the base and GSI tables (~85 MB per shard) into shared_buffers, so the first lookup of a key doesn't read from disk.
- `use_remote_estimate = false`: otherwise every query sends one `EXPLAIN` to each of the 5 shards while planning (~2.5 ms of planning). The shards have no local index, so the local estimate gives the same plans; planning drops to ~0.5 ms.
- `resolver_grace` and not `resolver_interval`: the worker keeps running every 5 s, but ignores prepared transactions younger than the grace, so it won't "steal" the manual resolve. Part 6c sets it back to 10 s to show the automatic path.

**Warm up C1** (open it from the Terminals section first; it must be the same session you present from): run every query of part 2 once with the GSI on and once with `SET gsi.enable_lookup = off;`, then `RESET gsi.enable_lookup;`. The first query of a session opens the shard connections and loads caches (planning 25-160 ms).

## Terminals (open all four, in `docker/`, side by side)

| Name | Command | Shows |
|---|---|---|
| **C1** | `docker compose exec coordinator psql -U postgres` | the presenter's session |
| **C2** | `docker compose exec coordinator psql -U postgres` | a second client (concurrency) |
| **LOG** | bash: `docker compose logs -f --since 0s coordinator shard1 shard2 shard3 shard4 shard5 \| grep --line-buffered -E "PREPARE\|fail point\|gsi resolver"` <br> PowerShell: `docker compose logs -f --since 0s coordinator shard1 shard2 shard3 shard4 shard5 \| Select-String "PREPARE\|fail point\|gsi resolver"` | what each node receives (2PC), crashes and the resolver |
| **SH** | `docker compose exec shardN psql -U postgres` (N chosen in each part) | one data node from the inside |

Keep the `grep` on LOG: the async appliers poll `gsi_outbox` on every shard several times a second, which floods an unfiltered log.

Helper queries, used throughout (run in **C1**):
```sql
-- which node holds the index entry of a key (0-based; node i = shard i+1)
SELECT gsi_node_for('demo1@x'::text) + 1 AS index_shard;
-- which node holds the base row of an id
SELECT r + 1 AS base_shard FROM generate_series(0, 4) r
WHERE satisfies_hash_partition('users'::regclass, 5, r, 9000001::bigint);
```
*Why `satisfies_hash_partition`:* it's PostgreSQL's own routing check for hash partitions, so the answer is exactly where the row will go; `gsi_node_for` is the index placement function every component of ours uses.

### Where every demo key lands

| Part | Row / key | Base shard | `email` index shard | `name` index shard |
|---|---|---|---|---|
| 2 | `user123456@mail.org` (id 123456) | 1 | 4 (`Index Node: 3`) | – |
| 2 | `Priya Shah` | – | – | 1 |
| 3 | `(9000001, 'demo1@x', 'Demo One')` | 1 | 5 | 5 |
| 3 | update to `demo1-new@x` | 1 | 3 | – |
| 4 | `race@x`: C1 `9000002`, C2 `9000003` | 2 / 3 | 1 | – |
| 5a | `(9000004, 'fail@x', 'Fail')` | 1 | **2** (kill this one) | 4 |
| 5b | `(9000005, 'survive@x', 'Survive')` | **2** (kill this one) | 1 | 4 |
| 6a | `(9000006, 'crash@x', 'Crash')` | 3 | 5 | 3 |
| 6b | `(9000007, 'lost@x', 'Lost')` | 4 | 1 | 3 |
| 6c | `(9000008, 'auto@x', 'Auto')` | 5 | 3 | 5 |

A shard that receives both an index entry and the base row of the same transaction is one participant with one GID, marked `_i` (index participants commit first).

---

## 1. The cluster (1 min)

**C1:**
```sql
SELECT * FROM gsi.gsi_node;                                     -- 5 data nodes
\d+ users                                                       -- hash-partitioned by id, 5 foreign partitions
SELECT index_name, column_name, is_unique, mode, status FROM gsi.gsi_index;
SELECT count(*) FROM users;                                     -- 1000000
```
**Say:** the table is split by `id`. A lookup by `email` or `name` therefore doesn't know which shard to ask. The GSI is a second table, split by the indexed value, that maps `key → (id, shard)`.

## 2. The read path: GSI vs broadcast (3 min)

Run each query **twice** and show the second run (the first can be a little slower).

**C1:**
```sql
EXPLAIN ANALYZE SELECT * FROM users WHERE email = 'user123456@mail.org';
```
**Expect:** `Custom Scan (GsiLookup)`, `GSI: users_email`, `Estimated Entries: 1`, `Index Node: 3` (= shard4), `Shards Fetched: 1`, planning ~0.5 ms, execution **~1 ms** (0.8-1.5). Two round trips: index entry on shard4, then the base row on shard1.
```sql
SET gsi.enable_lookup = off;
EXPLAIN ANALYZE SELECT * FROM users WHERE email = 'user123456@mail.org';
RESET gsi.enable_lookup;
```
**Expect:** `Append` over 5 foreign scans, each a full scan of its ~200k rows (the shards have no index on `email`), ~10 ms each: execution **~55 ms**, about 50x the GSI lookup.
```sql
EXPLAIN ANALYZE SELECT * FROM users WHERE name = 'Priya Shah';
SET gsi.enable_lookup = off;
EXPLAIN ANALYZE SELECT * FROM users WHERE name = 'Priya Shah';
RESET gsi.enable_lookup;
```
**Expect:** with the GSI: `Custom Scan (GsiLookup)`, 625 rows, `GSI: users_name`, `Estimated Entries: 497`, `Estimated Shards: 5.0`, `Index Node: 0` (= shard1), `Shards Fetched: 5`, execution **~3 ms** (2.4-4). Broadcast: `Append`, 625 rows, **~57 ms**.

**Say:** the planner costs both plans. This key's rows sit on all 5 shards, so the GSI asks every shard just like broadcast, but each shard only fetches its rows by primary key instead of scanning 200k rows, so the GSI still wins. Broadcast would win only for a key matching many thousands of rows, or if every shard had its own index on the column (the benchmark has those runs as a reference: the GSI is still 1.2-2.2x faster on 5 nodes).

## 3. One write = a distributed transaction (3 min)

**C1:**
```sql
SELECT gsi_node_for('demo1@x'::text) + 1 AS index_shard;        -- 5
SELECT r + 1 AS base_shard FROM generate_series(0, 4) r
WHERE satisfies_hash_partition('users'::regclass, 5, r, 9000001::bigint);   -- 1
SET gsi.trace = on;                                             -- 2PC steps as NOTICEs
INSERT INTO users VALUES (9000001, 'demo1@x', 'Demo One');
```
**Expect in LOG** (the `name` entry `Demo One` also goes to shard5, so there are two participants):
```
shard5 | LOG: statement: PREPARE TRANSACTION 'gsi_<xid>_shard5_i'     <- index participant
shard1 | LOG: statement: PREPARE TRANSACTION 'gsi_<xid>_shard1_b'     <- base participant
shard5 | LOG: statement: COMMIT PREPARED 'gsi_<xid>_shard5_i'         <- index participants first
shard1 | LOG: statement: COMMIT PREPARED 'gsi_<xid>_shard1_b'         <- base last
```
**Expect in C1:** `NOTICE: gsi: 2PC for xid ..., participants: shard5 (index), shard1 (base)`, a `PREPARE TRANSACTION ...` / `... prepared on` pair per node, `all participants prepared; the local commit of xid ... is the decision: commit`, then `COMMIT PREPARED ... ok` per node, index nodes first.

**Say:** same xid in every GID (that's how the resolver finds them later); every node votes with PREPARE; the decision is the coordinator's local commit; index entries are committed before the base row, so a reader never sees a base row whose index entry is missing.

**SH** on shard5: `SELECT * FROM gsi_users_email WHERE key = 'demo1@x';` → `(demo1@x, 9000001, 0)` (base node 0 = shard1).

Key change:
```sql
UPDATE users SET email = 'demo1-new@x' WHERE id = 9000001;      -- entry deleted on shard5, inserted on shard3
SELECT * FROM users WHERE email = 'demo1-new@x';
```

## 4. Global uniqueness across shards (2 min)

| Step | C1 | C2 |
|---|---|---|
| 1 | `BEGIN;` | `BEGIN;` |
| 2 | `INSERT INTO users VALUES (9000002, 'race@x', 'one');` | |
| 3 | | `INSERT INTO users VALUES (9000003, 'race@x', 'two');` → **blocks** |
| 4 | `COMMIT;` | → `ERROR: duplicate key value violates unique constraint ...` |
| 5 | | `ROLLBACK;` |

**Say:** the two rows would live on different base shards (shard2 and shard3), so no shard could catch the duplicate alone. Both index entries go to the same node (`gsi_node_for('race@x')` = shard1), whose unique index makes C2 wait for C1's outcome.

## 5. A data node fails during a transaction (3 min)

From here on containers restart, which empties their caches: don't go back to timing reads.

### 5a. Fails before the vote → everybody rolls back
**C1:**
```sql
SELECT gsi_node_for('fail@x'::text) + 1 AS index_shard;         -- 2
BEGIN;
INSERT INTO users VALUES (9000004, 'fail@x', 'Fail');           -- index entry now sits uncommitted on shard2
```
Another terminal (`docker/`): `docker compose kill shard2` (a crash, not a clean stop).
**C1:**
```sql
COMMIT;
```
**Expect:** an error about the connection to shard2; the transaction is rolled back. LOG shows `ROLLBACK` on the other participants (shard4 for the `name` entry, shard1 for the base row); no `COMMIT PREPARED` anywhere. With `gsi.trace = on`, C1 shows `PREPARE TRANSACTION ... on shard2 ...` with no `prepared` after it, then `decision: abort; rolling back every participant`.

`docker compose start shard2`, wait for `(healthy)`, then **C1:**
```sql
SELECT * FROM users WHERE id = 9000004;                         -- 0 rows
SELECT * FROM users WHERE email = 'fail@x';                     -- 0 rows
```
**SH** (shard2 and shard1): `SELECT gid FROM pg_prepared_xacts;` → empty.

### 5b. Fails after the vote → the prepared transaction survives the crash
**C1:**
```sql
SET gsi.pause_point = 'after_prepare';  SET gsi.pause_seconds = 20;
INSERT INTO users VALUES (9000005, 'survive@x', 'Survive');
```
C1 prints `NOTICE: gsi: paused at "after_prepare" for 20 s`. During the pause: `docker compose kill shard2` (the base shard of 9000005), then `docker compose start shard2`. The coordinator continues: local commit = decision **commit**; the index participants shard1 and shard4 commit; `COMMIT PREPARED` to shard2 fails → `WARNING: could not finish prepared transaction ..., left for gsi_resolve()`.

**SH** on shard2 after restart: `SELECT gid FROM pg_prepared_xacts;` → `gsi_<xid>_shard2_b` **survived the crash** (PREPARE wrote it to disk). **C1:** `SELECT gsi_resolve(0);` → 1; the row is visible. **Say:** this is the 2PC guarantee: once a node says "prepared", it can always commit, even across its own crash.

## 6. The coordinator crashes (3 min)

### 6a. After the decision → roll forward
**C1:**
```sql
SET gsi.fail_point = 'after_local_commit';
INSERT INTO users VALUES (9000006, 'crash@x', 'Crash');
```
**Expect:** `server closed the connection unexpectedly` (the coordinator backend `abort()`ed; PostgreSQL restarts its processes, the container keeps running). LOG: `gsi fail point "after_local_commit" reached`.

**SH** on shard5 and shard3: `SELECT gid FROM pg_prepared_xacts;` → leftovers `gsi_<xid>_shard5_i` (email entry) and `gsi_<xid>_shard3_i` (name entry and base row on the same shard).

**C1** (psql reconnects by itself on the next command):
```sql
SELECT * FROM users WHERE id = 9000006;      -- 0 rows: still prepared, invisible
SELECT gsi_resolve(0);                       -- 2
SELECT * FROM users WHERE id = 9000006;      -- 1 row
```
LOG: `gsi resolver: COMMIT PREPARED ..._shard5_i` and `..._shard3_i`. **Say:** the resolver doesn't decide anything; it reads the coordinator's commit log for that xid: committed → finish the commit.

### 6b. Before the decision → roll back
```sql
SET gsi.fail_point = 'after_prepare';
INSERT INTO users VALUES (9000007, 'lost@x', 'Lost');
-- reconnects
SELECT gsi_resolve(0);                       -- 3: shard1_i, shard3_i, shard4_b rolled back
SELECT * FROM users WHERE id = 9000007;      -- 0 rows
```
**Say:** no local commit record = no decision = abort is safe.

### 6c. Automatic: the background worker
```sql
ALTER SYSTEM SET gsi.resolver_grace = '10s';  SELECT pg_reload_conf();
SET gsi.fail_point = 'after_local_commit';
INSERT INTO users VALUES (9000008, 'auto@x', 'Auto');
```
Don't resolve. **SH** on shard3 (or shard5): `SELECT gid FROM pg_prepared_xacts \watch 2` → `..._shard3_i` (and `..._shard5_i` on shard5) disappear within ~15 s (10 s grace + up to 5 s until the worker's next run); LOG shows `gsi resolver: COMMIT PREPARED ...` from the worker.

**Finish:** from the repo root, `python demo/checker.py` → `missing=0 orphaned=0 misplaced=0 prepared=0` for both indexes.

---

## Appendix: for questions

**Recheck protects against a wrong entry.** On the index node of `ghost@x` (`SELECT gsi_node_for('ghost@x'::text) + 1;`), insert a fake entry pointing at an existing row: `INSERT INTO gsi_users_email VALUES ('ghost@x', 9000001, 0);` (base node 0 = shard1). **C1:** `SELECT * FROM users WHERE email = 'ghost@x';` → 0 rows; `EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)` shows `Rows Removed by Filter: 1`. Delete the fake entry afterwards.

**The crash window between index and base commit.** `SET gsi.fail_point = 'mid_commit_prepared';` then an insert: the index entry is committed, the base row is still prepared. A lookup returns 0 rows (the entry points at an invisible row; the recheck/visibility handle it), never a wrong row. `SELECT gsi_resolve(0);` → 1, then the row is visible.

**Why writes aren't pushed down.** `EXPLAIN VERBOSE UPDATE users SET email = 'z' WHERE name = 'x';` → `UPDATE ... WHERE ctid = $1` per row, so the index can be maintained. An UPDATE that sets no indexed column is pushed down whole.

**Generic plans use the GSI too.** `SET plan_cache_mode = force_generic_plan; PREPARE p(text) AS SELECT * FROM users WHERE email = $1; EXPLAIN EXECUTE p('demo1-new@x');` → GsiLookup.

**Known v1 restriction.** `UPDATE users SET id = 1 WHERE id = 9000001;` → `ERROR: cannot update column "id" of a table with a GSI`.

**A shard that hangs instead of crashing.** `docker compose pause shardK`, then in **C1** `SET statement_timeout = '5s';` and an insert touching shard K → timeout, full rollback; `docker compose unpause shardK`.

**Why is broadcast ~55 ms?** postgres_fdw's `Append` scans the 5 shards one after another, ~10 ms each. With `async_capable` it would scan them in parallel (~12 ms); the GSI lookup (~1 ms) still wins, by ~10x instead of ~50x.

**Is planning cheap?** The GSI cost model uses only coordinator-local information (catalog, statistics, server options), no remote calls. With `use_remote_estimate = true`, postgres_fdw would add one `EXPLAIN` per shard to price the broadcast plan (~2.5 ms); that only pays off when shards have their own indexes.

## Reset after the demo
```
docker compose exec -T coordinator psql -U postgres -c "ALTER SYSTEM RESET gsi.resolver_grace" -c "SELECT pg_reload_conf()"
for s in shard1 shard2 shard3 shard4 shard5; do
  docker compose exec -T coordinator psql -U postgres -c "ALTER SERVER $s OPTIONS (SET use_remote_estimate 'true')"
  docker compose exec -T $s psql -U postgres -c "ALTER SYSTEM RESET log_statement" -c "SELECT pg_reload_conf()"
done
```
or wipe everything: `docker compose down -v`.
