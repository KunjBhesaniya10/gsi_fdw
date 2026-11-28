# gsi_fdw: global secondary indexes for sharded PostgreSQL

`gsi_fdw` is a PostgreSQL 16 extension, built on `postgres_fdw`, that adds
global secondary indexes (GSIs) to a table hash-partitioned across data
nodes. The entry for key `k` lives on node `hash(k) mod N`, so a lookup by
an indexed column asks one index node and then only the shards that hold
matching rows, instead of scanning every shard.

- **Sync GSIs** are updated in the same transaction as the base row, with
  two-phase commit. A background resolver finishes transactions left
  prepared by a crash.
- **Async GSIs** are updated later from an outbox by background workers.
- **Unique GSIs** enforce uniqueness across all shards.
- **Online build:** `CALL gsi_create(...)` builds an index while reads and
  writes continue.

The design and the results are in [report/final_report.pdf](report/final_report.pdf).

## Layout

| Path | Contents |
|---|---|
| `src/gsi_fdw/` | The extension: `postgres_fdw` 16 renamed to `gsi_fdw`, plus the GSI code |
| `src/gsi_fdw/gsi.c` | Catalog access, placement (`hash(key) mod N`), index maintenance on writes |
| `src/gsi_fdw/gsi_scan.c` | Read path: planner hook, cost model and the `GsiLookup` custom scan |
| `src/gsi_fdw/gsi_build.c` | Online build, `CALL gsi_create()` |
| `src/gsi_fdw/gsi_async.c` | Async GSIs: outbox applier workers, `gsi_lag()` |
| `src/gsi_fdw/gsi_resolver.c` | Resolver for in-doubt prepared transactions, `gsi_resolve()` |
| `src/gsi_fdw/connection.c` | `postgres_fdw` connection code with the GSI two-phase commit added |
| `src/gsi_fdw/gsi_fdw--1.0.sql` | SQL objects: the `gsi` catalog schema, `gsi_create`, `gsi_drop`, ... |
| `docker/` | The cluster: `Dockerfile`, `docker-compose.yml`, init scripts, `playbook.sql` |
| `demo/` | Live demo: walkthrough (`demo.md`), setup and reset scripts, scripted run, consistency checker |
| `bench/` | Benchmarks (`experiments.py`), data loader, online build stress tests (`build_stress.py`) |
| `bench/results/` | CSV results used in the report (`sequential_hop2/`: an earlier run, before hop 2 queried shards in parallel) |
| `report/` | Final report (LaTeX source and PDF) |

## Requirements

- Docker with Compose v2 (tested with Docker Desktop on Windows).
- Bash. On Windows, use Git Bash; all scripts are run from the repo root.
- Python 3 with `psycopg2` (`pip install psycopg2-binary`), for the demo
  checker, benchmarks and stress tests.

## Running the cluster

The cluster has one coordinator and five data nodes, each running
PostgreSQL 16 in its own container. The image compiles and installs the
extension.

```bash
cd docker
docker compose up -d --build --wait     # build the image, start and wait until healthy
docker compose ps                       # 6 containers, all (healthy)
```

| Node | Host port | Connect with |
|---|---|---|
| coordinator | 5432 | `docker compose exec coordinator psql -U postgres` |
| shard1 ... shard5 | 5433 ... 5437 | `docker compose exec shard1 psql -U postgres` |

On first start the coordinator creates the extension, one foreign server
per data node, and the table `users(id, email, name)`, hash-partitioned by
`id` with one partition per node. Clients connect only to the coordinator.

```sql
-- on the coordinator
CALL gsi_create('users', 'email', true);   -- unique, sync GSI on email
INSERT INTO users (email, name) VALUES ('a@x.org', 'Ann');
EXPLAIN ANALYZE SELECT * FROM users WHERE email = 'a@x.org';   -- Custom Scan (GsiLookup)
```

Other useful commands:

```bash
bash bench/load_users.sh                 # from the repo root: 1M rows with GSIs on email (unique) and name, ~3 min
docker compose exec -T coordinator psql -U postgres -X -f - < playbook.sql   # in docker/: small sample data
docker compose down -v                   # stop and delete all data
NNODES=3 docker compose up -d --wait     # use only shard1..shard3 (after down -v)
```

After changing anything in `src/gsi_fdw/`, rebuild with
`docker compose down -v && docker compose up -d --build --wait`.

## Running the demo

The demo shows the read path, distributed writes, global uniqueness and
recovery from node and coordinator crashes. [demo/demo.md](demo/demo.md)
gives every step to type, with the expected output. From the repo root:

```bash
bash demo/setup.sh          # fresh cluster, 1M rows, demo settings (~5 min)
```

Then either follow `demo/demo.md` by hand (it uses four terminals, listed
under "Terminals"), or run it as a script:

```bash
bash demo/run_demo.sh                # pauses for Enter between sections
NOPAUSE=1 bash demo/run_demo.sh      # runs straight through
```

The scripted run saves its output to `demo/demo_output.txt`. Afterwards:

```bash
python demo/checker.py      # every GSI matches the base table, no prepared transactions left
bash demo/reset.sh          # undo the demo settings, keep the data
```

## Benchmarks and tests

Run these from the repo root with the cluster up. Results are written to
`bench/results/`.

```bash
python bench/experiments.py writes          # write cost: no GSI / sync / async (wipes users)
python bench/experiments.py build           # online build on 1M rows (run bench/load_users.sh first)
python bench/experiments.py tpcc            # TPC-C-derived customer workload (creates its own table)
bash bench/net_delay.sh 1                   # add 1 ms round trip between containers (0 removes it)
python bench/build_stress.py mixed          # online build under concurrent writes (wipes users)
python bench/build_stress.py window         # delete-only state test
```

The docstring at the top of each script lists all of its options.
