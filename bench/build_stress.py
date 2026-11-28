"""Stress tests for the online build (CALL gsi_create) under concurrent writes.

WIPES the users table.  Run from the repo root with the cluster up:

  python bench/build_stress.py mixed  [--rounds 3] [--clients 8] [--column name|email]
      random INSERT / DELETE / key UPDATE / key->NULL / non-key UPDATE /
      2-row transactions on the indexed column while the GSI builds; then the
      full base-vs-index check (missing, orphaned, misplaced, prepared).
  python bench/build_stress.py probe
      DELETE of the row the backfill reaches last, while the build runs:
      it must succeed at once (a not-yet-backfilled row has no entry yet).
  python bench/build_stress.py window
      a slow DELETE planned before gsi_create deletes a row that a new
      writer inserted after the build started: the build must still succeed
      with no orphaned entry (the delete-only state).

The build is slowed down (small chunks, a delay between chunks) so writes
land in every state of it.
"""
import argparse
import collections
import random
import sys
import threading
import time
import uuid

import psycopg2

from db import COORD, NNODES, SHARDS, check, connect, q


def reset(c, nrows):
    if q(c, "SELECT 1 FROM gsi.gsi_index WHERE base_table = 'users'::regclass"):
        q(c, "SELECT gsi_drop('users')")
    for s in SHARDS[:NNODES]:
        sc = connect(s)
        q(sc, "TRUNCATE users_shard")
        q(sc, "DROP TABLE IF EXISTS gsi_users_name, gsi_users_email")
        sc.close()
    q(c, "INSERT INTO users (id, email, name) SELECT g, 'u' || g || '@x', 'n' || (g %% 500) "
         "FROM generate_series(1, %s) g", (nrows,))
    q(c, "SELECT setval('users_id_seq', %s)", (nrows,))


def slow_build(c, chunk, delay_ms):
    q(c, f"SET gsi.build_chunk_size = {chunk}")
    q(c, f"SET gsi.build_chunk_delay = {delay_ms}")


def verdict(c, column):
    shards = [connect(s) for s in SHARDS[:NNODES]]
    r = check(c, shards, f"gsi_users_{column}", column)
    for s in shards:
        s.close()
    status = q(c, "SELECT status FROM gsi.gsi_index WHERE column_name = %s", (column,))
    ok = not (r["missing"] or r["orphaned"] or r["misplaced"] or r["prepared"])
    print(f"  status={status[0][0] if status else None} rows={r['rows']} "
          f"missing={len(r['missing'])} orphaned={len(r['orphaned'])} "
          f"misplaced={len(r['misplaced'])} prepared={len(r['prepared'])} -> "
          f"{'CONSISTENT' if ok else 'INCONSISTENT'}")
    for k in ("missing", "orphaned", "misplaced", "prepared"):
        if r[k]:
            print(f"    {k} (first 5): {r[k][:5]}")
    return ok


def classify(e):
    if "concurrent update of GSI" in (e.pgerror or ""):
        return "40001-gsi-entry-not-found"
    msg = " | ".join(l.strip() for l in (e.pgerror or "").splitlines()[:3])
    return f"{e.pgcode}: {msg}"


# --- mixed -------------------------------------------------------------------------

def mixed(a):
    c = connect(COORD)
    col, other = a.column, ("email" if a.column == "name" else "name")
    unique = col == "email"
    kinds = ["insert", "delete", "key", "other", "txn"] + ([] if unique else ["null"])
    all_ok = True

    def new_val():
        return f"{uuid.uuid4().hex}@x" if unique else f"n{random.randint(0, 999)}"

    for rnd in range(a.rounds):
        print(f"round {rnd + 1}/{a.rounds}: {a.rows} rows, {a.clients} clients, GSI on {col}"
              f"{' (unique)' if unique else ''}")
        reset(c, a.rows)
        stats = collections.Counter()
        lock = threading.Lock()
        phase = ["before"]
        stop = threading.Event()

        def worker():
            w = connect(COORD)
            while not stop.is_set():
                kind = random.choice(kinds)
                hi = q(w, "SELECT last_value FROM users_id_seq")[0][0]
                rid = random.randint(1, hi)
                ph = phase[0]
                try:
                    if kind == "insert":
                        q(w, "INSERT INTO users (email, name) VALUES (%s, %s)",
                          (f"{uuid.uuid4().hex}@x", new_val() if col == "email" else
                           random.choice([new_val(), None])))
                    elif kind == "delete":
                        q(w, "DELETE FROM users WHERE id = %s", (rid,))
                    elif kind == "key":
                        q(w, f"UPDATE users SET {col} = %s WHERE id = %s", (new_val(), rid))
                    elif kind == "null":
                        q(w, f"UPDATE users SET {col} = NULL WHERE id = %s", (rid,))
                    elif kind == "other":
                        v = f"{uuid.uuid4().hex}@x" if other == "email" else "o"
                        q(w, f"UPDATE users SET {other} = %s WHERE id = %s", (v, rid))
                    else:
                        q(w, "BEGIN")
                        q(w, f"UPDATE users SET {col} = %s WHERE id = %s", (new_val(), rid))
                        q(w, f"UPDATE users SET {col} = %s WHERE id = %s",
                          (new_val(), random.randint(1, hi)))
                        q(w, "COMMIT")
                    res = "ok"
                except psycopg2.Error as e:
                    res = classify(e)
                    try:
                        q(w, "ROLLBACK")
                    except psycopg2.Error:
                        pass
                with lock:
                    stats[(ph, kind, res)] += 1
            w.close()

        ws = [threading.Thread(target=worker) for _ in range(a.clients)]
        for t in ws:
            t.start()
        time.sleep(2)
        b = connect(COORD)
        slow_build(b, a.chunk, a.delay)
        phase[0] = "during"
        t0 = time.time()
        build_err = None
        try:
            q(b, "CALL gsi_create('users', %s, %s)", (col, unique))
        except psycopg2.Error as e:
            build_err = f"{e.pgcode}: {e.pgerror.strip()}"
        build_s = time.time() - t0
        phase[0] = "after"
        time.sleep(3)
        stop.set()
        for t in ws:
            t.join()
        b.close()

        print(f"  build {build_s:.1f}s" + (f"  FAILED: {build_err}" if build_err else "  ok"))
        for ph in ("before", "during", "after"):
            row = collections.defaultdict(collections.Counter)
            for (p, k, r), n in stats.items():
                if p == ph:
                    row[k][r] += n
            print(f"  {ph}:")
            for k in kinds:
                tot = sum(row[k].values())
                errs = {r: n for r, n in row[k].items() if r != "ok"}
                print(f"    {k:7s} {tot:6d} ops  ok={row[k]['ok']:6d}  errors={errs or '-'}")
        if build_err is None:
            all_ok &= verdict(c, col)
    sys.exit(0 if all_ok else 1)


# --- probe -------------------------------------------------------------------------

def probe(a):
    c = connect(COORD)
    reset(c, a.rows)
    last = connect(SHARDS[NNODES - 1])
    target = q(last, "SELECT max(id) FROM users_shard")[0][0]   # backfilled last
    last.close()
    b = connect(COORD)
    slow_build(b, a.chunk, a.delay)
    done = threading.Event()

    def build():
        q(b, "CALL gsi_create('users', 'name')")
        done.set()

    t = threading.Thread(target=build)
    t0 = time.time()
    t.start()
    while not q(c, "SELECT 1 FROM gsi.gsi_build_progress WHERE index_name = 'users_name'"):
        time.sleep(0.05)
    print(f"backfill started; deleting id {target} (last row in backfill order) until it works")
    fails, first_err = 0, None
    while True:
        try:
            q(c, "DELETE FROM users WHERE id = %s", (target,))
            break
        except psycopg2.Error as e:
            fails += 1
            first_err = first_err or e.pgerror.strip()
            time.sleep(0.2)
    took = time.time() - t0
    t.join()
    print(f"  DELETE failed {fails} times over {took:.1f}s (build took {time.time() - t0:.1f}s)")
    if first_err:
        print(f"  error: {first_err}")
    verdict(c, "name")


# --- window ------------------------------------------------------------------------

def window(a):
    """Old statement O (planned without the index) overlaps new writer N.

    O deletes name = 'win' rows partition by partition, sleeping per row on
    the first partition, so it reaches the last partition seconds later.  N
    inserts a 'win' row into the last partition after gsi_create has
    committed its first status change.  O then deletes N's row.  Without the
    delete-only state N would add an entry that nobody deletes, and
    validation would fail."""
    c = connect(COORD)
    reset(c, a.rows)
    q(c, "CREATE OR REPLACE FUNCTION gsi_test_slow(bigint) RETURNS bool VOLATILE "
         "LANGUAGE sql AS 'SELECT pg_sleep(0.3) IS NOT NULL'")
    first = q(c, "SELECT g FROM generate_series(20000000, 20010000) g "
                 "WHERE satisfies_hash_partition('users'::regclass, %s, 0, g::bigint) LIMIT 10",
              (NNODES,))
    last = q(c, "SELECT g FROM generate_series(20000000, 20010000) g "
                "WHERE satisfies_hash_partition('users'::regclass, %s, %s, g::bigint) LIMIT 1",
             (NNODES, NNODES - 1))[0][0]
    for (i,) in first:
        q(c, "INSERT INTO users (id, email, name) VALUES (%s, %s, 'win')", (i, f"w{i}@x"))

    # With remote estimates, planning O runs EXPLAIN on every partition, which
    # takes O's snapshot on every node before N's insert, so O never sees N's
    # row.  Off on the last partition, O's snapshot there comes after it.
    lastp = f"users_p{NNODES - 1}"
    q(c, f"ALTER FOREIGN TABLE {lastp} OPTIONS (ADD use_remote_estimate 'false')")
    o = connect(COORD)
    o_err = []

    def old_delete():
        try:
            n = q(o, "DELETE FROM users WHERE name = 'win' AND gsi_test_slow(id) RETURNING id")
            o_err.append(None)
            print(f"  O finished at {time.time() - T0:.1f}s, deleted {len(n)} rows")
        except psycopg2.Error as e:
            o_err.append(e.pgerror.strip())

    T0 = time.time()
    to = threading.Thread(target=old_delete)
    to.start()
    time.sleep(0.5)                      # O is planned and sleeping on partition 0

    b = connect(COORD)
    slow_build(b, 1000, 0)
    b_err = []

    def build():
        try:
            q(b, "CALL gsi_create('users', 'name')")
        except psycopg2.Error as e:
            b_err.append(e.pgerror.strip())

    tb = threading.Thread(target=build)
    tb.start()
    while not q(c, "SELECT 1 FROM gsi.gsi_index WHERE index_name = 'users_name'"):
        time.sleep(0.05)
    status = q(c, "SELECT status FROM gsi.gsi_index WHERE index_name = 'users_name'")[0][0]
    q(c, "INSERT INTO users (id, email, name) VALUES (%s, 'late@x', 'win')", (last,))
    print(f"N inserted at {time.time() - T0:.1f}s id {last} (last partition) while the index was {status}; O still running")
    to.join()
    tb.join()
    q(c, f"ALTER FOREIGN TABLE {lastp} OPTIONS (DROP use_remote_estimate)")
    left = q(c, "SELECT count(*) FROM users WHERE name = 'win'")[0][0]
    print(f"  O: {'error ' + o_err[0] if o_err and o_err[0] else 'ok'}; 'win' rows left: {left}")
    print(f"  gsi_create: {'FAILED ' + b_err[0] if b_err else 'ok'}")
    ok = not b_err and verdict(c, "name")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("test", choices=["mixed", "probe", "window"])
    ap.add_argument("--rows", type=int, default=20000)
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--clients", type=int, default=8)
    ap.add_argument("--column", choices=["name", "email"], default="name")
    ap.add_argument("--chunk", type=int, default=200, help="gsi.build_chunk_size")
    ap.add_argument("--delay", type=int, default=50, help="gsi.build_chunk_delay, ms")
    a = ap.parse_args()
    {"mixed": mixed, "probe": probe, "window": window}[a.test](a)
