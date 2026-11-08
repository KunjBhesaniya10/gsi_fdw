"""Connections to the cluster and the GSI consistency check."""
import os
import time

import psycopg2

COORD = dict(host="localhost", port=5432, user="postgres", dbname="postgres")
NNODES = int(os.environ.get("NNODES", 5))    # data nodes in use (docker compose NNODES)
SHARDS = [dict(COORD, port=5433 + i) for i in range(NNODES)]


def connect(params, autocommit=True):
    for _ in range(60):
        try:
            conn = psycopg2.connect(**params)
            conn.autocommit = autocommit
            return conn
        except psycopg2.OperationalError:
            time.sleep(1)
    raise RuntimeError(f"cannot connect to {params}")


def q(conn, sql, args=None):
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall() if cur.description else None


def gsis(coord, base="users"):
    """(index_name, column_name) of every GSI on base."""
    return q(coord, "SELECT index_name, column_name FROM gsi.gsi_index "
                    "WHERE base_table = %s::regclass ORDER BY index_name", (base,))


def check(coord, shards, index_table="gsi_users_email", column="email"):
    base = set()
    index = set()
    misplaced = []
    leftover = []
    for node, s in enumerate(shards):
        base |= {(k, i, node) for i, k in q(s, f"SELECT id, {column}::text FROM users_shard "
                                               f"WHERE {column} IS NOT NULL")}
        entries = q(s, f"SELECT key::text, id, shard FROM {index_table}")
        index |= set(entries)
        # placement of every entry on this node, in one round trip
        wrong = q(coord, f"SELECT k FROM unnest(%s::text[]) k "
                         f"WHERE gsi_node_for(k::{type_of(coord, column)}) <> %s",
                  ([k for k, _, _ in entries], node))
        misplaced += [(k, node) for (k,) in wrong]
        leftover += [g for (g,) in q(s, "SELECT gid FROM pg_prepared_xacts "
                                        "WHERE gid LIKE 'gsi\\_%'")]
    return {
        "missing": sorted(base - index),     # base row without entry
        "orphaned": sorted(index - base),    # entry without base row
        "misplaced": misplaced,              # entry on the wrong node
        "prepared": leftover,                # unfinished 2PC
        "rows": len(base),
    }


def type_of(coord, column, base="users"):
    return q(coord, "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
                    "WHERE attrelid = %s::regclass AND attname = %s", (base, column))[0][0]


def check_all(coord, shards):
    """{index_name: check result} for every GSI on users."""
    return {name: check(coord, shards, f"gsi_{name}", col) for name, col in gsis(coord)}
