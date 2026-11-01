/*-------------------------------------------------------------------------
 *
 * gsi.c
 *		  GSI catalog access and placement
 *
 * Catalog tables live in schema gsi on the coordinator:
 *	gsi.gsi_node(node_id, server_name)	maps node ids 0..N-1 to servers
 *	gsi.gsi_index(index_name, ...)		one row per GSI
 *
 * Each data node holds public.gsi_<index_name>(key, id, shard).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/table.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/partition.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "partitioning/partbounds.h"
#include "postgres_fdw.h"
#include "gsi.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/typcache.h"

PG_FUNCTION_INFO_V1(gsi_create);
PG_FUNCTION_INFO_V1(gsi_drop);
PG_FUNCTION_INFO_V1(gsi_node_for);

void
gsi_init(void)
{
	MarkGUCPrefixReserved("gsi");
}

/* Server OIDs of all nodes, ordered by node id. Caller must be in SPI. */
Oid *
gsi_load_nodes(int *nnodes, MemoryContext cxt)
{
	Oid		   *servers;

	if (SPI_execute("SELECT s.oid FROM gsi.gsi_node n "
					"JOIN pg_catalog.pg_foreign_server s ON s.srvname = n.server_name "
					"ORDER BY n.node_id", true, 0) != SPI_OK_SELECT)
		elog(ERROR, "could not read gsi.gsi_node");
	if (SPI_processed == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("gsi.gsi_node is empty")));

	*nnodes = SPI_processed;
	servers = MemoryContextAlloc(cxt, sizeof(Oid) * SPI_processed);
	for (uint64 i = 0; i < SPI_processed; i++)
	{
		bool		isnull;

		servers[i] = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i],
													SPI_tuptable->tupdesc,
													1, &isnull));
	}
	return servers;
}

/* GsiIndex rows: name, column, unique, ready, type, collation */
#define GSI_INDEX_COLUMNS \
	"SELECT i.index_name, i.column_name, i.is_unique, i.status = 'ready', " \
	"a.atttypid, a.attcollation " \
	"FROM gsi.gsi_index i JOIN pg_catalog.pg_attribute a " \
	"ON a.attrelid = i.base_table AND a.attname = i.column_name "

/*
 * Run sql, a query on gsi.gsi_index taking one parameter, and return a List
 * of GsiIndex (one per row) allocated in cxt.  Caller must be in SPI.
 *
 * read_only is false so that SPI advances the command counter and rows
 * written earlier in this transaction are visible.
 */
static List *
read_indexes(const char *sql, Oid argtype, Datum arg, MemoryContext cxt)
{
	List	   *indexes = NIL;
	Oid		   *servers;
	int			nnodes;

	if (SPI_execute_with_args(sql, 1, &argtype, &arg, NULL, false, 0) != SPI_OK_SELECT)
		elog(ERROR, "could not read gsi.gsi_index");
	if (SPI_processed == 0)
		return NIL;

	{
		SPITupleTable *tuptable = SPI_tuptable;
		uint64		nrows = SPI_processed;

		servers = gsi_load_nodes(&nnodes, cxt);
		for (uint64 r = 0; r < nrows; r++)
		{
			HeapTuple	tup = tuptable->vals[r];
			TupleDesc	desc = tuptable->tupdesc;
			bool		isnull;
			MemoryContext oldcxt = MemoryContextSwitchTo(cxt);
			GsiIndex   *idx = palloc0(sizeof(GsiIndex));

			idx->name = pstrdup(NameStr(*DatumGetName(SPI_getbinval(tup, desc, 1, &isnull))));
			idx->column = pstrdup(NameStr(*DatumGetName(SPI_getbinval(tup, desc, 2, &isnull))));
			idx->is_unique = DatumGetBool(SPI_getbinval(tup, desc, 3, &isnull));
			idx->is_ready = DatumGetBool(SPI_getbinval(tup, desc, 4, &isnull));
			idx->typid = DatumGetObjectId(SPI_getbinval(tup, desc, 5, &isnull));
			idx->collid = DatumGetObjectId(SPI_getbinval(tup, desc, 6, &isnull));
			idx->remote_table = psprintf("public.%s",
										 quote_identifier(psprintf("gsi_%s", idx->name)));
			idx->servers = servers;
			idx->nnodes = nnodes;
			indexes = lappend(indexes, idx);
			MemoryContextSwitchTo(oldcxt);
		}
	}
	return indexes;
}

/*
 * Return the GSIs on relid (a partitioned table or one of its partitions),
 * any status, ordered by index name, or NIL.
 */
List *
gsi_indexes_for_rel(Oid relid)
{
	MemoryContext cxt = CurrentMemoryContext;
	List	   *indexes;

	if (!OidIsValid(get_namespace_oid("gsi", true)))
		return NIL;
	if (get_rel_relispartition(relid))
		relid = get_partition_parent(relid, false);

	SPI_connect();
	indexes = read_indexes(GSI_INDEX_COLUMNS
						   "WHERE i.base_table = $1 ORDER BY i.index_name",
						   REGCLASSOID, ObjectIdGetDatum(relid), cxt);
	SPI_finish();
	return indexes;
}

/*
 * Index placement: the type's extended hash with the hash-partitioning seed,
 * mod N.  Every component (write, read, checker) must use this function.
 */
int
gsi_node_for_datum(Datum value, Oid typid, Oid collid, int nnodes)
{
	TypeCacheEntry *tce = lookup_type_cache(typid, TYPECACHE_HASH_EXTENDED_PROC_FINFO);
	uint64		h;

	if (!OidIsValid(tce->hash_extended_proc_finfo.fn_oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("type %s has no extended hash function", format_type_be(typid))));

	h = DatumGetUInt64(FunctionCall2Coll(&tce->hash_extended_proc_finfo, collid,
										 value, UInt64GetDatum(HASH_PARTITION_SEED)));
	return (int) (h % (uint64) nnodes);
}

int
gsi_server_node(GsiIndex *idx, Oid serverid)
{
	for (int i = 0; i < idx->nnodes; i++)
		if (idx->servers[i] == serverid)
			return i;
	elog(ERROR, "foreign server %u is not listed in gsi.gsi_node", serverid);
	return -1;					/* keep compiler quiet */
}

/* Run a DDL command on every node, as part of the distributed transaction. */
void
gsi_exec_on_all_nodes(GsiIndex *idx, const char *sql)
{
	for (int i = 0; i < idx->nnodes; i++)
	{
		PGconn	   *conn;

		conn = GetConnection(GetUserMapping(GetUserId(), idx->servers[i]),
							 false, NULL);
		do_sql_command(conn, sql);
	}
}

/* The GSI on column col, or NULL */
GsiIndex *
gsi_index_on_column(List *indexes, const char *col)
{
	ListCell   *lc;

	foreach(lc, indexes)
	{
		GsiIndex   *idx = lfirst(lc);

		if (strcmp(idx->column, col) == 0)
			return idx;
	}
	return NULL;
}

/*
 * gsi_create(base regclass, col name, is_unique bool) returns text
 *
 * A table can have one GSI per column.  v1 builds the index only on an
 * empty table (no backfill).
 */
Datum
gsi_create(PG_FUNCTION_ARGS)
{
	Oid			base = PG_GETARG_OID(0);
	char	   *col = NameStr(*PG_GETARG_NAME(1));
	bool		is_unique = PG_GETARG_BOOL(2);
	Relation	rel;
	AttrNumber	attnum;
	char	   *name;
	char	   *relname;
	GsiIndex   *idx;
	Oid			argtypes[4] = {NAMEOID, REGCLASSOID, NAMEOID, BOOLOID};
	Datum		args[4];

	rel = table_open(base, AccessExclusiveLock);
	if (rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a partitioned table",
						RelationGetRelationName(rel))));
	attnum = get_attnum(base, col);
	if (attnum == InvalidAttrNumber)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" does not exist", col)));
	if (get_atttype(base, get_attnum(base, "id")) != INT8OID)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("GSI tables need a bigint column \"id\"")));
	if (gsi_index_on_column(gsi_indexes_for_rel(base), col) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("column \"%s\" of table \"%s\" already has a GSI",
						col, RelationGetRelationName(rel))));

	relname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(rel)),
										 RelationGetRelationName(rel));
	name = psprintf("%s_%s", RelationGetRelationName(rel), col);

	SPI_connect();
	if (SPI_execute(psprintf("SELECT 1 FROM %s LIMIT 1", relname), true, 1) != SPI_OK_SELECT)
		elog(ERROR, "could not scan \"%s\"", relname);
	if (SPI_processed > 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("gsi_create requires an empty table")));

	args[0] = DirectFunctionCall1(namein, CStringGetDatum(name));
	args[1] = ObjectIdGetDatum(base);
	args[2] = DirectFunctionCall1(namein, CStringGetDatum(col));
	args[3] = BoolGetDatum(is_unique);
	if (SPI_execute_with_args("INSERT INTO gsi.gsi_index "
							  "(index_name, base_table, column_name, is_unique) "
							  "VALUES ($1, $2, $3, $4)",
							  4, argtypes, args, NULL, false, 0) != SPI_OK_INSERT)
		elog(ERROR, "could not insert into gsi.gsi_index");
	SPI_finish();

	idx = gsi_index_on_column(gsi_indexes_for_rel(base), col);
	gsi_exec_on_all_nodes(idx,
						  psprintf("CREATE TABLE %s (key %s NOT NULL, id bigint NOT NULL, "
								   "shard int NOT NULL, PRIMARY KEY (key, id))",
								   idx->remote_table,
								   format_type_be_qualified(idx->typid)));
	if (is_unique)
		gsi_exec_on_all_nodes(idx, psprintf("CREATE UNIQUE INDEX ON %s (key)",
											idx->remote_table));

	table_close(rel, NoLock);
	PG_RETURN_TEXT_P(cstring_to_text(name));
}

/*
 * Remove one GSI: its catalog row and index tables, in the current
 * distributed transaction.
 */
void
gsi_drop_index(GsiIndex *idx)
{
	Oid			argtypes[1] = {NAMEOID};
	Datum		args[1];

	args[0] = DirectFunctionCall1(namein, CStringGetDatum(idx->name));
	SPI_connect();
	if (SPI_execute_with_args("DELETE FROM gsi.gsi_index WHERE index_name = $1",
							  1, argtypes, args, NULL, false, 0) != SPI_OK_DELETE)
		elog(ERROR, "could not delete from gsi.gsi_index");
	SPI_finish();

	gsi_exec_on_all_nodes(idx, psprintf("DROP TABLE IF EXISTS %s", idx->remote_table));
}

/*
 * gsi_drop(base regclass, col name DEFAULT NULL)
 *
 * Drops the GSI on col, or every GSI of the table when col is NULL.
 */
Datum
gsi_drop(PG_FUNCTION_ARGS)
{
	Oid			base;
	List	   *indexes;
	Relation	rel;
	ListCell   *lc;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("table must not be NULL")));
	base = PG_GETARG_OID(0);

	rel = table_open(base, AccessExclusiveLock);
	indexes = gsi_indexes_for_rel(base);
	if (!PG_ARGISNULL(1))
	{
		char	   *col = NameStr(*PG_GETARG_NAME(1));
		GsiIndex   *idx = gsi_index_on_column(indexes, col);

		if (idx == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("column \"%s\" of table \"%s\" has no GSI",
							col, RelationGetRelationName(rel))));
		indexes = list_make1(idx);
	}
	else if (indexes == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("table \"%s\" has no GSI", RelationGetRelationName(rel))));

	foreach(lc, indexes)
		gsi_drop_index(lfirst(lc));

	/* cached plans of the table carry the dropped GSIs */
	CacheInvalidateRelcache(rel);
	table_close(rel, NoLock);
	PG_RETURN_VOID();
}

/* gsi_node_for(anyelement) returns the node id holding that key's entries */
Datum
gsi_node_for(PG_FUNCTION_ARGS)
{
	Oid			typid = get_fn_expr_argtype(fcinfo->flinfo, 0);
	int			nnodes;

	SPI_connect();
	(void) gsi_load_nodes(&nnodes, CurrentMemoryContext);
	SPI_finish();

	PG_RETURN_INT32(gsi_node_for_datum(PG_GETARG_DATUM(0), typid,
									   PG_GET_COLLATION(), nnodes));
}
