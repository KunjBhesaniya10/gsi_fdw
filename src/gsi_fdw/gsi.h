/*-------------------------------------------------------------------------
 *
 * gsi.h
 *		  Global secondary index (GSI) maintenance for gsi_fdw
 *
 * A GSI on column c of a hash-partitioned table stores one entry
 * (key, id, shard) per row in table gsi_<index> on node hash(key) mod N.
 *
 *-------------------------------------------------------------------------
 */
#ifndef GSI_H
#define GSI_H

#include "libpq-fe.h"
#include "foreign/foreign.h"
#include "nodes/execnodes.h"
#include "nodes/pathnodes.h"
#include "utils/relcache.h"

typedef struct GsiIndex
{
	char	   *name;			/* e.g. users_email; one GSI per column */
	char	   *remote_table;	/* quoted public.gsi_<name> */
	char	   *column;			/* indexed column name */
	Oid			typid;
	Oid			collid;
	bool		is_unique;
	bool		is_ready;		/* status 'ready': the planner may use it */
	int			nnodes;
	Oid		   *servers;		/* foreign server of each node id */
} GsiIndex;

/* gsi.c */
extern void gsi_init(void);
extern Oid *gsi_load_nodes(int *nnodes, MemoryContext cxt);
extern List *gsi_indexes_for_rel(Oid relid);
extern int	gsi_node_for_datum(Datum value, Oid typid, Oid collid, int nnodes);
extern int	gsi_server_node(GsiIndex *idx, Oid serverid);
extern GsiIndex *gsi_index_on_column(List *indexes, const char *col);
extern void gsi_exec_on_all_nodes(GsiIndex *idx, const char *sql);
extern void gsi_drop_index(GsiIndex *idx);

#endif							/* GSI_H */
