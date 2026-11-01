/* src/gsi_fdw/gsi_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gsi_fdw" to load this file. \quit

-- postgres_fdw 1.1 objects, renamed

CREATE FUNCTION gsi_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME', 'postgres_fdw_handler'
LANGUAGE C STRICT;

CREATE FUNCTION gsi_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME', 'postgres_fdw_validator'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER gsi_fdw
  HANDLER gsi_fdw_handler
  VALIDATOR gsi_fdw_validator;

CREATE FUNCTION gsi_fdw_get_connections (OUT server_name text,
    OUT valid boolean)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'postgres_fdw_get_connections'
LANGUAGE C STRICT PARALLEL RESTRICTED;

CREATE FUNCTION gsi_fdw_disconnect (text)
RETURNS bool
AS 'MODULE_PATHNAME', 'postgres_fdw_disconnect'
LANGUAGE C STRICT PARALLEL RESTRICTED;

CREATE FUNCTION gsi_fdw_disconnect_all ()
RETURNS bool
AS 'MODULE_PATHNAME', 'postgres_fdw_disconnect_all'
LANGUAGE C STRICT PARALLEL RESTRICTED;
