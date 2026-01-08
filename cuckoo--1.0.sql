/* cuckoo--1.0.sql */

-- Complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION cuckoo" to load this file. \quit

-- Handler function for the cuckoo access method
CREATE FUNCTION ckhandler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Create the access method
CREATE ACCESS METHOD cuckoo TYPE INDEX HANDLER ckhandler;
COMMENT ON ACCESS METHOD cuckoo IS 'cuckoo filter index access method';

-- =============================================================================
-- Integer types
-- =============================================================================

-- Operator class for int2
CREATE OPERATOR CLASS int2_ops
DEFAULT FOR TYPE int2 USING cuckoo AS
    OPERATOR    1   =(int2, int2),
    FUNCTION    1   hashint2(int2);

-- Operator class for int4
CREATE OPERATOR CLASS int4_ops
DEFAULT FOR TYPE int4 USING cuckoo AS
    OPERATOR    1   =(int4, int4),
    FUNCTION    1   hashint4(int4);

-- Operator class for int8
CREATE OPERATOR CLASS int8_ops
DEFAULT FOR TYPE int8 USING cuckoo AS
    OPERATOR    1   =(int8, int8),
    FUNCTION    1   hashint8(int8);

-- Operator class for oid
CREATE OPERATOR CLASS oid_ops
DEFAULT FOR TYPE oid USING cuckoo AS
    OPERATOR    1   =(oid, oid),
    FUNCTION    1   hashoid(oid);

-- =============================================================================
-- Floating point types
-- =============================================================================

-- Operator class for float4
CREATE OPERATOR CLASS float4_ops
DEFAULT FOR TYPE float4 USING cuckoo AS
    OPERATOR    1   =(float4, float4),
    FUNCTION    1   hashfloat4(float4);

-- Operator class for float8
CREATE OPERATOR CLASS float8_ops
DEFAULT FOR TYPE float8 USING cuckoo AS
    OPERATOR    1   =(float8, float8),
    FUNCTION    1   hashfloat8(float8);

-- Operator class for numeric
CREATE OPERATOR CLASS numeric_ops
DEFAULT FOR TYPE numeric USING cuckoo AS
    OPERATOR    1   =(numeric, numeric),
    FUNCTION    1   hash_numeric(numeric);

-- =============================================================================
-- String types
-- =============================================================================

-- Operator class for text
CREATE OPERATOR CLASS text_ops
DEFAULT FOR TYPE text USING cuckoo AS
    OPERATOR    1   =(text, text),
    FUNCTION    1   hashtext(text);

-- Operator class for name
CREATE OPERATOR CLASS name_ops
DEFAULT FOR TYPE name USING cuckoo AS
    OPERATOR    1   =(name, name),
    FUNCTION    1   hashname(name);

-- Operator class for "char" (internal single-byte char type)
CREATE OPERATOR CLASS char_ops
DEFAULT FOR TYPE "char" USING cuckoo AS
    OPERATOR    1   =("char", "char"),
    FUNCTION    1   hashchar("char");

-- Operator class for bpchar (fixed-length character)
CREATE OPERATOR CLASS bpchar_ops
DEFAULT FOR TYPE bpchar USING cuckoo AS
    OPERATOR    1   =(bpchar, bpchar),
    FUNCTION    1   hashbpchar(bpchar);

-- =============================================================================
-- Date/Time types
-- =============================================================================

-- Operator class for timestamp (without time zone)
CREATE OPERATOR CLASS timestamp_ops
DEFAULT FOR TYPE timestamp USING cuckoo AS
    OPERATOR    1   =(timestamp, timestamp),
    FUNCTION    1   timestamp_hash(timestamp);

-- Operator class for time (without time zone)
CREATE OPERATOR CLASS time_ops
DEFAULT FOR TYPE time USING cuckoo AS
    OPERATOR    1   =(time, time),
    FUNCTION    1   time_hash(time);

-- Operator class for timetz (with time zone)
CREATE OPERATOR CLASS timetz_ops
DEFAULT FOR TYPE timetz USING cuckoo AS
    OPERATOR    1   =(timetz, timetz),
    FUNCTION    1   timetz_hash(timetz);

-- Operator class for interval
CREATE OPERATOR CLASS interval_ops
DEFAULT FOR TYPE interval USING cuckoo AS
    OPERATOR    1   =(interval, interval),
    FUNCTION    1   interval_hash(interval);

-- =============================================================================
-- Network types
-- =============================================================================

-- Operator class for inet
CREATE OPERATOR CLASS inet_ops
DEFAULT FOR TYPE inet USING cuckoo AS
    OPERATOR    1   =(inet, inet),
    FUNCTION    1   hashinet(inet);

-- Operator class for macaddr
CREATE OPERATOR CLASS macaddr_ops
DEFAULT FOR TYPE macaddr USING cuckoo AS
    OPERATOR    1   =(macaddr, macaddr),
    FUNCTION    1   hashmacaddr(macaddr);

-- Operator class for macaddr8
CREATE OPERATOR CLASS macaddr8_ops
DEFAULT FOR TYPE macaddr8 USING cuckoo AS
    OPERATOR    1   =(macaddr8, macaddr8),
    FUNCTION    1   hashmacaddr8(macaddr8);

-- =============================================================================
-- Other types
-- =============================================================================

-- Operator class for uuid
CREATE OPERATOR CLASS uuid_ops
DEFAULT FOR TYPE uuid USING cuckoo AS
    OPERATOR    1   =(uuid, uuid),
    FUNCTION    1   uuid_hash(uuid);

-- Operator class for jsonb
CREATE OPERATOR CLASS jsonb_ops
DEFAULT FOR TYPE jsonb USING cuckoo AS
    OPERATOR    1   =(jsonb, jsonb),
    FUNCTION    1   jsonb_hash(jsonb);

-- Operator class for pg_lsn
CREATE OPERATOR CLASS pg_lsn_ops
DEFAULT FOR TYPE pg_lsn USING cuckoo AS
    OPERATOR    1   =(pg_lsn, pg_lsn),
    FUNCTION    1   pg_lsn_hash(pg_lsn);

-- Operator class for tid
CREATE OPERATOR CLASS tid_ops
DEFAULT FOR TYPE tid USING cuckoo AS
    OPERATOR    1   =(tid, tid),
    FUNCTION    1   hashtid(tid);

-- Operator class for oidvector
CREATE OPERATOR CLASS oidvector_ops
DEFAULT FOR TYPE oidvector USING cuckoo AS
    OPERATOR    1   =(oidvector, oidvector),
    FUNCTION    1   hashoidvector(oidvector);
