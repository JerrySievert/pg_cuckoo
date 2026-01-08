-- Benchmark comparing cuckoo filter index vs bloom filter index
-- Run with: psql -f benchmark/benchmark.sql

\timing on

-- Clean up any existing objects
DROP TABLE IF EXISTS bench_data CASCADE;
DROP EXTENSION IF EXISTS cuckoo CASCADE;
DROP EXTENSION IF EXISTS bloom CASCADE;

-- Load extensions
CREATE EXTENSION cuckoo;
CREATE EXTENSION bloom;

-- Create test table
CREATE TABLE bench_data (
    id serial PRIMARY KEY,
    int_val int,
    text_val text
);

-- Insert test data (100K rows)
\echo '=== Inserting 100,000 rows ==='
INSERT INTO bench_data (int_val, text_val)
SELECT
    (random() * 10000)::int,
    md5(random()::text)
FROM generate_series(1, 100000);

ANALYZE bench_data;

\echo ''
\echo '=== Index Creation Time ==='

-- Create bloom index
\echo 'Creating bloom index...'
DROP INDEX IF EXISTS bench_bloom_idx;
CREATE INDEX bench_bloom_idx ON bench_data USING bloom (int_val, text_val);

-- Create cuckoo indexes (one per column since cuckoo uses fingerprints)
\echo 'Creating cuckoo indexes...'
DROP INDEX IF EXISTS bench_cuckoo_int_idx;
DROP INDEX IF EXISTS bench_cuckoo_text_idx;
CREATE INDEX bench_cuckoo_int_idx ON bench_data USING cuckoo (int_val);
CREATE INDEX bench_cuckoo_text_idx ON bench_data USING cuckoo (text_val);

\echo ''
\echo '=== Index Size Comparison ==='
SELECT
    indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)) as size,
    pg_relation_size(indexname::regclass) as size_bytes
FROM pg_indexes
WHERE tablename = 'bench_data' AND indexname LIKE 'bench_%'
ORDER BY indexname;

\echo ''
\echo '=== Query Performance (disable seqscan) ==='
SET enable_seqscan = off;

-- Test integer lookups
\echo ''
\echo '--- Integer equality lookup (5 iterations) ---'

\echo 'Bloom index:'
SET enable_indexscan = off;
DROP INDEX IF EXISTS bench_cuckoo_int_idx;
DROP INDEX IF EXISTS bench_cuckoo_text_idx;
CREATE INDEX bench_cuckoo_int_idx ON bench_data USING cuckoo (int_val);
CREATE INDEX bench_cuckoo_text_idx ON bench_data USING cuckoo (text_val);
-- Force bloom by temporarily dropping cuckoo indexes
DROP INDEX bench_cuckoo_int_idx;
DROP INDEX bench_cuckoo_text_idx;
SET enable_indexscan = on;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT count(*) FROM bench_data WHERE int_val = 5000;
SELECT count(*) FROM bench_data WHERE int_val = 5000;
SELECT count(*) FROM bench_data WHERE int_val = 5000;
SELECT count(*) FROM bench_data WHERE int_val = 5000;
SELECT count(*) FROM bench_data WHERE int_val = 5000;
SELECT count(*) FROM bench_data WHERE int_val = 5000;

\echo ''
\echo 'Cuckoo index:'
-- Recreate cuckoo indexes and drop bloom
DROP INDEX bench_bloom_idx;
CREATE INDEX bench_cuckoo_int_idx ON bench_data USING cuckoo (int_val);
CREATE INDEX bench_cuckoo_text_idx ON bench_data USING cuckoo (text_val);
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT count(*) FROM bench_data WHERE int_val = 5000;
SELECT count(*) FROM bench_data WHERE int_val = 5000;
SELECT count(*) FROM bench_data WHERE int_val = 5000;
SELECT count(*) FROM bench_data WHERE int_val = 5000;
SELECT count(*) FROM bench_data WHERE int_val = 5000;
SELECT count(*) FROM bench_data WHERE int_val = 5000;

-- Recreate bloom for text test
CREATE INDEX bench_bloom_idx ON bench_data USING bloom (int_val, text_val);

\echo ''
\echo '--- Text equality lookup (5 iterations) ---'

\echo 'Bloom index:'
DROP INDEX bench_cuckoo_int_idx;
DROP INDEX bench_cuckoo_text_idx;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT count(*) FROM bench_data WHERE text_val = 'abc123';
SELECT count(*) FROM bench_data WHERE text_val = 'abc123';
SELECT count(*) FROM bench_data WHERE text_val = 'abc123';
SELECT count(*) FROM bench_data WHERE text_val = 'abc123';
SELECT count(*) FROM bench_data WHERE text_val = 'abc123';
SELECT count(*) FROM bench_data WHERE text_val = 'abc123';

\echo ''
\echo 'Cuckoo index:'
DROP INDEX bench_bloom_idx;
CREATE INDEX bench_cuckoo_int_idx ON bench_data USING cuckoo (int_val);
CREATE INDEX bench_cuckoo_text_idx ON bench_data USING cuckoo (text_val);
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT count(*) FROM bench_data WHERE text_val = 'abc123';
SELECT count(*) FROM bench_data WHERE text_val = 'abc123';
SELECT count(*) FROM bench_data WHERE text_val = 'abc123';
SELECT count(*) FROM bench_data WHERE text_val = 'abc123';
SELECT count(*) FROM bench_data WHERE text_val = 'abc123';
SELECT count(*) FROM bench_data WHERE text_val = 'abc123';

\echo ''
\echo '=== False Positive Rate Test ==='
-- Recreate both indexes
CREATE INDEX bench_bloom_idx ON bench_data USING bloom (int_val, text_val);

-- Count false positives for values we know don't exist
\echo 'Testing for false positives with non-existent value -99999...'

\echo 'Bloom false positives:'
DROP INDEX bench_cuckoo_int_idx;
DROP INDEX bench_cuckoo_text_idx;
EXPLAIN (ANALYZE, COSTS OFF, TIMING ON, SUMMARY OFF) SELECT count(*) FROM bench_data WHERE int_val = -99999;

\echo ''
\echo 'Cuckoo false positives:'
DROP INDEX bench_bloom_idx;
CREATE INDEX bench_cuckoo_int_idx ON bench_data USING cuckoo (int_val);
EXPLAIN (ANALYZE, COSTS OFF, TIMING ON, SUMMARY OFF) SELECT count(*) FROM bench_data WHERE int_val = -99999;

\echo ''
\echo '=== Cuckoo with Different bits_per_tag Settings ==='
DROP INDEX IF EXISTS bench_cuckoo_int_idx;

\echo 'bits_per_tag=8 (higher FPR, smaller index):'
CREATE INDEX bench_cuckoo_int_idx ON bench_data USING cuckoo (int_val) WITH (bits_per_tag=8);
SELECT pg_size_pretty(pg_relation_size('bench_cuckoo_int_idx'::regclass)) as size;
EXPLAIN (ANALYZE, COSTS OFF, TIMING ON, SUMMARY OFF) SELECT count(*) FROM bench_data WHERE int_val = 5000;
DROP INDEX bench_cuckoo_int_idx;

\echo ''
\echo 'bits_per_tag=12 (default):'
CREATE INDEX bench_cuckoo_int_idx ON bench_data USING cuckoo (int_val) WITH (bits_per_tag=12);
SELECT pg_size_pretty(pg_relation_size('bench_cuckoo_int_idx'::regclass)) as size;
EXPLAIN (ANALYZE, COSTS OFF, TIMING ON, SUMMARY OFF) SELECT count(*) FROM bench_data WHERE int_val = 5000;
DROP INDEX bench_cuckoo_int_idx;

\echo ''
\echo 'bits_per_tag=16 (lower FPR, larger index):'
CREATE INDEX bench_cuckoo_int_idx ON bench_data USING cuckoo (int_val) WITH (bits_per_tag=16);
SELECT pg_size_pretty(pg_relation_size('bench_cuckoo_int_idx'::regclass)) as size;
EXPLAIN (ANALYZE, COSTS OFF, TIMING ON, SUMMARY OFF) SELECT count(*) FROM bench_data WHERE int_val = 5000;

RESET enable_seqscan;

\echo ''
\echo '=== Cleanup ==='
DROP TABLE bench_data CASCADE;
DROP EXTENSION cuckoo;
DROP EXTENSION bloom;

\echo 'Benchmark complete!'
