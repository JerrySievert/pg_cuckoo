-- Comprehensive Benchmark: Cuckoo Filter vs Bloom Filter
-- Runs 10,000 iterations of each test for statistical reliability
-- Run with: psql -f benchmark/benchmark_full.sql
--
-- NOTE: This benchmark takes a significant amount of time to run.
-- For quick tests, use benchmark.sql instead.

\timing on

-- Clean up any existing objects
DROP TABLE IF EXISTS bench_data CASCADE;
DROP TABLE IF EXISTS bench_results CASCADE;
DROP EXTENSION IF EXISTS cuckoo CASCADE;
DROP EXTENSION IF EXISTS bloom CASCADE;

-- Load extensions
CREATE EXTENSION cuckoo;
CREATE EXTENSION bloom;

-- Table to store benchmark results (unlogged for performance)
CREATE UNLOGGED TABLE bench_results (
    id serial PRIMARY KEY,
    test_name text,
    index_type text,
    iteration int,
    duration_ms numeric,
    rows_returned bigint
);

-- Create test table
CREATE TABLE bench_data (
    id serial PRIMARY KEY,
    int_val int,
    text_val text,
    bigint_val bigint,
    float_val float8
);

\echo ''
\echo '================================================================================'
\echo '              COMPREHENSIVE BENCHMARK: CUCKOO vs BLOOM (10,000 iterations)'
\echo '================================================================================'
\echo ''

-- Insert test data (100K rows)
\echo '=== Phase 1: Data Setup ==='
\echo 'Inserting 100,000 rows...'
INSERT INTO bench_data (int_val, text_val, bigint_val, float_val)
SELECT
    (random() * 10000)::int,
    md5(random()::text),
    (random() * 1000000)::bigint,
    random() * 10000
FROM generate_series(1, 100000);

ANALYZE bench_data;

\echo 'Data insertion complete.'
\echo ''

-- ============================================================================
-- INDEX CREATION BENCHMARKS (100 iterations - index creation is slow)
-- ============================================================================

\echo '=== Phase 2: Index Creation Benchmarks (100 iterations each) ==='
\echo ''

-- Bloom index creation benchmark
\echo '--- Bloom Index Creation ---'
DO $$
DECLARE
    i int;
    start_ts timestamp;
    end_ts timestamp;
    duration_ms numeric;
BEGIN
    FOR i IN 1..100 LOOP
        DROP INDEX IF EXISTS bench_bloom_idx;
        start_ts := clock_timestamp();
        EXECUTE 'CREATE INDEX bench_bloom_idx ON bench_data USING bloom (int_val, text_val)';
        end_ts := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000;
        INSERT INTO bench_results (test_name, index_type, iteration, duration_ms)
        VALUES ('index_creation', 'bloom', i, duration_ms);
        IF i % 10 = 0 THEN
            RAISE NOTICE 'Bloom creation: % of 100 complete', i;
        END IF;
    END LOOP;
END $$;

\echo ''
\echo '--- Cuckoo Index Creation (int_val) ---'
DO $$
DECLARE
    i int;
    start_ts timestamp;
    end_ts timestamp;
    duration_ms numeric;
BEGIN
    FOR i IN 1..100 LOOP
        DROP INDEX IF EXISTS bench_cuckoo_int_idx;
        start_ts := clock_timestamp();
        EXECUTE 'CREATE INDEX bench_cuckoo_int_idx ON bench_data USING cuckoo (int_val)';
        end_ts := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000;
        INSERT INTO bench_results (test_name, index_type, iteration, duration_ms)
        VALUES ('index_creation_int', 'cuckoo', i, duration_ms);
        IF i % 10 = 0 THEN
            RAISE NOTICE 'Cuckoo int creation: % of 100 complete', i;
        END IF;
    END LOOP;
END $$;

\echo ''
\echo '--- Cuckoo Index Creation (text_val) ---'
DO $$
DECLARE
    i int;
    start_ts timestamp;
    end_ts timestamp;
    duration_ms numeric;
BEGIN
    FOR i IN 1..100 LOOP
        DROP INDEX IF EXISTS bench_cuckoo_text_idx;
        start_ts := clock_timestamp();
        EXECUTE 'CREATE INDEX bench_cuckoo_text_idx ON bench_data USING cuckoo (text_val)';
        end_ts := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000;
        INSERT INTO bench_results (test_name, index_type, iteration, duration_ms)
        VALUES ('index_creation_text', 'cuckoo', i, duration_ms);
        IF i % 10 = 0 THEN
            RAISE NOTICE 'Cuckoo text creation: % of 100 complete', i;
        END IF;
    END LOOP;
END $$;

-- ============================================================================
-- INDEX SIZE COMPARISON
-- ============================================================================

\echo ''
\echo '=== Phase 3: Index Size Comparison ==='
DROP INDEX IF EXISTS bench_bloom_idx;
DROP INDEX IF EXISTS bench_cuckoo_int_idx;
DROP INDEX IF EXISTS bench_cuckoo_text_idx;

CREATE INDEX bench_bloom_idx ON bench_data USING bloom (int_val, text_val);
CREATE INDEX bench_cuckoo_int_idx ON bench_data USING cuckoo (int_val);
CREATE INDEX bench_cuckoo_text_idx ON bench_data USING cuckoo (text_val);

SELECT
    indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)) as size,
    pg_relation_size(indexname::regclass) as size_bytes
FROM pg_indexes
WHERE tablename = 'bench_data' AND indexname LIKE 'bench_%'
ORDER BY indexname;

-- ============================================================================
-- QUERY PERFORMANCE BENCHMARKS (10,000 iterations)
-- ============================================================================

\echo ''
\echo '=== Phase 4: Query Performance Benchmarks (10,000 iterations each) ==='
SET enable_seqscan = off;

-- ----------------------------------------
-- Integer equality lookups
-- ----------------------------------------
\echo ''
\echo '--- Integer Equality Lookup (existing value: 5000) ---'

-- Bloom index lookup
\echo 'Testing Bloom (10,000 iterations)...'
DROP INDEX IF EXISTS bench_cuckoo_int_idx;
DROP INDEX IF EXISTS bench_cuckoo_text_idx;

DO $$
DECLARE
    i int;
    start_ts timestamp;
    end_ts timestamp;
    duration_ms numeric;
    cnt bigint;
BEGIN
    FOR i IN 1..10000 LOOP
        start_ts := clock_timestamp();
        EXECUTE 'SELECT count(*) FROM bench_data WHERE int_val = 5000' INTO cnt;
        end_ts := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000;
        INSERT INTO bench_results (test_name, index_type, iteration, duration_ms, rows_returned)
        VALUES ('int_equality_existing', 'bloom', i, duration_ms, cnt);
        IF i % 1000 = 0 THEN
            RAISE NOTICE 'Bloom int lookup: % of 10000 complete', i;
        END IF;
    END LOOP;
END $$;

-- Cuckoo index lookup
\echo 'Testing Cuckoo (10,000 iterations)...'
DROP INDEX IF EXISTS bench_bloom_idx;
CREATE INDEX bench_cuckoo_int_idx ON bench_data USING cuckoo (int_val);
CREATE INDEX bench_cuckoo_text_idx ON bench_data USING cuckoo (text_val);

DO $$
DECLARE
    i int;
    start_ts timestamp;
    end_ts timestamp;
    duration_ms numeric;
    cnt bigint;
BEGIN
    FOR i IN 1..10000 LOOP
        start_ts := clock_timestamp();
        EXECUTE 'SELECT count(*) FROM bench_data WHERE int_val = 5000' INTO cnt;
        end_ts := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000;
        INSERT INTO bench_results (test_name, index_type, iteration, duration_ms, rows_returned)
        VALUES ('int_equality_existing', 'cuckoo', i, duration_ms, cnt);
        IF i % 1000 = 0 THEN
            RAISE NOTICE 'Cuckoo int lookup: % of 10000 complete', i;
        END IF;
    END LOOP;
END $$;

-- ----------------------------------------
-- Integer lookup for non-existent value (false positive test)
-- ----------------------------------------
\echo ''
\echo '--- Integer Equality Lookup (non-existent value: -99999) ---'

-- Bloom index
\echo 'Testing Bloom (10,000 iterations)...'
DROP INDEX IF EXISTS bench_cuckoo_int_idx;
DROP INDEX IF EXISTS bench_cuckoo_text_idx;
CREATE INDEX bench_bloom_idx ON bench_data USING bloom (int_val, text_val);

DO $$
DECLARE
    i int;
    start_ts timestamp;
    end_ts timestamp;
    duration_ms numeric;
    cnt bigint;
BEGIN
    FOR i IN 1..10000 LOOP
        start_ts := clock_timestamp();
        EXECUTE 'SELECT count(*) FROM bench_data WHERE int_val = -99999' INTO cnt;
        end_ts := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000;
        INSERT INTO bench_results (test_name, index_type, iteration, duration_ms, rows_returned)
        VALUES ('int_equality_nonexistent', 'bloom', i, duration_ms, cnt);
        IF i % 1000 = 0 THEN
            RAISE NOTICE 'Bloom non-existent lookup: % of 10000 complete', i;
        END IF;
    END LOOP;
END $$;

-- Cuckoo index
\echo 'Testing Cuckoo (10,000 iterations)...'
DROP INDEX IF EXISTS bench_bloom_idx;
CREATE INDEX bench_cuckoo_int_idx ON bench_data USING cuckoo (int_val);
CREATE INDEX bench_cuckoo_text_idx ON bench_data USING cuckoo (text_val);

DO $$
DECLARE
    i int;
    start_ts timestamp;
    end_ts timestamp;
    duration_ms numeric;
    cnt bigint;
BEGIN
    FOR i IN 1..10000 LOOP
        start_ts := clock_timestamp();
        EXECUTE 'SELECT count(*) FROM bench_data WHERE int_val = -99999' INTO cnt;
        end_ts := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000;
        INSERT INTO bench_results (test_name, index_type, iteration, duration_ms, rows_returned)
        VALUES ('int_equality_nonexistent', 'cuckoo', i, duration_ms, cnt);
        IF i % 1000 = 0 THEN
            RAISE NOTICE 'Cuckoo non-existent lookup: % of 10000 complete', i;
        END IF;
    END LOOP;
END $$;

-- ----------------------------------------
-- Text equality lookups
-- ----------------------------------------
\echo ''
\echo '--- Text Equality Lookup (random existing value) ---'

-- Get a random existing text value for testing
DO $$
DECLARE
    test_text text;
BEGIN
    SELECT text_val INTO test_text FROM bench_data ORDER BY random() LIMIT 1;
    RAISE NOTICE 'Using test text value: %', test_text;
    CREATE TEMP TABLE IF NOT EXISTS test_params (key text PRIMARY KEY, value text);
    INSERT INTO test_params VALUES ('test_text', test_text) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
END $$;

-- Bloom index
\echo 'Testing Bloom (10,000 iterations)...'
DROP INDEX IF EXISTS bench_cuckoo_int_idx;
DROP INDEX IF EXISTS bench_cuckoo_text_idx;
CREATE INDEX bench_bloom_idx ON bench_data USING bloom (int_val, text_val);

DO $$
DECLARE
    i int;
    start_ts timestamp;
    end_ts timestamp;
    duration_ms numeric;
    cnt bigint;
    test_text text;
BEGIN
    SELECT value INTO test_text FROM test_params WHERE key = 'test_text';
    FOR i IN 1..10000 LOOP
        start_ts := clock_timestamp();
        EXECUTE format('SELECT count(*) FROM bench_data WHERE text_val = %L', test_text) INTO cnt;
        end_ts := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000;
        INSERT INTO bench_results (test_name, index_type, iteration, duration_ms, rows_returned)
        VALUES ('text_equality_existing', 'bloom', i, duration_ms, cnt);
        IF i % 1000 = 0 THEN
            RAISE NOTICE 'Bloom text lookup: % of 10000 complete', i;
        END IF;
    END LOOP;
END $$;

-- Cuckoo index
\echo 'Testing Cuckoo (10,000 iterations)...'
DROP INDEX IF EXISTS bench_bloom_idx;
CREATE INDEX bench_cuckoo_int_idx ON bench_data USING cuckoo (int_val);
CREATE INDEX bench_cuckoo_text_idx ON bench_data USING cuckoo (text_val);

DO $$
DECLARE
    i int;
    start_ts timestamp;
    end_ts timestamp;
    duration_ms numeric;
    cnt bigint;
    test_text text;
BEGIN
    SELECT value INTO test_text FROM test_params WHERE key = 'test_text';
    FOR i IN 1..10000 LOOP
        start_ts := clock_timestamp();
        EXECUTE format('SELECT count(*) FROM bench_data WHERE text_val = %L', test_text) INTO cnt;
        end_ts := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000;
        INSERT INTO bench_results (test_name, index_type, iteration, duration_ms, rows_returned)
        VALUES ('text_equality_existing', 'cuckoo', i, duration_ms, cnt);
        IF i % 1000 = 0 THEN
            RAISE NOTICE 'Cuckoo text lookup: % of 10000 complete', i;
        END IF;
    END LOOP;
END $$;

-- ----------------------------------------
-- Text lookup for non-existent value
-- ----------------------------------------
\echo ''
\echo '--- Text Equality Lookup (non-existent value) ---'

-- Bloom index
\echo 'Testing Bloom (10,000 iterations)...'
DROP INDEX IF EXISTS bench_cuckoo_int_idx;
DROP INDEX IF EXISTS bench_cuckoo_text_idx;
CREATE INDEX bench_bloom_idx ON bench_data USING bloom (int_val, text_val);

DO $$
DECLARE
    i int;
    start_ts timestamp;
    end_ts timestamp;
    duration_ms numeric;
    cnt bigint;
BEGIN
    FOR i IN 1..10000 LOOP
        start_ts := clock_timestamp();
        EXECUTE 'SELECT count(*) FROM bench_data WHERE text_val = ''nonexistent_value_xyz_123''' INTO cnt;
        end_ts := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000;
        INSERT INTO bench_results (test_name, index_type, iteration, duration_ms, rows_returned)
        VALUES ('text_equality_nonexistent', 'bloom', i, duration_ms, cnt);
        IF i % 1000 = 0 THEN
            RAISE NOTICE 'Bloom non-existent text: % of 10000 complete', i;
        END IF;
    END LOOP;
END $$;

-- Cuckoo index
\echo 'Testing Cuckoo (10,000 iterations)...'
DROP INDEX IF EXISTS bench_bloom_idx;
CREATE INDEX bench_cuckoo_int_idx ON bench_data USING cuckoo (int_val);
CREATE INDEX bench_cuckoo_text_idx ON bench_data USING cuckoo (text_val);

DO $$
DECLARE
    i int;
    start_ts timestamp;
    end_ts timestamp;
    duration_ms numeric;
    cnt bigint;
BEGIN
    FOR i IN 1..10000 LOOP
        start_ts := clock_timestamp();
        EXECUTE 'SELECT count(*) FROM bench_data WHERE text_val = ''nonexistent_value_xyz_123''' INTO cnt;
        end_ts := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000;
        INSERT INTO bench_results (test_name, index_type, iteration, duration_ms, rows_returned)
        VALUES ('text_equality_nonexistent', 'cuckoo', i, duration_ms, cnt);
        IF i % 1000 = 0 THEN
            RAISE NOTICE 'Cuckoo non-existent text: % of 10000 complete', i;
        END IF;
    END LOOP;
END $$;

-- ----------------------------------------
-- Random integer lookups (different value each time)
-- ----------------------------------------
\echo ''
\echo '--- Random Integer Lookups (10,000 different random values) ---'

-- Bloom index
\echo 'Testing Bloom (10,000 iterations)...'
DROP INDEX IF EXISTS bench_cuckoo_int_idx;
DROP INDEX IF EXISTS bench_cuckoo_text_idx;
CREATE INDEX bench_bloom_idx ON bench_data USING bloom (int_val, text_val);

DO $$
DECLARE
    i int;
    start_ts timestamp;
    end_ts timestamp;
    duration_ms numeric;
    cnt bigint;
    search_val int;
BEGIN
    FOR i IN 1..10000 LOOP
        search_val := (random() * 10000)::int;
        start_ts := clock_timestamp();
        EXECUTE format('SELECT count(*) FROM bench_data WHERE int_val = %s', search_val) INTO cnt;
        end_ts := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000;
        INSERT INTO bench_results (test_name, index_type, iteration, duration_ms, rows_returned)
        VALUES ('random_int_lookup', 'bloom', i, duration_ms, cnt);
        IF i % 1000 = 0 THEN
            RAISE NOTICE 'Bloom random lookup: % of 10000 complete', i;
        END IF;
    END LOOP;
END $$;

-- Cuckoo index
\echo 'Testing Cuckoo (10,000 iterations)...'
DROP INDEX IF EXISTS bench_bloom_idx;
CREATE INDEX bench_cuckoo_int_idx ON bench_data USING cuckoo (int_val);
CREATE INDEX bench_cuckoo_text_idx ON bench_data USING cuckoo (text_val);

DO $$
DECLARE
    i int;
    start_ts timestamp;
    end_ts timestamp;
    duration_ms numeric;
    cnt bigint;
    search_val int;
BEGIN
    FOR i IN 1..10000 LOOP
        search_val := (random() * 10000)::int;
        start_ts := clock_timestamp();
        EXECUTE format('SELECT count(*) FROM bench_data WHERE int_val = %s', search_val) INTO cnt;
        end_ts := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000;
        INSERT INTO bench_results (test_name, index_type, iteration, duration_ms, rows_returned)
        VALUES ('random_int_lookup', 'cuckoo', i, duration_ms, cnt);
        IF i % 1000 = 0 THEN
            RAISE NOTICE 'Cuckoo random lookup: % of 10000 complete', i;
        END IF;
    END LOOP;
END $$;

-- ============================================================================
-- CUCKOO BITS_PER_TAG COMPARISON (1,000 iterations each)
-- ============================================================================

\echo ''
\echo '=== Phase 5: Cuckoo bits_per_tag Settings Comparison (1,000 iterations each) ==='

-- Test bits_per_tag = 8
\echo ''
\echo '--- bits_per_tag = 8 (higher FPR, smaller index) ---'
DROP INDEX IF EXISTS bench_cuckoo_int_idx;
DROP INDEX IF EXISTS bench_cuckoo_text_idx;
DROP INDEX IF EXISTS bench_bloom_idx;

CREATE INDEX bench_cuckoo_int_idx ON bench_data USING cuckoo (int_val) WITH (bits_per_tag=8);
SELECT pg_size_pretty(pg_relation_size('bench_cuckoo_int_idx'::regclass)) as "Index Size (bits_per_tag=8)";

DO $$
DECLARE
    i int;
    start_ts timestamp;
    end_ts timestamp;
    duration_ms numeric;
    cnt bigint;
BEGIN
    FOR i IN 1..1000 LOOP
        start_ts := clock_timestamp();
        EXECUTE 'SELECT count(*) FROM bench_data WHERE int_val = 5000' INTO cnt;
        end_ts := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000;
        INSERT INTO bench_results (test_name, index_type, iteration, duration_ms, rows_returned)
        VALUES ('cuckoo_bits_8', 'cuckoo_8bit', i, duration_ms, cnt);
        IF i % 100 = 0 THEN
            RAISE NOTICE 'Cuckoo 8-bit: % of 1000 complete', i;
        END IF;
    END LOOP;
END $$;

-- Test bits_per_tag = 12 (default)
\echo ''
\echo '--- bits_per_tag = 12 (default) ---'
DROP INDEX bench_cuckoo_int_idx;
CREATE INDEX bench_cuckoo_int_idx ON bench_data USING cuckoo (int_val) WITH (bits_per_tag=12);
SELECT pg_size_pretty(pg_relation_size('bench_cuckoo_int_idx'::regclass)) as "Index Size (bits_per_tag=12)";

DO $$
DECLARE
    i int;
    start_ts timestamp;
    end_ts timestamp;
    duration_ms numeric;
    cnt bigint;
BEGIN
    FOR i IN 1..1000 LOOP
        start_ts := clock_timestamp();
        EXECUTE 'SELECT count(*) FROM bench_data WHERE int_val = 5000' INTO cnt;
        end_ts := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000;
        INSERT INTO bench_results (test_name, index_type, iteration, duration_ms, rows_returned)
        VALUES ('cuckoo_bits_12', 'cuckoo_12bit', i, duration_ms, cnt);
        IF i % 100 = 0 THEN
            RAISE NOTICE 'Cuckoo 12-bit: % of 1000 complete', i;
        END IF;
    END LOOP;
END $$;

-- Test bits_per_tag = 16
\echo ''
\echo '--- bits_per_tag = 16 (lower FPR, larger index) ---'
DROP INDEX bench_cuckoo_int_idx;
CREATE INDEX bench_cuckoo_int_idx ON bench_data USING cuckoo (int_val) WITH (bits_per_tag=16);
SELECT pg_size_pretty(pg_relation_size('bench_cuckoo_int_idx'::regclass)) as "Index Size (bits_per_tag=16)";

DO $$
DECLARE
    i int;
    start_ts timestamp;
    end_ts timestamp;
    duration_ms numeric;
    cnt bigint;
BEGIN
    FOR i IN 1..1000 LOOP
        start_ts := clock_timestamp();
        EXECUTE 'SELECT count(*) FROM bench_data WHERE int_val = 5000' INTO cnt;
        end_ts := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000;
        INSERT INTO bench_results (test_name, index_type, iteration, duration_ms, rows_returned)
        VALUES ('cuckoo_bits_16', 'cuckoo_16bit', i, duration_ms, cnt);
        IF i % 100 = 0 THEN
            RAISE NOTICE 'Cuckoo 16-bit: % of 1000 complete', i;
        END IF;
    END LOOP;
END $$;

-- Test bits_per_tag = 20
\echo ''
\echo '--- bits_per_tag = 20 (very low FPR, larger index) ---'
DROP INDEX bench_cuckoo_int_idx;
CREATE INDEX bench_cuckoo_int_idx ON bench_data USING cuckoo (int_val) WITH (bits_per_tag=20);
SELECT pg_size_pretty(pg_relation_size('bench_cuckoo_int_idx'::regclass)) as "Index Size (bits_per_tag=20)";

DO $$
DECLARE
    i int;
    start_ts timestamp;
    end_ts timestamp;
    duration_ms numeric;
    cnt bigint;
BEGIN
    FOR i IN 1..1000 LOOP
        start_ts := clock_timestamp();
        EXECUTE 'SELECT count(*) FROM bench_data WHERE int_val = 5000' INTO cnt;
        end_ts := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000;
        INSERT INTO bench_results (test_name, index_type, iteration, duration_ms, rows_returned)
        VALUES ('cuckoo_bits_20', 'cuckoo_20bit', i, duration_ms, cnt);
        IF i % 100 = 0 THEN
            RAISE NOTICE 'Cuckoo 20-bit: % of 1000 complete', i;
        END IF;
    END LOOP;
END $$;

-- ============================================================================
-- INSERT PERFORMANCE BENCHMARKS (1,000 iterations, 100 rows each)
-- ============================================================================

\echo ''
\echo '=== Phase 6: Insert Performance with Index (1,000 iterations, 100 rows each) ==='

-- Bloom insert performance
\echo ''
\echo '--- Bloom Insert Performance ---'
DROP INDEX IF EXISTS bench_cuckoo_int_idx;
DROP INDEX IF EXISTS bench_cuckoo_text_idx;
CREATE INDEX bench_bloom_idx ON bench_data USING bloom (int_val, text_val);

DO $$
DECLARE
    i int;
    start_ts timestamp;
    end_ts timestamp;
    duration_ms numeric;
BEGIN
    FOR i IN 1..1000 LOOP
        start_ts := clock_timestamp();
        INSERT INTO bench_data (int_val, text_val, bigint_val, float_val)
        SELECT
            (random() * 10000)::int,
            md5(random()::text),
            (random() * 1000000)::bigint,
            random() * 10000
        FROM generate_series(1, 100);
        end_ts := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000;
        INSERT INTO bench_results (test_name, index_type, iteration, duration_ms, rows_returned)
        VALUES ('insert_100_rows', 'bloom', i, duration_ms, 100);
        IF i % 100 = 0 THEN
            RAISE NOTICE 'Bloom insert: % of 1000 complete', i;
        END IF;
    END LOOP;
END $$;

-- Cuckoo insert performance
\echo ''
\echo '--- Cuckoo Insert Performance ---'
DROP INDEX IF EXISTS bench_bloom_idx;
CREATE INDEX bench_cuckoo_int_idx ON bench_data USING cuckoo (int_val);
CREATE INDEX bench_cuckoo_text_idx ON bench_data USING cuckoo (text_val);

DO $$
DECLARE
    i int;
    start_ts timestamp;
    end_ts timestamp;
    duration_ms numeric;
BEGIN
    FOR i IN 1..1000 LOOP
        start_ts := clock_timestamp();
        INSERT INTO bench_data (int_val, text_val, bigint_val, float_val)
        SELECT
            (random() * 10000)::int,
            md5(random()::text),
            (random() * 1000000)::bigint,
            random() * 10000
        FROM generate_series(1, 100);
        end_ts := clock_timestamp();
        duration_ms := EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000;
        INSERT INTO bench_results (test_name, index_type, iteration, duration_ms, rows_returned)
        VALUES ('insert_100_rows', 'cuckoo', i, duration_ms, 100);
        IF i % 100 = 0 THEN
            RAISE NOTICE 'Cuckoo insert: % of 1000 complete', i;
        END IF;
    END LOOP;
END $$;

RESET enable_seqscan;

-- ============================================================================
-- RESULTS SUMMARY
-- ============================================================================

\echo ''
\echo '================================================================================'
\echo '                           BENCHMARK RESULTS SUMMARY'
\echo '================================================================================'
\echo ''

\echo '=== Statistical Summary by Test and Index Type ==='
SELECT
    test_name,
    index_type,
    count(*) as iterations,
    round(avg(duration_ms)::numeric, 4) as avg_ms,
    round(min(duration_ms)::numeric, 4) as min_ms,
    round(max(duration_ms)::numeric, 4) as max_ms,
    round(stddev(duration_ms)::numeric, 4) as stddev_ms,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY duration_ms)::numeric, 4) as median_ms,
    round(percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)::numeric, 4) as p95_ms,
    round(percentile_cont(0.99) WITHIN GROUP (ORDER BY duration_ms)::numeric, 4) as p99_ms,
    round(avg(rows_returned)::numeric, 1) as avg_rows
FROM bench_results
GROUP BY test_name, index_type
ORDER BY test_name, index_type;

\echo ''
\echo '=== Performance Comparison: Cuckoo vs Bloom ==='
WITH comparisons AS (
    SELECT
        test_name,
        index_type,
        avg(duration_ms) as avg_ms,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY duration_ms) as median_ms
    FROM bench_results
    WHERE index_type IN ('bloom', 'cuckoo')
    GROUP BY test_name, index_type
)
SELECT
    c.test_name,
    round(b.avg_ms::numeric, 4) as bloom_avg_ms,
    round(c.avg_ms::numeric, 4) as cuckoo_avg_ms,
    round(b.median_ms::numeric, 4) as bloom_median_ms,
    round(c.median_ms::numeric, 4) as cuckoo_median_ms,
    round(((b.avg_ms - c.avg_ms) / NULLIF(b.avg_ms, 0) * 100)::numeric, 2) as "avg_improvement_%",
    round(((b.median_ms - c.median_ms) / NULLIF(b.median_ms, 0) * 100)::numeric, 2) as "median_improvement_%",
    CASE
        WHEN c.avg_ms < b.avg_ms THEN 'CUCKOO'
        WHEN c.avg_ms > b.avg_ms THEN 'BLOOM'
        ELSE 'TIE'
    END as winner
FROM comparisons c
JOIN comparisons b ON c.test_name = b.test_name AND b.index_type = 'bloom'
WHERE c.index_type = 'cuckoo'
ORDER BY c.test_name;

\echo ''
\echo '=== Cuckoo bits_per_tag Comparison ==='
SELECT
    test_name,
    index_type,
    count(*) as iterations,
    round(avg(duration_ms)::numeric, 4) as avg_ms,
    round(stddev(duration_ms)::numeric, 4) as stddev_ms,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY duration_ms)::numeric, 4) as median_ms,
    round(percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)::numeric, 4) as p95_ms,
    round(avg(rows_returned)::numeric, 1) as avg_rows
FROM bench_results
WHERE test_name LIKE 'cuckoo_bits_%'
GROUP BY test_name, index_type
ORDER BY test_name;

\echo ''
\echo '=== Histogram of Query Times (int_equality_existing) ==='
WITH buckets AS (
    SELECT
        index_type,
        width_bucket(duration_ms, 0, 10, 20) as bucket,
        count(*) as cnt
    FROM bench_results
    WHERE test_name = 'int_equality_existing'
    GROUP BY index_type, bucket
)
SELECT
    index_type,
    bucket,
    round((bucket * 0.5)::numeric, 1) || '-' || round(((bucket + 1) * 0.5)::numeric, 1) || ' ms' as range,
    cnt,
    repeat('*', (cnt / 50)::int) as histogram
FROM buckets
WHERE bucket BETWEEN 1 AND 20
ORDER BY index_type, bucket;

-- ============================================================================
-- CLEANUP
-- ============================================================================

\echo ''
\echo '=== Cleanup ==='
DROP TABLE IF EXISTS test_params;
DROP TABLE bench_data CASCADE;
DROP TABLE bench_results;
DROP EXTENSION cuckoo;
DROP EXTENSION bloom;

\echo ''
\echo '================================================================================'
\echo '                         BENCHMARK COMPLETE'
\echo '================================================================================'
