# pg_cuckoo

A PostgreSQL extension that provides a cuckoo filter-based index access method. Similar to the built-in bloom filter extension, but with better space efficiency and lower false positive rates.

## Overview

Cuckoo filters are probabilistic data structures that support fast set membership testing with a configurable false positive rate. Compared to bloom filters, cuckoo filters offer:

- **Better space efficiency**: ~7.2 bits per item vs ~10 bits for bloom at 1% FPR
- **Lower false positive rates**: Typically 5-10x fewer false positives with default settings
- **Faster lookups**: O(1) lookups vs O(k) for bloom filters
- **Support for deletion**: Unlike bloom filters, items can be removed

## Requirements

- PostgreSQL 16 or later
- C++17 compatible compiler

## Installation

```bash
make
make install
```

## Usage

```sql
-- Load the extension
CREATE EXTENSION cuckoo;

-- Create a table
CREATE TABLE users (
    id serial PRIMARY KEY,
    email text,
    status int
);

-- Create cuckoo indexes
CREATE INDEX idx_email ON users USING cuckoo (email);
CREATE INDEX idx_status ON users USING cuckoo (status);

-- Query using the index
SET enable_seqscan = off;
SELECT * FROM users WHERE email = 'user@example.com';
```

## Index Options

The cuckoo index supports several tuning parameters:

| Option            | Default | Range   | Description                                  |
| ----------------- | ------- | ------- | -------------------------------------------- |
| `bits_per_tag`    | 12      | 4-32    | Bits per fingerprint tag. Higher = lower FPR |
| `tags_per_bucket` | 4       | 2-8     | Tags per bucket. Affects space efficiency    |
| `max_kicks`       | 500     | 50-2000 | Max relocations during insert                |

### Example with custom options

```sql
-- Lower false positive rate (more bits per tag)
CREATE INDEX idx_precise ON users USING cuckoo (email)
    WITH (bits_per_tag = 16);

-- Higher false positive rate but faster (fewer bits)
CREATE INDEX idx_fast ON users USING cuckoo (status)
    WITH (bits_per_tag = 8);

-- Fine-tuned settings
CREATE INDEX idx_custom ON users USING cuckoo (email)
    WITH (bits_per_tag = 14, tags_per_bucket = 4, max_kicks = 1000);
```

## False Positive Rate

The theoretical false positive rate is approximately:

```
FPR = (2 * tags_per_bucket) / 2^bits_per_tag
```

| bits_per_tag | tags_per_bucket | False Positive Rate |
| ------------ | --------------- | ------------------- |
| 8            | 4               | ~3.1%               |
| 12           | 4               | ~0.2%               |
| 16           | 4               | ~0.01%              |
| 20           | 4               | ~0.0008%            |

## Supported Data Types

pg_cuckoo provides operator classes for 23 data types:

**Integer types**: int2, int4, int8, oid

**Float types**: float4, float8, numeric

**String types**: text, name, char, bpchar

**Date/Time types**: timestamp, time, timetz, interval

**Network types**: inet, macaddr, macaddr8

**Other types**: uuid, jsonb, pg_lsn, tid, oidvector

## Benchmark Results

Compared to the bloom filter extension on 100,000 rows:

| Metric                     | Bloom   | Cuckoo  | Improvement |
| -------------------------- | ------- | ------- | ----------- |
| Index size (single column) | 1584 KB | 1192 KB | 25% smaller |
| False positives            | ~200    | ~25     | 8x fewer    |
| Query time                 | ~0.4ms  | ~0.15ms | 2.5x faster |

Run your own benchmarks:

```bash
psql -d your_database -f benchmark/benchmark.sql
```

## Limitations

1. **Single-column queries work best**: Unlike bloom, cuckoo indexes combine all columns into a single fingerprint. Queries must specify all indexed columns or use single-column indexes.

2. **Equality only**: Only supports `=` operator, not range queries.

3. **False positives**: Like all probabilistic indexes, may return extra rows that must be rechecked against the heap.

## Development

```bash
# Build
make

# Install
make install

# Run tests
make installcheck

# Clean
make clean
```

## License

PostgreSQL License. See [LICENSE.md](LICENSE.md) for details.

## References

- [Cuckoo Filter: Practically Better Than Bloom](https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf) - Original paper by Fan et al.
- [PostgreSQL Bloom Filter Extension](https://www.postgresql.org/docs/current/bloom.html) - Inspiration for this project
