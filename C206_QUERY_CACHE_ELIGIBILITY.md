# C206 Query Cache Eligibility

Status: verified as already implemented.

ClickHouse documents query-cache admission as excluding nondeterministic
functions such as `now()` and `today()` by default. Hatrie Cache follows the
same safety rule for its result-cache paths. See the official ClickHouse
discussion in [Use the ClickHouse query cache for repeated dashboard
queries](https://clickhouse.com/resources/engineering/high-concurrency-sizing-user-analytics).

## Current behavior

- Raw SQL result-cache keys reject lexed `CURRENT_*`, `NOW`, `RAND`,
  `RANDOM`, `UUID`, and `GENERATE_UUID` identifiers.
- Parsed queries record `cacheVolatile` during token-key construction.
- Parsed-query result-cache keys refuse volatile queries before encoding a
  cache key.
- Unknown or user-defined SQL functions are conservatively ineligible for
  result caching.
- Deterministic queries retain the normal result-cache path.

The token guard is deliberately conservative. A name that looks volatile in
an identifier position can cause a cache bypass, which is safer than serving
a stale result.

## Verification

The focused tests cover volatile top-level admission, a counter-backed
nondeterministic UDF, the deterministic cache-hit control, and all currently
listed volatile token names:

```text
make test-c206
make race-c206
make vet-c206
```

All passed. No production code change was needed for this backlog item.

## Admission benchmark

Five benchmark samples were run with `make benchmark-c206` on Linux/amd64.
The table reports the median sample.

| Admission path | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Deterministic key construction | 3,351 | 2,221 | 26 |
| Volatile key rejection | 1,710 | 2,704 | 4 |

Volatile admission is about `1.96x` faster and uses `6.5x` fewer allocations
than constructing a deterministic key. Its byte count is `1.22x` higher in
this microbenchmark because the volatile query string is longer and lexer
work happens before rejection. This measures admission only, not query
execution or end-to-end latency, and is a baseline for the existing behavior,
not a before/after improvement claim.
