# CH-028 Query `max_threads`

Hatrie SQL now accepts a bounded ClickHouse-style per-query worker setting:

```sql
FROM CACHE('events')
SELECT id, latency
WHERE latency >= 100
SETTINGS max_threads = 4
```

`max_threads` is applied to the existing `SQLQueryOptions.Workers` execution
path. It is available through both materialized and streamed SQL entry points.
The setting is an upper bound: an explicit Go `Workers` value below it wins,
and a larger explicit value is reduced to the SQL setting. An absent setting
keeps the existing sequential default (`Workers == 0`).

The accepted range is `1..256`. Zero, values above 256, duplicate `SETTINGS`
clauses, and unknown setting names are rejected before execution. The cap is a
fixed parser admission limit; it does not create a process-wide pool or start a
background worker. Session-wide defaults remain caller-owned through
`SQLQueryOptions` or a higher-level query manager.

## Benchmark

The benchmark uses an eight-row `VALUES` query on the development host and
reports five samples per case:

| Case | Median time | Memory | Allocations |
| --- | ---: | ---: | ---: |
| Existing query, no setting | 6287 ns/op | 10304 B/op | 62 allocs/op |
| `SETTINGS max_threads = 2` | 18721 ns/op | 11400 B/op | 73 allocs/op |

The opt-in setting is intentionally slower for tiny work because worker
coordination dominates. The feature is for larger CPU-bound operators where a
caller has measured useful parallelism; it does not change the default path.
Raw samples are recorded in [BENCHMARK.md](BENCHMARK.md#ch-028-query-max_threads).

Run focused verification with:

```sh
make test-ch028-max-threads
make benchmark-ch028-max-threads
make race-ch028-max-threads
make vet-ch028-max-threads
```
