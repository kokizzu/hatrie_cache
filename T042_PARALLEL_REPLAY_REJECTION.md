# T042 Parallel Recovery Replay Rejection

Status: rejected and rolled back on 2026-09-22.

The Tarantool-inspired experiment added an opt-in `CommandJournal.ReplayParallel`
path for independent fast-path writes. It staged journal records, cloned request
values, and dispatched unique-key `SET`, `SETSTR`, `SETINT`, and `DEL` records to
workers. Journals with repeated keys, transactions, expirations, idempotency
tokens, or unsupported commands fell back to the existing serial replay.

## Raw measurements

Workload: 2,048 unique binary-journal `SETSTR` records, five benchmark samples,
Linux amd64 on an AMD Ryzen 9 5950X.

| Variant | ns/op samples | B/op | allocs/op |
|---|---:|---:|---:|
| Serial, partitioned trie | 2,338,753; 2,374,510; 2,386,985; 2,371,419; 2,537,952 | 678,707 | 10,262 |
| Parallel, partitioned trie | 4,355,872; 4,380,786; 4,476,425; 4,679,828; 4,560,363 | 2,946,367 | 10,352 |

Median comparison: parallel replay was `1.89x` slower (`4,476,425` vs
`2,374,510` ns/op), retained about `4.34x` the measured bytes per operation,
and used 90 more allocations per operation.

On the default non-partitioned trie, the first worker implementation was even
slower: roughly `5.6-6.1 ms/op` and `2.98 MB/op` versus `2.7-2.9 ms/op` and
`682 KB/op` for serial replay. A later guard made the opt-in method fall back
to serial when local partitions were absent, which removed that regression but
also removed the proposed speedup.

## Decision

The implementation was removed. Per-record goroutine fan-out does not amortize
request staging, channel scheduling, trie mutation coordination, or storage
write contention. The existing replay APIs and default behavior are unchanged.

A future attempt should parallelize partition-level restore batches or decode
work while committing each partition in larger sequential units. It should be
benchmarked against this report before any public API is added.
