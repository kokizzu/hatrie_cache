# TT-G08 Parallel Replay Experiment

## Decision

Rejected and rolled back. The opt-in shared-trie worker-pool design preserved
the focused correctness tests, but it was materially slower and substantially
more memory-hungry than the existing serial recovery path. No `ReplayParallel`
API or production code remains.

## Workload

- 16,384 durable `SETSTR` journal entries.
- Every entry used an independent key.
- The serial path was the existing `CommandJournal.Replay` implementation.
- The attempted path grouped entries by key and used four workers against one
  destination `HatTrie`.
- Measurements used `go test ./hat/hatCache -run '^$' -bench '^BenchmarkTTG08(Serial|Parallel)Replay$' -benchmem -count=3` for the final comparison. The serial baseline was also run with `-count=5` before implementation.

## Raw Results

| Path | Samples (ns/op) | Median ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Existing serial replay | 19,690,571; 32,819,961; 22,993,483; 20,882,721; 20,440,002 | 20,882,721 | 5,653,477 | 81,966 |
| Rejected four-worker replay | 47,239,272; 48,798,333; 51,144,423 | 48,798,333 | 35,377,609 | 98,452 |

Relative to the serial median, the attempted path was `2.58x` slower, used
`6.26x` more measured bytes, and used `1.20x` as many allocations. The
additional plan slice, per-key groups, worker scheduling, and shared-trie
contention outweighed parallel execution.

## Follow-up Boundary

The idea remains in the gap catalog, but a future attempt must avoid a single
shared mutation structure. A viable design would need shard-local replay state,
an explicit deterministic merge, and a benchmark that proves recovery speedup
without turning the final merge into a larger cost than serial replay.
