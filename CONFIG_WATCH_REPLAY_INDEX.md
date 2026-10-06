# Config Watch Replay Index

`hatTopology.ConfigWatchLog` retains a bounded chronological ring of versioned
configuration events. Reconnecting readers commonly resume near the newest
version. The old read path walked the entire retained ring to find that
cursor, even though `Publish` already guarantees strictly increasing versions.

The replay path now performs a binary search over the logical ring order and
copies only the remaining events requested by the caller. Physical wraparound,
history-gap detection, authorization, event cloning, and cursor semantics are
unchanged.

This is inspired by the ordered version cursors used by Tarantool-style
configuration/change streams and Materialize-style replay frontiers. It is
enabled unconditionally because it retains the existing bounded memory model,
adds no per-event index, and keeps the same allocation count for returned
events.

## Tradeoff

The seek cost changes from O(N) to O(log N) for a retained history of N
events. A read that requests events from the beginning still pays the normal
event-copy cost. The result slice capacity is also limited to the number of
events after the cursor, avoiding unused capacity for near-tail reads.

Measured with a 1,024-event retained ring:

| Resume point | Before | After | Improvement | Memory |
| --- | ---: | ---: | ---: | ---: |
| Near tail | 2,335 ns/op | 104.2 ns/op | 22.4x | 88 B/op, 2 allocs/op |
| Middle | 1,193 ns/op | 102.4 ns/op | 11.7x | 88 B/op, 2 allocs/op |

Raw five-sample output is recorded in [BENCHMARK.md](BENCHMARK.md#config-watch-replay-index).

## Verification

```text
make test-config-watch-index
make verify-config-watch-index
make benchmark-config-watch-index
```

The focused test covers wrapped physical storage, logical ordering, and
cursor boundaries. Verification also runs the complete `hatTopology` package,
config-watch race tests, and `go vet`.
