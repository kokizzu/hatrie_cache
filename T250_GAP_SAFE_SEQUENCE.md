# Gap-Safe Durable Sequence Allocation

Tarantool-style sequence allocation is available as the opt-in
`hatDataStructure.DurableSequence` primitive. It is useful for mutation IDs,
cursor identities, and other values that must not be published in memory before
their durable current value has been accepted by the caller's storage layer.

```go
sequence, err := hatDataStructure.NewDurableSequence(lastPersisted, func(next uint64) error {
	return storeCurrentSequenceAtomically(next)
})
if err != nil {
	return err
}

id, err := sequence.Next()
```

`Next` calls the persistence callback while holding the sequence lock. The
current value advances only after the callback returns `nil`; a failed write
leaves the same value available for retry. The callback must atomically publish
the value and must not call back into the sequence. On restart, load the last
accepted value into `NewDurableSequence`.

The zero/default behavior of existing APIs is unchanged. The sequence rejects
missing persistence callbacks and `uint64` overflow before invoking storage.

## Measurement

Five clean-worktree benchmark samples on AMD Ryzen 9 5950X, Go amd64:

| Operation | Median | Heap | Allocs | Tradeoff |
| --- | ---: | ---: | ---: | --- |
| Atomic increment baseline | 1.964 ns/op | 0 B/op | 0 | Existing in-memory control |
| `DurableSequence.Next` with successful no-op persistence callback | 8.304 ns/op | 0 B/op | 0 | 4.23x slower than atomic control for serialized durability |
| `DurableSequence.Current` | 4.840 ns/op | 0 B/op | 0 | Mutex-protected read |

The callback's actual storage latency is additional and workload-dependent.
This API is intentionally opt-in; no existing allocator or command path is
replaced by it.

Verification targets:

```text
make test-t250-durable-sequence
make race-t250-durable-sequence
make benchmark-t250-durable-sequence-baseline
make benchmark-t250-durable-sequence
```
