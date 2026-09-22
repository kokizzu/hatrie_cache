# T250 Durable Sequence Allocation

`hatDataStructure.DurableSequence` provides a monotonic `uint64` allocator
with an optional durable current value.

## Semantics

- `DurableSequenceOptions{}` creates an in-memory allocator.
- `Initial` is the last value already allocated, so the first allocation is
  `Initial + 1`.
- The allocator rejects exhaustion instead of wrapping to zero.
- The in-memory path uses an atomic compare-and-swap loop and allocates no
  heap memory per call.
- `MarshalBinary` and `UnmarshalDurableSequence` provide a 20-byte,
  checksummed transfer representation.
- Setting `Path` enables the durable path. The file is private (`0600`) and
  contains two fixed 20-byte slots. Each allocation writes the alternate slot
  and calls `fsync` before publishing the value in memory.
- On restart, the newest valid checksummed slot is selected. If one slot is
  torn or corrupt, the other valid slot can still be recovered.
- `Save` explicitly persists the current value, and `Close` releases the
  durable file descriptor.

The durable guarantee is no duplicate or regressed value after a successful
allocation is persisted. Strict gap-free allocation across a process crash is
not possible: a crash after `fsync` and before the caller receives the value
can leave a committed value that the caller did not observe. Callers that
require contiguous business records should commit the allocated value in the
same higher-level transaction.

## Example

```go
sequence, err := hatDataStructure.NewDurableSequence(
    hatDataStructure.DurableSequenceOptions{Path: "/var/lib/hatrie/sequence"},
)
if err != nil {
    return err
}
defer sequence.Close()

value, err := sequence.Allocate()
if err != nil {
    return err
}
fmt.Println(value)
```

## Measurements

Measurements were collected on Linux/amd64 with an AMD Ryzen 9 5950X. Each
benchmark was sampled five times; the table reports the median sample.

| Path | Median CPU | Allocations | Comparison |
| --- | ---: | ---: | ---: |
| Raw atomic counter baseline | 0.2600 ns/op | 0 B/op, 0 allocs/op | 1.00x |
| In-memory `DurableSequence` | 3.424 ns/op | 0 B/op, 0 allocs/op | 13.17x slower |
| `MarshalBinary` | 30.50 ns/op | 24 B/op, 1 alloc/op | 117.31x slower |
| Durable file allocation, previous rename implementation | 1,654,113 ns/op | 1,311 B/op, 18 allocs/op | 1.00x |
| Durable file allocation, two-slot implementation | 510,073 ns/op | 24 B/op, 1 alloc/op | 3.24x faster |

The durable path remains intentionally slower than memory-only allocation
because it performs an `fsync` for every allocation. The improvement removes
per-operation temporary files, directory syncs, and nearly all allocation
overhead while preserving crash recovery and corruption detection.
