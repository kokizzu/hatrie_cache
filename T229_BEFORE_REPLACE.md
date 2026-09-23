# T229 Before-Replace Triggers

T229 adds an opt-in validation and conflict-policy hook to `Space`. It is
available to both the default memtx engine and the Vinyl-style engine.

## API

Set `SpaceOptions.BeforeReplace` when constructing a space:

```go
space, err := hatDataStructure.NewSpace(hatDataStructure.SpaceOptions{
	BeforeReplace: func(change hatDataStructure.SpaceReplace) error {
		if change.Exists && !change.Delete && bytes.Equal(change.OldValue, change.NewValue) {
			return errors.New("duplicate replacement")
		}
		return nil
	},
})
```

`SpaceReplace` contains:

| Field | Meaning |
| --- | --- |
| `Key` | The key being inserted, replaced, or deleted. |
| `OldValue` | The previous value; nil for an insert. |
| `NewValue` | The candidate value; nil for a delete. |
| `Exists` | Whether the key existed before the operation. |
| `Delete` | Whether this event is a delete. |

The callback receives independent copies of both byte slices. Mutating the
event cannot mutate the input value or stored state.

## Semantics

- The hook is disabled by the zero value (`BeforeReplace == nil`).
- Empty keys, oversized values, and a full memtx space are rejected before the
  callback runs.
- Deleting a missing key remains a no-op and does not invoke the callback.
- A callback error rejects the operation and leaves the existing value intact.
- Hook-enabled `Put` and `Delete` operations are serialized with `Get` on the
  same space for a consistent old-value decision.
- The callback must not call back into the same `Space`; it runs while the
  space mutation lock is held.
- The hook validates or rejects a candidate. It is not a value-transform hook;
  the stored value remains the original input when the operation succeeds.

The callback is not part of snapshot encoding. A callback configured on the
options supplied to `UnmarshalSpace` remains attached to the restored space.

## Cost

The focused benchmark uses a repeated memtx replacement with a five-byte
value. Values are medians of five `-benchmem -cpu=1` samples on Linux/amd64
with an AMD Ryzen 9 5950X:

| Workload | CPU | Memory | Relative CPU |
| --- | ---: | ---: | ---: |
| Existing path before T229 | 47.86 ns/op | 8 B/op, 1 alloc/op | 1.00x |
| Hook disabled after T229 | 51.37 ns/op | 8 B/op, 1 alloc/op | 1.07x |
| Hook enabled, no-op callback | 102.4 ns/op | 24 B/op, 3 allocs/op | 2.14x |

The enabled cost is intentional: the callback receives copied old/new images,
which costs two additional allocations in this workload. The default does not
pay that copy cost. Full raw output and the benchmark command are recorded in
[BENCHMARK.md](BENCHMARK.md#t229-before-replace-triggers).
