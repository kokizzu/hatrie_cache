# Ordered Snapshot Cursor

`hatDataStructure.OrderedIndex` now exposes an opt-in
`OrderedIndexSnapshotCursor`. It walks the entries that existed when the
cursor was created and remains valid while inserts, updates, deletes, and
`Clear` run concurrently. The existing `First`, `Seek`, and `SeekAfter`
iterators keep their invalidation-on-mutation behavior.

## Example

```go
index, err := hatDataStructure.NewOrderedIndex[uint64, uint64](
	func(value uint64) uint64 { return value },
	func(left, right uint64) int {
		if left < right {
			return -1
		}
		if left > right {
			return 1
		}
		return 0
	},
	0,
)
if err != nil {
	return err
}
for _, value := range []uint64{1, 2, 3} {
	if err := index.Upsert(value, value); err != nil {
		return err
	}
}

cursor, ok := index.SnapshotCursor()
if !ok {
	return errors.New("empty index")
}
defer cursor.Close()

// Mutations after SnapshotCursor are not visible to this cursor.
_ = index.Upsert(0, 0)
_ = index.Delete(2)

for {
	entry, ok, err := cursor.Next()
	if err != nil {
		return err
	}
	if !ok {
		break
	}
	fmt.Println(entry.Value)
}
// 1
// 2
// 3
```

`SnapshotCursor` returns false for a nil or empty index. `Seek` and `SeekAfter`
operate on the captured view, not the current index. A cursor cannot be used
concurrently with itself. Reaching EOF releases its live snapshot; callers that
stop early must call `Close` promptly.

## Memory And Write Behavior

The cursor does not copy entries at creation. `OrderedIndex` already uses its
`active` reader count and copy-on-write mutations to keep live iterator backing
slices immutable. The snapshot cursor reuses that mechanism. While any cursor
or iterator is active, each mutation may copy the full entry slice; the cost is
proportional to the number and size of indexed entries. This is why prompt close
is part of the contract.

## Measurements

Measured on Linux/amd64, AMD Ryzen 9 5950X, with
`make benchmark-t-u41` (`go test -benchmem -benchtime=500ms -count=5`). The
index contains 1,024 `uint64` entries.

| Workload | Snapshot cursor | Existing iterator / no snapshot | Result |
| --- | ---: | ---: | --- |
| Sequential `Next` | 1.94 ns/op, 0 B/op, 0 allocs/op | 2.42 ns/op, 0 B/op, 0 allocs/op | 1.25x faster read path |
| Repeated `Upsert` | 18.5 µs/op, 24,576 B/op, 1 alloc/op | 14.9 µs/op, 0 B/op, 0 allocs/op | 1.25x slower while a view is live |

The read improvement comes from avoiding the generation check needed by the
invalidating iterator. The write allocation is intentional snapshot retention,
not an additional copy at cursor creation; it matches the ordered index's
existing active-reader protection.

## Verification

```text
make test-t-u41
make benchmark-t-u41
```

Tests cover stable results across upsert/delete, `Clear`, snapshot-local seek,
close and nil errors, and empty indexes.
