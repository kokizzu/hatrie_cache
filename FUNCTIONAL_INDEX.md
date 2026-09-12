# Functional Index

`hatDataStructure.FunctionalIndex[T, K]` is a typed secondary index for values
whose lookup key is derived by a caller-provided function. It is the reusable
primitive for Tarantool-style functional indexes; it is not automatically wired
into the SQL planner yet.

## Example

```go
type User struct {
	ID   uint64
	Name string
}

index, err := hatDataStructure.NewFunctionalIndex[User, string](
	func(user User) string { return strings.ToLower(user.Name) },
	1024,
)
if err != nil {
	return err
}

_ = index.Upsert(1, User{ID: 1, Name: "Ada"})
_ = index.Upsert(2, User{ID: 2, Name: "ADA"})

users := index.Lookup("ada")
// users contains both users, in posting insertion order.

scratch := make([]User, 0, 8)
users = index.LookupInto("ada", scratch)
// LookupInto reuses scratch capacity and avoids a result allocation.
```

The index stores stable caller IDs, so an update can remove the old posting
before inserting the new one:

```go
_ = index.Upsert(1, User{ID: 1, Name: "Grace"})
_ = index.Delete(2)
```

`LookupIDs` and `LookupIDsInto` return only stable IDs when the caller already
owns the source values. `Len`, `DistinctKeys`, and `Clear` expose basic
lifecycle state. `Upsert` and `Delete` are safe for concurrent callers.

## Correctness Contract

- The extractor is required and must be deterministic for the value being
  indexed. It runs before the index lock is acquired.
- A repeated ID replaces the stored value. If its derived key is unchanged,
  its position remains stable; changing the key moves it to the new posting.
- Duplicate derived keys are supported. Lookup order is posting insertion
  order, not sorted order.
- `Lookup` allocates a result slice. `LookupInto` resets and reuses the caller's
  scratch slice when it has enough capacity.
- Mutating a value outside `Upsert` leaves the index unaware of the change;
  rebuild or update it through the index.
- Persistence and wire encoding are intentionally outside this primitive. Save
  source rows and rebuild the derived index after restore.

## Cost And Tradeoff

The index keeps one entry per stable ID, one posting ID per row, and a map of
derived keys to postings. It therefore consumes more memory than a plain row
map, but avoids scanning every row for repeated equality lookups. Updates also
pay index-maintenance work, especially when a row changes key. This is most
useful for selective repeated lookups, joins, grouping keys, or adapters that
can retain a typed source ID.

## Benchmark

The benchmark creates 10,000 rows with 1,000 distinct keys and looks up a key
with 10 matches. It compares an exact linear scan with `LookupInto` and also
measures update maintenance. Command:

```text
make benchmark-functional-index-local-clean
```

Recorded on Linux amd64, AMD Ryzen 9 5950X, Go benchmark count 5:

```text
BenchmarkFunctionalIndexLookup/scan-32          43987  8091 ns/op  10.00 matches/op  0 B/op  0 allocs/op
BenchmarkFunctionalIndexLookup/scan-32          44996  8105 ns/op  10.00 matches/op  0 B/op  0 allocs/op
BenchmarkFunctionalIndexLookup/scan-32          44602  7803 ns/op  10.00 matches/op  0 B/op  0 allocs/op
BenchmarkFunctionalIndexLookup/scan-32          46900  8110 ns/op  10.00 matches/op  0 B/op  0 allocs/op
BenchmarkFunctionalIndexLookup/scan-32          45904  7605 ns/op  10.00 matches/op  0 B/op  0 allocs/op
BenchmarkFunctionalIndexLookup/index-32       2234623   147.9 ns/op  10.00 matches/op  0 B/op  0 allocs/op
BenchmarkFunctionalIndexLookup/index-32       2601668   138.8 ns/op  10.00 matches/op  0 B/op  0 allocs/op
BenchmarkFunctionalIndexLookup/index-32       2151735   163.4 ns/op  10.00 matches/op  0 B/op  0 allocs/op
BenchmarkFunctionalIndexLookup/index-32       2287224   153.5 ns/op  10.00 matches/op  0 B/op  0 allocs/op
BenchmarkFunctionalIndexLookup/index-32       2391812   149.6 ns/op  10.00 matches/op  0 B/op  0 allocs/op
BenchmarkFunctionalIndexUpsert/same-key-32    7553299    46.41 ns/op  0 B/op  0 allocs/op
BenchmarkFunctionalIndexUpsert/same-key-32    7271881    47.36 ns/op  0 B/op  0 allocs/op
BenchmarkFunctionalIndexUpsert/same-key-32    7423728    46.92 ns/op  0 B/op  0 allocs/op
BenchmarkFunctionalIndexUpsert/same-key-32    7601294    47.74 ns/op  0 B/op  0 allocs/op
BenchmarkFunctionalIndexUpsert/same-key-32    7489690    47.31 ns/op  0 B/op  0 allocs/op
BenchmarkFunctionalIndexUpsert/key-change-32  3081109   116.4 ns/op  8 B/op  1 allocs/op
BenchmarkFunctionalIndexUpsert/key-change-32  3079316   113.7 ns/op  8 B/op  1 allocs/op
BenchmarkFunctionalIndexUpsert/key-change-32  3153052   113.1 ns/op  8 B/op  1 allocs/op
BenchmarkFunctionalIndexUpsert/key-change-32  3118633   113.5 ns/op  8 B/op  1 allocs/op
BenchmarkFunctionalIndexUpsert/key-change-32  3209590   112.9 ns/op  8 B/op  1 allocs/op
```

The median is 8,091 ns for scanning and 149.6 ns for indexed lookup, or
54.1x faster for this repeated selective lookup. Both paths report zero
operation allocations because the scan uses no result slice and the indexed
path reuses scratch storage. Same-key updates have a 47.31 ns median with no
operation allocation; changing the derived key has a 113.5 ns median with 8
B/op and one allocation in this workload. This is a lookup benchmark, not a
claim that an index is always faster: high-cardinality writes, low lookup
reuse, broad postings, and memory pressure can favor a scan.

The implementation is inspired by Tarantool's functional-index model, where
index keys are derived from stored tuples. See the
[Tarantool `create_index` reference](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/create_index/)
for the upstream behavior this primitive is intended to make possible in Go.
