# T-G06 Typed Hash Secondary Index

## What changed

`hat/hatDataStructure` now exposes `HashIndex[T, K]` for exact tuple-field
lookups where `K` is comparable. It has an explicit unique mode for primary or
unique-secondary keys and a non-unique mode for sorted ID postings.

This is a direct embedded API. SQL planner integration remains a separate
decision because SQL index selection needs schema metadata, statistics, and
costing that this low-level type does not own.

## Example

```go
type User struct {
    ID    uint64
    Email string
    Country string
}

index, err := hatDataStructure.NewHashIndex(
    func(user User) string { return user.Email },
    hatDataStructure.HashIndexOptions{
        Unique:   true,
        Capacity: 10000,
    },
)
if err != nil {
    return err
}

if err := index.Upsert(user.ID, user); err != nil {
    return err
}

entry, ok := index.LookupOne("alice@example.com")
if ok {
    fmt.Println(entry.ID, entry.Value.Email)
}
```

For a non-unique field:

```go
index, err := hatDataStructure.NewHashIndex(
    func(user User) string { return user.Country },
    hatDataStructure.HashIndexOptions{Capacity: 10000},
)
ids := index.LookupIDs("SG")
```

`Unique` defaults to `false`. In non-unique mode, `LookupOne` returns the
lowest stable ID; use `Lookup`, `LookupInto`, `LookupIDs`, or `LookupIDsInto`
when all matches are required.

## Behavior

- `Upsert` derives the key before taking the index lock.
- A unique conflict returns `ErrHashIndexDuplicateKey` before changing the old
  entry or posting, so rejected updates are atomic.
- Replacing an ID with a new key removes its old posting and adds the new one.
- Non-unique IDs are returned in ascending stable-ID order.
- `LookupOne` returns a typed entry directly and does not create a result slice.
- `LookupInto` and `LookupIDsInto` reuse caller-owned destination slices.
- `Delete`, `Clear`, `Len`, `DistinctKeys`, and `Contains` are available in
  both modes.
- The index is safe for concurrent readers and writers under its internal
  read/write lock. The extractor itself must be safe if callers invoke it from
  multiple goroutines.

## Benchmark

The benchmark ran on Linux, `amd64`, AMD Ryzen 9 5950X, with 10,000 integer
entries and five 200 ms samples per case.

### Exact lookup

| Case | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Unique `HashIndex.LookupOne` | 34.14-36.29 | 0 | 0 |
| `FunctionalIndex.LookupInto` with reusable result | 41.69-44.32 | 0 | 0 |
| Non-unique `HashIndex.LookupIDsInto` | 37.08-38.19 | 0 | 0 |
| Non-unique `FunctionalIndex.LookupIDsInto` | 36.67-37.83 | 0 | 0 |

Unique exact lookup is about 1.2x faster than the reusable functional-index
lookup and has the same zero-allocation result profile. Non-unique lookup is
within measurement noise, so the specialized type should be selected for its
explicit hash/unique contract rather than expected non-unique lookup speed.

### Unique-index construction

| Case | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Unique `HashIndex` build | 736,161-850,046 | 732,576-732,579 | 69 |
| Unique `FunctionalIndex` build | 1,107,821-1,273,757 | 1,173,014-1,173,017 | 10,069 |

For 10,000 distinct keys, the unique hash layout is about 1.5x faster, uses
about 1.6x less allocated memory during construction, and performs about 146x
fewer allocations. The functional index creates one posting slice per distinct
key; unique mode does not need postings.

## Verification

```text
make test-t-g06-hash-index
make race-t-g06-hash-index
make test-t-g06-package
make vet-t-g06-hash-index
make benchmark-t-g06-hash-index
```

The focused contract, package, race, and vet targets pass.
