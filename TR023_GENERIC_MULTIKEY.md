# T-U23 Generic Tuple Multikey Index

`hatDataStructure.TupleMultikeyIndex[T, K]` maintains a typed inverted index
for tuple fields that expand to multiple comparable keys. The caller supplies
the extractor, so nested arrays, sets, dates, integers, and application
specific normalization stay type-safe and avoid reflection.

## Usage

```go
type Order struct {
    Tags []uint32
}

index, err := hatDataStructure.NewTupleMultikeyIndex(
    func(order Order) ([]uint32, error) {
        return order.Tags, nil
    },
    hatDataStructure.TupleMultikeyIndexOptions{
        MaxKeysPerItem: 16,
        MaxItems:       1_000_000,
    },
)
if err != nil {
    return err
}

if err := index.Upsert(orderID, order); err != nil {
    return err
}
matchingIDs := index.Lookup(uint32(42), reusableDestination)
```

`Upsert` deduplicates keys, validates limits, and replaces all old postings as
one operation. Extractor errors and bound violations leave the old state
untouched. `Delete` removes the reverse key set and its postings, `Contains`
checks one membership, `Len` and `KeyCount` expose bounded cardinalities, and
`Clear` removes all state while retaining configuration.

Posting lists reuse the existing compact sorted `uint64` representation: a
singleton is inline, larger lists use compact overflow storage, and lookups
return sorted IDs. A reusable destination slice makes lookup allocation-free.
The reverse map makes replacement and deletion exact; it is the intentional
memory cost compared with a one-way map of sets.

The key type is any comparable Go type, including integers and application
date/time representations. The index is safe for concurrent reads and writes,
but the extractor remains caller-owned and should return a deterministic key
set for a tuple.

The index is opt-in and is not automatically attached to SQL tables or every
tuple. Existing string multikey indexes and ordinary maps are unchanged.

## Verification and tradeoff

Tests cover duplicate normalization, sorted lookup, reused destinations,
replacement, deletion, clear, atomic bounds/extractor failures, and concurrent
access. The package was run under the race detector and `go vet`.

For 100,000 tuples with two `uint32` keys, the typed posting lookup measured
about 84 ns with zero allocations, versus about 8.4 microseconds for iterating
a map-of-sets. Building 1,024 tuples used about 228 KB versus 204 KB for a
map-of-sets plus reverse map, or roughly 11% more memory, and took about 1.46x
the fair reverse-map build CPU. This favors read-heavy membership queries; a
write-heavy workload should keep using a simpler map when ordered postings
are not needed.

Raw samples and the control definition are in `TR023_BENCHMARK_RAW.txt`.
