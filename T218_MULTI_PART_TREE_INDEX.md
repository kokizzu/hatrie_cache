# T218: Multi-Part TREE Indexes

`MultiPartTreeIndex` adds a composite ordered index for records whose lookup
key has several parts, such as `(region, account)` or `(tenant, event_time)`.
The parts are compared lexicographically and the index returns rows in that
same order.

The implementation reuses the existing compact `OrderedIndex` sorted-vector
and copy-on-write core. It is therefore a tree-style ordered access path, not
a separate pointer-heavy B-tree implementation. Existing iterator mutation
semantics are preserved: a mutation invalidates an active ordinary iterator.

## API

```go
type Record struct {
	ID      uint64
	Region  string
	Account string
}

index, err := NewMultiPartTreeIndex(
	func(record Record) []string {
		return []string{record.Region, record.Account}
	},
	compareString,
	2,
	128,
)
if err != nil {
	panic(err)
}

_ = index.Upsert(42, Record{ID: 42, Region: "ap-south-1", Account: "acct-7"})

iterator, ok := index.Prefix([]string{"ap-south-1"})
if ok {
	defer iterator.Close()
	for {
		entry, next, err := iterator.Next()
		if err != nil || !next {
			break
		}
		use(entry.ID, entry.Parts, entry.Value)
	}
}
```

The caller supplies one comparator for a key part type. The extractor must
return exactly the configured number of parts on every upsert. The index copies
the extracted parts, so a caller may reuse its input slice after `Upsert`.

The query methods are:

| Method | Behavior |
| --- | --- |
| `Seek(parts)` | Lower-bound lookup for an exact-width composite key. |
| `Prefix(prefix)` | Scans all keys beginning with the supplied leading parts. |
| `Range(start, end)` | Inclusive range scan between two exact-width keys. |
| `Upsert(id, value)` | Inserts or replaces one ID and maintains sorted order. |
| `Delete(id)` | Removes one ID. |
| `Clear()` | Removes all entries while retaining the index configuration. |
| `Len()` | Returns the indexed entry count. |

All methods are safe for concurrent index access. Iterators are single-consumer
values and should be closed when a scan stops before EOF.

## Cost Model

- Prefix and range positioning use binary search, then pay only for returned
  rows: `O(log n + k)` query work.
- Upserts and deletes retain the existing sorted-vector write behavior, which
  can move `O(n)` entries. This keeps read memory compact and avoids a second
  node allocation strategy, but a write-heavy workload should benchmark the
  tradeoff against a hash index.
- Prefixes up to four parts use inline iterator storage and do not allocate for
  the query. Longer prefixes allocate a small copied prefix slice.
- The key has a fixed part count. This catches malformed rows early and avoids
  storing per-row metadata describing variable key widths.
- The returned `Parts` slice belongs to the index entry and should be treated
  as read-only by callers.

## Benchmark

The benchmark loads 32,768 records across 16 regions and selects one region.
The baseline scans every record and checks the region. The indexed case uses a
one-part prefix scan on the two-part `(region, order)` key. Both final cases
reported zero allocations.

| Case | Median | Memory | Relative |
| --- | ---: | ---: | ---: |
| Full scan and filter | 35,019 ns/op | 0 B/op, 0 allocs/op | 1.0x |
| Multi-part prefix scan | 196.1 ns/op | 0 B/op, 0 allocs/op | **~179x faster** |

The three raw paired samples from `make benchmark-t218` were:

```text
Baseline: 35019, 33412, 38433 ns/op; 0 B/op; 0 allocs/op
Indexed:    196.1, 196.6, 180.9 ns/op; 0 B/op; 0 allocs/op
```

The result is a selective-read benchmark, not a claim that every workload is
179x faster. The main tradeoff remains sorted-vector mutation cost; this
feature adds a composite read path without adding another large per-entry
structure.
