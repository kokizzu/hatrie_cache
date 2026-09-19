# CH-U36 Stable SQL System Parts Catalog

This feature adds an opt-in, bounded metadata provider for
`CACHE('system.parts')`. It follows the ClickHouse idea of exposing physical
part state for operators, while keeping the contract small enough for an
embedded cache.

## API

Implement `SQLSystemPartProvider`, or use the function adapter:

```go
provider := hatCache.SQLSystemPartProviderFunc(func() ([]hatCache.SQLSystemPart, error) {
	return []hatCache.SQLSystemPart{
		{
			Name:           "orders-20260919-0001",
			Partition:      2,
			Rows:           1000,
			BytesOnDisk:    128000,
			Active:         true,
			State:          "active",
			Level:          1,
			DataVersion:    42,
			MinKey:         "2026-09-19/0000",
			MaxKey:         "2026-09-19/2359",
			Checksum:       "sha256:...",
			CreatedAt:      time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC),
			RetentionUntil: time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC),
		},
	}, nil
})

resolver := hatCache.NewSQLSystemTablesResolver(nil, hatCache.SQLSystemTablesResolverOptions{
	PartProvider: provider,
	PartLimit:    1024,
})
```

The provider owns synchronization and returns a point-in-time snapshot. The
resolver copies rows, sorts them by partition/name/version/level, normalizes
timestamps to UTC, and rejects invalid metadata. The SQL row columns are:

| Column | Meaning |
| --- | --- |
| `name` | Physical part name supplied by the owner |
| `partition` | Logical partition number |
| `rows` | Rows represented by the part |
| `bytes_on_disk` | Physical bytes represented by the part |
| `active` | Whether the part is currently active |
| `state` | Owner-defined state, defaulting to `active` or `inactive` |
| `level` | Merge/compaction level |
| `data_version` | Owner-defined source version |
| `min_key`, `max_key` | Bounded key-range metadata |
| `checksum` | Bounded integrity identifier |
| `created_at` | UTC creation time, or `NULL` when unavailable |
| `retention_until` | UTC retention deadline, or `NULL` when unavailable |

The default limit is 1,024 rows and the maximum accepted limit is 10,000.
Each string field is limited to 4,096 bytes. A snapshot exceeding the limit
fails closed instead of silently returning an incomplete catalog.

## Compatibility And Security

When `PartProvider` is omitted, the existing trie-only view remains unchanged:
it reports the root or local partition row counts and does not allocate rich
metadata. The feature is therefore opt-in and does not add storage tracking to
ordinary writes.

The catalog contains no filesystem path, value, query text, credential, or
automatic disk scan. Callers must publish only metadata they are authorized to
expose. Provider errors and invalid rows are returned to the SQL caller.

## Measurement

The benchmark uses 64 local partitions and 4,096 seeded keys for the legacy
path, and 64 rich provider rows for the opt-in path. Five `-benchmem` samples
were run through `make benchmark-chu36` on Linux/amd64 with an AMD Ryzen 9
5950X.

| Path | Raw ns/op samples | Median ns/op | Median B/op | Median allocs/op |
| --- | --- | ---: | ---: | ---: |
| Before: legacy view | 29,191; 28,117; 29,546; 30,096; 30,801 | 29,546 | 24,635 | 258 |
| After: legacy fast path | 25,932; 25,161; 25,498; 25,610; 25,810 | 25,610 | 24,635 | 258 |
| After: rich provider | 47,946; 48,053; 47,965; 46,713; 48,268 | 47,965 | 59,128 | 517 |

The default path is about 1.15x faster with unchanged allocations in this
run. The rich provider path is about 1.87x slower than the legacy fast path,
with 2.40x transient heap and 2.00x allocations; that is the explicit cost of
copying, sorting, validating, and materializing 64 metadata rows. No default
behavior pays that cost.

Focused tests cover ordering, schema, timestamp normalization, invalid input,
row limits, provider errors, and path non-disclosure. Broader package and race
verification is required before release.
