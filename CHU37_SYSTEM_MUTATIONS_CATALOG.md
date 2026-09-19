# CH-U37 Stable SQL System Mutations Catalog

This feature adds an opt-in bounded metadata provider for
`CACHE('system.mutations')`. It follows the ClickHouse idea of exposing
mutation lifecycle and failure state while keeping the existing journal-tail
view compatible.

## API

Implement `SQLSystemMutationProvider`, or use the function adapter:

```go
provider := hatCache.SQLSystemMutationProviderFunc(func() ([]hatCache.SQLSystemMutation, error) {
	return []hatCache.SQLSystemMutation{
		{
			MutationID:    "mut-42",
			Sequence:      42,
			Command:       "MERGE",
			Key:           "orders",
			State:         "failed",
			Progress:      100,
			AffectedParts: []string{"orders-20260919-0001"},
			ErrorCode:     "E_IO",
			ErrorMessage:  "redacted storage failure",
			StartedAt:     time.Now().UTC(),
		},
	}, nil
})

resolver := hatCache.NewSQLSystemTablesResolver(nil, hatCache.SQLSystemTablesResolverOptions{
	MutationProvider: provider,
	MutationLimit:    1000,
})
```

The provider owns synchronization and returns a point-in-time snapshot. The
resolver copies affected-part lists, sorts by sequence and mutation ID,
normalizes timestamps to UTC, and rejects invalid metadata. The SQL row
columns are:

| Column | Meaning |
| --- | --- |
| `sequence` | Monotone journal/source sequence |
| `mutation_id` | Provider-owned stable mutation identifier |
| `command` | Mutation command name |
| `key` | Affected logical key or source identifier |
| `state` | Provider-owned lifecycle state |
| `progress` | Integer progress from 0 through 100 |
| `affected_parts` | Copied physical part identifiers |
| `error_code` | Bounded error classification |
| `error_message` | Bounded message that must already be redacted by the provider |
| `started_at` | UTC start time, or `NULL` |
| `finished_at` | UTC completion time, or `NULL` |

The default mutation limit is 1,000 rows and the maximum is 10,000. At most
256 affected parts are accepted per row, and every string field is limited to
4,096 bytes. A snapshot exceeding the limit fails closed instead of silently
returning an incomplete catalog.

## Compatibility And Security

When `MutationProvider` is omitted, the existing journal-backed view remains
unchanged: it returns the bounded tail with sequence, command, key, state, and
progress. The provider path is opt-in and does not change journal retention or
write behavior.

The resolver never reads mutation values and does not perform a disk scan. The
provider must supply only metadata it is authorized to expose, and its error
message must be redacted before returning it. Invalid or oversized metadata is
rejected; provider errors propagate to the SQL caller.

## Measurement

The benchmark uses 256 rows. The legacy path reads a 256-entry journal tail;
the provider path materializes 256 rows with two affected parts each. Five
`-benchmem` samples were run through `make benchmark-chu37` on Linux/amd64
with an AMD Ryzen 9 5950X.

| Path | Raw ns/op samples | Median ns/op | Median B/op | Median allocs/op |
| --- | --- | ---: | ---: | ---: |
| Before: legacy journal tail | 289,160; 333,756; 309,822; 319,661; 308,487 | 309,822 | 228,076 | 2,575 |
| After: legacy journal tail | 313,686; 296,551; 331,982; 304,737; 325,440 | 313,686 | 228,076 | 2,575 |
| After: rich provider | 278,617; 239,539; 243,983; 250,774; 235,639 | 243,983 | 252,426 | 2,565 |

The legacy path is allocation-neutral and within benchmark noise for CPU. The
rich provider path is about 1.29x faster than the legacy path and uses 1.11x
the transient heap while performing the additional metadata copy, validation,
and sorting. It uses 10 fewer allocations in this workload. No default write
or journal-retention path pays the provider cost.

Focused tests cover ordering, deep-copy isolation, timestamps, limits,
validation, provider errors, and value non-disclosure.
