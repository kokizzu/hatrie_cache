# TT-028 Upsert Conflict Handlers

Status: adopted as an opt-in `hatSchema.MaterializedSource.Upsert` API.

`MaterializedSource` can now merge an incoming row with the existing row found
through a maintained unique index. The caller must name the unique conflict
field and provide a trusted in-process callback that returns the complete
replacement row:

```go
result, err := source.Upsert(hatSchema.Row{
	"id":    "item-1",
	"value": int64(3),
}, hatSchema.MaterializedUpsertOptions{
	ConflictField: "id",
	OnConflict: func(existing, incoming hatSchema.Row) (hatSchema.Row, error) {
		return hatSchema.Row{
			"id":    existing["id"],
			"value": existing["value"].(int64) + incoming["value"].(int64),
		}, nil
	},
})
```

The conflict field must already have a unique index, normally installed with
`BuildUniqueIndex`. `Insert` remains unchanged and continues to reject
duplicate unique values. A `NULL` conflict value does not select an existing
row, matching the existing nullable-unique behavior.

The handler receives isolated row copies. Derived values are recomputed before
publication, and ordinary, covering, and functional index postings are updated
in place. Handler errors, invalid derived values, unique violations, and a
changed conflict key leave the source and its indexes unchanged. This callback
is not a sandbox; callers must keep it deterministic and trusted.

## Measurement

The workload uses 1,024 rows and merges one keyed row repeatedly. The legacy
control copies and scans the complete row set for every merge. Raw samples are
also recorded in [BENCHMARK.md](BENCHMARK.md#tt-028-upsert-conflict-handlers).

| Path | Median ns/op | Median B/op | Median allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Legacy manual copy/scan/merge, before implementation | 373,233 | 353,550 | 2,050 | 1.00x |
| Legacy manual copy/scan/merge, same post-change run | 353,538 | 353,548 | 2,050 | 1.00x |
| Indexed `Upsert` conflict handler | 3,801 | 2,542 | 24 | 93.01x faster than same-run control |

Compared with the same-run control, the opt-in path uses about 139x fewer
bytes and 85x fewer allocations. The callback and index maintenance still
have a per-operation cost, and the API does not provide transaction or
cross-process conflict coordination; those remain caller-owned.
