# T-U24 Conditional Space Indexes

Conditional space indexes keep a named, schema-declared index only for rows
that have a scalar value in a second field. They are useful when a query has a
stable status or tenant predicate plus a selective lookup key, for example
`id = 9000 AND status = 'active'`.

## Declaration

```go
definition := hatSchema.SpaceDefinition{
	Name: "jobs",
	Source: hatSchema.Source{
		Name: "jobs",
		Columns: []hatSchema.Column{
			{Name: "id", Type: hatSchema.TypeInteger},
			{Name: "status", Type: hatSchema.TypeText},
		},
	},
	Indexes: []hatSchema.IndexDefinition{{
		Name:    "active_id",
		Kind:    hatSchema.IndexKindConditional,
		Columns: []string{"id"},
		Condition: &hatSchema.IndexCondition{
			Field: "status",
			Value: "active",
		},
	}},
}
catalog, err := hatSchema.NewSpaceCatalog([]hatSchema.SpaceDefinition{definition})
```

Validation requires exactly one key column, a different existing condition
field, and a scalar condition value. Supported values are nil, booleans,
strings, signed and unsigned integers, and floats. Arbitrary functions,
arrays, and nested predicates are intentionally rejected so the planner can
make a deterministic decision.

## Runtime lifecycle

`MaterializedSource.BuildConditionalIndex` builds the posting lists from the
current rows and publishes the complete result atomically. `Insert` maintains
an already-built conditional index for subsequent rows. `DropConditionalIndex`
removes it, while `HasConditionalIndex` and `ConditionalIndexStats` expose
state for operators and diagnostics.

The runtime postings are in memory. Declaring an index does not create a
background worker or automatically rebuild it; a restore or bulk replacement
can explicitly call `BuildConditionalIndex` after the rows are available.

## SQL planner behavior

The SQL resolver uses the index only when the `WHERE` expression contains
equality literals for both the indexed key and the declared condition. The
planner accepts either unqualified fields or the source alias. It returns only
candidate rows, then the ordinary SQL filter rechecks the complete predicate.
Queries with a different condition, missing equality, unsupported expression,
or no runtime index keep the normal scan path.

Example:

```sql
FROM CACHE('jobs')
WHERE id = 9000 AND status = 'active'
SELECT id, status
```

The matching query can report `CONDITIONAL INDEX SCAN` in `EXPLAIN ANALYZE`.
An inactive query such as `status = 'inactive'` falls back to the source and
remains correct.

## Measurement

Command:

```sh
make codex-tu24-benchmark
```

Five samples on Linux/amd64, AMD Ryzen 9 5950X, 10,000 rows with 10% active:

| Workload | Median ns/op | B/op | Allocs/op | Result |
| --- | ---: | ---: | ---: | --- |
| Full scan query | 6,536,811 | 8,754,676 | 40,061 | baseline |
| Conditional indexed query | 9,764 | 9,481 | 51 | 669x faster, 923x fewer bytes, 786x fewer allocations |
| Index build | 11,614,805 | 8,137,138 | 101,560 | one-time cost for the fixture |

Raw samples:

```text
Baseline ns/op: 6349850 6536811 6498493 6676691 6648389
Indexed ns/op:  9606 9764 10071 9258 9942
Build ns/op:    11054024 10996958 11755154 11614805 11827121
```

`B/op` is transient benchmark allocation, not retained index size. The result
supports using the feature for repeated selective reads; a one-off query does
not justify building the index. The default behavior remains unchanged until
the caller declares and builds the index.

## Verification

The focused tests cover schema cloning and validation, direct resolver lookup,
planner selection, residual predicate fallback, inserts after build, and
drop/rebuild behavior. The test source is
`hat/hatSchema/tu24_conditional_index_test.go`.
