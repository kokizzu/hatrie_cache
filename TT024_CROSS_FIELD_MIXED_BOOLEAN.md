# TT-024 Cross-Field Mixed Boolean Text Unions

## What changed

Mixed Boolean text predicates can now use one optional multi-field candidate
lookup when every `OR` branch contains an indexed phrase/proximity predicate.
For example:

```sql
FROM CACHE('docs') AS doc
WHERE (CONTAINS_PHRASE(doc.title, 'alpha beta') AND doc.kind = 'a')
   OR (CONTAINS_PROXIMITY(doc.body, 'gamma delta', 1) AND doc.kind = 'b')
SELECT doc.id
```

Resolvers that already implement the single-field
`TextProximityUnionIndexedSourceResolver` keep the existing fast path. A
resolver that owns indexes for more than one text field can additionally
implement `TextProximityMultiFieldUnionIndexedSourceResolver`:

```go
func (r *Resolver) ResolveSQLTextProximityMultiFieldUnionSource(
    name, key string,
    queries []hatSql.SQLTextProximityFieldQuery,
) ([]hatSql.Row, bool, error)
```

Each query includes its `Field` and `SQLTextProximityQuery`. The resolver must
deduplicate candidates and return them in source order. The SQL executor still
re-evaluates the complete Boolean expression, so the index result is a
conservative candidate set rather than a trusted final result.

Resolvers that only provide the old single-field interface continue to fall
back to a full scan for cross-field expressions. This preserves correctness
and avoids guessing how to merge independently ordered result streams.

## Benchmark

Command:

```text
make benchmark-tt024-cross-field
```

Fixture: 50,000 rows, two indexed text fields, approximately 500 candidate
rows, five benchmark samples per path on an AMD Ryzen 9 5950X.

| Path | Raw ns/op | Raw B/op | Raw allocs/op |
| --- | --- | --- | --- |
| Full scan | 53,878,050; 54,545,227; 55,093,708; 56,299,610; 53,593,320 | 59,099,292; 59,099,320; 59,099,282; 59,099,336; 59,099,544 | 601,075; 601,076; 601,075; 601,076; 601,075 |
| Multi-field indexed union | 596,295; 597,922; 601,186; 603,589; 604,794 | 691,296; 691,295; 691,295; 691,297; 691,295 | 6,027; 6,027; 6,027; 6,027; 6,027 |

Median comparison:

| Metric | Full scan | Indexed union | Improvement |
| --- | ---: | ---: | ---: |
| Time | 54,545,227 ns/op | 601,186 ns/op | 90.7x faster |
| Allocated bytes | 59,099,320 B/op | 691,295 B/op | 85.5x lower |
| Allocations | 601,075 allocs/op | 6,027 allocs/op | 99.7x fewer |

The indexed path adds a small request slice and delegates cross-field merge
ordering/deduplication to the source owner. There is no behavior or memory
cost for existing resolvers that do not opt into the new interface.

## Verification

- `make test-tt024-text`
- `make race-tt024-text`
- `make vet-tt024-text`
- `make test-tt024-package`
- `make benchmark-tt024-cross-field`
