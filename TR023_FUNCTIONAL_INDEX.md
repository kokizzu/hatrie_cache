# TR-023 Functional Indexes

This change completes the SQL/materialized-source part of the Tarantool-style
functional-index idea. `hatDataStructure.FunctionalIndex` remains the generic
typed primitive; `hatSchema.MaterializedSource.BuildFunctionalIndex` now builds
an equality index from a deterministic row evaluator, publishes it atomically,
and maintains it for later inserts.

The feature is opt-in. Existing sources, scans, storage formats, and wire
formats are unchanged unless a caller explicitly builds an index.

## Example

```go
source := hatSchema.NewMaterializedSource([]hatSchema.DerivedColumn{
	{Name: "id"},
	{Name: "name"},
})

_, err := source.BuildFunctionalIndex(
	hatSql.LowerIndexField("name"),
	[]string{"name"},
	func(row hatSchema.Row) (interface{}, error) {
		name, ok := row["name"].(string)
		if !ok {
			return nil, errors.New("name is not text")
		}
		return strings.ToLower(name), nil
	},
)
```

With `hatSchema.SQLResolverAdapter`, the existing SQL form uses the index when
available:

```sql
FROM CACHE('people') AS person
WHERE LOWER(person.name) = 'ada'
SELECT person.id
```

The evaluator receives a row copy. It must be deterministic and should be
read-only. Dependencies are validated before publication, and a concurrent
insert causes a generation-checked rebuild retry. An evaluator error aborts
the build without publishing a partial index.

## Measurement

Command:

```text
make benchmark-tr023-functional-index
```

Linux/amd64, AMD Ryzen 9 5950X, Go benchmark `-benchmem -count=5`, 10,000
materialized rows and 100 matching rows for `name-042`. Values are medians of
the five samples.

| Workload | Before | After | Result |
| --- | ---: | ---: | ---: |
| SQL `LOWER` equality lookup | 9.891 ms/op, 9,986,044 B/op, 70,234 allocs/op | 100.409 us/op, 112,900 B/op, 734 allocs/op | 98.5x faster, 88.5x fewer bytes, 95.7x fewer allocs |
| Equivalent lower-case full scan | 523.431 us/op, 80,000 B/op, 10,000 allocs/op | index path above | The indexed path is 5.2x faster than this direct scan |
| Build a lower-case posting map | 1.187 ms/op, 1,071,074 B/op, 10,833 allocs/op | 5.705 ms/op, 4,836,898 B/op, 50,845 allocs/op | 4.81x slower, 4.51x more build-time bytes |

The query result is the large win. Index construction is deliberately more
expensive than a hand-written map because the public evaluator receives cloned
rows for mutation safety and the build is generation-checked. This is why the
feature is not enabled by default: use it for repeated selective predicates,
not one-off scans or memory-constrained workloads where the index will rarely
be queried.

Raw samples:

```text
SQL scan: 11319107, 10170620, 9890964, 9358616, 9335781 ns/op; 9986421, 9986147, 9986042, 9986137, 9986044 B/op; 70234 alloc/op
SQL functional lookup: 98605, 105404, 100409, 100333, 101477 ns/op; 112900, 112900, 112900, 112899, 112899 B/op; 734 alloc/op
Equivalent build: 1186679, 1244475, 1126707, 1123766, 1203796 ns/op; 1071074, 1071075, 1071074, 1071074, 1071074 B/op; 10833 alloc/op
Functional build: 5706206, 5705401, 5670097, 5844510, 5586561 ns/op; 4836914, 4836898, 4836913, 4836897, 4836898 B/op; 50845 alloc/op
```

## Verification

```text
make format-tr023-functional-index
make test-tr023-functional-index
make race-tr023-functional-index
make vet-tr023-functional-index
make package-tr023-functional-index
```

The tests cover SQL lookup, duplicate functional keys, post-build insert
maintenance, evaluator mutation isolation, failed builds, validation, and
online-build retry after a concurrent insert.
