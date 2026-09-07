# Distributed Join Data Movement

Typed joins can expose cumulative ingress accounting when the join is created
with `TypedTableJoinOptions{TrackDataMovement: true}`. The same snapshot is
available from a shared `TypedTableJoinArrangement`. This gives a distributed
operator a deterministic estimate of what crossed each join input boundary
without changing join results or retaining extra per-change objects.

## Enable It

```go
definition := hatSql.TypedTableJoinDefinition{
	LeftField:  "customer_id",
	RightField: "customer_id",
}
join, err := hatSql.NewTypedTableJoinWithOptions(
	leftTable,
	rightTable,
	definition,
	hatSql.TypedTableJoinOptions{TrackDataMovement: true},
)
if err != nil {
	return err
}

if err := join.ApplyLeft(leftChanges); err != nil {
	return err
}
if err := join.ApplyRight(rightChanges); err != nil {
	return err
}
movement := join.DataMovement()
```

The output is a `TypedTableJoinDataMovement` value:

```go
hatSql.TypedTableJoinDataMovement{
	LeftChanges:  1200,
	RightChanges: 800,
	LeftRows:     1400,
	RightRows:    900,
	LeftBytes:    118400,
	RightBytes:   74400,
}
```

`TypedTableJoinArrangement.DataMovement()` returns the same value through a
shared arrangement and returns an error after the arrangement lease is
released.

## Accounting Definition

The counters are cumulative from join construction:

- `LeftChanges` and `RightChanges` count every change record offered to
  `ApplyLeft` or `ApplyRight`, including stale, duplicate, and rejected input.
  Those records still crossed the caller's input boundary.
- `LeftRows` and `RightRows` count non-nil row images in `Before` and `After`.
  An insert or delete contributes one image; an update contributes two.
- `LeftBytes` and `RightBytes` use a deterministic logical estimate. Each
  record includes 8 bytes for sequence, the operation and key lengths, and
  each non-nil row includes its value count plus per-value kind/valid markers.
  Strings include a four-byte length and their contents; integers and floats
  use eight bytes; booleans use one byte.

The byte fields are deliberately codec-neutral. They are useful for comparing
partition movement and enforcing a logical budget, but they are not a claim
about JSON, binary, compression, or any other actual wire representation.
Counters saturate at `uint64` maximum instead of wrapping.

Tracking is disabled by default. Existing constructors retain the previous
join maintenance cost, and the option is forwarded by
`NewTypedTableJoinArrangementsWithOptions`.

## Benchmark

Run:

```text
make benchmark-c091-clean
```

The benchmark applies 4,096 two-column changes to a fresh join and includes
join construction in both paths (AMD Ryzen 9 5950X, Linux/amd64):

| Path | Median ns/op | B/op | allocs/op | Relative latency | Relative heap | Relative allocs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Tracking disabled | 1,596,047 | 1,622,135 | 5,961 | 1.00x | 1.00x | 1.00x |
| Tracking enabled | 1,644,166 | 1,622,132 | 5,961 | 1.03x slower | 1.00x | 1.00x |

The enabled accounting path costs about 3% CPU in this workload and creates no
additional allocations. Keep it enabled on distributed joins that need the
metric and leave it off on ordinary local joins.
