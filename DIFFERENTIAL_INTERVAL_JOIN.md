# Differential Interval Join

`DifferentialTemporalJoin` also supports an inclusive distance interval. Set
both `MinTimeDistance` and `MaxTimeDistance` in its definition:

```go
join, err := hatSql.NewDifferentialTemporalJoin(hatSql.DifferentialTemporalJoinDefinition{
	MinTimeDistance: 2,
	MaxTimeDistance: 5,
	LeftKey:         leftKey,
	RightKey:        rightKey,
})
```

A pair matches when its equality keys match and the absolute timestamp
difference satisfies:

```text
MinTimeDistance <= abs(left.Time - right.Time) <= MaxTimeDistance
```

Both bounds are inclusive. The zero-value lower bound preserves the original
behavior, so existing definitions that set only `MaxTimeDistance` are
unchanged. A lower bound greater than the upper bound returns
`hatSql.ErrDifferentialTemporalJoinInvalidInterval` without creating a join.

All weighted multiplicity, signed output, row-prefix, cloning, and concurrency
semantics remain those described in
`DIFFERENTIAL_TEMPORAL_JOIN.md`.

The interval checks add only two integer comparisons to each indexed candidate
match. The existing temporal-join benchmark remains approximately `2.37-2.53
ms/op` for 256 retained left rows and 1,024 right updates producing 4,096
pairs, with about `2.386 MB/op` and `28,686` allocations.
