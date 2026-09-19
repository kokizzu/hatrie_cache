# M-U11: Arrangement Reuse Advisor

M-U11 adds a read-only advisor for existing typed-table aggregate and join
arrangement catalogs. It helps a planner decide whether a requested dataflow
can reuse an exact arrangement, must hydrate a stale arrangement first, or
needs a new arrangement.

## Scope

The advisor is deliberately conservative:

- Aggregate compatibility requires the same table name and the same ordered
  `GROUP BY`, `SUM`, `MIN`, `MAX`, and `COUNT DISTINCT` definition.
- Join compatibility requires the same left table, right table, field pair,
  and left/right orientation.
- A candidate is stale when its snapshot says so or when either checkpoint is
  behind its source sequence.
- Stale candidates receive `hydrate_then_reuse`; they are never reported as
  immediately reusable. The caller must handle `ErrTypedTableChangesCompacted`
  and rebuild when the retained changefeed is insufficient.
- The advisor does not acquire, mutate, hydrate, or release an arrangement.

## Usage

Take a catalog snapshot, then ask for advice at plan construction time:

```go
request := hatSql.TypedTableAggregateArrangementRequest{
	TableName: "orders",
	Definition: hatSql.TypedTableAggregateDefinition{
		GroupBy:  []string{"region"},
		SumField: "amount",
	},
	EstimatedStateRows: 10000,
}
advice := hatSql.AdviseTypedTableAggregateArrangement(
	arrangements.Snapshot(),
	request,
	hatSql.DefaultTypedTableArrangementAdvisorOptions(),
)

switch advice.Action {
case hatSql.TypedTableArrangementAdvisorReuse:
	// Acquire the exact definition and use the fresh shared state.
case hatSql.TypedTableArrangementAdvisorHydrateThenReuse:
	// Acquire, hydrate in bounded batches, and rebuild if the changefeed gap
	// has already been compacted.
case hatSql.TypedTableArrangementAdvisorCreate:
	// Acquire and build a new arrangement.
}
```

Use `AdviseTypedTableJoinArrangement` with a
`TypedTableJoinArrangementRequest` for joins. `Candidates` is deterministic:
fresh reuse candidates sort before hydration candidates, then higher reference
counts and newer checkpoints are preferred. The same catalog in a different
input order produces the same advice.

## Memory model

`TypedTableArrangementAdvisorOptions` estimates incremental memory, not exact
allocator usage:

| Setting | Default | Used for |
| --- | ---: | --- |
| `AggregateStateBytesPerRow` | 160 | One retained aggregate group |
| `JoinStateBytesPerRow` | 128 | One retained join pair |
| `FixedArrangementBytes` | 256 | Arrangement metadata and fixed indexes |
| `ReplayBytesPerChange` | 64 | Transient stale-hydration replay |

Fresh reuse reports zero additional memory. Creation reports fixed bytes plus
estimated retained state rows. Stale hydration reports transient bytes for
the checkpoint gap. Arithmetic saturates at `math.MaxUint64` so a bad estimate
cannot wrap into an artificially small admission decision. Set nonzero custom
values when workload-specific measurements are available; zero fields use the
sane defaults.

## Cost and tradeoff

The advisor is a planning operation and is not called by row processing. The
following measurements use an AMD Ryzen 9 5950X, Go benchmark defaults, five
runs, and a 64-entry catalog for the fresh-reuse cases:

| Case | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Aggregate advisor, fresh exact match | 1,101 | 344 | 9 |
| Join advisor, fresh exact match | 504.3 | 184 | 2 |
| Aggregate advisor, create/no match | 245.2 | 80 | 4 |
| Join advisor, create/no match | 79.83 | 24 | 1 |

The existing snapshot path remains at 160 B/op and 2 allocs/op for aggregate
snapshots, and 128 B/op and 1 alloc/op for join snapshots. Separate runs on a
busy shared host measured aggregate snapshot medians of 203.4 ns before and
224.7 ns after, and join snapshot medians of 149.3 ns before and 167.2 ns
after; the snapshot implementation was not changed, so this timing difference
is treated as host noise rather than a claimed regression. The advisor adds no
cost unless a planner invokes it.

The tradeoff is therefore explicit: a planner pays a small bounded allocation
cost at plan construction to avoid duplicate long-lived arrangement state and
to avoid accidentally using stale results. It should not be called inside a
per-row or per-update loop.

## Verification

```text
make test-mu11
make benchmark-mu11-baseline
make benchmark-mu11
make verify-mu11
```

The focused tests cover deterministic catalog ordering, exact aggregate and
join matching, orientation, stale checkpoints, memory estimates, and the
create fallback. `verify-mu11` runs normal tests, race tests, and vet.
