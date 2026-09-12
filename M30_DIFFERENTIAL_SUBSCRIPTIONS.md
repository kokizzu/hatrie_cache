# M-G30: Differential Subscription Envelopes

Status: accepted and implemented as an opt-in API.

## Motivation

Materialize-style differential output lets a consumer maintain a multiset from
signed row changes instead of receiving the complete query result after every
refresh. This is useful for remote consumers and projections where one changed
row should not transfer every unchanged row.

The existing `QuerySubscription` API and its snapshot channel are unchanged.
Callers opt in with `SubscribeDifferential`.

## Usage

```go
subscription, err := registry.SubscribeDifferential(
    ctx,
    hatSql.QuerySubscriptionDefinition{
        Query:        "FROM CACHE('people') SELECT id, name",
        Dependencies: []string{"people"},
        EmitProgress: true,
    },
    resolver,
    hatSql.QueryOptions{},
)
if err != nil {
    // handle invalid query or source failure
}
defer subscription.Close()

for batch := range subscription.Updates() {
    if batch.Reset {
        // discard the consumer's current multiset before applying this batch
    }
    for _, delta := range batch.Deltas {
        // apply delta.Row delta.Diff times to the consumer's multiset
    }
    if batch.Progress {
        // the consumer has reached batch.Frontier
    }
}
```

`QuerySubscriptionDeltaBatch` contains the subscription ID, revision, logical
frontier, result columns, signed row deltas, and completion state. Positive
`Diff` inserts multiplicity; negative `Diff` retracts multiplicity. Duplicate
rows are combined into one delta, and row values retain their original Go
types, including `int64`, `uint64`, `[]byte`, and `nil`.

The initial non-`StartLive` snapshot is emitted as positive deltas. A changed
result emits only the net multiplicity difference: retractions are ordered by
the old result and additions by the new result. `EmitProgress` adds frontier
records with no row deltas. `UpTo` completion is propagated and closes the
channel after the final records.

## Bounded delivery and reset

The differential channel is bounded. Like the existing snapshot subscription,
rapid updates may be coalesced. A differential consumer cannot safely apply a
later delta if an earlier delta was discarded, so coalescing drains pending
batches and emits `Reset: true` with the complete current result as positive
deltas. Consumers must clear their local multiset before applying that batch.
This makes overload visible and prevents silent state divergence.

## Benchmark

Command:

```text
make benchmark-m-g30-differential-subscription-local-clean
```

Environment: Linux, amd64, AMD Ryzen 9 5950X. Five samples with
`-benchtime=200ms`. The fixture contains 1,024 rows and changes one row. The
timed operation includes the existing full-snapshot clone and JSON encoding for
the baseline, or exact row-difference construction and JSON encoding for the
differential path.

| Workload | Full snapshot | Differential batch | Differential result |
| --- | ---: | ---: | ---: |
| CPU | 599,613 ns/op | 1,124,194 ns/op | 1.87x slower |
| Heap | 542,645 B/op | 545,535 B/op | 0.5% higher |
| Allocations | 7,174 allocs/op | 12,275 allocs/op | 1.71x higher |
| Serialized payload | 31,681 B/op | 211 B/op | 150.1x smaller |

This is a transfer optimization, not a local CPU optimization. It is kept
opt-in because the exact diff currently scans both result row sets and costs
CPU and allocations. It is a good fit when network bandwidth or downstream
snapshot copying dominates; the legacy snapshot path remains preferable for
local, CPU-bound consumers.

Raw samples:

```text
full_snapshot: 599549 589217 601519 660249 599613 ns/op, 31681 payload-B/op, about 7174 allocs/op
differential:  1118527 1143709 1124194 1146625 1099202 ns/op, 211 payload-B/op, 12275 allocs/op
```

## Verification

Passed:

- `make test-m-g30-differential-subscription-local-clean`
- `make test-m-g30-package-local-clean`
- `make race-m-g30-differential-subscription-local-clean`
- `make vet-m-g30-differential-subscription-local-clean`
- `make benchmark-m-g30-differential-subscription-local-clean`
