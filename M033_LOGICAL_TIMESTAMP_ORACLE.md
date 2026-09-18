# M033: Batched Logical Timestamp Oracle

Status: partially adopted as an opt-in `hatSql` primitive.

## Scope

`SQLLogicalTimestampOracle` provides monotone process-local logical timestamps
for source batches and other callers that need deterministic ordering. The
`Reserve(count)` method allocates one contiguous range with one atomic update;
`Next()` is the single-record form, and `Observe()` advances the local frontier
when a newer externally assigned timestamp is received.

The zero value is not used implicitly anywhere in SQL execution or source
ingestion. Callers construct the oracle explicitly, so ordinary queries and
writes retain their current behavior and cost.

```go
oracle := hatSql.NewSQLLogicalTimestampOracle(lastRestoredTimestamp)
reserved, err := oracle.Reserve(len(sourceBatch))
if err != nil {
	return err
}
for index, record := range sourceBatch {
	commitTimestamp := reserved.Start + uint64(index)
	_ = commitTimestamp // attach it to the caller-owned source record
	_ = record
}
```

Reservations are contiguous and non-overlapping under concurrent callers.
Overflow and non-positive counts are rejected without changing the frontier.

## Distributed boundary

This is the local allocation half of a timestamp-oracle design. It does not
provide cross-process uniqueness, durable recovery, clock uncertainty bounds,
or consensus. A distributed deployment must restore the frontier and combine
the oracle with an existing leader, quorum, or consensus protocol before
using timestamps as a global commit order.

## Test-first verification

The new tests were run before implementation and failed on the missing public
API. After implementation these targets passed:

```text
make test-m033
make race-m033
make vet-m033
make test-m033-package
```

The tests cover monotone reservations, external observation, nil/count/
overflow validation, and concurrent range non-overlap.

## Benchmark

The workload allocates 1,024 timestamps per benchmark iteration. The control
performs 1,024 individual `atomic.Uint64.Add` operations. The new path
performs one `Reserve(1024)` operation. Five `-benchmem` samples ran on
Linux/amd64 with an AMD Ryzen 9 5950X.

| Path | Raw ns/op samples | Median ns/op | B/op | Allocs/op | Relative result |
| --- | --- | ---: | ---: | ---: | --- |
| Individual atomic increments | 1,769; 1,798; 1,777; 1,806; 1,849 | 1,798 | 0 | 0 | baseline |
| `Reserve(1024)` | 2.268; 2.190; 2.221; 2.406; 2.162 | 2.221 | 0 | 0 | 810x faster for the same 1,024 timestamps |

The comparison measures timestamp allocation only; it does not claim that
source parsing or durable writes become 810x faster. The gain comes from
amortizing one atomic frontier update across a source batch, with no retained
dictionary, heap allocation, or default-path behavior change.

Benchmark commands:

```text
make baseline-m033
make benchmark-m033
```
