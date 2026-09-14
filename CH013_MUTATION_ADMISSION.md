# CH-013: Mutation Admission Throttling

Status: implemented as an opt-in SQL mutation gate.

## Purpose

ClickHouse-style mutation control is useful when a burst of writes, updates,
or deletes would compete with foreground reads. `SQLMutationAdmission` gives
callers one bounded gate that can space mutation starts and pause them during
configured daily maintenance windows.

The default is unchanged: `SQLQueryOptions.MutationAdmission` is `nil`, and
ordinary SQL mutations do not take a clock or gate lock. A zero-value admission
object is also a no-op.

## Usage

```go
admission, err := hatSql.NewSQLMutationAdmission(
    hatSql.SQLMutationAdmissionOptions{
        MinInterval: 10 * time.Millisecond,
        MaintenanceWindows: []hatSql.SQLMutationMaintenanceWindow{
            {
                Weekdays: [7]bool{true, true, true, true, true, true, true},
                Start:    2 * time.Hour,
                End:      4 * time.Hour,
            },
        },
    },
)
if err != nil {
    return err
}

options := hatCache.SQLQueryOptions{MutationAdmission: admission}
result, err := hatCache.ExecuteSQLMutation(ctx, trie, query, args, options)
```

`Weekdays` uses `time.Weekday` indexes, so Sunday is index 0. Window times are
interpreted in UTC unless `Location` is supplied. An end before a start means
the window crosses midnight. Windows are copied and validated at construction.

All callers sharing an admission object share its serialized interval. The
first caller proceeds immediately; later callers wait for reserved slots.
Maintenance windows take precedence over interval slots. Context cancellation
returns without executing the mutation. A canceled caller may already have
reserved a future interval, so a retry should use the same policy deliberately.

The gate is applied by `ExecuteSQLMutation` and by the journal-backed
`ExecuteSQLMutationIdempotent` path. It is not applied to read queries. No
durable state, wire format, journal record, or authentication policy is added;
the caller owns the admission object and should scope it per tenant or service
when independent budgets are required.

## Configuration guidance

- Leave `MutationAdmission` nil for the normal low-latency default.
- Use a small positive `MinInterval` to smooth bursts without imposing a
  fixed batch delay.
- Use a maintenance window only when writes must wait for a controlled period;
  callers should set a context deadline so shutdown does not wait forever.
- Use separate admission objects for independent tenants or regions. One
  shared object intentionally serializes all mutations that reference it.

## Measurements

The paired mutation benchmark uses a zero-interval admission object to isolate
configuration overhead without sleeping. It retained the same 8,096 B/op and
18 allocations/op as the paired nil/default path. The median was 6,259 ns/op
for the default path and 6,461 ns/op with the configured no-op gate, about 1.03x
or 3.2% slower in this noisy end-to-end workload.

The exact legacy mutation benchmark was mixed across runs: `SET` measured
8,345 ns/op before and 8,230 ns/op after; `ON CONFLICT DO NOTHING` measured
6,510 and 6,765 ns/op; `ON CONFLICT DO UPDATE` measured 6,184 and 6,294
ns/op. Every case retained its original bytes/op and allocations/op. The
default path therefore has no measured memory cost; interval and maintenance
waiting are intentional costs paid only when configured.

See [BENCHMARK.md](BENCHMARK.md#ch-013-mutation-admission-throttling) for raw
samples and commands.

## Verification

Focused tests cover invalid intervals, invalid windows, serialized slot
reservation, overnight windows, cancellation, and end-to-end mutation
rejection before trie execution. The full `hatSql` and `hatCache` tests, race,
vet, and diff checks pass.
