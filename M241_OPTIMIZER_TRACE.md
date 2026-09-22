# M241 Optimizer Trace

M241 adds an opt-in trace for the existing SQL optimizer-rule API. It records
the ordered rule applications and lets a rule report alternatives it examined
but rejected.

## Usage

Set `SQLQueryOptions.OptimizerTrace` to `true`. A rule can call
`RejectAlternative` while evaluating a candidate:

```go
result, err := hatSql.ExecuteSQLQueryContext(ctx,
    "EXPLAIN FROM CACHE('orders') AS o WHERE o.id = 1 SELECT o.id",
    resolver,
    hatSql.SQLQueryOptions{
        OptimizerTrace: true,
        Optimizer: hatSql.NewSQLQueryOptimizer(
            func(plan *hatSql.SQLQueryOptimizationContext) error {
                plan.RejectAlternative("bitmap index", "requires a composite key")
                plan.IndexHint = hatSql.SQLIndexHint{
                    Source: "o",
                    Field:  "id",
                    Mode:   hatSql.SQLIndexHintForce,
                }
                return nil
            },
        ),
    },
)
```

The result contains `OptimizerTrace` with format
`hatrie-cache-sql-optimizer-trace/v1`. Rule numbers are one-based and retain
the order passed to `NewSQLQueryOptimizer`. Each rule emits an `applied` event;
each `RejectAlternative` call emits a following `rejected` event for that
rule. `Changed` identifies whether the rule changed the planner's index-hint
state. Trace output is also included for non-EXPLAIN materialized results when
the option is enabled.

Example:

```json
{
  "optimizer_trace": {
    "format": "hatrie-cache-sql-optimizer-trace/v1",
    "events": [
      {"rule": 1, "kind": "rule", "action": "applied", "expression": "rule_1", "changed": true},
      {"rule": 1, "kind": "alternative", "action": "rejected", "expression": "bitmap index", "reason": "requires a composite key"}
    ]
  }
}
```

The default is off. With tracing disabled, `RejectAlternative` is a no-op and
the normal result omits `optimizer_trace`.

## Tradeoff

The benchmark used five one-second `-benchmem` samples on Linux amd64,
AMD Ryzen 9 5950X. The parent was `d4d07b29`; it contains M240 plus the
cleanup-target commit but no M241 package changes. The workload was
`EXPLAIN FROM VALUES (1) AS values(id) SELECT id`.

| Workload | Parent median ns/op | M241 median ns/op | M241 B/op | M241 allocs/op | Wire bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Default EXPLAIN | 8,317 | 9,980 | 9,280 | 35 | n/a |
| Optimizer, trace off | 11,628 | 12,351 | 11,856 | 44 | n/a |
| Optimizer, trace on | n/a | 13,808 | 12,421 | 51 | n/a |
| JSON serialization with trace | n/a | 3,935 | 1,090 | 14 | 570 |

The default and optimizer-without-trace paths retain the parent's allocation
profiles. CPU is noisy for this short diagnostic workload, so the table does
not claim a speedup. Trace collection adds seven allocations and about 565
bytes/op versus the current optimizer-without-trace fixture; JSON adds a
570-byte payload for one applied rule and one rejected alternative.

Raw samples and the repeatable command are in [BENCHMARK.md](BENCHMARK.md).
