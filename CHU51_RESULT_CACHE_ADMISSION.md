# CH-U51 Result-Cache Admission

This feature adds an opt-in cost gate for result-cache retention. It is useful
when a workload contains many one-shot or cheap queries whose snapshot-copy
cost is greater than the value of retaining their results.

## API

```go
cache, err := hatSql.NewSQLResultCacheWithAdmission(
	1024,
	hatSql.ResultCacheAdmissionPolicy{
		MinExecutionDuration: time.Millisecond,
	},
)
```

Successful executions shorter than the configured threshold are returned to
the caller but are not retained. Successful executions at or above the
threshold are retained normally. The policy applies to both `Execute` and
`ExecuteVersioned`; dependency-aware constructors are available through
`NewResultCacheWithDependenciesAndAdmission` and
`NewSQLResultCacheWithDependenciesAndAdmission`.

The default constructors remain unchanged and always admit successful fresh
results. A zero threshold also preserves that behavior. Negative thresholds
return `ErrResultCacheAdmissionInvalid`.

Use `AdmissionStats()` to inspect admitted and rejected retention decisions.
Rejected results increment the normal cache bypass counter, while executor
errors and stale version results are returned without being counted as
admission rejections.

## Measurement

Environment: Linux/amd64, AMD Ryzen 9 5950X 16-Core Processor. Five samples
were collected with the same 1,024 distinct-key workload and `-benchmem`.
Each executor returned a small one-row result. The baseline used the existing
`NewSQLResultCache`; the final path used a 1 ms admission threshold, so all
fast results were returned but not retained.

| Path | Median ns/op | Median B/op | Median allocs/op | Entries | Rejected | Relative time | Relative memory |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Existing cache, retain every result | 306.3 | 344 | 3 | 1,024 | 0 | 1.00x | 1.00x |
| Admission gate, reject fast results | 106.1 | 0 | 0 | 0 | all measured operations | 2.89x faster | 0 B/op |

Raw baseline samples:

```text
315.1 ns/op 344 B/op 3 allocs/op
306.4 ns/op 344 B/op 3 allocs/op
306.3 ns/op 344 B/op 3 allocs/op
306.2 ns/op 344 B/op 3 allocs/op
304.8 ns/op 344 B/op 3 allocs/op
```

Raw admission samples:

```text
104.0 ns/op 0 B/op 0 allocs/op
106.1 ns/op 0 B/op 0 allocs/op
106.6 ns/op 0 B/op 0 allocs/op
103.5 ns/op 0 B/op 0 allocs/op
112.1 ns/op 0 B/op 0 allocs/op
```

The rejected count is cumulative over each benchmark process and is included
to verify that the gate actually bypasses retention. The improvement comes
from avoiding the result snapshot and map insertion; it is not a claim that
admitted cache hits are faster. A threshold that is too high can reduce hit
rate, so callers should choose it from measured execution cost and query
reuse, then monitor `AdmissionStats` and regular cache hit/miss counters.

The feature is disabled by default. Enabling it adds one timestamp and one
duration check per successful cache miss; the default constructor does not
take a timestamp and has no admission counters incremented.
