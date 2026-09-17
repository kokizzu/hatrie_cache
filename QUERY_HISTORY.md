# Query History Retention

`SQLQueryManager` retains privacy-safe status records, never SQL text,
parameters, source names, or row values. `HistoryCapacity` bounds retained
records. `HistorySampleEvery` optionally retains every Nth completed status;
zero or one keeps the legacy retain-every-completion behavior. Sampling is
deterministic and is applied before the bounded oldest-first ring buffer.

For example, capacity `2` with `HistorySampleEvery: 2` retains the third and
fifth completions after five requests. The option is available through
`NewSQLQueryManagerWithOptions`; `NewSQLQueryManager(capacity)` is unchanged.
The behavior is verified by `make test-query-history-sampling` and its race
counterpart.

The append benchmark remains allocation-free: retaining every status measures
`5.5-5.9 ns/op`, while retaining every 100th status measures `3.6-3.8 ns/op`,
with `0 B/op` and `0 allocs/op` in both cases. Use
`make benchmark-query-history-sampling` to reproduce it.

The optional durable `SQLQueryLog` has a separate probabilistic sampler for
reducing retained query-log volume. Configure `SQLQueryLogOptions.SampleRate`
and `SampleSeed`; `0` preserves retain-all behavior, while a value between zero
and one records a deterministic sample and exposes aggregate counters through
`SamplingStats`. See [CHU38_QUERY_LOG_SAMPLING.md](CHU38_QUERY_LOG_SAMPLING.md).
