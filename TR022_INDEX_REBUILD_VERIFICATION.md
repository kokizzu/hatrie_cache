# TR-022 Index Rebuild Verification

`SQLIndexRebuildQueue` now accepts an optional `Verify` callback on each
`SQLIndexRebuildRequest`. The queue invokes it only after `Run` returns nil and
before publishing `SQLIndexRebuildSucceeded`.

```go
request := hatSql.SQLIndexRebuildRequest{
	ID:   "orders-by-customer-v3",
	Name: "orders_by_customer",
	Run: func(ctx context.Context, report hatSql.SQLIndexRebuildProgressFunc) error {
		return buildIndex(ctx, report)
	},
	Verify: func(ctx context.Context) error {
		return verifyIndex(ctx, "orders_by_customer")
	},
}
```

Verification uses the same cancellable task context. A verifier error marks the
task `failed` and stores the error text. A rebuild error skips verification.
`SQLIndexRebuildStatus.VerificationRequested` and `.Verified` make the result
observable. Requests without `Verify` retain the previous behavior and succeed
after `Run` completes.

This is an orchestration hook, not an automatic index validator: callers own
the verification query or checksum and any double-storage policy. The queue
remains bounded and background workers remain disabled when `Workers` is zero.

## Measured Cost

Five 100 ms samples on Linux/amd64 with an AMD Ryzen 9 5950X:

| Path | Median ns/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Rebuild without verification | 3,209 | 819 | 8 |
| Rebuild with no-op verification | 3,005 | 844 | 8 |

The CPU medians are within run-to-run noise; the no-op verifier retained 25
additional bytes per operation in this workload and did not add allocations.
The callback is opt-in, so callers that do not need verification pay no new
work. See [BENCHMARK.md](BENCHMARK.md#tr-022-index-rebuild-verification).
