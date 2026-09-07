# Explicit Read Quorum

`hatReplication.ExecuteReadQuorum` is an opt-in executor for reads that need a
matching value from an explicit number of named replicas. It complements
`ExecuteWriteQuorum` without changing ordinary read routing or asynchronous
replication.

```go
result, err := hatReplication.ExecuteReadQuorum(
	ctx,
	[]string{"local", "east", "west"},
	2,
	func(ctx context.Context, node string) (any, error) {
		return readFromReplica(ctx, node, key)
	},
	func(left, right any) bool {
		return left.(string) == right.(string)
	},
)
if err != nil {
		return err
}
value := result.Value
```

## Contract

- Every supplied replica callback runs concurrently and results remain in the
  caller's input order.
- `required` counts matching successful values, not merely successful network
  calls. A successful callback is recorded in its attempt even when its value
  disagrees with the returned quorum value.
- `ErrReadQuorumUnsatisfied` means fewer than `required` callbacks succeeded.
  `ErrReadQuorumInconsistent` means enough callbacks succeeded, but no value
  group reached `required`.
- A nil comparator uses `reflect.DeepEqual`. Typed comparators avoid reflection
  on hot paths and should define the application's version or value semantics.
- A nil or canceled context is rejected, and callbacks should honor context
  cancellation themselves. Callback errors are retained in `Attempts`.
- The helper does not reconcile stale values, retry failed replicas, or mutate
  replication state. The caller owns transport authentication, version choice,
  repair, and any value copying required after return.

The executor reads all targets before deciding, so the cost is explicit and
predictable. It is not enabled by any default configuration.

## Measured Cost

Five `-count=5` benchmark runs of three no-allocation callbacks on Linux/amd64
with an AMD Ryzen 9 5950X measured:

| Mode | Time | Heap | Allocations |
| --- | ---: | ---: | ---: |
| Typed comparator | 1.42-1.48 us/op | 864 B/op | 12/op |
| `reflect.DeepEqual` fallback | 1.53-1.56 us/op | 864 B/op | 12/op |

The benchmark measures coordination and result grouping only; network and
replica storage time are supplied by the callback.

## Verification

```text
make test-read-quorum-clean
make verify-quorum-evidence-clean
make benchmark-read-quorum-clean
```
