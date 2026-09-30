# T210 Master-Master Conflict Hooks

`ConflictPolicyRegistry.ResolveWithHook` adds an explicit, opt-in observer for
master-master conflicts. The hook receives both competing `ConflictVersion`
values, so `NodeID` and `Sequence` identify the source and source-local order.
It runs after the configured policy has selected a winner or rejected the
conflict.

```go
registry, err := hatReplication.NewConflictPolicyRegistry(
	hatReplication.ConflictPolicy{Mode: hatReplication.ConflictPolicyLastWriteWins},
)
if err != nil {
	return err
}

winner, err := registry.ResolveWithHook(
	"orders",
	left,
	right,
	func(event hatReplication.ConflictResolutionEvent) {
		logConflict(event.Space, event.Left.NodeID, event.Left.Sequence,
			event.Right.NodeID, event.Right.Sequence, event.Decision)
	},
)
```

Hooks are called only for valid, non-equal conflicts. A rejected conflict has
`Decision == ConflictResolutionRejected` and a zero `Winner`; a successful
event identifies the left or right winner. The callback cannot change the
result and must be safe for concurrent calls. The event contains bounded
version and space metadata only; callers remain responsible for any redacted
key digest they want to store.

`ConflictPolicyRegistry.Resolve` remains the default path and does not invoke
a callback or add callback-related allocations.

## Benchmark

The benchmark used three one-second samples on an AMD Ryzen 9 5950X. The
baseline is the clean T208 commit before hooks.

| Operation | Baseline | Current | Relative latency |
| --- | ---: | ---: | ---: |
| Resolve without hook | 11.19 ns/op, 0 B/op, 0 allocs/op | 10.85 ns/op, 0 B/op, 0 allocs/op | 1.03x faster observed |
| Resolve with hook | n/a | 26.28 ns/op, 0 B/op, 0 allocs/op | 2.42x the current no-hook latency |

The no-hook result is within normal benchmark noise and preserves the default
cost. Hook dispatch adds about 15 ns/op but no heap allocation, making the
tradeoff explicit and opt-in.
