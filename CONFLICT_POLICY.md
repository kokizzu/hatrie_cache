# Per-Space Conflict Policy

`hat/hatReplication.ConflictPolicyRegistry` adds explicit conflict behavior
for named spaces without changing the existing `ResolveConflictVersion` API or
its default last-write-wins behavior.

## Configure

```go
registry, err := hatReplication.NewConflictPolicyRegistry(
    hatReplication.ConflictPolicy{Mode: hatReplication.ConflictPolicyLastWriteWins},
)
if err != nil {
    return err
}

err = registry.Set("payments", hatReplication.ConflictPolicy{
    Mode:           hatReplication.ConflictPolicySourcePriority,
    SourcePriority: []string{"region-eu", "region-us"},
})
```

The first source in `SourcePriority` wins. Sources not listed are considered
after every listed source; if both are unknown, the normal timestamp, node, and
sequence ordering is used as a deterministic tie-breaker.

## Modes

| Mode | Behavior |
| --- | --- |
| `ConflictPolicyLastWriteWins` | Existing timestamp, node, then sequence ordering. This is the zero-value/default mode. |
| `ConflictPolicySourcePriority` | Prefer configured sources, then use last-write-wins when both versions have equal priority. |
| `ConflictPolicyReject` | Return `ErrConflictRejected` for distinct versions; equal versions remain idempotent. |

Resolve a named space explicitly:

```go
winner, err := registry.Resolve("payments", left, right)
```

`Set` trims and copies space/source names, rejects duplicate or empty sources,
and bounds the registry to `MaxConflictPolicySpaces` spaces and
`MaxConflictPolicySources` sources per policy. `Delete` removes one override;
after deletion the registry default applies again.

The registry is an opt-in policy object. It does not install global state, add
work to callers that continue using `ResolveConflictVersion`, or make a
transport decision. The caller remains responsible for invoking it at the
point where a space's conflict policy is known.

## Benchmark

Command:

```text
make benchmark-t-u11
```

Five local runs on the AMD Ryzen 9 5950X environment:

| Path | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Direct existing LWW resolver | 2.624 | 0 | 0 |
| Registry default LWW | 17.02 | 0 | 0 |
| Registry source priority | 22.10 | 0 | 0 |

The registry adds a locked map lookup and, for source priority, a bounded
linear scan. It remains allocation-free; the direct resolver remains the
lowest-overhead choice when no per-space policy is needed.
