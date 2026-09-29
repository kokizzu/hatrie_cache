# Per-Space Synchronous Write Quorum

T201 adds `hatReplication.SpaceWriteQuorum` for deployments that need
synchronous acknowledgement only for selected critical spaces. It reuses the
existing `JournalWriteQuorum` validation and exact sequence/fence contract;
ordinary spaces remain on the caller's asynchronous replication path.

## Configuration

The zero value is disabled. Enable it explicitly and list only the critical
spaces:

```go
policy, err := hatReplication.NewSpaceWriteQuorum(hatReplication.SpaceWriteQuorumOptions{
    Enabled: true,
    Spaces: map[string]hatReplication.JournalWriteQuorumOptions{
        "payments": {
            Enabled:  true,
            Voters:   []string{"primary", "replica-a", "replica-b"},
            Required: 2,
        },
    },
})
```

`NewSpaceWriteQuorum` copies the configuration, normalizes names, rejects
duplicate normalized names, and bounds the number and size of configured
spaces. A disabled configuration does not validate or retain space entries.

Resolve a policy once when a space is initialized and reuse the returned
`*JournalWriteQuorum` in the write loop:

```go
payments, err := policy.ForSpace("payments")
if err != nil {
    return err
}
decision, err := payments.Execute(ctx, proposal, acknowledge)
```

`ForSpace` returns `ErrSpaceWriteQuorumDisabled` for the default-off policy and
`ErrSpaceWriteQuorumNotConfigured` for a non-critical space. The convenience
`SpaceWriteQuorum.Evaluate` and `Execute` methods perform a map lookup on each
call and are intended for low-frequency administrative paths; `ForSpace` is the
hot-path API.

The selector does not perform transport, storage, or transaction work. The
embedding service must submit the write to the selected quorum and decide how
to handle an unsatisfied result. No quorum policy is installed implicitly.

## Safety

Each selected quorum still requires exact proposal sequence and fence-token
matches. Stale, unknown, duplicate, or rejected acknowledgements do not count.
Invalid configuration is rejected before construction. Since the policy map is
copied and not mutated after construction, concurrent reads are safe when the
returned coordinator is shared by space workers.

## Measurement

The parent control was measured before implementation with the existing
`BenchmarkTU10JournalWriteQuorumEvaluate`: five 200 ms samples had a median of
`38.52 ns/op`, `0 B/op`, and `0 allocs/op` (that benchmark includes setup in
its timer). The final same-fixture comparison resets the timer after setup and
uses the following five samples on an AMD Ryzen 9 5950X, linux/amd64:

| Path | Raw ns/op samples | Median ns/op | B/op | Allocs/op | Relative CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| Cached `ForSpace` policy | 23.69, 23.25, 23.23, 23.43, 23.51 | 23.43 | 0 | 0 | 1.00x control |
| Journal quorum control | 22.70, 22.56, 23.35, 26.08, 26.51 | 23.35 | 0 | 0 | baseline |
| Per-call selector convenience | 41.75, 41.37, 41.87, 39.55, 37.89 | 41.37 | 0 | 0 | 1.77x control |
| Default-off check | 0.5295, 0.4910, 0.5071, 0.5225, 0.4853 | 0.5071 | 0 | 0 | no allocation |

The feature is retained because the intended cached-policy path adds no memory
or allocation cost and only a small CPU difference within normal benchmark
noise; the existing quorum path is unchanged. Do not call the convenience
selector for every record in a hot loop.

Run the reproducible benchmark with:

```text
make benchmark-t201 COUNT=5 BENCHTIME=200ms
```
