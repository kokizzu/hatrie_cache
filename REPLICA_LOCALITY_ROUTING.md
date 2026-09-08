# Replica Locality Routing

`hatReplication.SelectReadReplicaWithConsistency` accepts an optional ordered
`ReadReplicaPolicy.PreferredRegions` list and `ReadReplicaProgress.Region` for
each candidate:

```go
selected, err := hatReplication.SelectReadReplicaWithConsistency(
    candidates,
    hatReplication.ReadReplicaPolicy{
        ObservedFrontier:  currentFrontier,
        RequiredFrontier:  sessionFrontier,
        MaxLag:            2,
        PreferredRegions:  []string{"asia", "us"},
    },
    hatReplication.ReadConsistencyReadAfterWrite,
)
```

Consistency filtering always runs first. Among eligible candidates, the first
matching preferred region wins; freshness, health, and node name remain the
deterministic tie-breakers. If no preferred region is available, selection
falls back to every eligible candidate. Region matching trims whitespace and
is case-insensitive.

The zero value has no preferred regions and preserves the existing selection
order. This is a routing policy primitive: it does not discover endpoints,
move data, or force cross-region failover. Callers choose whether a locality
miss should be accepted or surfaced as an error.

## Measured Cost

On Linux/amd64 with an AMD Ryzen 9 5950X, selecting from 1,024 candidates over
five benchmark samples measured a median of `16.7 us/op` for the unchanged
selector and `17.4 us/op` with two preferred regions. Both paths used `0 B/op`
and `0 allocs/op`; the locality preference is opt-in, so the default path does
not pay the region-ranking loop.
