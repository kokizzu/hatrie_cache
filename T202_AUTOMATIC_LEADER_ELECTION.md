# T202: Automatic Leader Election

Hatrie Cache can keep replica liveness current and automatically select the
first available owner for each shard. The existing primary-first selection
rule is unchanged: when the primary is healthy it remains the leader; when it
is offline or its heartbeat expires, the next configured replica is selected.

## Configuration

`ElectionOptions.RequireHeartbeat` defaults to `false`. In the default mode,
nodes that have not sent a heartbeat are treated as available, preserving the
behavior of existing deployments.

Set it to `true` when every serving node must prove liveness before it can be
elected:

```go
election := hatTopology.NewElectionStore(topology, hatTopology.ElectionOptions{
    Timeout:          15 * time.Second,
    RequireHeartbeat: true,
})
```

## Running Heartbeats

`ElectionStore.Run` records an immediate heartbeat, then refreshes it at the
requested interval until the context is canceled. The caller owns the
goroutine and its lifecycle:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

go func() {
    if err := election.Run(ctx, "node-a", 5*time.Second); err != nil {
        log.Printf("election heartbeat stopped: %v", err)
    }
}()
```

Passing an interval of `0` or less uses
`DefaultElectionHeartbeatInterval` (`DefaultElectionTimeout / 3`). Running
the loop on each serving node makes `LeaderForKey` observe promotion without a
separate election coordinator. A missing or invalid node ID returns an error.

The same options and `Run` method are exposed through the compatibility
`hatCache` package wrapper.

## Safety Boundary

This feature is deterministic, topology-driven liveness selection. It does
not provide consensus, a lease, quorum authorization, or fencing against a
stale writer. A deployment that permits writes must still use the existing
leader-write enforcement and the planned stale-writer fencing work before
treating a failover as a split-brain-safe write handoff.

## Measurement

On the repository benchmark host, `BenchmarkT202LeaderForKey` produced these
five-run medians:

| Mode | Median lookup | Memory | Allocations |
| --- | ---: | ---: | ---: |
| Legacy assumed-online (default) | 419.1 ns/op | 172 B/op | 7 allocs/op |
| Heartbeat required | 431.4 ns/op | 172 B/op | 7 allocs/op |

The opt-in check was about `12.3 ns` (`2.9%`) slower in this lookup workload,
with no additional heap allocation.
