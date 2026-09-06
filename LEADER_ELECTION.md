# Leader Election

`hat/hatTopology` provides deterministic leader selection that is independent
of query workers. `ElectionStore` reads a topology snapshot, tracks heartbeats,
expires nodes after the configured timeout, and selects the first healthy
candidate for each shard. `LeaderForKey` uses the same topology routing as the
node so callers can inspect the selected leader for a key.

```go
election := hatTopology.NewElectionStore(topology, hatTopology.ElectionOptions{
	Timeout: 15 * time.Second,
})
if err := election.Heartbeat("node-a"); err != nil {
	return err
}

route, ok := election.LeaderForKey("account:42")
if !ok || !route.Leader.Available {
	return errors.New("no available leader")
}
```

The election store exposes status, active and inactive nodes, orphan pruning,
heartbeats, and explicit offline marking. Selection is deterministic for the
same topology and liveness state, which lets peers compare election results.
The topology provider remains the source of shard membership; this component
does not claim consensus or replace split-brain fencing. Mutating command
enforcement remains opt-in through the existing node configuration.

The cache process starts the heartbeat loop independently from query workers,
and the `election` CLI/API commands expose route and status inspection.
