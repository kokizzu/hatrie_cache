# Staleness-Bounded Read Replica Selection

`hatReplication.SelectReadReplica` provides an explicit read policy for
replicas whose applied frontiers differ. A caller supplies the frontier it has
observed, an optional minimum frontier required by the read, and the maximum
lag it accepts.

```go
selected, err := hatReplication.SelectReadReplica(replicas,
	hatReplication.ReadReplicaPolicy{
		ObservedFrontier: 120,
		RequiredFrontier: 118,
		MaxLag:           2,
	})
```

Candidates older than `RequiredFrontier` or more than `MaxLag` behind
`ObservedFrontier` are excluded. `MaxLag: 0` requires a candidate at or ahead
of the observed frontier. Among eligible candidates, selection is deterministic:
newer frontier first, then higher `HealthScore`, then lexical node name.

The function is pure and does not perform network calls or mutate candidate
metadata. An empty or fully stale set returns
`hatReplication.ErrNoEligibleReadReplica`; blank node names return
`hatReplication.ErrReadReplicaNameRequired`.
