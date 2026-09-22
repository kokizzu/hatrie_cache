# T202 Replica-Set Leader Election

T202 adds an opt-in, transport-neutral leader-election state machine for a
fixed replica-set voter list. It turns authenticated heartbeat observations
into a deterministic promotion proposal, but it does not start a background
goroutine, send network traffic, persist state, or replace the existing
consensus/replication transport.

## Defaults

The feature is disabled unless `Enabled: true` is supplied. The default
election timeout is five seconds, the default member bound is 64, and
`QuorumSize: 0` means a strict majority of the configured voters. The existing
replication and failover paths are unchanged when this state machine is not
constructed or is left disabled.

```go
election, err := hatReplication.NewReplicaSetLeaderElection(
	hatReplication.ReplicaSetLeaderElectionOptions{
		Enabled:       true,
		Voters:        []string{"node-a", "node-b", "node-c"},
		ElectionTimeout: 5 * time.Second,
		InitialLeader:  "node-a",
		InitialTerm:    1,
	},
)
```

The constructor canonicalizes voter order, rejects duplicates and invalid
identities, and requires an initial term when an initial leader is supplied.

## Election lifecycle

1. The caller authenticates a heartbeat and calls `ObserveHeartbeat` with the
   voter ID, applied sequence, health bit, and the local receipt time.
2. The caller periodically calls `TryElect`. A healthy current leader with a
   heartbeat inside the timeout blocks a new election.
3. When the current leader is expired or absent, the state machine requires a
   quorum of fresh, healthy voters. It selects the highest applied sequence;
   equal sequences use lexicographically smallest node ID for deterministic
   convergence.
4. `TryElect` returns a proposal with the next term and fencing token and
   records exactly one pending proposal. A second proposal is rejected until
   the first is committed or cancelled.
5. The caller performs its external consensus/topology action, then calls
   `Commit` with the exact proposal. Commit rechecks that the candidate is
   fresh, healthy, and still at least as caught up as when selected. A stale
   proposal cannot commit.
6. `Cancel` invalidates a pending proposal and advances the generation so a
   previously copied proposal cannot be reused.

`Snapshot` returns detached, node-sorted observation state for status or
persistence by the embedding service. Heartbeats must be authenticated and
bound to the same voter configuration by the caller; this package deliberately
does not claim that an arbitrary local call is trustworthy.

## Safety boundary

This is an election decision and fencing primitive, not a complete consensus
protocol. The caller still owns heartbeat transport, authentication, durable
term/configuration storage, replicated commit, fencing of the old leader, and
serving-topology publication. A deployment that needs automatic failover must
run its own supervised timer or event loop around this API and must make the
returned term/fencing token part of the write authorization check.

The default-off setting is intentional. Enabling it without a real transport
and durable fencing path would create a local promotion decision without
preventing stale writers.

## Verification and measurement

The red test was run before implementation with `make test-t202` and failed on
the missing constructor and API. After implementation the focused tests,
package tests, race detector, and vet checks pass through the T202 Makefile
targets. The benchmark is reproducible with `make benchmark-t202`.

The benchmark compares the pre-existing authenticated-observation evaluator
as a control with the new election decision path. It is not presented as a
data-path speedup: election does more work and supplies availability/fencing
semantics that the control does not provide.

See [BENCHMARK.md#t202-replica-set-leader-election](BENCHMARK.md#t202-replica-set-leader-election)
for raw samples and the measured CPU/allocation tradeoff.
