# T201 Per-Space Synchronous Replication Quorum

Tarantool-style synchronous replication is useful for critical spaces, but
making every write wait for a quorum would change the default durability and
latency contract. `hatReplication.PerSpaceWriteQuorum` keeps that choice
explicit: only configured spaces use the existing proposal-bound journal
quorum, while every other space remains on the caller's asynchronous path.

```go
quorum, err := hatReplication.NewPerSpaceWriteQuorum(
	hatReplication.PerSpaceWriteQuorumOptions{
		Policies: []hatReplication.PerSpaceWriteQuorumPolicy{{
			Space:  "critical_orders",
			Voters: []string{"local", "east", "west"},
			// Zero means strict majority: two of three.
		}},
	})
if err != nil {
	return err
}

result, err := quorum.Execute(ctx, "critical_orders", proposal,
	func(ctx context.Context, space, node string, proposal hatReplication.JournalWriteQuorumProposal) (hatReplication.JournalWriteQuorumAcknowledgement, error) {
		return durableAcknowledge(ctx, space, node, proposal)
	})
if err != nil || !result.Enforced || !result.Decision.Satisfied {
	return err
}
```

For an unconfigured space, `Execute` returns successfully with
`Enforced=false` and never calls the acknowledgement callback. Callers can
then submit that write through their existing asynchronous replication path.
`EnabledFor` is an allocation-free routing check, `ConfiguredSpaces` returns a
deterministic inspection snapshot, and `Evaluate` validates caller-collected
acknowledgements without sending network work. Space names are trimmed and
duplicate policies are rejected at construction. The underlying quorum still
binds every acknowledgement to the proposal sequence and fence token.

This is a local coordination API, not an automatic replication transport. The
acknowledgement callback is responsible for durable remote application and
must honor the supplied context. A satisfied quorum does not roll back failed
non-voters; the returned journal decision and the existing repair/reconcile
path remain authoritative.
