# T-U10 Journal Write Quorum State

`hatReplication.JournalWriteQuorumState` is the bounded, transport-neutral
state for one journal sequence that must wait for synchronous replica
acknowledgements.

```go
state, err := hatReplication.NewJournalWriteQuorumState(
	journalSequence,
	2, // required acknowledgements, including the local journal
	3, // local participant 0 plus two remote participants
)
if err != nil {
	return err
}

state, err = state.Acknowledge(1)
if err != nil {
	return err
}
decision, err := state.Decision()
if err != nil {
	// ErrWriteQuorumUnsatisfied means the caller must keep waiting or fail
	// the write according to its timeout/retry policy.
	return err
}
if !decision.Satisfied {
	return errors.New("write quorum is not satisfied")
}
```

Participant `0` is acknowledged at construction because the local journal
write is the first durable participant. Remote participant indexes are bounded
to `1..63`. Acknowledging the same participant repeatedly is idempotent, and
the state uses one `uint64` bitset rather than a per-write map. JSON decoding
validates sequence, quorum, participant, and acknowledgement-mask bounds.

This is a partial T-U10 adoption. The state is importable and can be persisted
with a journal progress record, but it does not start network calls or change
`CommandJournal` behavior by itself. The caller still owns synchronous HTTP/2
or gRPC transport, deadlines, retry ordering, journal append/commit timing,
and the policy for a locally committed write whose remote quorum fails. The
existing default remains off because callers only construct this state when a
positive quorum is explicitly configured.

The cache/HTTP integration is intentionally deferred while the concurrent SQL
and storage refactor is restoring a buildable `hatCache` package. No transport
or authentication behavior is changed by this state object.
