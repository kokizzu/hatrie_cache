# Replica-Wide Read-Only Enforcement

`hatCache.HatTrie` can be attached to an opt-in `hatReplication.ReplicaReadOnlyGate` when a replica must serve reads without accepting external mutations.

```go
gate := hatReplication.NewReplicaReadOnlyGate()
trie.SetReplicaReadOnlyGate(gate)

gate.SetReadOnly("catching up with leader")
if err := trie.UpsertStringChecked("key", "value"); errors.Is(err, hatReplication.ErrReplicaReadOnly) {
	// reject the client write and keep serving reads
}

status := gate.Status()
_ = status.Generation
_ = status.Reason

gate.SetWritable()
```

The default is off: a trie with no gate, or with `SetReplicaReadOnlyGate(nil)`, keeps existing write behavior. Partition children share the same gate. The gate covers direct scalar and collection writes, TTL and delete operations, counters, CAS, atomic batches, typed structures, public command batches, and direct gRPC scalar batches.

Read commands continue to work while the gate is active. Methods with an existing no-error signature preserve that signature and silently leave the data unchanged; use the `Checked` variant when the caller must observe `ErrReplicaReadOnly`. `VacuumExpired` and background expiration vacuuming also pause while read-only.

Journal/snapshot replay uses package-private apply paths and is intentionally allowed to update a read-only trie. This is the replication exception; external callers cannot use the public mutation APIs to bypass the gate. Configuration and operator maintenance APIs remain separate from the data-write gate.

## Verification

- `make test-tg26-gate` passes the gate state-transition test.
- `make test-tg26-tu06` contains the public-write, read, and internal-replay regression tests.
- `make benchmark-tg26-tu06` contains baseline and writable-gate `UpsertStringChecked` benchmarks. In the TG26 worktree it is currently blocked by the pre-existing `hat/hatSql` compile errors documented in [BENCHMARK.md](BENCHMARK.md#t-u06-replica-wide-read-only-enforcement).
