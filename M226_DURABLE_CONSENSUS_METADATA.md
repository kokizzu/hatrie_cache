# M226: Durable Consensus Metadata

`SQLShardConsensusMetadataRegistry` keeps the durable metadata needed to bind a
state shard owner to its current frontier. It stores an owner, term, fencing
token, frontier, per-record revision, and registry generation. It is the
metadata layer around M225; it is not a network consensus algorithm, quorum
election, or replication transport.

## Ownership And Frontier

```go
registry, err := hatSql.NewSQLShardConsensusMetadataRegistry(
	hatSql.SQLShardConsensusMetadataRegistryOptions{},
)
if err != nil {
	panic(err)
}

metadata, err := registry.Claim("region-eu/shard-7", "worker-42", lease.FencingToken)
if err != nil {
	panic(err)
}

metadata, err = registry.AdvanceFrontier(
	"region-eu/shard-7", "worker-42", metadata.FencingToken, 1042,
)
```

`Claim` accepts a new owner only when its M225 fencing token is strictly larger
than the current record. A handoff increments `Term` and preserves the last
frontier. Repeating the same owner/token is idempotent. `AdvanceFrontier`
requires the exact owner/token and rejects lower frontiers; the first observed
zero is represented by `FrontierObserved`.

`Generation` is a registry-wide compare-and-swap value. `Revision` identifies
the mutation sequence for one shard. Downstream writes should carry both the
fencing token and the generation they observed, and reject stale generations
according to the storage transaction's isolation policy.

## Durable CAS Store

The caller supplies a durable implementation:

```go
type SQLShardConsensusMetadataCheckpointStore interface {
	Load(context.Context) (SQLShardConsensusMetadataSnapshot, bool, error)
	Commit(context.Context, uint64, SQLShardConsensusMetadataSnapshot) (bool, error)
}
```

`CommitTo(ctx, store, expectedGeneration)` publishes a complete deterministic
snapshot only if the store still has the expected generation. A false result is
a conflict, not success; the caller must reload, reconcile, and retry. The
store should commit this metadata atomically with the state ownership record or
WAL entry it protects. `RestoreFrom` validates before replacing in-memory
state, so a malformed or conflicting checkpoint does not partially apply.

The `HSM1` binary checkpoint is bounded, length-prefixed, deterministic, and
CRC32 checked. CRC32 detects accidental corruption but is not authentication;
use authenticated durable storage when an attacker can modify checkpoints.

## Defaults And Tradeoffs

The registry is opt-in and has no effect on existing SQL execution. Defaults
are 16 configured metadata shards, 65,536 records, 256-byte shard IDs, and
128-byte owner IDs. Caller limits are capped at 4,096 bytes for each string.

The frontier update path adds owner/token validation, revision assignment, and
generation assignment to the existing frontier-only tracker. In the measured
single-shard control it remains allocation-free; checkpoint encode/decode is a
cold persistence path and is intentionally allowed to allocate detached
records.
