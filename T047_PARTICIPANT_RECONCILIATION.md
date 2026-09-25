# T047f Participant Reconciliation

The two-phase write coordinator can return an indeterminate outcome after a
commit callback starts. A participant therefore needs a compact local phase
record that can be reconciled after restart or a transport failure.

`hatReplication.ClusterWriteCommitParticipant.PreparedRecords` returns an
independently owned, transaction-ID-ordered snapshot of prepared records.
Callers apply the coordinator's authoritative outcomes with
`ClusterWriteCommitParticipant.Reconcile`:

- decisions are strictly ordered by normalized transaction ID;
- the complete batch is validated before any record changes;
- only `Committed` and `Aborted` are accepted as terminal decisions;
- repeating the same terminal decision is idempotent;
- an unknown transaction, proposal mismatch, conflicting terminal phase, or
  invalid order leaves the participant unchanged.

The API is transport-neutral and opt-in. It does not invent a quorum result,
persist state by itself, or wire a new HTTP/gRPC endpoint. Callers continue to
own the journal/snapshot durability and reconciliation authority.

## Measurement

The paired benchmark reconciles 32 prepared transactions after restoring the
same prepared snapshot. The old path performs a `Status` and `Commit` call for
each transaction; the new path validates and applies one ordered batch.

| Path | Median time | Bytes/op | Allocs/op | Relative time |
| --- | ---: | ---: | ---: | ---: |
| Per-record `Status` + `Commit` | 3.705 us | 0 | 0 | 1.00x |
| Batch `Reconcile` | 3.418 us | 0 | 0 | 1.08x faster |

The batch path removes repeated lock/unlock cycles without adding retained or
transient allocations for the canonical ordered input. The full raw output is
recorded in [BENCHMARK.md](BENCHMARK.md#t047f-participant-reconciliation).
