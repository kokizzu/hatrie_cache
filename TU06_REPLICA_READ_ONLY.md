# Replica-wide read-only enforcement

`HatTrie` can reject local mutations while a replica is serving reads or
catching up. The setting is opt-in and defaults to writable for compatibility.

## API

```go
trie.SetReplicaReadOnly(true)
if trie.ReplicaReadOnly() {
    // Serve reads, but reject local mutations.
}

if err := trie.UpsertStringChecked("key", "value"); errors.Is(err, hatCache.ErrReplicaReadOnly) {
    // The caller attempted a local write on a read-only replica.
}

trie.SetReplicaReadOnly(false) // operator-controlled resume
```

The zero value and constructors are writable. `SetReplicaReadOnly` is an
atomic state change, so readers can inspect the state without taking the main
trie lock.

## Enforced paths

The guard covers the checked direct mutation APIs for string, counter, delete,
expiration, and persistence operations. It also covers public `ExecuteCommand`
mutation dispatch, including rejected writes returning `OK=false` and the
`ErrReplicaReadOnly` message.

Reads remain available. A rejected operation does not create or remove a key.
The state is shared by local keyspace partitions, so enabling it on a trie
cannot leave a partition writable by accident.

Internal replication apply paths retain a narrow scoped bypass. This allows a
replica to apply an already-authorized upstream mutation without making the
replica writable to local callers. Background expiration and storage cleanup
continue to use their existing internal paths. Authentication and operator
authorization for changing the flag remain the responsibility of the caller.

## Verification

The focused test suite covers:

- direct mutation rejection and command-dispatch rejection;
- reads while locked and writes after operator resume;
- internal replication set/delete while local writes are blocked;
- state sharing across local partitions;
- race detection and `go vet`.

Run the repeatable checks with:

```text
make test-round39-replica-read-only
make race-round39-replica-read-only
make vet-round39-replica-read-only
make verify-round39-replica-read-only
```

## Measured cost

The benchmark used Go 1.26.6 on an AMD Ryzen 9 5950X with
`-benchtime=1s -count=5`. The reported values are medians from five samples:

| Path | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Baseline `UpsertStringChecked` | 114.1 | 0 | 0 |
| Read-only state, normal write | 112.8 | 0 | 0 |
| Read-only state, rejected write | 17.69 | 0 | 0 |

The normal writable path is statistically neutral in this run: the observed
1.1% difference is within benchmark noise and is not claimed as a speedup.
Rejected writes return before trie mutation work, at roughly 6.5x the
throughput of the successful write path in this microbenchmark. The guard adds
no measured allocations. Full raw samples are recorded in `BENCHMARK.md`.
