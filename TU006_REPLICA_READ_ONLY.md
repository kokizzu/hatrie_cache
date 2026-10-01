# T-U06 Replica-Wide Read-Only Enforcement

Replica-wide read-only enforcement is an opt-in admission fence for a
`hatCache.HatTrie`. It prevents local client writes while allowing reads and
trusted recovery/replication work to continue.

## Enable It

```go
if err := trie.SetReplicaReadOnly(true); err != nil {
    return err
}
defer trie.SetReplicaReadOnly(false)

if trie.ReplicaReadOnly() {
    // Drain or reject upstream writes before a planned promotion or backup.
}
```

The default is `false`. No configuration file, environment variable, or
server flag enables it implicitly. The state is stored on the trie and is
copied to local partition children when they are created; changing the state
also propagates to existing children.

## Behavior

When enabled:

- Checked direct mutation APIs return `hatCache.ErrReplicaReadOnly`.
- Legacy mutation wrappers that do not return errors become no-ops, preserving
  their existing signatures.
- `SET`, `DEL`, TTL/CAS writes, atomic command batches, SQL mutations, and
  pending SQL transaction commits are rejected before they change live state.
- In-place typed mutations are fenced too, including slice pop, set remove,
  priority-queue pop, radix-tree delete, bitmap removal, and Cuckoo-filter
  deletion.
- Read commands, snapshots, backup/restore inspection, and monitoring reads
  remain available.

`RunAtomic` is rejected before its callback runs because the cache cannot know
whether a callback will mutate the trie without executing it. A transaction
that was staged before fencing remains open; its commit returns
`ErrReplicaReadOnly` and can be retried after the fence is removed.

## Trusted Replication Boundary

The internal prepared replication apply path uses a private, non-exported
bypass around the fence. This keeps journal replay and trusted snapshot
application possible during controlled recovery. Calling an internal command
through the public `ExecuteCommand` API does **not** grant that bypass and is
rejected while read-only. The replication transport still needs its normal
authentication, authorization, and fencing controls; this API is not a
replacement for them.

## Cutover Semantics

`SetReplicaReadOnly(true)` is an atomic admission fence, not a drain barrier.
A mutation already past its read-only check may finish after the flag changes.
For a strict promotion or backup boundary, stop or drain upstream writers,
then enable the fence, then capture the snapshot or promote the node. Keep the
fence enabled until the operation and any verification are complete.

The fence does not elect a leader, resolve split brain, or authorize its own
toggle. Expose the toggle only through an authenticated operator/control-plane
path.

## Cost Measurement

The benchmark was run on Linux/amd64 with an AMD Ryzen 9 5950X using five
`-benchmem` samples before and after the change. The baseline was run from the
previous commit in an isolated worktree with the same benchmark source.

| Path | Before median | After median | Relative result | Before/after memory |
| --- | ---: | ---: | ---: | ---: |
| Default-off `UpsertStringChecked` | 120.7 ns/op | 123.9 ns/op | 1.027x, 2.7% slower | 0 / 0 B/op, 0 / 0 allocs/op |
| Default-off `ExecuteCommand(SET)` | 235.9 ns/op | 232.1 ns/op | 0.984x, 1.6% faster; within noise | 0 / 0 B/op, 0 / 0 allocs/op |

The direct write path adds a constant atomic admission check but no heap work
or per-key metadata. This is a small safety cost, not a throughput feature.
The benchmark target is:

```sh
make benchmark-t-u06
```

Raw samples (`ns/op`, `B/op`, `allocs/op`):

```text
baseline UpsertStringChecked: 116.7 0 0; 120.7 0 0; 128.6 0 0; 124.9 0 0; 117.1 0 0
after    UpsertStringChecked: 116.3 0 0; 117.2 0 0; 125.0 0 0; 123.9 0 0; 125.0 0 0
baseline ExecuteCommand SET: 223.0 0 0; 239.8 0 0; 235.9 0 0; 240.6 0 0; 232.6 0 0
after    ExecuteCommand SET: 212.0 0 0; 230.8 0 0; 232.3 0 0; 232.1 0 0; 236.6 0 0
```

## Verification

```sh
make test-t-u06
make race-t-u06
make vet-t-u06
```

The focused tests cover default-off behavior, scalar and typed mutation
rejection, SQL mutation and transaction commit fencing, local partitions,
trusted internal replication apply, and read preservation.
