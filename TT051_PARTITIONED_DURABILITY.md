# TT051 Partitioned Synchronous Durability

This adopts the Tarantool-style per-partition WAL durability idea as an
explicit opt-in API. `PartitionedCommandJournal` opens one command journal
file per configured local HAT-trie partition and defaults each journal to
`GroupCommitMaxBatch=1`, so a successful mutation is synchronously flushed in
the file for the partition that owns its key.

The existing `CommandJournal` remains the default. `ConfigureLocalPartitions`
also remains default-off, so existing callers do not receive extra files,
file descriptors, or recovery ordering requirements.

## Example

```go
trie, err := hatCache.CreateHatTrieWithDiskDir("./data", false)
if err != nil {
    return err
}
defer trie.Destroy()
if err := trie.ConfigureLocalPartitions(4); err != nil {
    return err
}

journal, err := hatCache.OpenPartitionedCommandJournal(
    "./data/command-journals",
    hatCache.PartitionedCommandJournalOptions{Partitions: 4},
)
if err != nil {
    return err
}
defer journal.Close()

response := journal.ExecuteCommand(trie, hatCache.CacheCommandRequest{
    Command: "SETSTR",
    Key:     "region:sg:session:42",
    Value:   "ready",
})
if !response.OK {
    return errors.New(response.Message)
}

watermarks, err := journal.Sequences()
if err != nil {
    return err
}
// Persist watermarks with the corresponding partition snapshots.
_ = watermarks
```

The directory layout is deterministic:

```text
command-journals/
  partition-000/commands.journal
  partition-001/commands.journal
  partition-002/commands.journal
  partition-003/commands.journal
```

Directories are created with mode `0700`; journal files use the existing
`0600` command-journal policy.

## Command And Recovery Rules

- Key-addressed journaled commands are routed by the trie’s existing local
  partition hash.
- A `BATCH` is accepted only when every child request maps to one partition;
  its existing atomic behavior is then preserved by that partition journal.
- A batch spanning partitions is rejected before any journal bytes are
  appended. This prevents pretending that independent files provide one
  cross-partition transaction order.
- Keyless journaled commands are rejected by this API. Use the ordinary global
  `CommandJournal` for global mutations.
- Read-only commands bypass the partition journals and use the normal trie
  read path.
- `Sequences()` returns one local watermark per partition; there is no global
  sequence number.
- `PartitionPath(partition)` returns the stable `commands.journal` path for
  backup manifests, so callers do not need to construct directory names.

Recovery replays all local journals in deterministic partition order:

```go
sequences, err := journal.Replay(restoredTrie, snapshotWatermarks)
```

`snapshotWatermarks` must contain one value per partition, or be `nil` to
replay from the beginning. A backup manifest must therefore capture the
partition count, the ordered watermark array, every partition snapshot, and
every matching `commands.journal` file. The caller still owns quiescing writes,
atomically publishing that manifest, and deciding how cross-partition business
transactions are recovered. The API deliberately does not claim a global
ordering that the files cannot provide.

## Configuration And Tradeoffs

`PartitionedCommandJournalOptions{Partitions: N}` is opt-in and synchronous by
default. Set `Journal.GroupCommitMaxBatch` above one to trade per-command
durability latency for group-commit throughput, using the same semantics as
`CommandJournal`.

The mode costs one open append file and journal state per partition. It is most
useful when concurrent writers are naturally partition-affine. A single writer
does not gain meaningful throughput; the standard global journal remains the
simpler choice when global ordering or cross-partition transactions matter.

## Benchmark

Command: `make benchmark-tt051` with five samples per benchmark and
`-benchtime=1s`. The medians below come from the same paired run on an AMD Ryzen 9
5950X, Linux amd64, with synchronous binary journals and four partitions.

| Workload | Global one-file journal | Partitioned journal | Result |
|---|---:|---:|---:|
| Single writer | 836,240 ns/op, 313 B/op, 4 allocs/op | 812,132 ns/op, 283 B/op, 2 allocs/op | 1.03x faster, 9.6% fewer bytes, 50% fewer allocs |
| Parallel writers | 886,485 ns/op, 354 B/op, 4 allocs/op | 332,949 ns/op, 298 B/op, 2 allocs/op | 2.66x faster, 15.8% fewer bytes, 50% fewer allocs |

Raw `ns/op` samples from that invocation, in emitted order:

| Workload | Global one-file journal | Partitioned journal |
|---|---:|---:|
| Single writer | 845,627; 803,973; 869,871; 833,068; 836,240 | 835,108; 812,132; 845,333; 703,682; 740,629 |
| Parallel writers | 867,099; 850,313; 886,485; 894,509; 2,761,705 | 324,907; 339,512; 332,949; 316,181; 353,332 |

The parallel win comes from independent journal mutexes and sync paths. The
five samples include one slower global-journal sample; medians are reported to
avoid letting that outlier define the comparison. The mode still costs one
append file per partition and has no global transaction order.
