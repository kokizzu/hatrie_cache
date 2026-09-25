# TT-007: Snapshot-plus-WAL Join

`hatCache.JoinCommandJournalSnapshot` is the reusable data-plane operation for
joining a node from a live journal source:

1. Download and atomically validate a source snapshot.
2. Replace the target trie and journal checkpoint at the snapshot sequence.
3. Pull journal batches strictly after that sequence until the source reports
   no more entries.
4. Call the optional persistence callback only after the complete catch-up.

```go
result, err := hatriecache.JoinCommandJournalSnapshot(ctx, trie, journal, hatriecache.CommandJournalJoinOptions{
	Source:     "https://leader.example:8080",
	Limit:      1000,
	MaxBatches: 100,
	Persist:    persistFullState,
})
if err != nil {
	// Do not activate the node. Retry from a new join lifecycle or discard it.
	return err
}
fmt.Println(result.Snapshot.JournalSequence, result.Pull.AppliedThrough)
```

An empty `SnapshotPath` uses a temporary snapshot directory and removes it on
return. A configured path is retained and replaced atomically. `HasMore` is
treated as an error (`ErrCommandJournalJoinIncomplete`), so a bounded join
cannot be reported ready after only a partial WAL interval. Authentication,
wire format, timeout, dirty tracking, and replication throttling are forwarded
through the options.

The helper does not publish topology membership or enable traffic. Pair it with
the caller-owned `hatReplication.SnapshotWALBootstrapCoordinator` and activate
only after the returned state is complete. A source that compacts the required
interval during the join returns an error. A failed join may have installed the
snapshot and some WAL entries, so the caller should keep the node out of
service and retry or discard that local state.

## Measurement

Command:

```text
make benchmark-tt007-snapshot-wal-join
```

The benchmark compares the composed helper with the equivalent manual
`PullCommandJournalSnapshot` + `ReplaceWithSnapshot` + `PullCommandJournal`
sequence. It uses an in-process HTTP source, one snapshot, one post-snapshot
WAL mutation, `Limit=16`, and `MaxBatches=4` on Linux/amd64 (AMD Ryzen 9 5950X,
Go 1.26.6):

| Path | Median time | Median bytes | Median allocs |
| --- | ---: | ---: | ---: |
| Composed join | 7.376 ms/op | 315,810 B/op | 763 allocs/op |
| Manual sequence | 7.991 ms/op | 316,625 B/op | 719 allocs/op |

Raw runs are preserved in `BENCHMARK.md`. The helper was about 7.7% faster in
this noisy end-to-end setup, with nearly identical memory and 6.1% more
allocations. This is an API/correctness improvement, not a claim of a faster
wire or storage path; existing lower-level callers are unchanged.
