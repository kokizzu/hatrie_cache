# MZ-050 Timeline Branching And Replay

`hatCache.CommandJournal.BranchAt` creates an isolated in-memory what-if
timeline from a retained journal prefix. The source trie and journal are never
modified by branch commands.

```go
branch, err := journal.BranchAt(committedSequence)
if err != nil {
	return err
}
defer branch.Close()

response := branch.Execute(hatCache.CacheCommandRequest{
	Command: "SETSTR",
	Key:     "price",
	Value:   "12.50",
})
if !response.OK {
	return errors.New(response.Message)
}

candidate := hatCache.CreateHatTrie()
defer candidate.Destroy()
if err := branch.ReplayInto(candidate); err != nil {
	return err
}
```

`BranchAt(0)` uses the current journal tail. Successful branch commands are
cloned and retained in order; failed commands are not retained. `Requests`
and `CommandCount` expose the retained what-if workload without sharing caller
buffers. `ReplayInto` expects an empty caller-owned trie and requires the
source journal to retain the branch base prefix. A compacted prefix must first
be restored from its snapshot. Branches are process-local and intentionally do
not append to the journal, replicate, or provide a durable snapshot.

## Measured Cost

Five 500 ms samples on Linux/amd64, AMD Ryzen 9 5950X, with a 128-command
journal prefix. The baseline is the existing manual sequence of creating an
empty trie and calling `ReplayThrough`.

| Path | Median time | Memory | Allocations | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Manual replay | 393878 ns/op | 42826 B/op | 663 | 1.00x |
| `BranchAt` | 431629 ns/op | 42890 B/op | 664 | 1.10x |
| Manual replay plus hypothetical command | 435949 ns/op | 42826 B/op | 663 | 1.00x |
| `ReplayInto` | 364972 ns/op | 43050 B/op | 664 | 0.84x |

Branch creation adds 64 retained bytes and one allocation in this workload. The
replay timing is noisy because every sample creates and destroys a disk-backed
temporary trie; the feature's predictable cost is the retained branch request
log and one branch wrapper allocation. It is opt-in and does not change the
normal journal or trie write path.

Verification:

```text
make test-mz050-timeline-branch
make race-mz050-timeline-branch
make verify-mz050-timeline-branch
make benchmark-mz050-timeline-branch
```
