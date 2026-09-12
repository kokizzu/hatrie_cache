# M249: Point-Read and Subscription Fences

`CommandJournal.WithReadFence` closes the race between reading current state and starting a journal subscription. It runs a read-only callback while journaled mutations are paused, then returns the journal sequence after the callback succeeds.

```go
var value string
sequence, err := journal.WithReadFence(func() error {
	value = trie.GetString("account:42")
	return nil
})
if err != nil {
	return err
}

subscription, err := journal.Subscribe(ctx, hatCache.CommandJournalSubscribeOptions{
	AfterSequence: sequence,
})
```

The subscription may be created after the fence returns. Existing subscription replay handles records appended between the fence and subscription registration, so they cannot be skipped. `WithReadFence` returns sequence `0` when the callback fails or the journal is closed.

The callback must not call methods that acquire the same journal lock, and it must not mutate the trie through an unjournaled path. The fence coordinates journaled mutations; direct `HatTrie` writes remain outside the journal’s ordering contract.

## Compatibility and cost

The API is additive. Existing reads and subscriptions are unchanged. The fence briefly extends the journal lock over the callback, so callbacks should perform only the required point read and avoid I/O or long-running work.

The pre-change baseline was measured on an AMD Ryzen 9 5950X, Linux amd64, Go, with five samples and `-benchtime=1s -benchmem`:

| Handoff | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Existing point read then `Sequence()` | 63.56 | 0 | 0 |

The final retained benchmark used the same command and measured:

| Handoff | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Existing point read then `Sequence()` | 65.47 | 0 | 0 |
| `WithReadFence` | 67.59 | 0 | 0 |

The final fenced path is about `1.03x` the old pattern (`+3.2%`) with no allocation or memory cost. The samples overlap, so this small coordination overhead should be treated as a workload- and machine-dependent measurement rather than a guaranteed fixed percentage.

Raw final `ns/op` samples: existing `65.32, 65.13, 65.47, 66.39, 66.62`; fence `69.24, 68.49, 67.59, 67.50, 63.76`.

## Verification

```text
make verify-m249
make benchmark-m249
```
