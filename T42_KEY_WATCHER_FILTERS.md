# T-G42 Key Watcher Filters And Coalescing

This feature extends the existing exact-key watcher with an opt-in prefix
filter and bounded latest-event coalescing. Existing `WatchKey` and
`WatchKeyWithBuffer` behavior is unchanged: every successful mutation is
delivered in order and a full channel applies backpressure.

## API

```go
watcher, err := trie.WatchKeyWithOptions(hatCache.KeyWatcherOptions{
	Prefix: "user:",
	Buffer: 128,
})
if err != nil {
	return err
}
defer watcher.Close()

for event := range watcher.Events() {
	// event.Key is one matching key.
}
```

Set exactly one of `Key` or `Prefix`. Prefix matching is literal and uses
`strings.HasPrefix`; there is no glob or regular-expression execution on the
write path. `Buffer` defaults to `DefaultKeyWatcherBuffer` and is bounded by
`MaxKeyWatcherBuffer`.

Coalescing is explicit:

```go
watcher, err := trie.WatchKeyWithOptions(hatCache.KeyWatcherOptions{
	Prefix:         "user:",
	Buffer:         128,
	Coalesce:       true,
	CoalesceWindow: 2 * time.Millisecond,
})
```

Within each window, the watcher emits at most one event per matching key. The
event keeps the first-seen key order but contains that key's latest operation
and mutation epoch. The pending distinct-key set is bounded by `Buffer`; a
slow consumer therefore applies backpressure instead of losing events. A
coalesced watcher uses one delivery goroutine and a small timer, so this mode
is intended for invalidation consumers that can refresh each key once per
window, not for audit logs that require every mutation.

Prefix watchers are rejected on a partitioned root trie. Exact watches continue
to route to the local partition as before. This avoids falsely claiming to
observe writes that are applied by independent partition tries; register a
prefix watcher on each partition when that topology is required.

## Benchmark

Commands:

```text
make benchmark-t-g42-key-watchers
```

Five runs on Linux/amd64, AMD Ryzen 9 5950X:

| Path | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| No watcher | 118.6-122.7 | 0 | 0 |
| Existing exact watcher | 211.0-224.9 | 0 | 0 |
| Exact options watcher | 218.5-228.5 | 0 | 0 |
| Prefix watcher | 262.8-273.2 | 0 | 0 |
| Coalesced repeated-key watcher | 208.4-214.5 | 2-3 | 0 |

The exact options wrapper is within benchmark noise of the existing exact
watcher. Prefix matching is about 1.20x slower than the existing exact watcher
in this one-prefix workload, while coalesced repeated-key publishing is about
1.02x faster and retains 2-3 bytes per operation for the pending event state.
Run it on the target deployment hardware before choosing a coalescing window.
The coalesced mode trades per-mutation delivery work for bounded batching and
should not be compared with exact delivery when every event is required.

The no-watcher path remains the baseline: `notifyKeyWatchersLocked` first looks
up exact watchers and only scans prefixes when at least one prefix registration
exists. No new allocation or watcher send is introduced for ordinary writes
without watchers.

## Verification

Focused tests cover prefix exclusion, latest event and epoch selection,
first-seen ordering across multiple keys, invalid options, and clean trie
destruction. Package, race, and vet checks are available through the T-G42
Makefile targets.
