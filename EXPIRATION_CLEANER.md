# Deadline-Aware Expiration Cleaner

`HatTrie` expiration remains lazy by default. `StartExpirationCleaner` is still
opt-in, but its worker now uses the existing indexed expiration min-heap more
directly:

- with no scheduled TTL, it wakes at the configured interval;
- with scheduled TTLs, it sleeps until the earliest deadline or that interval,
  whichever comes first;
- adding or refreshing a deadline only wakes the worker when the new deadline
  is earlier than the current heap head;
- a root cleaner registers its partition children, so a deadline added to a
  local partition wakes the same worker;
- `StartExpirationCleanerContext` and the returned idempotent stop function
  keep their existing cancellation and shutdown behavior.

The `interval` argument remains a positive maximum fallback interval. It is
not a TTL granularity setting. A short TTL can be removed near its actual
deadline even when the configured interval is long. Reads and mutations still
remove expired values lazily when no cleaner is running.

## Example

```go
trie := hatCache.CreateHatTrie()
defer trie.Destroy()

trie.UpsertString("session:42", "active")
trie.Expire("session:42", 30*time.Minute)

stop := trie.StartExpirationCleanerContext(ctx, time.Second)
defer stop()
```

The cleaner is useful when expired values must be reclaimed without requiring
foreground traffic. It adds one goroutine and one wake channel while running,
but no allocation per TTL update. The active cleaner path has a small measured
update cost; the default no-cleaner path remains allocation-free and retains
the previous heap update behavior.

This is a lower-risk, heap-based part of the Tarantool-inspired expiration
idea. A hierarchical timing wheel is not enabled: the current heap gives exact
deadlines, bounded live entries, and straightforward persistence/restore
semantics. Replacing it with bucketed expiration would need a separate
workload study for deadline resolution, churn, memory retention, and recovery.

## Verification

The focused tests cover deadlines scheduled before cleaner startup, earlier
deadlines added after startup, partitioned children, cancellation, stopping,
and destruction. Run:

```sh
make test-expiration-deadline-cleaner
make test-race-expiration-deadline-cleaner
make benchmark-expiration-deadline-cleaner
```
