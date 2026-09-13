# TR-16: Storage Key Pinning

Storage spilling is already opt-in through `LevelDBSpillOptions` and the
memory governor. This feature adds an explicit admission policy for values
that must stay materialized while `SpillCold` runs.

## API

```go
if err := trie.PinStorageKey("tenant:hot-config"); err != nil {
	return err
}

// A pin may be installed before the key exists.
trie.UpsertString("tenant:hot-config", "current configuration")

if pinned, err := trie.IsStorageKeyPinned("tenant:hot-config"); err != nil || !pinned {
	return err
}

keys := trie.PinnedStorageKeys() // sorted detached copy
_ = keys

// The next eligible spill may reclaim the value after this call.
return trie.UnpinStorageKey("tenant:hot-config")
```

`PinStorageKey` validates the key, records the policy, and hydrates the value
when the current value is already a cold LevelDB reference. A missing key is
valid and remains pinned for a future write. `UnpinStorageKey` is idempotent;
it does not spill immediately. A deleted key stays in the policy until it is
explicitly unpinned, which makes pre-created pins deterministic.

## Spill Semantics

LevelDB and Pebble share the same candidate-admission helper, so a pinned key
is excluded consistently from both `SpillCold` implementations. Its bytes
still contribute to `HotBytesBefore` and hot-byte accounting. Therefore the
configured soft cap can remain above target when pinned values alone exceed
the cap. This is intentional: pinning is a residency guarantee, not a memory
limit override.

The policy is process-local. Pins are not serialized into snapshots, backup
bundles, replication commands, or wire responses. Reapply them after restore
when the application requires the same residency policy. On a trie with local
partitions, the root APIs route to the owning child; pins created before
partition configuration are transferred during configuration.

## Measurement

Command:

```text
make benchmark-tr016
```

AMD Ryzen 9 5950X, Go benchmark with `-benchmem`:

| Case | Median result | Heap | Interpretation |
| --- | ---: | ---: | --- |
| Default off | 11.48 ns/op | 0 B, 0 allocs/op | Normal spill admission with no pin map |
| Active unrelated pin | 16.74 ns/op | 0 B, 0 allocs/op | 1.46x the admission CPU for one map lookup |
| Active matching pin | 10.29 ns/op | 0 B, 0 allocs/op | Candidate rejected; no candidate append |

These are medians from five runs. The raw ns/op samples in run order were
`12.20, 11.43, 11.12, 11.48, 11.54` (default off),
`17.49, 16.74, 17.11, 16.23, 16.17` (active unrelated pin), and
`10.29, 10.44, 10.36, 10.29, 10.15` (active matching pin). This is not a
general throughput optimization. The default path remains allocation-free,
while enabling pins adds a small cost only during an explicit spill candidate scan and intentionally retains more heap memory.
The focused tests verify that pinned values survive spilling, cold values are
hydrated before pinning, unpinning restores spill eligibility, invalid keys
are rejected, and partition routing remains correct.

## Operational Guidance

Pin only values with a known latency or availability requirement. Monitor
`HotBytesBefore` and `HotBytesAfter`; a large pin set can make a soft memory
cap ineffective. Keep the pin list in application configuration if it must be
reapplied after process restart or recovery.
