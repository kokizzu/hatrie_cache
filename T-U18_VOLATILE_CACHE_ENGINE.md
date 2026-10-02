# T-U18 Volatile Cache Engine

`CreateVolatileHatTrie()` is an explicit, opt-in memory-only constructor for
ephemeral caches. It keeps raw byte values in the existing Go byte arena even
when they exceed `DiskBytesThreshold`, so construction does not create a
temporary `hatrie-cache-*` directory and large-value reads/writes do not cross
the filesystem boundary.

```go
trie := hatriecache.CreateVolatileHatTrie()
defer trie.Destroy()

if !trie.IsVolatile() {
	panic("unexpected storage engine")
}
```

The default `CreateHatTrie()` behavior is unchanged: large raw byte values use
the disk-backed value store. This keeps the safer default for workloads that
need lower Go-heap retention or persistence-compatible operations.

Volatile mode rejects snapshots, backups, and LevelDB/Pebble save, load, and
checkpoint APIs with `ErrVolatilePersistence`. It is therefore suitable for
rebuildable caches, request-local state, and tests, not for data that must
survive process loss. Returned byte slices retain the existing copy-on-read
contract.

## Measured Tradeoff

The benchmark uses a `DiskBytesThreshold+1` byte payload and performs one
update plus one read per iteration. On Linux/amd64 with an AMD Ryzen 9 5950X,
the matched post-change run measured:

| Engine | Median ns/op | B/op | Allocs/op | Result |
| --- | ---: | ---: | ---: | --- |
| Disk-backed | 2,609,202 | 79,351 | 23 | Lower Go-heap retention, filesystem I/O |
| Volatile | 28,014 | 147,457 | 2 | 93.2x lower operation time, 1.86x higher allocated bytes |

The volatile win is CPU and latency from avoiding file writes and reads. The
cost is resident Go heap: every large value remains in process memory, so use
the default constructor when memory pressure or durability matters.
