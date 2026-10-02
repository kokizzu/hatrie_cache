# Disk Read Cache

`DiskStorage.Get` can use an opt-in bounded LRU for repeated reads of large
values. The default is disabled, so existing tries keep the current filesystem
behavior and memory profile.

```go
trie := hatCache.CreateHatTrie()
defer trie.Destroy()

err := trie.ConfigureDiskReadCache(hatCache.DiskStorageReadCacheOptions{
	MaxBytes:       64 << 20,
	MaxValueBytes:  1 << 20,
	AdmissionReads: 2,
})
if err != nil {
	return err
}
stats := trie.DiskReadCacheStats()
```

`MaxBytes` is the hard resident-value limit. `MaxValueBytes` skips values that
are too large for the cache. `AdmissionReads` avoids retaining one-shot values;
zero means two reads. Set `MaxBytes` to zero to disable and clear the cache.
Writes, deletes, index reuse, and memory compaction invalidate or rebuild the
cache state, so a cached value cannot survive a replacement under the same
storage index. Hits return independent byte slices. Streaming reads through
the internal file handle path are unchanged.

## Measurement

The benchmark uses a repeated 64 KiB value on Linux/amd64, AMD Ryzen 9 5950X,
with three 200 ms samples per case. The filesystem row is the unchanged
control; the cache row is warmed before timing.

| Workload | Median ns/op | B/op | Allocs/op | Result |
| --- | ---: | ---: | ---: | --- |
| Filesystem control | 19,648 | 74,120 | 5 | Baseline |
| Admitted LRU cache | 10,366 | 65,536 | 1 | 1.90x faster, 11.6% lower allocated bytes, 80% fewer allocations |

The cache retains up to the configured value bytes plus small LRU metadata, so
the speedup is only appropriate when repeated reads justify that bounded
resident memory. No default workload pays this cost because the feature is
off until configured.
