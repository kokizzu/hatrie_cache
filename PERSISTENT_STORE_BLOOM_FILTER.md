# Persistent Store Run Bloom Filters

`hatrie_cache` can ask the LevelDB and Pebble persistent engines to write a
native Bloom filter with each new table/run. The filter is a conservative
negative lookup precheck: a definite miss can avoid reading the table data,
while a possible hit still follows the normal exact lookup path.

## Default

The default is disabled:

```text
db-storage-bloom-filter-bits-per-key = 0
```

This is deliberate. The local warm-cache benchmark showed only a small
LevelDB miss improvement, slower LevelDB hits, no clear Pebble CPU improvement,
and about 12% to 14% more compacted bytes. The filter is most useful for a
cold, miss-heavy persistent workload where it can avoid an uncached table read.

## Enable From The CLI

Set the option before starting a node that opens the persistent store:

```text
hatrie-cache \
  -db-path /var/lib/hatrie-cache/cache \
  -db-storage-bloom-filter-bits-per-key 10
```

`10` is the recommended starting point and targets about a 1% false-positive
rate in the native Bloom implementations. `0` disables the filter. Values are
bounded to `0..64`; changing the option affects stores opened afterward.

## Enable From Go

Configure the importable root package before opening a store:

```go
if err := hatriecache.ConfigurePersistentStoreBloomFilterBitsPerKey(10); err != nil {
	return err
}
store, err := hatriecache.OpenPersistentStore(path)
if err != nil {
	return err
}
defer store.Close()
```

The setting is process-local and applies to future persistent-store opens. It
does not rewrite existing runs and does not reconfigure an already-open store.
Existing databases remain readable regardless of whether their older runs
contain filters. New runs receive filters when the option is enabled, including
normal saves, compactions, and future opens. The same setting is used by the
LevelDB replication outbox.

## Measured Tradeoff

The paired benchmark uses one compacted 4,096-key store and five
`-benchmem` samples per case on `linux/amd64`, AMD Ryzen 9 5950X. The disabled
case is the control; the enabled case uses 10 bits per key.

| Backend | Lookup | Disabled median | Enabled median | Relative | Heap / allocs |
| --- | --- | ---: | ---: | --- | --- |
| LevelDB | miss | 274.5 ns/op | 268.8 ns/op | 1.02x faster | 128 B / 5 -> 128 B / 5 |
| LevelDB | hit | 1,630 ns/op | 1,717 ns/op | 1.05x slower | 1,072 B / 18 -> 1,080 B / 19 |
| Pebble | miss | 399.8 ns/op | 400.9 ns/op | 1.00x slower | 128 B / 3 -> 128 B / 3 |
| Pebble | hit | 1,654 ns/op | 1,661 ns/op | 1.00x slower | 539 B / 9 -> 538 B / 9 |

Compacted directory bytes for the same fixture:

| Backend | Disabled | Enabled | Change |
| --- | ---: | ---: | ---: |
| LevelDB | 45,215 bytes | 50,498 bytes | +11.7% |
| Pebble | 36,797 bytes | 42,098 bytes | +14.4% |

The byte increase is also the expected extra write/read bandwidth for filter
blocks. The benchmark keeps the database warm, so it does not claim a cold-disk
I/O win. The default therefore stays at `0`, while operators with a measured
miss-heavy cold-read workload can opt in.

Run the repeatable checks with:

```text
make test-tt017
make benchmark-tt017
```
