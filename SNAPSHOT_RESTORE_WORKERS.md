# Snapshot Restore Workers

Partitioned snapshot and Pebble restoration already apply each key to its
deterministic local partition. `HatTrie.ConfigureSnapshotRestoreWorkers`
controls how many partition workers consume that restore stream.

The policy is:

- `0` is the default and chooses `min(GOMAXPROCS, local partitions)`.
- `1` forces serial partition hydration and removes restore queue goroutines.
- `2` through `256` enable bounded parallel hydration. The value is clamped
  to the configured local partition count at restore time.
- A non-partitioned trie remains serial because it has no independent local
  sources to hydrate.

The setting can be applied before or after local partitions are configured. It
is propagated to existing children and copied into staged restore generations.
It changes only execution scheduling; snapshot, Pebble, persistence, and wire
formats are unchanged.

```go
trie := hatCache.CreateHatTrie()
defer trie.Destroy()

if err := trie.ConfigureLocalPartitions(16); err != nil {
	return err
}
if err := trie.ConfigureSnapshotRestoreWorkers(8); err != nil {
	return err
}
if err := trie.LoadSnapshot("cache.snapshot"); err != nil {
	return err
}
```

Use `ConfigureSnapshotRestoreWorkers(1)` when restore writes are disk-bound or
when the process must leave CPU headroom for foreground work. Use a moderate
value such as `4` or `8` when independent partition writes have enough I/O
parallelism. The automatic default remains available with
`ConfigureSnapshotRestoreWorkers(0)`.

## Benchmark

Command:

```sh
make benchmark-mz017-restore-workers
```

The fixture contains 100,000 deterministic 256-byte values across 16 local
partitions. The rows below are three samples from `go test -cpu 32`; each cell
reports the raw samples followed by the median. `0` is automatic sizing, which
is 16 workers in this run because the partition count is 16.

| Policy | ns/op samples | Median ns/op | Median B/op | Median allocs/op | Relative CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| Automatic (`0`) | 117797676, 112640257, 105387530 | 112640257 | 66492352 | 400589 | `1.00x` |
| Serial (`1`) | 97124035, 102791080, 100515158 | 100515158 | 66419408 | 400444 | `1.12x` faster |
| `2` workers | 125248949, 120861870, 127737026 | 125248949 | 66428200 | 400475 | `0.90x` |
| `4` workers | 113444759, 112503313, 113204482 | 113444759 | 66436088 | 400487 | `0.99x` |
| `8` workers | 106926582, 118869836, 119518623 | 118869836 | 66452320 | 400510 | `0.95x` |
| `16` workers | 114834376, 117095358, 104510416 | 114834376 | 66481528 | 400538 | `0.98x` |

Serial restore was 1.12x faster than automatic sizing in this disk-bound run
and used 72,944 fewer bytes per operation. The result is workload and host
dependent; parallelism can help when partition writes overlap on faster
storage. Higher worker counts retain bounded queue/goroutine state, with the
measured 16-worker case using 62,120 more bytes and 94 more allocations than
serial. Because no default path was changed, users should benchmark their
storage before selecting an explicit cap.

The raw output is written to
`build/benchmarks/mz017-restore-workers.txt` and is intentionally not part of
the source tree.
