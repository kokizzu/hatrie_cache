# T215: Per-Space Storage Policy

`hatDataStructure.StorageSpace` adopts the useful Tarantool distinction between
an in-memory `memtx` space and an on-disk space without changing existing
`HatTrie` or `TypedTable` defaults.

## Defaults

An empty `StorageSpaceOptions.Mode` selects `StorageSpaceMemtx`. This keeps
the fast path in process memory and preserves the existing no-disk behavior.
Each space is independent, so a process can keep hot lookup data in memtx and
put a larger or colder space on disk.

```go
hot, err := hatDataStructure.NewStorageSpace(hatDataStructure.StorageSpaceOptions{
	Name: "sessions",
})
if err != nil {
	return err
}
defer hot.Close()

cold, err := hatDataStructure.NewStorageSpace(hatDataStructure.StorageSpaceOptions{
	Name:             "events",
	Mode:             hatDataStructure.StorageSpaceOnDisk,
	Directory:        "/var/lib/hatrie-cache/events",
	MemoryLimitBytes: 8 << 20,
	MaxDiskBytes:     128 << 30,
})
if err != nil {
	return err
}
defer cold.Close()
```

`Set`, `Get`, `Delete`, and `Snapshot` are shared by both modes. Returned and
input values are copied, and snapshots are sorted by key. `Flush` spills hot
values and syncs the on-disk segment; call it before reporting a checkpoint as
durable. `Sync` only syncs records already written, while `Compact` reclaims
obsolete records after deletes or replacements.

To reopen an on-disk space, retain `space.Path()` and pass it as `SpillPath`
with `Mode: StorageSpaceOnDisk`. A caller-provided `Directory` makes the
segment survive process close; an omitted directory uses a private temporary
directory and is appropriate for ephemeral work.

## Resource Model

The on-disk policy reuses `SpillableArrangement`: value payloads are bounded by
`MemoryLimitBytes`, while keys and lookup metadata remain resident. `MaxDiskBytes`,
`MaxKeyBytes`, and `MaxValueBytes` provide explicit bounds. Disk records have
CRC validation and existing path checks reject symlinks and non-regular files.

The policy is deliberately low-level and importable. SQL `TypedTable` and the
whole-trie persistent stores are unchanged; this avoids silently converting
existing tables to a slower or newly durable mode.

## Benchmark

Command:

```text
make benchmark-t215
```

Matched run: `-benchtime=3s -count=3 -benchmem`, Ryzen 9 5950X, amd64.
The map baseline clones the returned value so it matches the public isolation
contract of `StorageSpace.Get`.

| Workload | Median ns/op | B/op | allocs/op | Relative to map baseline |
| --- | ---: | ---: | ---: | ---: |
| Direct map, cloned value | 42.09 | 16 | 1 | 1.00x |
| `StorageSpace` memtx | 42.81 | 16 | 1 | 1.02x cost |
| `StorageSpace` on-disk, cold read | 911.9 | 48 | 1 | 21.66x cost |

The default memtx policy is effectively neutral in this lookup benchmark. The
on-disk policy is substantially slower for cold reads; its benefit is bounded
resident value memory and restart recovery, not latency. It should therefore be
selected per space only when persistence or memory bounds justify the cost.
