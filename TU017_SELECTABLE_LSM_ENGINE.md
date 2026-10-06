# T-U17 Selectable LSM Engine

Hatrie Cache exposes the two local persistent engines as explicit LSM
profiles. The default remains Pebble for a new path; the legacy LevelDB
profile is available when compatibility with an existing LevelDB directory is
required.

## Select A Profile

Library users can select a concrete engine for each persistent store or SQL
namespace and inspect the tradeoff metadata:

```go
profile, err := hatStorage.ProfileForBackend(hatStorage.BackendPebble)
if err != nil {
    return err
}
store, err := hatCache.OpenPersistentStoreWithProfile(
    "data/eu-west/cache",
    profile,
    hatCache.StorageFormatBinary,
)
if err != nil {
    return err
}
defer store.Close()

report, err := hatStorage.Inspect(store)
if err != nil {
    return err
}
fmt.Println(report.Profile.Name, report.Profile.WriteAmplification)
```

`hatStorage.EngineProfiles()` returns fresh profile values for UI, config, or
operator tooling. `BackendAuto` is intentionally not a profile because it is
a path-resolution policy, not an engine. Use `OpenPersistentStore` or
`OpenPersistentStoreWithFormat` when automatic marker resolution is desired.

## Profiles And Tradeoffs

| Profile | Default | Read path | Write path | Compaction | Backup/recovery |
| --- | --- | --- | --- | --- | --- |
| `pebble-lsm` | Yes for a new path | Low-to-moderate qualitative amplification with block cache and filters | Moderate qualitative amplification with background compaction | Automatic LSM compaction plus explicit compaction | Native Pebble checkpoints, generation bundles, and journal recovery |
| `leveldb-legacy-lsm` | No | Moderate qualitative amplification with block cache and filters | Moderate-to-high qualitative amplification while legacy levels compact | Explicit and periodic legacy LevelDB compaction | Directory/generation snapshots and journal replay; no native Pebble checkpoint |

These descriptions are workload guidance, not performance guarantees. Measure
with the target key distribution, value sizes, sync policy, and disk before
switching an existing deployment. A durable `<DB_PATH>.backend` marker still
rejects an engine mismatch, so choosing a profile cannot silently open a
directory with the wrong engine.

The profile API does not change the record format, backup contents, or command
semantics. It makes an existing per-store choice explicit and includes the
selected engine profile in `hatStorage.Inspect` reports.

## Measured Selection Cost

Environment: Linux/amd64, AMD Ryzen 9 5950X 16-Core Processor. The benchmark
opens and closes a fresh Pebble store for each iteration; it measures startup
selection, not steady-state reads or writes.

| Path | ns/op | B/op | allocs/op | Relative open time |
| --- | ---: | ---: | ---: | ---: |
| Existing explicit backend selector | 8,067,000 approximate median | 304,657 approximate median | 591 approximate median | 1.00x |
| Explicit `pebble-lsm` profile | 8,128,000 approximate median | 303,966 approximate median | 590 approximate median | 1.01x |

Repeated samples varied because each iteration creates and closes a filesystem
database. The full three-run sample range was 7.98-8.22 ms for the existing
selector and 8.02-9.87 ms for the profile path. The profile validation adds no
work to data operations; its only cost is at store open and is within startup
noise on this workload.
