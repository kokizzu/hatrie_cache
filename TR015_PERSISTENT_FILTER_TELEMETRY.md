# TR-015 Persistent Filter and Read Amplification Telemetry

This adopts the useful part of Tarantool/LSM operational telemetry without
changing the storage format or adding work to the cache read path. Pebble
already maintains filter and read-amplification counters; persistent-store
inspection now exposes those counters through `hatStorage.Properties`.

## Fields

`hatStorage.Properties` adds these additive JSON fields:

- `read_amplification`: Pebble's current LSM read-amplification estimate.
- `filter_hits`: filter checks that avoided a data-block read.
- `filter_misses`: filter checks that could not avoid a data-block read.

The counters are cumulative for the lifetime of the opened Pebble database.
They are diagnostic signals, not per-request measurements. A filter miss is not
itself a proven false positive: it means the filter could not reject the read,
which may be a real key or a false positive. The values are omitted by JSON
when zero because the fields retain `omitempty` compatibility.

LevelDB does not expose equivalent counters through its portable properties,
so its values remain zero/omitted. This keeps the common inspection contract
usable across both backends without fabricating measurements.

## Compatibility and Cost

The implementation reads `pebble.DB.Metrics()` from the existing
`Properties()` call and copies three already-maintained values. It does not
change Bloom-filter defaults, reads, writes, compaction, journals, or any
on-disk format. The public struct change is additive.

## Measurement

Workload: five Go benchmark runs of `Properties()` on an empty Pebble store.

| Metric | Before | After | Change |
| --- | ---: | ---: | ---: |
| `ns/op` median | 83,659 | 83,812 | +0.18% |
| `B/op` median | 19,882 | 19,874 | -0.04% |
| `allocs/op` median | 400 | 400 | 0 |

The small timing and byte differences are within normal benchmark noise; the
important result is no measurable cost. The raw benchmark command is
`make benchmark-tr015`.

## Verification

The contract test compares the exposed values with the live Pebble metrics:

```text
make test-tr015
```
