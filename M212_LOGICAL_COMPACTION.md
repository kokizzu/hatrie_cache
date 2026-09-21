# M212 Logical MVCC Compaction

M212 adopts the Materialize-style separation between advancing a logical
history frontier and physically rewriting retained state.

## API

`(*TypedTable).AdvanceMVCCCompactionThrough(sequence)` advances the lower
frontier for new snapshots in O(1). It validates the same sequence bounds as
physical compaction and returns `ErrTypedTableMVCCDisabled` when MVCC is not
enabled.

The operation does not rewrite version-chain heads or copy row values. Existing
snapshots remain valid, while new snapshots before the logical frontier are
rejected as compacted. `CompactMVCCThrough(sequence)` remains the physical
reclamation path and can be called later at the same frontier; M212 tracks the
physical frontier separately so this follow-up is not a no-op.

## Default And Tradeoff

The default is unchanged: existing callers of `CompactMVCCThrough` still get
physical reclamation. Logical compaction is explicit because it trades memory
reclamation for a much shorter writer pause. It retains historical version
nodes until physical compaction is run, so callers should use it when latency
matters and schedule physical reclamation according to their memory budget.

## Benchmark

Commands:

```text
make benchmark-m212-logical-compaction-baseline
make benchmark-m212-logical-compaction
```

Fixture: 256 keys, 16 MVCC versions per key, two typed columns; five samples,
`-benchtime=1s`, `-benchmem`. The physical baseline was measured with the
existing `CompactMVCCThrough` path. Logical-transition samples reset only the
scalar frontier between calls.

| Path | Raw ns/op samples | Median ns/op | B/op | allocs/op | Retained version nodes | Relative CPU |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Physical `CompactMVCCThrough` baseline | 75139; 74363; 74189; 71529; 69176 | 74189 | 73728 | 1024 | 512 | 1.00x |
| Logical `AdvanceMVCCCompactionThrough` | 8.987; 8.727; 8.767; 8.855; 8.047 | 8.767 | 0 | 0 | 4096 | 8460x faster |
| Logical frontier transition | 8.363; 8.766; 8.785; 8.342; 9.078 | 8.766 | 0 | 0 | 4096 | 8460x faster |

The logical path removes the measured compaction allocation cost and is about
8.5k times faster on this fixture. Its retained version-node count is 8x the
physically compacted result; that memory cost is why it is not the default.
