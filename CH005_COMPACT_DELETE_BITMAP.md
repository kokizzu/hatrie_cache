# CH-005 Compact Delete-Bitmap Snapshots

Typed-table patch parts remain opt-in through `TypedTableSchema.PatchParts`.
New `MarshalPatchState` calls now write version 2 snapshots containing the
schema name, physical row/deleted counts, a 128-bit deterministic xxHash
fingerprint of the length-delimited physical key order, the bitmap words, and
the existing CRC32 checksum. The physical key list is no longer duplicated in
every snapshot.

`RestorePatchState` continues to accept version 1 snapshots, so existing
backups can be upgraded without a migration step. The fingerprint is an
integrity/layout guard, not an authentication mechanism; callers must still
authenticate snapshots when they cross a trust boundary.

The fingerprint is cached by physical-key layout generation. Logical deletes
do not invalidate it. Physical appends, ordinary physical deletes, batch
appends, and patch compaction invalidate it, preserving exact restore checks
while avoiding repeated hashing of unchanged layouts.

## Benchmark

Command: `make benchmark-ch005-compact-patch` on the AMD Ryzen 9 5950X
Linux/amd64 host. The fixture has 4,096 physical rows and 820 logical deletes.
Each result is the median of five 100 ms samples.

| Path | Snapshot bytes | ns/op | B/op | allocs/op | Relative |
| --- | ---: | ---: | ---: | ---: | --- |
| Legacy v1 key list, marshal | 172,589 | 57,864 | 180,224 | 1 | 1.00x |
| Compact v2, warm marshal | 573 | 2,411 | 640 | 1 | 23.99x faster; 301.20x smaller |
| Compact v2, cold marshal after layout change | 573 | 126,734 | 640 | 1 | 2.19x slower; 301.20x smaller |
| Legacy v1 key list, restore | 172,589 | 36,835 | 528 | 2 | 1.00x |
| Compact v2, warm restore | 573 | 303 | 528 | 2 | 121.57x faster; 301.20x smaller |
| Compact v2, cold restore after layout change | 573 | 125,665 | 528 | 2 | 3.41x slower; 301.20x smaller |

Raw samples from the same run:

```text
Legacy marshal:       66782 56556 57864 57518 63958 ns/op
Compact warm marshal:  2454 2411 2395 2396 2479 ns/op
Compact cold marshal:  124769 126734 130706 127703 124381 ns/op
Legacy restore:        36233 36870 35869 36905 36835 ns/op
Compact warm restore:   327.5 298.5 302.0 302.7 306.2 ns/op
Compact cold restore:  123877 127433 125665 124675 127261 ns/op
```

Warm snapshots are the normal repeated-save/read path. A physical layout
change pays one rehash, after which the cached fingerprint is reused. The
default behavior of tables without patch parts is unchanged.

Focused correctness and compatibility coverage:

```text
make test-ch005-compact-patch
make test-ch005-compact-package
make race-ch005-compact-patch
```
