# CH-005 Adaptive Delete Bitmap Encoding

The persistent delete bitmap keeps the same packed in-memory representation,
CRC protection, bounds, and public API. `MarshalBinary` now writes version 2
and chooses the smaller of two payloads:

- Dense: one `uint64` word per 64 physical rows.
- Sparse: ascending deleted row ordinals encoded as unsigned deltas.

The sparse form is selected only when its encoded payload is smaller. Dense
version 2 is the automatic fallback for high delete density. `Decode` and
`UnmarshalBinary` continue to accept version 1 dense snapshots.

## Measurement

Command:

```text
make benchmark-ch005-adaptive-delete-bitmap
```

Fixture: 1,048,576 physical rows and four deleted rows. Five samples were
collected before and after the change on the same Linux/amd64 host.

| Metric | Before | After | Improvement |
| --- | ---: | ---: | ---: |
| Marshal + decode CPU | 114,148 ns/op | 47,658 ns/op | 2.40x faster |
| Go heap per round trip | 270,385 B/op | 131,152 B/op | 2.06x lower |
| Allocations | 3 | 3 | unchanged |
| Snapshot bytes | 131,089 | 19 | 6,899x lower |

Raw samples (`ns/op`, `B/op`, `allocs/op`):

```text
before: 120212 115065 114148 114000 110957; 270386 270385 270385 270387 270387; 3 3 3 3 3
after:   48488  47658  47320  42349  49538; 131152 131152 131152 131152 131152; 3 3 3 3 3
```

The high-cardinality dense fixture remains packed. Its snapshot grows by one
byte for the version-2 encoding tag (`12,522` to `12,523` bytes); the decoder
still materializes the same dense words. The existing dense benchmark remains
bounded at a median `6,388 ns/op` encode and `4,502 ns/op` decode for 100,000
rows with every third row deleted.

## Correctness and recovery

The implementation validates CRC, version, representation, row bounds,
monotone deltas, duplicate rows, population count, tail bits, and exact
payload consumption. Sparse decode validates the ordinal stream before
allocating the packed words. Tests also construct and restore a legacy
version-1 dense snapshot.

```text
make test-ch005-adaptive-delete-bitmap
make test-ch005-package
make race-ch005-adaptive-delete-bitmap
make vet-ch005-adaptive-delete-bitmap
```

There is no new configuration flag and no change to delete, read, compaction,
backup, or restore defaults. The only dense-format tradeoff is the one-byte
version-2 representation tag; sparse snapshots avoid retaining the old dense
backing allocation.
