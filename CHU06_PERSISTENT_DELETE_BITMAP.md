# CH-U06 Persistent Delete Bitmap

`hatSql.SQLColumnarDeleteBitmap` is an importable sidecar primitive for
columnar storage adapters. It records deleted physical row positions without
coupling delete metadata to the part's column bytes.

## Usage

Build a bitmap from the physical rows deleted in a stored part:

```go
bitmap, err := hatSql.NewSQLColumnarDeleteBitmapFromRows(
    rowCount,
    []uint32{17, 4, 17, 900},
)
if err != nil {
    return err
}

sidecar, err := bitmap.MarshalBinary()
if err != nil {
    return err
}

restored, err := hatSql.UnmarshalSQLColumnarDeleteBitmap(sidecar)
if err != nil {
    return err
}
if restored.IsDeleted(17) {
    // Skip the physical row while reading the part.
}
```

Input row IDs may be out of order and may contain duplicates. The constructor
sorts and deduplicates only when necessary, and it copies sparse input so a
caller cannot mutate a completed bitmap. `AppendLiveRows`,
`AppendDeletedRows`, and `FilterLiveRows` support scan adapters without
exposing internal storage.

## Encoding

The constructor chooses the smaller representation:

- `sparse`: sorted row IDs encoded as unsigned-varint deltas;
- `dense`: one bit per physical row, stored as little-endian `uint64` words.

The sidecar format is deterministic:

```text
bytes 0..3    HDB1 magic
byte 4        format version (1)
byte 5        encoding (1 sparse, 2 dense)
bytes 6..7    reserved, must be zero
bytes 8..11   physical row count, little-endian uint32
bytes 12..15  deleted row count, little-endian uint32
bytes 16..19  payload byte count, little-endian uint32
bytes 20..N   sparse varints or dense words
last 4 bytes  CRC32 of header plus payload
```

Decoding validates the magic, version, reserved bytes, payload length, CRC,
row bounds, strict sparse ordering, dense padding bits, and deleted-count
consistency. The decoder caps row count at `MaxSQLColumnarDeleteBitmapRows`
and the complete sidecar at `MaxSQLColumnarDeleteBitmapBytes` before making
large allocations.

## Storage Adapter Contract

The bitmap is deliberately opt-in. A stored-part implementation owns the
sidecar filename, part identity, atomic publication, and the decision about
when to merge or discard it during compaction. Readers should load the sidecar
only after validating that its row count matches the columnar part, then apply
the mask before exposing rows. The bitmap is not a backup of column data and
does not replace part checksums or snapshot metadata.

## Measured Tradeoffs

The five-sample benchmark uses 1,000,000-row parts on an AMD Ryzen 9 5950X.
The comparison is an always-dense bitmap with the same 20-byte header and
4-byte CRC framing.

| Workload | Adaptive result | Always dense | Result |
| --- | ---: | ---: | --- |
| Build + marshal, 1% deletes | 53.5-60.0 us/op; 51,264 B/op; 3 allocs | 54.6-59.1 us/op; 262,144 B/op; 2 allocs | 92.0% lower wire bytes; similar CPU; one extra allocation |
| Build + marshal, 50% deletes | 0.87-0.90 ms/op; 262,208 B/op; 3 allocs | 0.87-0.88 ms/op; 262,145 B/op; 2 allocs | same wire size; about 1-3% CPU/bytes overhead |
| Unmarshal, 1% deletes | 32.1-33.0 us/op; 41,024 B/op; 2 allocs | dense payload is 125,024 B | sparse restore retains about 3.2x less decoded payload |
| Point lookup, 1% deletes | 7.7-10.8 ns/op | 1.65-1.66 ns/op | sparse lookup is about 5-6x slower |
| Point lookup, 50% deletes | 1.84-1.92 ns/op | 1.64-1.65 ns/op | dense lookup is within about 17% |

The adaptive format is therefore a storage and transfer optimization, not a
universal lookup optimization. Sparse sidecars are the default outcome for
low delete density; readers that need very high-rate point membership checks
should compact to dense representation or use a dense-producing workload.
The benchmark command is:

```text
make benchmark-ch-u06-delete-bitmap
```

The benchmark is standalone because the current branch also has unrelated
pre-existing `hatSql` compile gaps (`MaxDataflowTextBytes`,
`TypedTableDate`, and `TypedTableTimestamp`). The focused source-file test,
race test, and vet test pass independently; the normal package target should
be rerun once those baseline symbols are restored.
