# MZ-005 Immutable Sealed Upsert Runs

`hatDataStructure.SealedUpsertRun` is an importable, immutable batch/run format
for persisted or transferred keyed updates. It is designed for compaction
inputs: duplicate keys are consolidated with last-write-wins semantics, final
records are sorted by key, and the result is encoded once into a bounded binary
buffer.

This is separate from `SpillableArrangement`. The spillable arrangement is a
mutable local memory tier with append-only spill records. A sealed run is a
standalone sorted artifact that can be marshaled, transferred, validated, and
reconstructed without replaying the source updates.

## API

```go
updates := []hatDataStructure.UpsertRecord[[]byte]{
	{Key: "customer:42", Value: []byte("active")},
	{Key: "customer:17", Value: []byte("pending")},
	{Key: "customer:42", Value: []byte("closed")}, // last write wins
	{Key: "customer:99", Deleted: true},           // tombstone
}

run, err := hatDataStructure.NewSealedUpsertRun(updates, hatDataStructure.SealedUpsertRunOptions{})
if err != nil {
	return err
}
wire, err := run.MarshalBinary()
if err != nil {
	return err
}

restored, err := hatDataStructure.UnmarshalSealedUpsertRun(wire, hatDataStructure.SealedUpsertRunOptions{})
if err != nil {
	return err
}
record, found := restored.Lookup("customer:42")
// record.Value is "closed".
_ = record
_ = found
```

`Lookup` and `ForEach` return independent value copies. `ForEach` visits final
records in sorted key order. Deleted records contain no value. A nil or empty
key is rejected, and duplicate input keys do not survive sealing.

## Format And Limits

- The header contains the version, record count, restart stride, payload length,
  and CRC-32 checksum.
- Keys are front-coded against the previous sorted key. Every restart point
  stores a complete key so lookup does not scan the entire run.
- Values are length-delimited binary payloads; tombstones have a flag and zero
  value length.
- `UnmarshalSealedUpsertRun` copies the input and validates the full buffer,
  including lengths, ordering, restart points, and checksum.
- Zero options select a maximum of 1,048,576 input records, 1 MiB keys,
  64 MiB values, 256 MiB encoded bytes, and a restart every 16 records.
- `IndexStride` can be increased to reduce retained index metadata or lowered to
  reduce point-lookup scan work. The default 16 was selected from the measured
  stride sweep below.

The format is not encryption. Sensitive values require an encrypted transport
or filesystem layer. It is also not a replication or commit protocol: callers
own atomic publication, source offsets, and recovery policy.

## Measured Tradeoff

The benchmark uses 4,096 sorted customer keys with common prefixes and short
binary values. It compares the sealed run with a mutable `map[string][]byte`
copy-on-read path and `encoding/json` for serialization. Five 500 ms samples
were run on Linux/amd64, AMD Ryzen 9 5950X.

| Operation | Baseline median | Sealed median | Tradeoff |
| --- | ---: | ---: | --- |
| Point lookup | 51.57 ns/op, 16 B, 1 alloc | 355.4 ns/op, 16 B, 1 alloc | 6.89x slower CPU; same transient read allocation |
| Build | 211,471 ns/op, 393,538 B, 17 allocs | 1,568,152 ns/op, 886,521 B, 24 allocs | 7.42x slower CPU; 2.25x cumulative allocation |
| Marshal | 614,412 ns/op; 299,009 wire bytes | 16,126 ns/op; 90,022 wire bytes | 38.1x faster; 69.9% smaller wire |
| Unmarshal/validate | 3,529,767 ns/op, 876,114 B, 8,213 allocs | 213,065 ns/op, 107,840 B, 259 allocs | 16.6x faster; 87.7% fewer bytes; 31.7x fewer allocs |

The lookup path is intentionally not a replacement for a hot mutable map. The
run wins when data is sealed once and then transferred, persisted, validated,
or merged during compaction. The default remains opt-in, so existing mutable
operations pay no index or encoding cost.

### Restart Stride Sweep

| Index stride | Median lookup | Index behavior |
| ---: | ---: | --- |
| 8 | 239.0 ns/op | More restart metadata, shorter scans |
| 16 | 366.8 ns/op | Selected default balance |
| 64 | 1,148 ns/op | Smallest sparse index, longest scans |

All three variants used 16 B/op and one allocation for the returned value.

### Raw Five-Sample Results

```text
BenchmarkMZ005MutableMapLookup: 50.50, 49.50, 51.57, 52.21, 51.87 ns/op
BenchmarkMZ005SealedRunLookup: 392.0, 382.4, 355.4, 343.3, 347.5 ns/op
BenchmarkMZ005SealedRunLookupStride8: 238.9, 239.0, 244.3, 262.1, 228.6 ns/op
BenchmarkMZ005SealedRunLookupStride16: 349.0, 364.3, 370.3, 366.8, 377.6 ns/op
BenchmarkMZ005MutableMapBuild: 224273, 202591, 211471, 219950, 203618 ns/op
BenchmarkMZ005SealedRunBuild: 1574549, 1547555, 1568152, 1605979, 1523259 ns/op
BenchmarkMZ005JSONMarshal: 618895, 594580, 641110, 614412, 602809 ns/op; 299009 wire-bytes/op
BenchmarkMZ005SealedRunMarshal: 15340, 15877, 16126, 16527, 16456 ns/op; 90022 wire-bytes/op
BenchmarkMZ005JSONUnmarshal: 3300581, 3471815, 3564970, 3529767, 3555550 ns/op; 299009 wire-bytes/op
BenchmarkMZ005SealedRunUnmarshal: 207477, 197815, 213065, 218959, 214680 ns/op; 90022 wire-bytes/op
```

## Verification

```text
make test-mz005-sealed-run
make benchmark-mz005-sealed-run
```
