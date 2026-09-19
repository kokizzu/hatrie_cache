# M-U47 Progress-Only Subscription Frames

M-U47 adds an explicit compact wire representation for subscription progress
updates. `EncodeQuerySubscriptionProgressFrame` and
`DecodeQuerySubscriptionProgressFrame` use the versioned QPF1 format without
changing the existing subscription channels or JSON behavior.

## API

```go
batch := hatSql.QuerySubscriptionDeltaBatch{
	ID:       7,
	Revision: 42,
	Frontier: 9001,
	Progress: true,
	Complete: false,
}

payload, err := hatSql.EncodeQuerySubscriptionProgressFrame(batch)
if err != nil {
	return err
}

decoded, err := hatSql.DecodeQuerySubscriptionProgressFrame(payload)
if err != nil {
	return err
}
```

The encoder accepts only a progress batch: `Progress` must be true, and
`Deltas`, `Columns`, and `Reset` must be empty or false. The decoder always
returns a batch with `Progress` true and no row data. Data batches continue to
use their existing channel or application-selected codec.

## QPF1 Format

The frame contains:

1. Four-byte `QPF1` magic.
2. One-byte version, currently `1`.
3. One-byte flags, currently only the `Complete` bit.
4. Uvarint subscription id, revision, and frontier.
5. Four-byte little-endian CRC32C over the preceding bytes.

The format is deterministic and bounded to 64 bytes on decode. Unknown
versions or flags, truncated varints, trailing bytes, and checksum failures
return `ErrQuerySubscriptionProgressFrameCorrupt`. The CRC detects accidental
corruption; it is not authentication. Use the existing authenticated
transport or an authenticated storage layer for untrusted peers.

The frame carries the revision and frontier needed by consumers to preserve
ordering. It does not enforce cross-frame monotonicity because a stream
adapter owns cancellation, reconnect, replay, and the last accepted revision;
it can compare the decoded `Revision` and `Frontier` before advancing its
consumer state. `Complete` marks an upper-frontier completion without adding a
synthetic row update.

The codec is explicit. Existing JSON and other application protocols remain
available as compatibility paths; malformed QPF1 input is never silently
interpreted as JSON.

## Measurements

Commands:

```sh
make benchmark-mu47-baseline
make benchmark-mu47
make measure-mu47-payload
```

Five `-count=5` samples on Linux/amd64, AMD Ryzen 9 5950X. The baseline uses
the existing `encoding/json` representation of the same progress batch.

| Workload | JSON raw ns/op samples | QPF1 raw ns/op samples | JSON median | QPF1 median | CPU improvement | JSON B/op | QPF1 B/op | JSON allocs/op | QPF1 allocs/op | Payload JSON/QPF1 |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Encode | 275.2; 285.8; 295.0; 291.5; 259.6 | 41.21; 34.26; 38.83; 35.13; 38.05 | 285.8 | 38.05 | 7.51x faster | 128 | 32 | 1 | 1 | 113 B / 14 B, 8.07x smaller |
| Decode | 1646; 1528; 1532; 1578; 1528 | 18.40; 20.64; 21.57; 22.46; 20.53 | 1532 | 20.64 | 74.2x faster | 296 | 0 | 5 | 0 | 113 B / 14 B, 8.07x smaller |

QPF1 is therefore a wire, CPU, and decode-allocation win for progress-only
frames. The existing JSON path remains unchanged for compatibility and for
data-bearing snapshots.
