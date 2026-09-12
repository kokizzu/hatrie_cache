# Changefeed Checkpoints

`hatReplication.ChangefeedCheckpoint` is the durable handoff record for a
changefeed consumer. It binds the last applied progress sequence to a source,
so a checkpoint for one stream cannot silently be used for another stream.
Applications persist the returned binary frame using their existing durable
store; this package does not create files or choose a storage backend.

## Checkpoint And Resume

```go
checkpoint, err := hatReplication.NewChangefeedCheckpoint("orders", 0)
if err != nil {
	return err
}

progress, err := frontier.Advance(batchLastSequence)
if err != nil {
	return err
}
checkpoint, err = checkpoint.Advance(progress)
if err != nil {
	return err
}

frame, err := checkpoint.MarshalBinary()
if err != nil {
	return err
}
persist(frame)

checkpoint, err = hatReplication.UnmarshalChangefeedCheckpoint(frame)
if err != nil {
	return err
}
resumeSequence := checkpoint.Sequence
```

On reconnect, load the checkpoint, resume the source from `Sequence`, apply
updates, and advance the checkpoint only after the batch is durably applied.
Equal progress is idempotent. A lower progress sequence returns
`ErrChangefeedCheckpointRegressed`, preventing a stale consumer state from
overwriting a newer watermark. Progress messages with `Progressed == false`
are rejected.

## Binary Contract

The frame is a versioned 14-byte header followed by a UTF-8-agnostic source
identifier:

| Bytes | Field |
| ---: | --- |
| 0-3 | `hcp1` magic/version |
| 4-5 | Big-endian source length |
| 6-13 | Big-endian sequence |
| 14+ | Source bytes |

The source is trimmed and must be non-empty and at most 256 bytes. The complete
frame is capped at 270 bytes. Decoding rejects bad magic, truncated frames,
trailing bytes, invalid lengths, and surrounding source whitespace. Decoded
source storage is owned by the returned checkpoint. The frame is integrity-
checked by framing, not encrypted or authenticated; use the HMAC-protected
`hatDataStructure.CursorTokenCodec` when the checkpoint crosses an untrusted
transport.

## Benchmark

Run with `make benchmark-m202`. Five runs on an AMD Ryzen 9 5950X:

| Operation | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Advance | 5.932 | 0 | 0 |
| Marshal | 23.22 | 24 | 1 |
| Unmarshal | 19.86 | 8 | 1 |
| Advance prepared | 5.666 | 0 | 0 |

There is no previous checkpoint implementation baseline. Existing replication
and read-consistency defaults remain unchanged.
