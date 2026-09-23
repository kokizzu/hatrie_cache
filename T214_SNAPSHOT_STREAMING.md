# T214: Streaming Snapshots

## Purpose

T214 lets a replica receive a snapshot through an `io.Reader` instead of a
shared snapshot path. The source writes to an `io.Writer`, so an `io.Pipe`,
HTTP request body, gRPC stream, or another backpressured transport can carry
the bytes directly.

The feature is opt-in. Existing `SaveSnapshot*` and `LoadSnapshot*` methods
continue to use files and retain their existing behavior.

## Usage

```go
reader, writer := io.Pipe()
writeErr := make(chan error, 1)
go func() {
	err := source.WriteSnapshotToWithJournalSequenceAndFormat(
		writer,
		journalSequence,
		hatCache.SnapshotFormatGzipBinary,
	)
	_ = writer.CloseWithError(err)
	writeErr <- err
}()

metadata, err := target.LoadSnapshotFromWithMetadata(reader)
if err != nil {
	return err
}
if err := <-writeErr; err != nil {
	return err
}
_ = metadata.JournalSequence
```

`WriteSnapshotTo` uses the default snapshot format and a zero journal sequence.
`WriteSnapshotToWithJournalSequenceAndFormat` makes both values explicit.
`LoadSnapshotFrom` discards metadata; the `WithMetadata` variant returns the
embedded journal sequence. Neither method closes the caller-owned reader or
writer.

All existing snapshot formats are supported, including binary, JSON, and the
gzip variants. The stream is the same validated snapshot format used by file
restore, so no separate wire codec is needed.

## Safety And Lifecycle

The receiver parses and applies entries to a staged generation. It adopts that
generation only after the stream reaches a valid end and all entries pass the
existing validation rules. A truncated, malformed, duplicate, or otherwise
invalid stream leaves the live trie unchanged. The sender and receiver do not
share files during transfer.

The API deliberately does not own transport concerns. Callers must provide
authentication, encryption, timeouts, cancellation, retry policy, fencing,
and any replica bootstrap lifecycle around the stream. A failed stream is not
resumable; restart from a new snapshot or use the existing WAL bootstrap
protocol after a complete snapshot has been installed. The receiver may hold
its snapshot-restore lock while waiting for the stream, so transport deadlines
should be finite.

## Cost

The end-to-end benchmark sends one small binary snapshot from a source trie to
a target trie three times per mode on an AMD Ryzen 9 5950X:

| Mode | Median time | Bytes/op | Allocs/op | Relative time | Relative bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Filesystem save + load | 2.226 ms | 127,987 | 53 | 1.00x | 1.00x |
| Pipe stream + staged load | 0.141 ms | 122,747 | 39 | 0.063x (15.8x faster) | 0.959x (4.1% lower) |

The stream avoids a synced sender-side file and its second filesystem pass. It
still uses the receiver's existing staged restore storage, and larger real
snapshots will be dominated by network bandwidth and receiver I/O rather than
this small fixture.

## Verification

```text
make test-t214
make test-t214-package
make race-t214
make vet-t214
make benchmark-t214-before
make benchmark-t214
make cleanup-hatrie-tmp-after-test
make audit-hatrie-tmp
```
