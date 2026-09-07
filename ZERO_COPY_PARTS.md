# Immutable Part Transfer

`hatMerkle` provides two transfer paths for immutable parts:

| API | Integrity work | Transfer behavior | Use for |
| --- | --- | --- | --- |
| `CopyImmutablePart` | Hashes bytes while copying and checks exact size, checksum, and trailing input. | Bounded streaming; never materializes a second full part. | Untrusted readers, receivers, and compatibility paths. |
| `CopyImmutablePartFile` | Validates the file's remaining length but does not rehash it. | Calls `io.Copy` directly so native `WriterTo`/`ReaderFrom` hooks remain available, including `sendfile` where supported. | Published immutable files whose checksum was already established. |

## Verified Stream

```go
checksum := hatMerkle.ChecksumPart(part)
written, err := hatMerkle.CopyImmutablePart(destination, source, checksum)
if err != nil {
	return err
}
if written != int64(checksum.Size) {
	return errors.New("incomplete immutable part")
}
```

The function copies at most the declared size, rejects a short or oversized
source, and returns `ErrInvalidPartChecksum` for same-length corruption. The
destination can contain partial bytes when a transfer fails, so receive into a
temporary file or other unpublished destination before making it visible.

## File Fast Path

```go
checksum := hatMerkle.ChecksumPart(publishedPart)
file, err := os.Open(partPath)
if err != nil {
	return err
}
defer file.Close()

_, err = hatMerkle.CopyImmutablePartFile(responseWriter, file, checksum)
```

The file path checks the current file offset and remaining length before it
writes anything. It intentionally does not hash the file during transfer:
the source must be immutable for the duration of the copy, and its checksum
must come from the publication/manifest step. A receiver that does not trust
the sender should write to a temporary destination and run
`CopyImmutablePart` or `VerifyPartChecksum` before publishing the part.

## Benchmark

The following raw `go test -benchmem -count=5` result used a 960 KiB part on
an AMD Ryzen 9 5950X, Linux amd64:

| Path | Sample 1 | Sample 2 | Sample 3 | Sample 4 | Sample 5 | Median | Memory | Allocs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Verified reader | 477426 ns | 498288 ns | 488370 ns | 470784 ns | 498654 ns | 488370 ns | 33057 B/op | 8 |
| File fast path | 113920 ns | 115142 ns | 114962 ns | 117170 ns | 115141 ns | 115141 ns | 209 B/op | 1 |

The file path was about `4.24x` faster, with about `158x` lower measured
allocation. This is not a like-for-like integrity cost: the verified path
computes SHA-256 and the fast path trusts the immutable-part publication
boundary. Benchmark again on the deployment filesystem and transport because
kernel and endpoint support determine whether a native zero-copy hook is used.
