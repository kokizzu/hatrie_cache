package hatMerkle

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
)

var (
	ErrPartTransferEndpoint = errors.New("hatriecache: immutable part transfer endpoint is nil")
	ErrPartTransferSize     = errors.New("hatriecache: immutable part transfer size is invalid")
)

// CopyImmutablePart streams one immutable part from src to dst while checking
// its exact size and SHA-256 checksum. It writes at most checksum.Size bytes,
// never builds a second full-payload buffer, and rejects trailing source data.
// Callers should use a temporary destination when a failed checksum must not
// leave partial data visible.
func CopyImmutablePart(dst io.Writer, src io.Reader, checksum PartChecksum) (int64, error) {
	if dst == nil || src == nil {
		return 0, ErrPartTransferEndpoint
	}
	if checksum.Size > math.MaxInt64 {
		return 0, fmt.Errorf("%w: %d bytes exceed the supported stream size", ErrPartTransferSize, checksum.Size)
	}

	digest := sha256.New()
	written, err := io.Copy(io.MultiWriter(dst, digest), io.LimitReader(src, int64(checksum.Size)))
	if err != nil {
		return written, err
	}
	if uint64(written) != checksum.Size {
		return written, fmt.Errorf("%w: copied %d bytes, expected %d", ErrPartTransferSize, written, checksum.Size)
	}

	var trailing [1]byte
	trailingRead, trailingErr := io.ReadFull(src, trailing[:])
	if trailingRead != 0 {
		return written, fmt.Errorf("%w: source contains bytes beyond the declared part", ErrPartTransferSize)
	}
	if trailingErr != io.EOF {
		if trailingErr == nil {
			return written, fmt.Errorf("%w: source contains bytes beyond the declared part", ErrPartTransferSize)
		}
		return written, trailingErr
	}
	if !bytesEqualDigest(digest.Sum(nil), checksum.Digest) {
		return written, ErrInvalidPartChecksum
	}
	return written, nil
}

// CopyImmutablePartFile copies the remaining bytes of an immutable part file
// after validating that its length exactly matches checksum.Size. It leaves
// the file and checksum untouched so io.Copy can use native WriterTo or
// ReaderFrom transfer hooks, including sendfile where the endpoints support
// it. The caller must ensure the file content is immutable and that the
// checksum was established at publication time; use CopyImmutablePart when
// the source is not trusted.
func CopyImmutablePartFile(dst io.Writer, src *os.File, checksum PartChecksum) (int64, error) {
	if dst == nil || src == nil {
		return 0, ErrPartTransferEndpoint
	}
	if checksum.Size > math.MaxInt64 {
		return 0, fmt.Errorf("%w: %d bytes exceed the supported stream size", ErrPartTransferSize, checksum.Size)
	}
	info, err := src.Stat()
	if err != nil {
		return 0, err
	}
	offset, err := src.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	if offset < 0 || offset > info.Size() {
		return 0, fmt.Errorf("%w: file offset %d is outside file size %d", ErrPartTransferSize, offset, info.Size())
	}
	remaining := info.Size() - offset
	if remaining != int64(checksum.Size) {
		return 0, fmt.Errorf("%w: file has %d remaining bytes, expected %d", ErrPartTransferSize, remaining, checksum.Size)
	}

	written, err := io.Copy(dst, src)
	if err != nil {
		return written, err
	}
	if uint64(written) != checksum.Size {
		return written, fmt.Errorf("%w: copied %d bytes, expected %d", ErrPartTransferSize, written, checksum.Size)
	}
	return written, nil
}

func bytesEqualDigest(got []byte, want [sha256.Size]byte) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range want {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}
