package hatMappedPart

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
)

var (
	ErrMappedReadOnlyPartOptionsInvalid   = errors.New("hatriecache: mapped read-only part options are invalid")
	ErrMappedReadOnlyPartTooLarge         = errors.New("hatriecache: mapped read-only part exceeds byte budget")
	ErrMappedReadOnlyPartSizeMismatch     = errors.New("hatriecache: mapped read-only part size mismatch")
	ErrMappedReadOnlyPartChecksumMismatch = errors.New("hatriecache: mapped read-only part checksum mismatch")
	ErrMappedReadOnlyPartNotRegular       = errors.New("hatriecache: mapped read-only part is not a regular file")
	ErrMappedReadOnlyPartClosed            = errors.New("hatriecache: mapped read-only part is closed")
	ErrMappedReadOnlyPartUnsupported       = errors.New("hatriecache: read-only file mapping is unsupported on this platform")
)

// MappedReadOnlyPartOptions bounds and validates one immutable part mapping.
// MaxBytes is required to prevent an accidental mapping of an unbounded file.
// ExpectedSize is checked when VerifySize is true. ExpectedSHA256 is optional
// and must be a 64-character hexadecimal SHA-256 digest when supplied.
type MappedReadOnlyPartOptions struct {
	MaxBytes       uint64
	ExpectedSize   uint64
	VerifySize     bool
	ExpectedSHA256 string
}

// MappedReadOnlyPart is a validated read-only view over one regular file.
// Callers must keep the underlying file immutable and must not use Bytes after
// Close. The returned bytes are backed by the OS mapping, not a Go heap copy.
type MappedReadOnlyPart struct {
	mu       sync.RWMutex
	file     *os.File
	data     []byte
	size     uint64
	checksum string

	closeOnce sync.Once
	closeErr  error
}

// OpenMappedReadOnlyPart opens, validates, and maps one regular immutable
// file. The file descriptor remains open until Close so the mapping's lifetime
// is explicit. The function performs no writes and follows no symlink path.
func OpenMappedReadOnlyPart(path string, options MappedReadOnlyPartOptions) (*MappedReadOnlyPart, error) {
	if strings.TrimSpace(path) == "" || strings.IndexByte(path, 0) >= 0 || options.MaxBytes == 0 {
		return nil, ErrMappedReadOnlyPartOptionsInvalid
	}
	requestedChecksum, err := normalizeChecksum(options.ExpectedSHA256)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("stat mapped read-only part: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, ErrMappedReadOnlyPartNotRegular
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open mapped read-only part: %w", err)
	}
	closeFile := func(openErr error) (*MappedReadOnlyPart, error) {
		_ = file.Close()
		return nil, openErr
	}
	openedInfo, err := file.Stat()
	if err != nil {
		return closeFile(fmt.Errorf("stat opened mapped read-only part: %w", err))
	}
	if !openedInfo.Mode().IsRegular() {
		return closeFile(ErrMappedReadOnlyPartNotRegular)
	}
	if openedInfo.Size() < 0 {
		return closeFile(ErrMappedReadOnlyPartOptionsInvalid)
	}
	size := uint64(openedInfo.Size())
	if size > options.MaxBytes {
		return closeFile(fmt.Errorf("%w: size=%d max=%d", ErrMappedReadOnlyPartTooLarge, size, options.MaxBytes))
	}
	if options.VerifySize && size != options.ExpectedSize {
		return closeFile(fmt.Errorf("%w: got=%d want=%d", ErrMappedReadOnlyPartSizeMismatch, size, options.ExpectedSize))
	}
	if uint64(int(size)) != size {
		return closeFile(ErrMappedReadOnlyPartTooLarge)
	}
	data, err := mapReadOnly(file, int(size))
	if err != nil {
		return closeFile(fmt.Errorf("%w: %v", ErrMappedReadOnlyPartUnsupported, err))
	}
	part := &MappedReadOnlyPart{file: file, data: data, size: size}
	if len(requestedChecksum) > 0 {
		digest := sha256.Sum256(data)
		if !bytes.Equal(digest[:], requestedChecksum) {
			_ = part.Close()
			return nil, ErrMappedReadOnlyPartChecksumMismatch
		}
		part.checksum = hex.EncodeToString(digest[:])
	}
	return part, nil
}

func normalizeChecksum(value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return nil, ErrMappedReadOnlyPartOptionsInvalid
	}
	return decoded, nil
}

// Bytes returns the mapped bytes, or nil after Close. The returned slice must
// not be retained after Close and must never be mutated.
func (part *MappedReadOnlyPart) Bytes() []byte {
	if part == nil {
		return nil
	}
	part.mu.RLock()
	data := part.data
	part.mu.RUnlock()
	return data
}

// Size returns the validated file size in bytes.
func (part *MappedReadOnlyPart) Size() uint64 {
	if part == nil {
		return 0
	}
	return part.size
}

// ChecksumSHA256 returns the verified lowercase digest, or an empty string
// when the caller did not request checksum validation.
func (part *MappedReadOnlyPart) ChecksumSHA256() string {
	if part == nil {
		return ""
	}
	return part.checksum
}

// Close unmaps the part and closes its file descriptor. It is idempotent.
func (part *MappedReadOnlyPart) Close() error {
	if part == nil {
		return nil
	}
	part.closeOnce.Do(func() {
		part.mu.Lock()
		data := part.data
		file := part.file
		part.data = nil
		part.file = nil
		part.mu.Unlock()

		var closeErr error
		if err := unmapReadOnly(data); err != nil {
			closeErr = err
		}
		if file != nil {
			if err := file.Close(); err != nil && closeErr == nil {
				closeErr = err
			}
		}
		part.closeErr = closeErr
	})
	return part.closeErr
}
