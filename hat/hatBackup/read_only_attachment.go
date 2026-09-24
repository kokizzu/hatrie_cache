package hatBackup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"strings"
)

var (
	// ErrReadOnlyBackupAttachmentFileNotFound indicates that a requested path
	// is not part of the attached backup manifest.
	ErrReadOnlyBackupAttachmentFileNotFound = errors.New("hatriecache: read-only backup attachment file not found")
	// ErrReadOnlyBackupAttachmentClosed indicates that a closed attachment file
	// reader was used again.
	ErrReadOnlyBackupAttachmentClosed = errors.New("hatriecache: read-only backup attachment file is closed")
)

// ReadOnlyBackupAttachment exposes immutable backup objects without creating
// a restore directory. The manifest is loaded when the attachment is created;
// payload bytes are fetched only when Open, ReadFile, or Verify is called.
//
// Open validates the payload checksum when the returned reader reaches EOF.
// Callers that stop before EOF should call Verify when complete integrity
// validation is required.
type ReadOnlyBackupAttachment struct {
	target   *ObjectStoreTarget
	manifest BundleManifest
	layout   ObjectStoreLayout
	files    map[string]BundleFile
}

// AttachReadOnly downloads and validates an object-store backup manifest for
// read-only access. It does not create, restore, or mutate any object.
func (target *ObjectStoreTarget) AttachReadOnly(ctx context.Context) (*ReadOnlyBackupAttachment, error) {
	if err := target.validate(ctx); err != nil {
		return nil, err
	}
	manifest, err := target.downloadManifest(ctx)
	if err != nil {
		return nil, err
	}
	layout, err := restoreObjectStoreLayout(manifest)
	if err != nil {
		return nil, err
	}
	files := make(map[string]BundleFile, len(manifest.Files))
	for _, file := range manifest.Files {
		files[file.Path] = file
	}
	return &ReadOnlyBackupAttachment{
		target:   target,
		manifest: manifest,
		layout:   layout,
		files:    files,
	}, nil
}

// Manifest returns the attached backup metadata and a copy of its file list.
func (attachment *ReadOnlyBackupAttachment) Manifest() BundleManifest {
	if attachment == nil {
		return BundleManifest{}
	}
	manifest := attachment.manifest
	manifest.Files = cloneBundleFiles(attachment.manifest.Files)
	return manifest
}

// Files returns the immutable file declarations in manifest order.
func (attachment *ReadOnlyBackupAttachment) Files() []BundleFile {
	if attachment == nil {
		return nil
	}
	return cloneBundleFiles(attachment.manifest.Files)
}

func cloneBundleFiles(files []BundleFile) []BundleFile {
	cloned := append([]BundleFile(nil), files...)
	for index := range cloned {
		cloned[index].Chunks = append([]BundleChunk(nil), files[index].Chunks...)
	}
	return cloned
}

// Open opens one manifest-listed backup file as a checksum-verifying stream.
// The path must be a relative slash-separated manifest path; traversal,
// absolute, and unknown paths are rejected before the object store is read.
func (attachment *ReadOnlyBackupAttachment) Open(ctx context.Context, name string) (io.ReadCloser, error) {
	if attachment == nil || attachment.target == nil {
		return nil, ErrObjectStoreNil
	}
	if err := attachment.target.validate(ctx); err != nil {
		return nil, err
	}
	if err := validateObjectStoreRelativePath(name); err != nil {
		return nil, err
	}
	file, ok := attachment.files[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrReadOnlyBackupAttachmentFileNotFound, name)
	}
	if len(file.Chunks) > 0 {
		return attachment.openChunked(ctx, file)
	}
	objectKey, objectRelative, err := attachment.target.fileObjectKey(attachment.layout, file, attachment.manifest)
	if err != nil {
		return nil, err
	}
	body, err := attachment.target.store.Get(ctx, objectKey)
	if err != nil {
		return nil, fmt.Errorf("hatriecache: open attached backup file %q: %w", name, err)
	}
	reader, err := attachment.target.payloadReader(ctx, body, file, attachment.manifest, objectRelative)
	if err != nil {
		_ = body.Close()
		return nil, err
	}
	if file.Size < math.MaxInt64 {
		reader = io.LimitReader(reader, file.Size+1)
	}
	return &readOnlyBackupFileReader{
		body:           body,
		reader:         reader,
		expectedSize:   file.Size,
		expectedSHA256: file.SHA256,
		digest:         sha256.New(),
	}, nil
}

// ReadFile reads one attached file completely and verifies its size and
// checksum before returning. Use Open for large files to avoid buffering them.
func (attachment *ReadOnlyBackupAttachment) ReadFile(ctx context.Context, name string) ([]byte, error) {
	reader, err := attachment.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, fmt.Errorf("hatriecache: close attached backup file %q: %w", name, closeErr)
	}
	return data, nil
}

// Verify reads and verifies every file in the attached manifest without
// creating a restore directory.
func (attachment *ReadOnlyBackupAttachment) Verify(ctx context.Context) error {
	if attachment == nil || attachment.target == nil {
		return ErrObjectStoreNil
	}
	for _, file := range attachment.manifest.Files {
		reader, err := attachment.Open(ctx, file.Path)
		if err != nil {
			return err
		}
		_, readErr := io.Copy(io.Discard, reader)
		closeErr := reader.Close()
		if readErr != nil {
			return fmt.Errorf("hatriecache: verify attached backup file %q: %w", file.Path, readErr)
		}
		if closeErr != nil {
			return fmt.Errorf("hatriecache: close verified attached backup file %q: %w", file.Path, closeErr)
		}
	}
	return nil
}

type readOnlyBackupFileReader struct {
	body           io.ReadCloser
	reader         io.Reader
	expectedSize   int64
	expectedSHA256 string
	digest         hash.Hash
	readBytes      int64
	verified       bool
	closed         bool
}

func (reader *readOnlyBackupFileReader) Read(buffer []byte) (int, error) {
	if reader.closed {
		return 0, ErrReadOnlyBackupAttachmentClosed
	}
	if reader.verified {
		return 0, io.EOF
	}
	n, err := reader.reader.Read(buffer)
	if n > 0 {
		_, _ = reader.digest.Write(buffer[:n])
		reader.readBytes += int64(n)
		if reader.readBytes > reader.expectedSize {
			return n, fmt.Errorf("hatriecache: attached backup file exceeds declared size %d", reader.expectedSize)
		}
	}
	if err != io.EOF {
		return n, err
	}
	if reader.readBytes != reader.expectedSize {
		return n, fmt.Errorf("hatriecache: attached backup file has %d bytes, want %d", reader.readBytes, reader.expectedSize)
	}
	if got := hex.EncodeToString(reader.digest.Sum(nil)); !strings.EqualFold(got, reader.expectedSHA256) {
		return n, errors.New("hatriecache: attached backup file checksum mismatch")
	}
	reader.verified = true
	return n, io.EOF
}

func (reader *readOnlyBackupFileReader) Close() error {
	if reader.closed {
		return nil
	}
	reader.closed = true
	return reader.body.Close()
}
