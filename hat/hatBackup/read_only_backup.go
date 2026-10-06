package hatBackup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"math"
	"strings"
)

// ErrReadOnlyBackupClosed is returned after a read-only backup file is closed.
var ErrReadOnlyBackupClosed = errors.New("hatriecache: read-only backup file is closed")

// ReadOnlyBackup is a verified, non-mutating view of one object-store backup.
// It fetches the manifest when opened and streams payload files on demand; it
// never creates a restore directory or changes a live cache.
type ReadOnlyBackup struct {
	target   *ObjectStoreTarget
	manifest BundleManifest
	layout   ObjectStoreLayout
}

// OpenReadOnly opens a backup manifest without downloading or restoring its
// payload files. Each file is verified when it is read to EOF or closed.
func (target *ObjectStoreTarget) OpenReadOnly(ctx context.Context) (*ReadOnlyBackup, error) {
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
	return &ReadOnlyBackup{
		target:   target,
		manifest: cloneReadOnlyBundleManifest(manifest),
		layout:   layout,
	}, nil
}

// Manifest returns an independent copy of the verified backup manifest.
func (backup *ReadOnlyBackup) Manifest() BundleManifest {
	if backup == nil {
		return BundleManifest{}
	}
	return cloneReadOnlyBundleManifest(backup.manifest)
}

// Files returns independent file metadata in manifest order.
func (backup *ReadOnlyBackup) Files() []BundleFile {
	if backup == nil {
		return nil
	}
	files := make([]BundleFile, len(backup.manifest.Files))
	copy(files, backup.manifest.Files)
	return files
}

// OpenFile returns a verified stream for one exact manifest path. Closing the
// stream before EOF drains and verifies the remaining payload.
func (backup *ReadOnlyBackup) OpenFile(ctx context.Context, relative string) (io.ReadCloser, error) {
	if backup == nil || backup.target == nil {
		return nil, ErrObjectStoreNil
	}
	if err := checkObjectStoreContext(ctx); err != nil {
		return nil, err
	}
	if err := validateObjectStoreRelativePath(relative); err != nil {
		return nil, err
	}
	var file BundleFile
	found := false
	for _, candidate := range backup.manifest.Files {
		if candidate.Path == relative {
			file = candidate
			found = true
			break
		}
	}
	if !found {
		return nil, fs.ErrNotExist
	}
	objectKey, objectRelative, err := backup.target.fileObjectKey(backup.layout, file, backup.manifest)
	if err != nil {
		return nil, err
	}
	body, err := backup.target.store.Get(ctx, objectKey)
	if err != nil {
		return nil, fmt.Errorf("hatriecache: open read-only backup file %q: %w", relative, err)
	}
	if body == nil {
		return nil, fmt.Errorf("hatriecache: open read-only backup file %q: object store returned a nil body", relative)
	}
	reader, err := backup.target.payloadReader(ctx, body, file, backup.manifest, objectRelative)
	if err != nil {
		_ = body.Close()
		return nil, err
	}
	if file.Size == math.MaxInt64 {
		return &readOnlyBackupFileReader{
			body:     body,
			reader:   reader,
			metadata: file,
			digest:   sha256.New(),
		}, nil
	}
	return &readOnlyBackupFileReader{
		body:     body,
		reader:   io.LimitReader(reader, file.Size+1),
		metadata: file,
		digest:   sha256.New(),
	}, nil
}

// ReadFile reads and verifies one complete manifest file.
func (backup *ReadOnlyBackup) ReadFile(ctx context.Context, relative string) ([]byte, error) {
	reader, err := backup.OpenFile(ctx, relative)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return data, nil
}

type readOnlyBackupFileReader struct {
	body      io.ReadCloser
	reader    io.Reader
	metadata  BundleFile
	digest    hash.Hash
	count     int64
	verifyErr error
	closeErr  error
	closed    bool
}

func (reader *readOnlyBackupFileReader) Read(destination []byte) (int, error) {
	if reader.closed {
		return 0, ErrReadOnlyBackupClosed
	}
	if reader.verifyErr != nil {
		return 0, reader.verifyErr
	}
	n, err := reader.reader.Read(destination)
	if n > 0 {
		reader.count += int64(n)
		_, _ = reader.digest.Write(destination[:n])
	}
	if err == io.EOF {
		reader.verifyErr = reader.verify()
		if reader.verifyErr != nil {
			return n, reader.verifyErr
		}
	}
	if err != nil && err != io.EOF {
		reader.verifyErr = err
		return n, err
	}
	return n, err
}

func (reader *readOnlyBackupFileReader) Close() error {
	if reader.closed {
		return reader.closeErr
	}
	if reader.verifyErr == nil {
		_, err := io.Copy(io.Discard, reader)
		if err != nil {
			reader.verifyErr = err
		}
	}
	reader.closeErr = reader.body.Close()
	reader.closed = true
	if reader.verifyErr != nil {
		return reader.verifyErr
	}
	return reader.closeErr
}

func (reader *readOnlyBackupFileReader) verify() error {
	if reader.count != reader.metadata.Size {
		return fmt.Errorf("hatriecache: read-only backup file %q has %d bytes, want %d", reader.metadata.Path, reader.count, reader.metadata.Size)
	}
	got := hex.EncodeToString(reader.digest.Sum(nil))
	if !strings.EqualFold(got, reader.metadata.SHA256) {
		return fmt.Errorf("hatriecache: checksum mismatch for read-only backup file %q", reader.metadata.Path)
	}
	return nil
}

func cloneReadOnlyBundleManifest(manifest BundleManifest) BundleManifest {
	clone := manifest
	clone.NewObjectHashes = append([]string(nil), manifest.NewObjectHashes...)
	clone.ReusedObjectHashes = append([]string(nil), manifest.ReusedObjectHashes...)
	clone.KeyPrefixes = append([]string(nil), manifest.KeyPrefixes...)
	clone.Files = append([]BundleFile(nil), manifest.Files...)
	if manifest.Partition != nil {
		partition := *manifest.Partition
		partition.Partitions = append([]string(nil), manifest.Partition.Partitions...)
		partition.KeyPrefixes = append([]string(nil), manifest.Partition.KeyPrefixes...)
		clone.Partition = &partition
	}
	if manifest.Encryption != nil {
		encryption := *manifest.Encryption
		clone.Encryption = &encryption
	}
	if manifest.Consistency != nil {
		consistency := *manifest.Consistency
		consistency.Parts = append([]BundlePart(nil), manifest.Consistency.Parts...)
		if manifest.Consistency.Journal != nil {
			journal := *manifest.Consistency.Journal
			consistency.Journal = &journal
		}
		clone.Consistency = &consistency
	}
	return clone
}

var _ io.ReadCloser = (*readOnlyBackupFileReader)(nil)
