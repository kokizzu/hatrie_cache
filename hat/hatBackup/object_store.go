package hatBackup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	objectStoreManifestName            = "manifest.json"
	DefaultObjectStoreManifestMaxBytes = 8 << 20
	maxObjectStoreManifestFiles        = 1_000_000
)

var (
	ErrObjectStoreNil             = errors.New("hatriecache: object store is nil")
	ErrObjectStorePrefixInvalid   = errors.New("hatriecache: object store prefix is invalid")
	ErrObjectStoreManifestInvalid = errors.New("hatriecache: object store manifest is invalid")
)

// ObjectStore is the minimal streaming API required by an object-store backup
// target. Implementations may map Put and Get to S3, GCS, Azure Blob Storage,
// or an internal immutable-object service.
type ObjectStore interface {
	Put(ctx context.Context, key string, body io.Reader, size int64) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
}

// ObjectStoreTarget uploads and restores a backup bundle below one object-key
// prefix. The manifest is written last, so a reader never treats an incomplete
// upload as a complete backup.
type ObjectStoreTarget struct {
	store  ObjectStore
	prefix string
}

// NewObjectStoreTarget validates an object-store target prefix.
func NewObjectStoreTarget(store ObjectStore, prefix string) (*ObjectStoreTarget, error) {
	if store == nil {
		return nil, ErrObjectStoreNil
	}
	normalized, err := normalizeObjectStorePrefix(prefix)
	if err != nil {
		return nil, err
	}
	return &ObjectStoreTarget{store: store, prefix: normalized}, nil
}

// Backup scans source, streams every regular file to the object store, and
// writes a verified manifest after all payloads succeed. The input manifest's
// metadata is retained; its Files, Version, and CreatedAt fields are produced
// from the source when omitted or stale.
func (target *ObjectStoreTarget) Backup(ctx context.Context, source string, manifest BundleManifest) (BundleManifest, error) {
	if err := target.validate(ctx); err != nil {
		return BundleManifest{}, err
	}
	if err := checkObjectStoreContext(ctx); err != nil {
		return BundleManifest{}, err
	}
	root, err := filepath.Abs(source)
	if err != nil {
		return BundleManifest{}, fmt.Errorf("hatriecache: object backup source: %w", err)
	}
	info, err := os.Lstat(root)
	if err != nil {
		return BundleManifest{}, fmt.Errorf("hatriecache: object backup source: %w", err)
	}
	if !info.IsDir() {
		return BundleManifest{}, fmt.Errorf("hatriecache: object backup source %q is not a directory", source)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return BundleManifest{}, fmt.Errorf("hatriecache: object backup source %q is a symlink", source)
	}
	if err := RejectRestoreSymlinkComponents(root); err != nil {
		return BundleManifest{}, err
	}

	paths, err := collectObjectStoreFiles(root)
	if err != nil {
		return BundleManifest{}, err
	}
	if manifest.Version == 0 {
		manifest.Version = BundleVersion
	}
	if manifest.Version != BundleVersion {
		return BundleManifest{}, fmt.Errorf("%w: version %d, want %d", ErrObjectStoreManifestInvalid, manifest.Version, BundleVersion)
	}
	if manifest.CreatedAt.IsZero() {
		manifest.CreatedAt = time.Now().UTC()
	}
	manifest.Files = make([]BundleFile, 0, len(paths))
	for _, relative := range paths {
		if err := checkObjectStoreContext(ctx); err != nil {
			return BundleManifest{}, err
		}
		file, err := target.uploadFile(ctx, root, relative)
		if err != nil {
			return BundleManifest{}, err
		}
		manifest.Files = append(manifest.Files, file)
	}
	if err := validateObjectStoreManifest(manifest); err != nil {
		return BundleManifest{}, err
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return BundleManifest{}, fmt.Errorf("hatriecache: encode object backup manifest: %w", err)
	}
	if err := target.store.Put(ctx, target.objectKey(objectStoreManifestName), bytes.NewReader(encoded), int64(len(encoded))); err != nil {
		return BundleManifest{}, fmt.Errorf("hatriecache: upload object backup manifest: %w", err)
	}
	if err := checkObjectStoreContext(ctx); err != nil {
		return BundleManifest{}, err
	}
	return manifest, nil
}

// Restore downloads and verifies a manifest and all referenced files into an
// isolated staging directory, then atomically publishes it at destination.
// The manifest object itself is metadata and is not copied into the restored
// data directory.
func (target *ObjectStoreTarget) Restore(ctx context.Context, destination string, overwrite bool) (BundleManifest, error) {
	if err := target.validate(ctx); err != nil {
		return BundleManifest{}, err
	}
	if err := checkObjectStoreContext(ctx); err != nil {
		return BundleManifest{}, err
	}
	manifest, err := target.downloadManifest(ctx)
	if err != nil {
		return BundleManifest{}, err
	}
	restore, err := prepareObjectStoreRestoreDestination(destination, overwrite)
	if err != nil {
		return BundleManifest{}, err
	}
	published := false
	defer func() {
		if !published {
			restore.Cleanup()
		}
	}()
	for _, file := range manifest.Files {
		if err := checkObjectStoreContext(ctx); err != nil {
			return BundleManifest{}, err
		}
		path, err := objectStoreRestorePath(restore.staging, file.Path)
		if err != nil {
			return BundleManifest{}, err
		}
		body, err := target.store.Get(ctx, target.objectKey(file.Path))
		if err != nil {
			return BundleManifest{}, fmt.Errorf("hatriecache: download object backup file %q: %w", file.Path, err)
		}
		err = restoreObjectFile(ctx, body, path, file)
		closeErr := body.Close()
		if err != nil {
			return BundleManifest{}, err
		}
		if closeErr != nil {
			return BundleManifest{}, fmt.Errorf("hatriecache: close object backup file %q: %w", file.Path, closeErr)
		}
	}
	if err := SyncRestoreTree(restore.staging); err != nil {
		return BundleManifest{}, err
	}
	if err := restore.Publish(overwrite); err != nil {
		return BundleManifest{}, err
	}
	published = true
	return manifest, nil
}

// Verify downloads the manifest and streams every referenced object through a
// checksum and size check without creating or publishing a restore directory.
// It is useful for validating a copied object-store prefix during a recovery
// drill.
func (target *ObjectStoreTarget) Verify(ctx context.Context) (BundleManifest, error) {
	if err := target.validate(ctx); err != nil {
		return BundleManifest{}, err
	}
	manifest, err := target.downloadManifest(ctx)
	if err != nil {
		return BundleManifest{}, err
	}
	for _, file := range manifest.Files {
		if err := checkObjectStoreContext(ctx); err != nil {
			return BundleManifest{}, err
		}
		body, err := target.store.Get(ctx, target.objectKey(file.Path))
		if err != nil {
			return BundleManifest{}, fmt.Errorf("hatriecache: verify object backup file %q: %w", file.Path, err)
		}
		digest := sha256.New()
		reader := io.Reader(&objectStoreContextReader{ctx: ctx, reader: body})
		if file.Size < math.MaxInt64 {
			reader = io.LimitReader(reader, file.Size+1)
		}
		readBytes, readErr := io.Copy(io.Discard, io.TeeReader(reader, digest))
		closeErr := body.Close()
		if readErr != nil {
			return BundleManifest{}, fmt.Errorf("hatriecache: verify object backup file %q: %w", file.Path, readErr)
		}
		if closeErr != nil {
			return BundleManifest{}, fmt.Errorf("hatriecache: close verified object backup file %q: %w", file.Path, closeErr)
		}
		if readBytes != file.Size {
			return BundleManifest{}, fmt.Errorf("hatriecache: verified file %q has %d bytes, want %d", file.Path, readBytes, file.Size)
		}
		if got := hex.EncodeToString(digest.Sum(nil)); !strings.EqualFold(got, file.SHA256) {
			return BundleManifest{}, fmt.Errorf("hatriecache: checksum mismatch for verified file %q", file.Path)
		}
	}
	return manifest, nil
}

func (target *ObjectStoreTarget) validate(ctx context.Context) error {
	if target == nil || target.store == nil {
		return ErrObjectStoreNil
	}
	return checkObjectStoreContext(ctx)
}

func (target *ObjectStoreTarget) objectKey(relative string) string {
	if target.prefix == "" {
		return relative
	}
	return target.prefix + "/" + relative
}

func (target *ObjectStoreTarget) uploadFile(ctx context.Context, root, relative string) (BundleFile, error) {
	filePath := filepath.Join(root, filepath.FromSlash(relative))
	file, err := os.Open(filePath)
	if err != nil {
		return BundleFile{}, fmt.Errorf("hatriecache: open object backup file %q: %w", relative, err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return BundleFile{}, fmt.Errorf("hatriecache: stat object backup file %q: %w", relative, err)
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return BundleFile{}, fmt.Errorf("hatriecache: object backup file %q is not regular", relative)
	}
	size := info.Size()
	digest := sha256.New()
	counted := &countingObjectReader{reader: io.LimitReader(file, size)}
	body := &objectStoreContextReader{ctx: ctx, reader: io.TeeReader(counted, digest)}
	err = target.store.Put(ctx, target.objectKey(relative), body, size)
	closeErr := file.Close()
	if err != nil {
		return BundleFile{}, fmt.Errorf("hatriecache: upload object backup file %q: %w", relative, err)
	}
	if closeErr != nil {
		return BundleFile{}, fmt.Errorf("hatriecache: close object backup file %q: %w", relative, closeErr)
	}
	if counted.count != size {
		return BundleFile{}, fmt.Errorf("hatriecache: object backup file %q read %d bytes, want %d", relative, counted.count, size)
	}
	if err := checkObjectStoreContext(ctx); err != nil {
		return BundleFile{}, err
	}
	latest, err := os.Stat(filePath)
	if err != nil {
		return BundleFile{}, fmt.Errorf("hatriecache: restat object backup file %q: %w", relative, err)
	}
	if latest.Size() != size {
		return BundleFile{}, fmt.Errorf("hatriecache: object backup file %q changed during upload", relative)
	}
	return BundleFile{Path: relative, Size: size, SHA256: hex.EncodeToString(digest.Sum(nil))}, nil
}

func (target *ObjectStoreTarget) downloadManifest(ctx context.Context) (BundleManifest, error) {
	body, err := target.store.Get(ctx, target.objectKey(objectStoreManifestName))
	if err != nil {
		return BundleManifest{}, fmt.Errorf("hatriecache: download object backup manifest: %w", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(&objectStoreContextReader{ctx: ctx, reader: body}, DefaultObjectStoreManifestMaxBytes+1))
	closeErr := body.Close()
	if readErr != nil {
		return BundleManifest{}, fmt.Errorf("hatriecache: read object backup manifest: %w", readErr)
	}
	if closeErr != nil {
		return BundleManifest{}, fmt.Errorf("hatriecache: close object backup manifest: %w", closeErr)
	}
	if len(data) > DefaultObjectStoreManifestMaxBytes {
		return BundleManifest{}, fmt.Errorf("%w: manifest exceeds %d bytes", ErrObjectStoreManifestInvalid, DefaultObjectStoreManifestMaxBytes)
	}
	var manifest BundleManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return BundleManifest{}, fmt.Errorf("%w: decode: %v", ErrObjectStoreManifestInvalid, err)
	}
	if err := validateObjectStoreManifest(manifest); err != nil {
		return BundleManifest{}, err
	}
	return manifest, nil
}

func collectObjectStoreFiles(root string) ([]string, error) {
	paths := make([]string, 0)
	err := filepath.WalkDir(root, func(filePath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("hatriecache: object backup source contains symlink %q", filePath)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("hatriecache: object backup source contains non-regular file %q", filePath)
		}
		relative, err := filepath.Rel(root, filePath)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if err := validateObjectStoreRelativePath(relative); err != nil {
			return err
		}
		if relative == objectStoreManifestName {
			return fmt.Errorf("%w: source file %q is reserved", ErrObjectStoreManifestInvalid, relative)
		}
		paths = append(paths, relative)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("hatriecache: scan object backup source: %w", err)
	}
	sort.Strings(paths)
	return paths, nil
}

func validateObjectStoreManifest(manifest BundleManifest) error {
	if manifest.Version != BundleVersion {
		return fmt.Errorf("%w: version %d, want %d", ErrObjectStoreManifestInvalid, manifest.Version, BundleVersion)
	}
	if len(manifest.Files) > maxObjectStoreManifestFiles {
		return fmt.Errorf("%w: file count %d exceeds %d", ErrObjectStoreManifestInvalid, len(manifest.Files), maxObjectStoreManifestFiles)
	}
	seen := make(map[string]struct{}, len(manifest.Files))
	for _, file := range manifest.Files {
		if err := validateObjectStoreRelativePath(file.Path); err != nil {
			return err
		}
		if file.Path == objectStoreManifestName {
			return fmt.Errorf("%w: manifest file path is reserved", ErrObjectStoreManifestInvalid)
		}
		if _, ok := seen[file.Path]; ok {
			return fmt.Errorf("%w: duplicate file path %q", ErrObjectStoreManifestInvalid, file.Path)
		}
		seen[file.Path] = struct{}{}
		if file.Size < 0 {
			return fmt.Errorf("%w: negative size for %q", ErrObjectStoreManifestInvalid, file.Path)
		}
		digest, err := hex.DecodeString(file.SHA256)
		if err != nil || len(digest) != sha256.Size {
			return fmt.Errorf("%w: invalid SHA256 for %q", ErrObjectStoreManifestInvalid, file.Path)
		}
	}
	return nil
}

func validateObjectStoreRelativePath(relative string) error {
	if relative == "" || relative == "." || strings.ContainsRune(relative, 0) || strings.Contains(relative, "\\") || path.IsAbs(relative) {
		return fmt.Errorf("%w: unsafe file path %q", ErrObjectStoreManifestInvalid, relative)
	}
	if path.Clean(relative) != relative || relative == ".." || strings.HasPrefix(relative, "../") {
		return fmt.Errorf("%w: traversal file path %q", ErrObjectStoreManifestInvalid, relative)
	}
	return nil
}

func normalizeObjectStorePrefix(prefix string) (string, error) {
	if strings.ContainsRune(prefix, 0) || strings.Contains(prefix, "\\") || path.IsAbs(prefix) {
		return "", ErrObjectStorePrefixInvalid
	}
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return "", nil
	}
	clean := path.Clean(prefix)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", ErrObjectStorePrefixInvalid
	}
	return clean, nil
}

func prepareObjectStoreRestoreDestination(destination string, overwrite bool) (RestoreDestination, error) {
	target, err := filepath.Abs(destination)
	if err != nil {
		return RestoreDestination{}, fmt.Errorf("hatriecache: restore target: %w", err)
	}
	if target == string(filepath.Separator) {
		return RestoreDestination{}, errors.New("hatriecache: restore target must not be a filesystem root")
	}
	parent := filepath.Dir(target)
	if err := RejectRestoreSymlinkComponents(parent); err != nil {
		return RestoreDestination{}, err
	}
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return RestoreDestination{}, fmt.Errorf("hatriecache: create restore parent: %w", err)
	}
	if err := RejectRestoreSymlinkComponents(parent); err != nil {
		return RestoreDestination{}, err
	}
	if _, err := ValidateRestoreTarget(target, overwrite); err != nil {
		return RestoreDestination{}, err
	}
	staging, err := os.MkdirTemp(parent, ".hatrie-object-restore-")
	if err != nil {
		return RestoreDestination{}, fmt.Errorf("hatriecache: create restore staging: %w", err)
	}
	return RestoreDestination{target: target, staging: staging}, nil
}

func objectStoreRestorePath(root, relative string) (string, error) {
	if err := validateObjectStoreRelativePath(relative); err != nil {
		return "", err
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	filePath, err := filepath.Abs(filepath.Join(root, filepath.FromSlash(relative)))
	if err != nil {
		return "", err
	}
	if filePath == root || !strings.HasPrefix(filePath, root+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: unsafe restore path %q", ErrObjectStoreManifestInvalid, relative)
	}
	return filePath, nil
}

func restoreObjectFile(ctx context.Context, body io.Reader, filePath string, metadata BundleFile) error {
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return fmt.Errorf("hatriecache: create restore directory for %q: %w", metadata.Path, err)
	}
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("hatriecache: create restored file %q: %w", metadata.Path, err)
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.Remove(filePath)
		}
	}()
	digest := sha256.New()
	reader := &objectStoreContextReader{ctx: ctx, reader: body}
	if metadata.Size < math.MaxInt64 {
		reader.reader = io.LimitReader(reader.reader, metadata.Size+1)
	}
	written, err := io.Copy(file, io.TeeReader(reader, digest))
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("hatriecache: restore object backup file %q: %w", metadata.Path, err)
	}
	if written != metadata.Size {
		_ = file.Close()
		return fmt.Errorf("hatriecache: restored file %q has %d bytes, want %d", metadata.Path, written, metadata.Size)
	}
	if got := hex.EncodeToString(digest.Sum(nil)); !strings.EqualFold(got, metadata.SHA256) {
		_ = file.Close()
		return fmt.Errorf("hatriecache: checksum mismatch for restored file %q", metadata.Path)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("hatriecache: sync restored file %q: %w", metadata.Path, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("hatriecache: close restored file %q: %w", metadata.Path, err)
	}
	keep = true
	return nil
}

func checkObjectStoreContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("hatriecache: object-store context is nil")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

type countingObjectReader struct {
	reader io.Reader
	count  int64
}

func (reader *countingObjectReader) Read(p []byte) (int, error) {
	n, err := reader.reader.Read(p)
	reader.count += int64(n)
	return n, err
}

type objectStoreContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader *objectStoreContextReader) Read(p []byte) (int, error) {
	if err := checkObjectStoreContext(reader.ctx); err != nil {
		return 0, err
	}
	return reader.reader.Read(p)
}
