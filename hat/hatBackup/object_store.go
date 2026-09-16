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
	objectStoreContentPrefix           = "objects"
	DefaultObjectStoreManifestMaxBytes = 8 << 20
	maxObjectStoreManifestFiles        = 1_000_000
)

// ObjectStoreLayout controls how payload objects are addressed. Auto keeps
// legacy path keys for snapshots and uses content-addressed keys for
// incremental Pebble backups.
type ObjectStoreLayout string

const (
	ObjectStoreLayoutAuto             ObjectStoreLayout = ""
	ObjectStoreLayoutPath             ObjectStoreLayout = "path"
	ObjectStoreLayoutContentAddressed ObjectStoreLayout = "content-addressed"
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

// ObjectStoreObjectExists is an optional capability used by content-addressed
// incremental backups. Stores that implement it can skip uploading objects
// already present under their immutable hash key.
type ObjectStoreObjectExists interface {
	Exists(ctx context.Context, key string) (bool, error)
}

// ObjectStoreTarget uploads and restores a backup bundle below one object-key
// prefix. The manifest is written last, so a reader never treats an incomplete
// upload as a complete backup.
type ObjectStoreTarget struct {
	store           ObjectStore
	prefix          string
	encryption      *objectStoreEncryptionConfig
	layout          ObjectStoreLayout
	manifestCatalog *BackupManifestCatalog
}

// NewObjectStoreTarget validates an object-store target prefix.
func NewObjectStoreTarget(store ObjectStore, prefix string) (*ObjectStoreTarget, error) {
	return NewObjectStoreTargetWithOptions(store, prefix, ObjectStoreTargetOptions{})
}

// NewObjectStoreTargetWithOptions validates an object-store target prefix and
// optional encryption keyring. Restores accept any key in the keyring while
// new backups use ActiveEncryptionKeyID.
func NewObjectStoreTargetWithOptions(store ObjectStore, prefix string, options ObjectStoreTargetOptions) (*ObjectStoreTarget, error) {
	if store == nil {
		return nil, ErrObjectStoreNil
	}
	normalized, err := normalizeObjectStorePrefix(prefix)
	if err != nil {
		return nil, err
	}
	encryption, err := newObjectStoreEncryptionConfig(options)
	if err != nil {
		return nil, err
	}
	layout, err := normalizeObjectStoreLayout(options.Layout)
	if err != nil {
		return nil, err
	}
	return &ObjectStoreTarget{
		store:           store,
		prefix:          normalized,
		encryption:      encryption,
		layout:          layout,
		manifestCatalog: options.ManifestCatalog,
	}, nil
}

func normalizeObjectStoreLayout(value ObjectStoreLayout) (ObjectStoreLayout, error) {
	switch ObjectStoreLayout(strings.ToLower(strings.TrimSpace(string(value)))) {
	case ObjectStoreLayoutAuto:
		return ObjectStoreLayoutAuto, nil
	case ObjectStoreLayoutPath:
		return ObjectStoreLayoutPath, nil
	case ObjectStoreLayoutContentAddressed:
		return ObjectStoreLayoutContentAddressed, nil
	default:
		return "", fmt.Errorf("%w: unsupported object layout %q", ErrObjectStoreManifestInvalid, value)
	}
}

func (target *ObjectStoreTarget) backupLayout(manifest BundleManifest) (ObjectStoreLayout, error) {
	requested, err := normalizeObjectStoreLayout(ObjectStoreLayout(manifest.ObjectLayout))
	if err != nil {
		return "", err
	}
	if requested != ObjectStoreLayoutAuto {
		if target.layout != ObjectStoreLayoutAuto && target.layout != requested {
			return "", fmt.Errorf("%w: manifest layout %q conflicts with target layout %q", ErrObjectStoreManifestInvalid, requested, target.layout)
		}
		return requested, nil
	}
	if target.layout != ObjectStoreLayoutAuto {
		return target.layout, nil
	}
	if manifest.Mode == ModePebbleIncremental {
		return ObjectStoreLayoutContentAddressed, nil
	}
	return ObjectStoreLayoutPath, nil
}

func restoreObjectStoreLayout(manifest BundleManifest) (ObjectStoreLayout, error) {
	if strings.TrimSpace(manifest.ObjectLayout) == "" {
		return ObjectStoreLayoutPath, nil
	}
	return normalizeObjectStoreLayout(ObjectStoreLayout(manifest.ObjectLayout))
}

func (target *ObjectStoreTarget) fileObjectKey(layout ObjectStoreLayout, file BundleFile, manifest BundleManifest) (string, string, error) {
	switch layout {
	case ObjectStoreLayoutPath:
		return target.objectKey(file.Path), file.Path, nil
	case ObjectStoreLayoutContentAddressed:
		keyID := ""
		if manifest.Encryption != nil {
			keyID = manifest.Encryption.KeyID
		}
		relative, err := contentObjectRelative(file.SHA256, keyID)
		if err != nil {
			return "", "", fmt.Errorf("%w: invalid content object for %q: %v", ErrObjectStoreManifestInvalid, file.Path, err)
		}
		return target.objectKey(relative), relative, nil
	default:
		return "", "", fmt.Errorf("%w: unsupported object layout %q", ErrObjectStoreManifestInvalid, layout)
	}
}

func contentObjectRelative(hash, encryptionKeyID string) (string, error) {
	hash = strings.ToLower(strings.TrimSpace(hash))
	if decoded, err := hex.DecodeString(hash); err != nil || len(decoded) != sha256.Size {
		return "", fmt.Errorf("invalid SHA-256 hash %q", hash)
	}
	if strings.TrimSpace(encryptionKeyID) == "" {
		return path.Join(objectStoreContentPrefix, hash), nil
	}
	keyID, err := normalizeObjectStoreEncryptionKeyID(encryptionKeyID)
	if err != nil {
		return "", err
	}
	return path.Join(objectStoreContentPrefix, keyID, hash), nil
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
	if target.encryption == nil {
		if manifest.Encryption != nil {
			return BundleManifest{}, fmt.Errorf("%w: encrypted manifest requires an encryption-enabled target", ErrObjectStoreEncryptionInvalid)
		}
	} else {
		keyID, _, err := target.encryption.active()
		if err != nil {
			return BundleManifest{}, err
		}
		manifest.Encryption = encryptionMetadataForKey(keyID)
	}
	layout, err := target.backupLayout(manifest)
	if err != nil {
		return BundleManifest{}, err
	}
	manifest.ObjectLayout = string(layout)
	encryptionKeyID := ""
	if manifest.Encryption != nil {
		encryptionKeyID = manifest.Encryption.KeyID
	}
	manifest.NewObjects = 0
	manifest.ReusedObjects = 0
	manifest.NewObjectBytes = 0
	manifest.ReusedObjectBytes = 0
	manifest.NewObjectHashes = nil
	manifest.ReusedObjectHashes = nil
	manifest.Files = make([]BundleFile, 0, len(paths))
	seenObjects := make(map[string]struct{}, len(paths))
	for _, relative := range paths {
		if err := checkObjectStoreContext(ctx); err != nil {
			return BundleManifest{}, err
		}
		result, err := target.uploadFile(ctx, root, relative, layout, encryptionKeyID, seenObjects)
		if err != nil {
			return BundleManifest{}, err
		}
		manifest.Files = append(manifest.Files, result.file)
		if result.newObject {
			manifest.NewObjects++
			manifest.NewObjectBytes += result.file.Size
			manifest.NewObjectHashes = append(manifest.NewObjectHashes, result.file.SHA256)
		}
		if result.reusedObject {
			manifest.ReusedObjects++
			manifest.ReusedObjectBytes += result.file.Size
			manifest.ReusedObjectHashes = append(manifest.ReusedObjectHashes, result.file.SHA256)
		}
	}
	if err := validateObjectStoreManifest(manifest); err != nil {
		return BundleManifest{}, err
	}
	encoded, err := target.encodeManifest(manifest)
	if err != nil {
		return BundleManifest{}, fmt.Errorf("hatriecache: encode object backup manifest: %w", err)
	}
	if err := target.store.Put(ctx, target.objectKey(objectStoreManifestName), bytes.NewReader(encoded), int64(len(encoded))); err != nil {
		return BundleManifest{}, fmt.Errorf("hatriecache: upload object backup manifest: %w", err)
	}
	if err := checkObjectStoreContext(ctx); err != nil {
		return BundleManifest{}, err
	}
	if target.manifestCatalog != nil && manifest.Mode == ModePebbleIncremental {
		if err := target.manifestCatalog.Append(manifest); err != nil {
			return BundleManifest{}, fmt.Errorf("hatriecache: append object backup manifest to catalog: %w", err)
		}
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
	layout, err := restoreObjectStoreLayout(manifest)
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
		objectKey, objectRelative, keyErr := target.fileObjectKey(layout, file, manifest)
		if keyErr != nil {
			return BundleManifest{}, keyErr
		}
		body, err := target.store.Get(ctx, objectKey)
		if err != nil {
			return BundleManifest{}, fmt.Errorf("hatriecache: download object backup file %q: %w", file.Path, err)
		}
		reader, readerErr := target.payloadReader(ctx, body, file, manifest, objectRelative)
		err = readerErr
		if err == nil {
			err = restoreObjectFile(ctx, reader, path, file)
		}
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
	layout, err := restoreObjectStoreLayout(manifest)
	if err != nil {
		return BundleManifest{}, err
	}
	for _, file := range manifest.Files {
		if err := checkObjectStoreContext(ctx); err != nil {
			return BundleManifest{}, err
		}
		objectKey, objectRelative, keyErr := target.fileObjectKey(layout, file, manifest)
		if keyErr != nil {
			return BundleManifest{}, keyErr
		}
		body, err := target.store.Get(ctx, objectKey)
		if err != nil {
			return BundleManifest{}, fmt.Errorf("hatriecache: verify object backup file %q: %w", file.Path, err)
		}
		digest := sha256.New()
		reader, readerErr := target.payloadReader(ctx, body, file, manifest, objectRelative)
		if readerErr != nil {
			_ = body.Close()
			return BundleManifest{}, readerErr
		}
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

func (target *ObjectStoreTarget) encodeManifest(manifest BundleManifest) ([]byte, error) {
	data, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("hatriecache: encode object backup manifest: %w", err)
	}
	if len(data) > DefaultObjectStoreManifestMaxBytes {
		return nil, fmt.Errorf("%w: manifest exceeds %d bytes", ErrObjectStoreManifestInvalid, DefaultObjectStoreManifestMaxBytes)
	}
	if target.encryption == nil {
		return data, nil
	}
	keyID, key, err := target.encryption.active()
	if err != nil {
		return nil, err
	}
	encoded, err := encryptObjectStoreManifest(data, keyID, key)
	if err != nil {
		return nil, fmt.Errorf("hatriecache: encrypt object backup manifest: %w", err)
	}
	if len(encoded) > maxObjectStoreEncryptedManifestBytes {
		return nil, fmt.Errorf("%w: encrypted manifest exceeds %d bytes", ErrObjectStoreManifestInvalid, DefaultObjectStoreManifestMaxBytes)
	}
	return encoded, nil
}

func (target *ObjectStoreTarget) payloadReader(ctx context.Context, body io.Reader, file BundleFile, manifest BundleManifest, objectRelative string) (io.Reader, error) {
	if manifest.Encryption == nil {
		return &objectStoreContextReader{ctx: ctx, reader: body}, nil
	}
	if err := validateObjectStoreEncryptionMetadata(manifest.Encryption); err != nil {
		return nil, err
	}
	if target.encryption == nil {
		return nil, fmt.Errorf("%w: encryption keyring is required for key %q", ErrObjectStoreEncryptionInvalid, manifest.Encryption.KeyID)
	}
	key, ok := target.encryption.lookup(manifest.Encryption.KeyID)
	if !ok {
		return nil, fmt.Errorf("%w: encryption key %q is not available", ErrObjectStoreEncryptionInvalid, manifest.Encryption.KeyID)
	}
	return newObjectStorePayloadDecryptReader(ctx, body, objectRelative, *manifest.Encryption, file.Size, key)
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

type objectStoreFileResult struct {
	file         BundleFile
	newObject    bool
	reusedObject bool
}

func (target *ObjectStoreTarget) uploadFile(ctx context.Context, root, relative string, layout ObjectStoreLayout, encryptionKeyID string, seenObjects map[string]struct{}) (objectStoreFileResult, error) {
	filePath := filepath.Join(root, filepath.FromSlash(relative))
	if layout != ObjectStoreLayoutContentAddressed {
		file, err := target.uploadFileAtKey(ctx, filePath, relative, relative, nil)
		if err != nil {
			return objectStoreFileResult{}, err
		}
		return objectStoreFileResult{file: file}, nil
	}

	file, err := hashObjectStoreFile(ctx, filePath, relative)
	if err != nil {
		return objectStoreFileResult{}, err
	}
	hash := strings.ToLower(file.SHA256)
	file.SHA256 = hash
	objectRelative, err := contentObjectRelative(hash, encryptionKeyID)
	if err != nil {
		return objectStoreFileResult{}, err
	}
	if _, exists := seenObjects[hash]; exists {
		return objectStoreFileResult{file: file}, nil
	}
	if checker, ok := target.store.(ObjectStoreObjectExists); ok {
		if err := checkObjectStoreContext(ctx); err != nil {
			return objectStoreFileResult{}, err
		}
		exists, err := checker.Exists(ctx, target.objectKey(objectRelative))
		if err != nil {
			return objectStoreFileResult{}, fmt.Errorf("hatriecache: check existing object %q: %w", hash, err)
		}
		if exists {
			seenObjects[hash] = struct{}{}
			return objectStoreFileResult{file: file, reusedObject: true}, nil
		}
	}
	if _, err := target.uploadFileAtKey(ctx, filePath, relative, objectRelative, &file); err != nil {
		return objectStoreFileResult{}, err
	}
	seenObjects[hash] = struct{}{}
	return objectStoreFileResult{file: file, newObject: true}, nil
}

func hashObjectStoreFile(ctx context.Context, filePath, relative string) (BundleFile, error) {
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
	counted := &countingObjectReader{reader: &objectStoreContextReader{ctx: ctx, reader: io.LimitReader(file, size)}}
	_, readErr := io.Copy(io.Discard, io.TeeReader(counted, digest))
	closeErr := file.Close()
	if readErr != nil {
		return BundleFile{}, fmt.Errorf("hatriecache: hash object backup file %q: %w", relative, readErr)
	}
	if closeErr != nil {
		return BundleFile{}, fmt.Errorf("hatriecache: close hashed object backup file %q: %w", relative, closeErr)
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
		return BundleFile{}, fmt.Errorf("hatriecache: object backup file %q changed while hashing", relative)
	}
	return BundleFile{Path: relative, Size: size, SHA256: hex.EncodeToString(digest.Sum(nil))}, nil
}

func (target *ObjectStoreTarget) uploadFileAtKey(ctx context.Context, filePath, relative, objectRelative string, expected *BundleFile) (BundleFile, error) {
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
	if expected != nil && expected.Size != size {
		_ = file.Close()
		return BundleFile{}, fmt.Errorf("hatriecache: object backup file %q changed between reads", relative)
	}
	digest := sha256.New()
	plaintext := &countingObjectReader{reader: io.LimitReader(file, size)}
	plaintextReader := io.Reader(io.TeeReader(plaintext, digest))
	transferSize := size
	if target.encryption != nil {
		_, key, activeErr := target.encryption.active()
		if activeErr != nil {
			_ = file.Close()
			return BundleFile{}, activeErr
		}
		transferSize, err = encryptedObjectSize(size, DefaultObjectStoreEncryptionChunkSize)
		if err != nil {
			_ = file.Close()
			return BundleFile{}, err
		}
		plaintextReader, err = newObjectStorePayloadEncryptReader(ctx, plaintextReader, objectRelative, size, key, DefaultObjectStoreEncryptionChunkSize)
		if err != nil {
			_ = file.Close()
			return BundleFile{}, err
		}
	}
	transferred := &countingObjectReader{reader: &objectStoreContextReader{ctx: ctx, reader: plaintextReader}}
	err = target.store.Put(ctx, target.objectKey(objectRelative), transferred, transferSize)
	closeErr := file.Close()
	if err != nil {
		return BundleFile{}, fmt.Errorf("hatriecache: upload object backup file %q: %w", relative, err)
	}
	if closeErr != nil {
		return BundleFile{}, fmt.Errorf("hatriecache: close object backup file %q: %w", relative, closeErr)
	}
	if plaintext.count != size {
		return BundleFile{}, fmt.Errorf("hatriecache: object backup file %q read %d plaintext bytes, want %d", relative, plaintext.count, size)
	}
	if transferred.count != transferSize {
		return BundleFile{}, fmt.Errorf("hatriecache: object backup file %q uploaded %d bytes, want %d", relative, transferred.count, transferSize)
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
	result := BundleFile{Path: relative, Size: size, SHA256: hex.EncodeToString(digest.Sum(nil))}
	if expected != nil && result.SHA256 != expected.SHA256 {
		return BundleFile{}, fmt.Errorf("hatriecache: object backup file %q changed during upload", relative)
	}
	return result, nil
}

func (target *ObjectStoreTarget) downloadManifest(ctx context.Context) (BundleManifest, error) {
	body, err := target.store.Get(ctx, target.objectKey(objectStoreManifestName))
	if err != nil {
		return BundleManifest{}, fmt.Errorf("hatriecache: download object backup manifest: %w", err)
	}
	raw, readErr := io.ReadAll(io.LimitReader(&objectStoreContextReader{ctx: ctx, reader: body}, maxObjectStoreEncryptedManifestBytes+1))
	closeErr := body.Close()
	if readErr != nil {
		return BundleManifest{}, fmt.Errorf("hatriecache: read object backup manifest: %w", readErr)
	}
	if closeErr != nil {
		return BundleManifest{}, fmt.Errorf("hatriecache: close object backup manifest: %w", closeErr)
	}
	if len(raw) > maxObjectStoreEncryptedManifestBytes {
		return BundleManifest{}, fmt.Errorf("%w: manifest exceeds %d bytes", ErrObjectStoreManifestInvalid, DefaultObjectStoreManifestMaxBytes)
	}
	data, keyID, encrypted, err := decryptObjectStoreManifest(raw, target.encryption)
	if err != nil {
		return BundleManifest{}, err
	}
	if len(data) > DefaultObjectStoreManifestMaxBytes {
		return BundleManifest{}, fmt.Errorf("%w: manifest exceeds %d bytes", ErrObjectStoreManifestInvalid, DefaultObjectStoreManifestMaxBytes)
	}
	var manifest BundleManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return BundleManifest{}, fmt.Errorf("%w: decode: %v", ErrObjectStoreManifestInvalid, err)
	}
	if encrypted {
		if manifest.Encryption == nil || manifest.Encryption.KeyID != keyID {
			return BundleManifest{}, fmt.Errorf("%w: encrypted manifest metadata does not match its envelope", ErrObjectStoreEncryptionInvalid)
		}
	} else if manifest.Encryption != nil {
		return BundleManifest{}, fmt.Errorf("%w: unencrypted manifest declares encryption metadata", ErrObjectStoreEncryptionInvalid)
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
	if err := validateObjectStoreEncryptionMetadata(manifest.Encryption); err != nil {
		return err
	}
	if _, err := restoreObjectStoreLayout(manifest); err != nil {
		return err
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
