package hatBackup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
)

func (target *ObjectStoreTarget) chunkObjectKey(layout ObjectStoreLayout, file BundleFile, chunk BundleChunk, manifest BundleManifest) (string, string, error) {
	if layout != ObjectStoreLayoutContentAddressed {
		return "", "", fmt.Errorf("%w: chunked file %q requires content-addressed layout", ErrObjectStoreManifestInvalid, file.Path)
	}
	keyID := ""
	if manifest.Encryption != nil {
		keyID = manifest.Encryption.KeyID
	}
	relative, err := contentObjectRelative(chunk.SHA256, keyID)
	if err != nil {
		return "", "", fmt.Errorf("%w: invalid chunk for %q: %v", ErrObjectStoreManifestInvalid, file.Path, err)
	}
	return target.objectKey(relative), relative, nil
}

func (target *ObjectStoreTarget) openChunk(ctx context.Context, layout ObjectStoreLayout, file BundleFile, chunk BundleChunk, manifest BundleManifest) (io.Reader, io.ReadCloser, error) {
	objectKey, objectRelative, err := target.chunkObjectKey(layout, file, chunk, manifest)
	if err != nil {
		return nil, nil, err
	}
	body, err := target.store.Get(ctx, objectKey)
	if err != nil {
		return nil, nil, fmt.Errorf("hatriecache: download object backup chunk for %q: %w", file.Path, err)
	}
	metadata := BundleFile{Path: file.Path, Size: chunk.Size, SHA256: chunk.SHA256}
	reader, err := target.payloadReader(ctx, body, metadata, manifest, objectRelative)
	if err != nil {
		_ = body.Close()
		return nil, nil, err
	}
	if chunk.Size < math.MaxInt64 {
		reader = io.LimitReader(reader, chunk.Size+1)
	}
	return reader, body, nil
}

func (target *ObjectStoreTarget) restoreChunkedFile(ctx context.Context, staging string, file BundleFile, manifest BundleManifest, layout ObjectStoreLayout) error {
	path, err := objectStoreRestorePath(staging, file.Path)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("hatriecache: create restore directory for %q: %w", file.Path, err)
	}
	destination, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("hatriecache: create restored file %q: %w", file.Path, err)
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.Remove(path)
		}
	}()
	fullDigest := sha256.New()
	var total int64
	for index, chunk := range file.Chunks {
		if err := checkObjectStoreContext(ctx); err != nil {
			_ = destination.Close()
			return err
		}
		reader, body, err := target.openChunk(ctx, layout, file, chunk, manifest)
		if err != nil {
			_ = destination.Close()
			return err
		}
		chunkDigest := sha256.New()
		count, copyErr := io.Copy(io.MultiWriter(destination, fullDigest, chunkDigest), reader)
		closeErr := body.Close()
		if copyErr != nil {
			_ = destination.Close()
			return fmt.Errorf("hatriecache: restore chunk %d for %q: %w", index, file.Path, copyErr)
		}
		if closeErr != nil {
			_ = destination.Close()
			return fmt.Errorf("hatriecache: close restore chunk %d for %q: %w", index, file.Path, closeErr)
		}
		if err := validateChunkRead(file.Path, index, count, chunk, chunkDigest); err != nil {
			_ = destination.Close()
			return err
		}
		if count > math.MaxInt64-total {
			_ = destination.Close()
			return fmt.Errorf("hatriecache: restored file %q size overflow", file.Path)
		}
		total += count
	}
	if total != file.Size {
		_ = destination.Close()
		return fmt.Errorf("hatriecache: restored file %q has %d bytes, want %d", file.Path, total, file.Size)
	}
	if got := hex.EncodeToString(fullDigest.Sum(nil)); !strings.EqualFold(got, file.SHA256) {
		_ = destination.Close()
		return fmt.Errorf("hatriecache: checksum mismatch for restored file %q", file.Path)
	}
	if err := destination.Sync(); err != nil {
		_ = destination.Close()
		return fmt.Errorf("hatriecache: sync restored file %q: %w", file.Path, err)
	}
	if err := destination.Close(); err != nil {
		return fmt.Errorf("hatriecache: close restored file %q: %w", file.Path, err)
	}
	keep = true
	return nil
}

func (target *ObjectStoreTarget) verifyChunkedFile(ctx context.Context, file BundleFile, manifest BundleManifest, layout ObjectStoreLayout) error {
	fullDigest := sha256.New()
	var total int64
	for index, chunk := range file.Chunks {
		if err := checkObjectStoreContext(ctx); err != nil {
			return err
		}
		reader, body, err := target.openChunk(ctx, layout, file, chunk, manifest)
		if err != nil {
			return err
		}
		chunkDigest := sha256.New()
		count, readErr := io.Copy(io.MultiWriter(fullDigest, chunkDigest), reader)
		closeErr := body.Close()
		if readErr != nil {
			return fmt.Errorf("hatriecache: verify chunk %d for %q: %w", index, file.Path, readErr)
		}
		if closeErr != nil {
			return fmt.Errorf("hatriecache: close verified chunk %d for %q: %w", index, file.Path, closeErr)
		}
		if err := validateChunkRead(file.Path, index, count, chunk, chunkDigest); err != nil {
			return err
		}
		if count > math.MaxInt64-total {
			return fmt.Errorf("hatriecache: verified file %q size overflow", file.Path)
		}
		total += count
	}
	if total != file.Size {
		return fmt.Errorf("hatriecache: verified file %q has %d bytes, want %d", file.Path, total, file.Size)
	}
	if got := hex.EncodeToString(fullDigest.Sum(nil)); !strings.EqualFold(got, file.SHA256) {
		return fmt.Errorf("hatriecache: checksum mismatch for verified file %q", file.Path)
	}
	return nil
}

func validateChunkRead(path string, index int, count int64, chunk BundleChunk, digest hash.Hash) error {
	if count != chunk.Size {
		return fmt.Errorf("hatriecache: chunk %d for %q has %d bytes, want %d", index, path, count, chunk.Size)
	}
	if got := hex.EncodeToString(digest.Sum(nil)); !strings.EqualFold(got, chunk.SHA256) {
		return fmt.Errorf("hatriecache: checksum mismatch for chunk %d of %q", index, path)
	}
	return nil
}
