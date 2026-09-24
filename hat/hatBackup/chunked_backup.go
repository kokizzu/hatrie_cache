package hatBackup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
)

func (target *ObjectStoreTarget) uploadChunkedFile(ctx context.Context, root, relative string, layout ObjectStoreLayout, encryptionKeyID string, seenObjects map[string]struct{}) (objectStoreFileResult, error) {
	filePath := filepath.Join(root, filepath.FromSlash(relative))
	file, err := os.Open(filePath)
	if err != nil {
		return objectStoreFileResult{}, fmt.Errorf("hatriecache: open chunked object backup file %q: %w", relative, err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return objectStoreFileResult{}, fmt.Errorf("hatriecache: stat chunked object backup file %q: %w", relative, err)
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return objectStoreFileResult{}, fmt.Errorf("hatriecache: chunked object backup file %q is not regular", relative)
	}
	size := info.Size()
	if size <= int64(target.chunkSize) {
		_ = file.Close()
		return target.uploadFile(ctx, root, relative, layout, encryptionKeyID, seenObjects)
	}

	buffer := make([]byte, target.chunkSize)
	fileDigest := sha256.New()
	chunks := make([]BundleChunk, 0)
	result := objectStoreFileResult{}
	readBytes := int64(0)
	for {
		if err := checkObjectStoreContext(ctx); err != nil {
			_ = file.Close()
			return objectStoreFileResult{}, err
		}
		count, readErr := io.ReadFull(file, buffer)
		if readErr == io.EOF && count == 0 {
			break
		}
		if readErr != nil && readErr != io.ErrUnexpectedEOF {
			_ = file.Close()
			return objectStoreFileResult{}, fmt.Errorf("hatriecache: read chunked object backup file %q: %w", relative, readErr)
		}
		if count == 0 {
			break
		}
		chunkData := buffer[:count]
		_, _ = fileDigest.Write(chunkData)
		chunkDigest := sha256.Sum256(chunkData)
		chunkHash := hex.EncodeToString(chunkDigest[:])
		chunks = append(chunks, BundleChunk{Size: int64(count), SHA256: chunkHash})
		if _, seen := seenObjects[chunkHash]; !seen {
			objectRelative, keyErr := contentObjectRelative(chunkHash, encryptionKeyID)
			if keyErr != nil {
				_ = file.Close()
				return objectStoreFileResult{}, keyErr
			}
			reused := false
			if checker, ok := target.store.(ObjectStoreObjectExists); ok {
				exists, existsErr := checker.Exists(ctx, target.objectKey(objectRelative))
				if existsErr != nil {
					_ = file.Close()
					return objectStoreFileResult{}, fmt.Errorf("hatriecache: check existing chunk %q: %w", chunkHash, existsErr)
				}
				reused = exists
			}
			if reused {
				result.reusedObjects++
				result.reusedObjectBytes += int64(count)
				result.reusedObjectHashes = append(result.reusedObjectHashes, chunkHash)
			} else {
				if err := target.uploadChunkBytes(ctx, relative, objectRelative, chunkData, chunkHash); err != nil {
					_ = file.Close()
					return objectStoreFileResult{}, err
				}
				result.newObjects++
				result.newObjectBytes += int64(count)
				result.newObjectHashes = append(result.newObjectHashes, chunkHash)
			}
			seenObjects[chunkHash] = struct{}{}
		}
		readBytes += int64(count)
		if readErr == io.ErrUnexpectedEOF {
			break
		}
	}
	closeErr := file.Close()
	if closeErr != nil {
		return objectStoreFileResult{}, fmt.Errorf("hatriecache: close chunked object backup file %q: %w", relative, closeErr)
	}
	if readBytes != size {
		return objectStoreFileResult{}, fmt.Errorf("hatriecache: chunked object backup file %q read %d bytes, want %d", relative, readBytes, size)
	}
	if err := checkObjectStoreContext(ctx); err != nil {
		return objectStoreFileResult{}, err
	}
	latest, err := os.Stat(filePath)
	if err != nil {
		return objectStoreFileResult{}, fmt.Errorf("hatriecache: restat chunked object backup file %q: %w", relative, err)
	}
	if latest.Size() != size {
		return objectStoreFileResult{}, fmt.Errorf("hatriecache: chunked object backup file %q changed while hashing", relative)
	}
	result.file = BundleFile{
		Path:   relative,
		Size:   size,
		SHA256: hex.EncodeToString(fileDigest.Sum(nil)),
		Chunks: chunks,
	}
	return result, nil
}

func (target *ObjectStoreTarget) uploadChunkBytes(ctx context.Context, logicalRelative, objectRelative string, data []byte, expectedHash string) error {
	if err := checkObjectStoreContext(ctx); err != nil {
		return err
	}
	digest := sha256.New()
	plaintext := &countingObjectReader{reader: bytes.NewReader(data)}
	plaintextReader := io.Reader(io.TeeReader(plaintext, digest))
	transferSize := int64(len(data))
	if target.encryption != nil {
		_, key, err := target.encryption.active()
		if err != nil {
			return err
		}
		transferSize, err = encryptedObjectSize(int64(len(data)), DefaultObjectStoreEncryptionChunkSize)
		if err != nil {
			return err
		}
		plaintextReader, err = newObjectStorePayloadEncryptReader(ctx, plaintextReader, objectRelative, int64(len(data)), key, DefaultObjectStoreEncryptionChunkSize)
		if err != nil {
			return err
		}
	}
	transferred := &countingObjectReader{reader: &objectStoreContextReader{ctx: ctx, reader: plaintextReader}}
	if err := target.store.Put(ctx, target.objectKey(objectRelative), transferred, transferSize); err != nil {
		return fmt.Errorf("hatriecache: upload object backup chunk for %q: %w", logicalRelative, err)
	}
	if plaintext.count != int64(len(data)) || transferred.count != transferSize {
		return fmt.Errorf("hatriecache: uploaded object backup chunk for %q has unexpected size", logicalRelative)
	}
	if got := hex.EncodeToString(digest.Sum(nil)); !strings.EqualFold(got, expectedHash) {
		return fmt.Errorf("hatriecache: object backup chunk for %q changed during upload", logicalRelative)
	}
	return checkObjectStoreContext(ctx)
}

func validateBundleFileChunks(file BundleFile) error {
	if len(file.Chunks) == 0 {
		return nil
	}
	var total int64
	for index, chunk := range file.Chunks {
		if chunk.Size <= 0 {
			return fmt.Errorf("chunk %d for %q has non-positive size", index, file.Path)
		}
		if chunk.Size > math.MaxInt64-total {
			return fmt.Errorf("chunks for %q overflow total size", file.Path)
		}
		if len(chunk.SHA256) != sha256.Size*2 || strings.ToLower(chunk.SHA256) != chunk.SHA256 {
			return fmt.Errorf("chunk %d for %q has an invalid SHA-256", index, file.Path)
		}
		if _, err := hex.DecodeString(chunk.SHA256); err != nil {
			return fmt.Errorf("chunk %d for %q has an invalid SHA-256", index, file.Path)
		}
		total += chunk.Size
	}
	if total != file.Size {
		return fmt.Errorf("chunks for %q total %d bytes, want %d", file.Path, total, file.Size)
	}
	return nil
}
