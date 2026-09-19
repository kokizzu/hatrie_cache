package hatStorage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrRemoteMultipartUploadInvalid         = errors.New("hatriecache: remote multipart upload is invalid")
	ErrRemoteMultipartUploadContextRequired = errors.New("hatriecache: remote multipart upload context is required")
	ErrRemoteMultipartUploadStoreRequired   = errors.New("hatriecache: remote multipart upload store is required")
	ErrRemoteMultipartUploadStateInvalid    = errors.New("hatriecache: remote multipart upload state is invalid")
)

// DefaultRemoteMultipartUploadPartSize keeps per-part metadata bounded while
// avoiding a large retained working buffer; the caller's payload is sliced.
const DefaultRemoteMultipartUploadPartSize uint64 = 8 << 20

// MaxRemoteMultipartUploadParts bounds persisted state and completion calls.
const MaxRemoteMultipartUploadParts = 10_000

// RemoteMultipartUploadStore is the small object-store contract needed by the
// resumable coordinator. UploadMultipartPart must consume data before return
// and must not retain or mutate the caller-owned slice.
type RemoteMultipartUploadStore interface {
	InitiateMultipartUpload(ctx context.Context, objectURI string, sizeBytes uint64, checksum string) (uploadID string, err error)
	UploadMultipartPart(ctx context.Context, uploadID string, partNumber int, data []byte, checksum string) (etag string, err error)
	CompleteMultipartUpload(ctx context.Context, uploadID string, parts []RemoteMultipartUploadPart) error
	AbortMultipartUpload(ctx context.Context, uploadID string) error
}

// RemoteMultipartUploadPart is the durable manifest entry for one uploaded
// part. Offset and SizeBytes are validated against the current payload before
// a resumed upload is allowed to contact the store.
type RemoteMultipartUploadPart struct {
	PartNumber int    `json:"part_number"`
	Offset     uint64 `json:"offset"`
	SizeBytes  uint64 `json:"size_bytes"`
	Checksum   string `json:"checksum"`
	ETag       string `json:"etag"`
}

// RemoteMultipartUploadState is safe to serialize after each successful part.
// Parts with an ETag are treated as remotely complete on resume.
type RemoteMultipartUploadState struct {
	UploadID          string                      `json:"upload_id"`
	ObjectURI         string                      `json:"object_uri"`
	LocalMetadataPath string                      `json:"local_metadata_path"`
	SizeBytes         uint64                      `json:"size_bytes"`
	Checksum          string                      `json:"checksum"`
	PartSize          uint64                      `json:"part_size"`
	Parts             []RemoteMultipartUploadPart `json:"parts"`
}

// RemoteMultipartUploadOptions controls one upload or resume attempt. State
// is optional for a new upload. Persist, when supplied, receives independent
// snapshots after initiation and after every successful part.
type RemoteMultipartUploadOptions struct {
	PartSize uint64
	State    RemoteMultipartUploadState
	Persist  func(RemoteMultipartUploadState) error
}

// RemoteMultipartUploadResult reports progress even when the operation fails,
// allowing the caller to retain State and resume without re-uploading parts.
type RemoteMultipartUploadResult struct {
	Reference     RemotePartReference
	State         RemoteMultipartUploadState
	UploadedParts int
	SkippedParts  int
}

// UploadRemotePartMultipart uploads data through a resumable multipart
// protocol and returns a validated immutable remote-part reference on success.
// The store is deliberately injected so S3, GCS, Azure, or a test backend can
// supply their own authentication, retries, and wire implementation.
func UploadRemotePartMultipart(ctx context.Context, store RemoteMultipartUploadStore, objectURI, localMetadataPath string, data []byte, options RemoteMultipartUploadOptions) (RemoteMultipartUploadResult, error) {
	result := RemoteMultipartUploadResult{}
	if ctx == nil {
		return result, ErrRemoteMultipartUploadContextRequired
	}
	if store == nil {
		return result, ErrRemoteMultipartUploadStoreRequired
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}

	partSize := options.PartSize
	if partSize == 0 {
		partSize = DefaultRemoteMultipartUploadPartSize
	}
	sizeBytes := uint64(len(data))
	checksum := remoteMultipartChecksum(data)
	reference, err := NewRemotePartReference(objectURI, localMetadataPath, checksum, sizeBytes)
	if err != nil {
		return result, fmt.Errorf("%w: reference: %v", ErrRemoteMultipartUploadInvalid, err)
	}
	result.Reference = reference
	partCount := remoteMultipartPartCount(sizeBytes, partSize)
	if partCount > MaxRemoteMultipartUploadParts {
		return result, fmt.Errorf("%w: %d parts exceed maximum %d", ErrRemoteMultipartUploadInvalid, partCount, MaxRemoteMultipartUploadParts)
	}

	state := cloneRemoteMultipartUploadState(options.State)
	if strings.TrimSpace(state.UploadID) == "" {
		if !emptyRemoteMultipartUploadState(state) {
			return result, fmt.Errorf("%w: partial state without upload ID", ErrRemoteMultipartUploadStateInvalid)
		}
		uploadID, err := store.InitiateMultipartUpload(ctx, reference.ObjectURI(), sizeBytes, checksum)
		if err != nil {
			return result, err
		}
		uploadID = strings.TrimSpace(uploadID)
		if uploadID == "" {
			return result, fmt.Errorf("%w: store returned an empty upload ID", ErrRemoteMultipartUploadInvalid)
		}
		state = RemoteMultipartUploadState{
			UploadID:          uploadID,
			ObjectURI:         reference.ObjectURI(),
			LocalMetadataPath: reference.LocalMetadataPath(),
			SizeBytes:         sizeBytes,
			Checksum:          checksum,
			PartSize:          partSize,
		}
		if err := persistRemoteMultipartUploadState(options.Persist, state); err != nil {
			result.State = cloneRemoteMultipartUploadState(state)
			return result, err
		}
	} else {
		state.UploadID = strings.TrimSpace(state.UploadID)
		if err := validateRemoteMultipartUploadState(state, reference, partSize, partCount); err != nil {
			return result, err
		}
	}

	partStates, err := validateRemoteMultipartUploadParts(state.Parts, data, partSize, partCount)
	if err != nil {
		result.State = cloneRemoteMultipartUploadState(state)
		return result, err
	}
	state.Parts = compactRemoteMultipartUploadParts(partStates)
	result.State = cloneRemoteMultipartUploadState(state)
	for index := 0; index < int(partCount); index++ {
		if err := ctx.Err(); err != nil {
			state.Parts = compactRemoteMultipartUploadParts(partStates)
			result.State = cloneRemoteMultipartUploadState(state)
			return result, err
		}
		part := partStates[index]
		if part.ETag != "" {
			result.SkippedParts++
			continue
		}
		end := part.Offset + part.SizeBytes
		etag, err := store.UploadMultipartPart(ctx, state.UploadID, part.PartNumber, data[int(part.Offset):int(end)], part.Checksum)
		if err != nil {
			state.Parts = compactRemoteMultipartUploadParts(partStates)
			result.State = cloneRemoteMultipartUploadState(state)
			return result, err
		}
		etag = strings.TrimSpace(etag)
		if etag == "" {
			state.Parts = compactRemoteMultipartUploadParts(partStates)
			result.State = cloneRemoteMultipartUploadState(state)
			return result, fmt.Errorf("%w: store returned an empty ETag for part %d", ErrRemoteMultipartUploadInvalid, part.PartNumber)
		}
		part.ETag = etag
		partStates[index] = part
		result.UploadedParts++
		if options.Persist != nil {
			state.Parts = compactRemoteMultipartUploadParts(partStates)
			result.State = cloneRemoteMultipartUploadState(state)
			if err := persistRemoteMultipartUploadState(options.Persist, state); err != nil {
				return result, err
			}
		}
	}

	if err := ctx.Err(); err != nil {
		state.Parts = compactRemoteMultipartUploadParts(partStates)
		result.State = cloneRemoteMultipartUploadState(state)
		return result, err
	}
	state.Parts = compactRemoteMultipartUploadParts(partStates)
	if err := store.CompleteMultipartUpload(ctx, state.UploadID, cloneRemoteMultipartUploadParts(state.Parts)); err != nil {
		result.State = cloneRemoteMultipartUploadState(state)
		return result, err
	}
	result.State = cloneRemoteMultipartUploadState(state)
	return result, nil
}

// AbortRemoteMultipartUpload explicitly abandons an active remote upload. The
// normal upload path does not abort on error so persisted state remains usable.
func AbortRemoteMultipartUpload(ctx context.Context, store RemoteMultipartUploadStore, state RemoteMultipartUploadState) error {
	if ctx == nil {
		return ErrRemoteMultipartUploadContextRequired
	}
	if store == nil {
		return ErrRemoteMultipartUploadStoreRequired
	}
	uploadID := strings.TrimSpace(state.UploadID)
	if uploadID == "" {
		return fmt.Errorf("%w: upload ID is required", ErrRemoteMultipartUploadStateInvalid)
	}
	return store.AbortMultipartUpload(ctx, uploadID)
}

func validateRemoteMultipartUploadState(state RemoteMultipartUploadState, reference RemotePartReference, partSize, partCount uint64) error {
	if state.ObjectURI != reference.ObjectURI() || state.LocalMetadataPath != reference.LocalMetadataPath() || state.SizeBytes != reference.SizeBytes() || state.Checksum != reference.Checksum() || state.PartSize != partSize {
		return fmt.Errorf("%w: request does not match persisted upload", ErrRemoteMultipartUploadStateInvalid)
	}
	if uint64(len(state.Parts)) > partCount || partCount > MaxRemoteMultipartUploadParts {
		return fmt.Errorf("%w: invalid part count", ErrRemoteMultipartUploadStateInvalid)
	}
	return nil
}

func validateRemoteMultipartUploadParts(parts []RemoteMultipartUploadPart, data []byte, partSize, partCount uint64) ([]RemoteMultipartUploadPart, error) {
	partStates := make([]RemoteMultipartUploadPart, int(partCount))
	seen := make([]bool, int(partCount))
	for _, part := range parts {
		if part.PartNumber < 1 || uint64(part.PartNumber) > partCount || seen[part.PartNumber-1] {
			return nil, fmt.Errorf("%w: invalid part number %d", ErrRemoteMultipartUploadStateInvalid, part.PartNumber)
		}
		index := part.PartNumber - 1
		expected := remoteMultipartExpectedPart(data, partSize, index)
		if part.Offset != expected.Offset || part.SizeBytes != expected.SizeBytes || part.Checksum != expected.Checksum {
			return nil, fmt.Errorf("%w: part %d metadata does not match payload", ErrRemoteMultipartUploadStateInvalid, part.PartNumber)
		}
		part.ETag = strings.TrimSpace(part.ETag)
		partStates[index] = part
		seen[index] = true
	}
	for index := range partStates {
		if !seen[index] {
			partStates[index] = remoteMultipartExpectedPart(data, partSize, index)
		}
	}
	return partStates, nil
}

func remoteMultipartExpectedPart(data []byte, partSize uint64, index int) RemoteMultipartUploadPart {
	offset := uint64(index) * partSize
	remaining := uint64(len(data)) - offset
	sizeBytes := partSize
	if remaining < sizeBytes {
		sizeBytes = remaining
	}
	end := offset + sizeBytes
	return RemoteMultipartUploadPart{
		PartNumber: index + 1,
		Offset:     offset,
		SizeBytes:  sizeBytes,
		Checksum:   remoteMultipartChecksum(data[int(offset):int(end)]),
	}
}

func remoteMultipartPartCount(sizeBytes, partSize uint64) uint64 {
	if sizeBytes == 0 {
		return 0
	}
	count := sizeBytes / partSize
	if sizeBytes%partSize != 0 {
		count++
	}
	return count
}

func remoteMultipartChecksum(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func emptyRemoteMultipartUploadState(state RemoteMultipartUploadState) bool {
	return state.ObjectURI == "" && state.LocalMetadataPath == "" && state.SizeBytes == 0 && state.Checksum == "" && state.PartSize == 0 && len(state.Parts) == 0
}

func persistRemoteMultipartUploadState(persist func(RemoteMultipartUploadState) error, state RemoteMultipartUploadState) error {
	if persist == nil {
		return nil
	}
	return persist(cloneRemoteMultipartUploadState(state))
}

func compactRemoteMultipartUploadParts(parts []RemoteMultipartUploadPart) []RemoteMultipartUploadPart {
	completed := 0
	for _, part := range parts {
		if part.PartNumber > 0 && part.ETag != "" {
			completed++
		}
	}
	if completed == 0 {
		return nil
	}
	compact := make([]RemoteMultipartUploadPart, 0, completed)
	for _, part := range parts {
		if part.PartNumber > 0 && part.ETag != "" {
			compact = append(compact, part)
		}
	}
	return compact
}

func cloneRemoteMultipartUploadState(state RemoteMultipartUploadState) RemoteMultipartUploadState {
	state.Parts = cloneRemoteMultipartUploadParts(state.Parts)
	return state
}

func cloneRemoteMultipartUploadParts(parts []RemoteMultipartUploadPart) []RemoteMultipartUploadPart {
	if len(parts) == 0 {
		return nil
	}
	partsCopy := make([]RemoteMultipartUploadPart, len(parts))
	copy(partsCopy, parts)
	return partsCopy
}
