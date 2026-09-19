package hatStorage_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatStorage"
)

type chu31MultipartUploadCall struct {
	partNumber int
	data       []byte
	checksum   string
}

type chu31MultipartUploadStore struct {
	beginCalls    int
	uploadCalls   []chu31MultipartUploadCall
	completeParts []hatStorage.RemoteMultipartUploadPart
	failPart      int
	abortCalls    int
}

func (store *chu31MultipartUploadStore) InitiateMultipartUpload(context.Context, string, uint64, string) (string, error) {
	store.beginCalls++
	return "upload-1", nil
}

func (store *chu31MultipartUploadStore) UploadMultipartPart(_ context.Context, _ string, partNumber int, data []byte, checksum string) (string, error) {
	store.uploadCalls = append(store.uploadCalls, chu31MultipartUploadCall{
		partNumber: partNumber,
		data:       append([]byte(nil), data...),
		checksum:   checksum,
	})
	if partNumber == store.failPart {
		return "", errors.New("injected multipart failure")
	}
	return fmt.Sprintf("etag-%d", partNumber), nil
}

func (store *chu31MultipartUploadStore) CompleteMultipartUpload(_ context.Context, _ string, parts []hatStorage.RemoteMultipartUploadPart) error {
	store.completeParts = append([]hatStorage.RemoteMultipartUploadPart(nil), parts...)
	return nil
}

func (store *chu31MultipartUploadStore) AbortMultipartUpload(context.Context, string) error {
	store.abortCalls++
	return nil
}

func TestCHU31MultipartUploadSplitsChecksumsAndCompletes(t *testing.T) {
	store := new(chu31MultipartUploadStore)
	data := []byte("abcdefghij")
	var persisted []hatStorage.RemoteMultipartUploadState
	result, err := hatStorage.UploadRemotePartMultipart(context.Background(), store, "s3://bucket/parts/p.bin", "parts/p.json", data, hatStorage.RemoteMultipartUploadOptions{
		PartSize: 4,
		Persist: func(state hatStorage.RemoteMultipartUploadState) error {
			persisted = append(persisted, state)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("UploadRemotePartMultipart() error = %v", err)
	}
	if result.UploadedParts != 3 || result.SkippedParts != 0 {
		t.Fatalf("result counts = %#v, want uploaded=3 skipped=0", result)
	}
	if store.beginCalls != 1 || len(store.uploadCalls) != 3 || len(store.completeParts) != 3 {
		t.Fatalf("store calls = begin %d upload %d complete %d, want 1/3/3", store.beginCalls, len(store.uploadCalls), len(store.completeParts))
	}
	wantParts := []hatStorage.RemoteMultipartUploadPart{
		{PartNumber: 1, Offset: 0, SizeBytes: 4, Checksum: chu31Checksum([]byte("abcd")), ETag: "etag-1"},
		{PartNumber: 2, Offset: 4, SizeBytes: 4, Checksum: chu31Checksum([]byte("efgh")), ETag: "etag-2"},
		{PartNumber: 3, Offset: 8, SizeBytes: 2, Checksum: chu31Checksum([]byte("ij")), ETag: "etag-3"},
	}
	if !reflect.DeepEqual(result.State.Parts, wantParts) || !reflect.DeepEqual(store.completeParts, wantParts) {
		t.Fatalf("parts = %#v, want %#v", result.State.Parts, wantParts)
	}
	if result.Reference.ObjectURI() != "s3://bucket/parts/p.bin" || result.Reference.LocalMetadataPath() != "parts/p.json" || result.Reference.SizeBytes() != uint64(len(data)) || result.Reference.Checksum() != chu31Checksum(data) {
		t.Fatalf("reference = %#v, want normalized uploaded reference", result.Reference.Metadata())
	}
	if len(persisted) != 4 || persisted[0].UploadID != "upload-1" || len(persisted[3].Parts) != 3 {
		t.Fatalf("persisted states = %#v, want initiation plus three parts", persisted)
	}
}

func TestCHU31MultipartUploadResumesAfterPartFailure(t *testing.T) {
	store := &chu31MultipartUploadStore{failPart: 2}
	data := []byte("abcdefghij")
	first, err := hatStorage.UploadRemotePartMultipart(context.Background(), store, "s3://bucket/parts/p.bin", "parts/p.json", data, hatStorage.RemoteMultipartUploadOptions{PartSize: 4})
	if err == nil || first.UploadedParts != 1 || len(first.State.Parts) != 1 {
		t.Fatalf("first result = %#v, error %v, want one persisted part and an error", first, err)
	}
	store.failPart = 0
	second, err := hatStorage.UploadRemotePartMultipart(context.Background(), store, "s3://bucket/parts/p.bin", "parts/p.json", data, hatStorage.RemoteMultipartUploadOptions{
		PartSize: 4,
		State:    first.State,
	})
	if err != nil {
		t.Fatalf("resume error = %v", err)
	}
	if store.beginCalls != 1 || second.UploadedParts != 2 || second.SkippedParts != 1 || len(store.uploadCalls) != 4 {
		t.Fatalf("resume counts = begin %d result %#v uploads %d, want begin=1 uploaded=2 skipped=1 uploads=4", store.beginCalls, second, len(store.uploadCalls))
	}
}

func TestCHU31MultipartUploadRejectsCorruptStateBeforeNetwork(t *testing.T) {
	store := new(chu31MultipartUploadStore)
	data := []byte("abcdefgh")
	state := hatStorage.RemoteMultipartUploadState{
		UploadID:          "upload-1",
		ObjectURI:         "s3://bucket/parts/p.bin",
		LocalMetadataPath: "parts/p.json",
		SizeBytes:         uint64(len(data)),
		Checksum:          chu31Checksum(data),
		PartSize:          4,
		Parts: []hatStorage.RemoteMultipartUploadPart{{
			PartNumber: 1,
			Offset:     1,
			SizeBytes:  4,
			Checksum:   chu31Checksum([]byte("abcd")),
			ETag:       "etag-1",
		}},
	}
	_, err := hatStorage.UploadRemotePartMultipart(context.Background(), store, "s3://bucket/parts/p.bin", "parts/p.json", data, hatStorage.RemoteMultipartUploadOptions{PartSize: 4, State: state})
	if !errors.Is(err, hatStorage.ErrRemoteMultipartUploadStateInvalid) {
		t.Fatalf("error = %v, want ErrRemoteMultipartUploadStateInvalid", err)
	}
	if store.beginCalls != 0 || len(store.uploadCalls) != 0 || len(store.completeParts) != 0 {
		t.Fatalf("network calls after corrupt state = %#v, want none", store)
	}
}

func TestCHU31MultipartUploadHonorsCancellationAndSupportsAbort(t *testing.T) {
	store := new(chu31MultipartUploadStore)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := hatStorage.UploadRemotePartMultipart(canceled, store, "s3://bucket/parts/p.bin", "parts/p.json", []byte("data"), hatStorage.RemoteMultipartUploadOptions{PartSize: 2})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled upload error = %v, want context.Canceled", err)
	}
	if store.beginCalls != 0 {
		t.Fatalf("begin calls after cancellation = %d, want 0", store.beginCalls)
	}
	if err := hatStorage.AbortRemoteMultipartUpload(context.Background(), store, hatStorage.RemoteMultipartUploadState{UploadID: "upload-1"}); err != nil {
		t.Fatalf("AbortRemoteMultipartUpload() error = %v", err)
	}
	if store.abortCalls != 1 {
		t.Fatalf("abort calls = %d, want 1", store.abortCalls)
	}
}

func chu31Checksum(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}
