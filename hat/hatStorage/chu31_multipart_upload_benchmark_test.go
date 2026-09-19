package hatStorage_test

import (
	"context"
	"crypto/sha256"
	"testing"

	"hatrie_cache/hat/hatStorage"
)

var chu31ChecksumSink [sha256.Size]byte
var chu31PartChecksumSink [sha256.Size]byte
var chu31MultipartUploadSink hatStorage.RemoteMultipartUploadResult

func BenchmarkCHU31ChecksumBaseline(b *testing.B) {
	payload := make([]byte, 4<<20)
	for index := range payload {
		payload[index] = byte(index * 31)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		chu31ChecksumSink = sha256.Sum256(payload)
	}
}

func BenchmarkCHU31MultipartChecksumBaseline(b *testing.B) {
	payload := make([]byte, 4<<20)
	for index := range payload {
		payload[index] = byte(index * 31)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		chu31ChecksumSink = sha256.Sum256(payload)
		chu31PartChecksumSink = sha256.Sum256(payload)
	}
}

type chu31NoopMultipartUploadStore struct{}

func (chu31NoopMultipartUploadStore) InitiateMultipartUpload(context.Context, string, uint64, string) (string, error) {
	return "upload-1", nil
}

func (chu31NoopMultipartUploadStore) UploadMultipartPart(context.Context, string, int, []byte, string) (string, error) {
	return "etag-1", nil
}

func (chu31NoopMultipartUploadStore) CompleteMultipartUpload(context.Context, string, []hatStorage.RemoteMultipartUploadPart) error {
	return nil
}

func (chu31NoopMultipartUploadStore) AbortMultipartUpload(context.Context, string) error {
	return nil
}

func BenchmarkCHU31MultipartUpload(b *testing.B) {
	payload := make([]byte, 4<<20)
	for index := range payload {
		payload[index] = byte(index * 31)
	}
	store := chu31NoopMultipartUploadStore{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := hatStorage.UploadRemotePartMultipart(context.Background(), store, "s3://bucket/parts/p.bin", "parts/p.json", payload, hatStorage.RemoteMultipartUploadOptions{PartSize: uint64(len(payload))})
		if err != nil {
			b.Fatal(err)
		}
		chu31MultipartUploadSink = result
	}
}

func BenchmarkCHU31MultipartUploadFourParts(b *testing.B) {
	payload := make([]byte, 4<<20)
	for index := range payload {
		payload[index] = byte(index * 31)
	}
	store := chu31NoopMultipartUploadStore{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := hatStorage.UploadRemotePartMultipart(context.Background(), store, "s3://bucket/parts/p.bin", "parts/p.json", payload, hatStorage.RemoteMultipartUploadOptions{PartSize: 1 << 20})
		if err != nil {
			b.Fatal(err)
		}
		chu31MultipartUploadSink = result
	}
}
