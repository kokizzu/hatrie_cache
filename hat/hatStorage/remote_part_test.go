package hatStorage_test

import (
	"errors"
	"path/filepath"
	"testing"

	"hatrie_cache/hat/hatStorage"
)

func TestRemotePartReferenceNormalizesAndResolvesLocalMetadata(t *testing.T) {
	reference, err := hatStorage.NewRemotePartReference(
		"S3://bucket/parts/part-001.bin?versionId=7",
		"parts/part-001.json",
		"sha256:abc123",
		4096,
	)
	if err != nil {
		t.Fatal(err)
	}
	if reference.ObjectURI() != "s3://bucket/parts/part-001.bin?versionId=7" {
		t.Fatalf("ObjectURI() = %q", reference.ObjectURI())
	}
	if reference.LocalMetadataPath() != "parts/part-001.json" || reference.SizeBytes() != 4096 || reference.Checksum() != "sha256:abc123" {
		t.Fatalf("reference metadata = %#v", reference)
	}
	metadata := reference.Metadata()
	if metadata.ObjectURI != reference.ObjectURI() || metadata.LocalMetadataPath != reference.LocalMetadataPath() || metadata.SizeBytes != 4096 || metadata.Checksum != reference.Checksum() {
		t.Fatalf("Metadata() = %#v", metadata)
	}
	resolved, err := reference.ResolveMetadataPath("/var/lib/hatrie")
	if err != nil {
		t.Fatal(err)
	}
	if resolved != filepath.Join("/var/lib/hatrie", "parts/part-001.json") {
		t.Fatalf("resolved metadata path = %q", resolved)
	}
}

func TestRemotePartReferenceRejectsUnsafeOrUnsupportedMetadata(t *testing.T) {
	for name, input := range map[string][4]interface{}{
		"missing uri":        {"", "parts/p.json", "sha256:x", uint64(1)},
		"local uri":          {"file://bucket/parts/p", "parts/p.json", "sha256:x", uint64(1)},
		"missing host":       {"s3:///parts/p", "parts/p.json", "sha256:x", uint64(1)},
		"missing object":     {"s3://bucket/", "parts/p.json", "sha256:x", uint64(1)},
		"userinfo":           {"s3://user@bucket/parts/p", "parts/p.json", "sha256:x", uint64(1)},
		"fragment":           {"s3://bucket/parts/p#fragment", "parts/p.json", "sha256:x", uint64(1)},
		"absolute metadata":  {"s3://bucket/parts/p", "/tmp/p.json", "sha256:x", uint64(1)},
		"traversal metadata": {"s3://bucket/parts/p", "../p.json", "sha256:x", uint64(1)},
		"empty checksum":     {"s3://bucket/parts/p", "parts/p.json", "", uint64(1)},
	} {
		objectURI, _ := input[0].(string)
		metadataPath, _ := input[1].(string)
		checksum, _ := input[2].(string)
		sizeBytes, _ := input[3].(uint64)
		if _, err := hatStorage.NewRemotePartReference(objectURI, metadataPath, checksum, sizeBytes); !errors.Is(err, hatStorage.ErrRemotePartReferenceInvalid) {
			t.Errorf("%s error = %v", name, err)
		}
	}

	reference, err := hatStorage.NewRemotePartReference("https://objects.example.test/parts/p?signature=x", "parts/p.json", "etag:x", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reference.ResolveMetadataPath(""); !errors.Is(err, hatStorage.ErrRemotePartReferenceInvalid) {
		t.Fatalf("empty metadata root error = %v", err)
	}
}

func BenchmarkRemotePartReferenceResolveMetadataPath(b *testing.B) {
	reference, err := hatStorage.NewRemotePartReference("s3://bucket/parts/p", "parts/p.json", "sha256:x", 1)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := reference.ResolveMetadataPath("/var/lib/hatrie"); err != nil {
			b.Fatal(err)
		}
	}
}
