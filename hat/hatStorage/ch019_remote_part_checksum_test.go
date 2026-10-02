package hatStorage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
)

func ch019TestChecksum(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func ch019TestReference(t *testing.T, data []byte, checksum string) RemotePartReference {
	t.Helper()
	reference, err := NewRemotePartReference("https://example.invalid/ch019-part", "ch019-part.meta", checksum, uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	return reference
}

func ch019TestCache(t *testing.T, verifyChecksums bool) *RemotePartCache {
	t.Helper()
	cache, err := NewRemotePartCache(RemotePartCacheOptions{
		MaxBytes:        1 << 20,
		MaxEntries:      2,
		VerifyChecksums: verifyChecksums,
	})
	if err != nil {
		t.Fatal(err)
	}
	return cache
}

func TestRemotePartCacheVerifiesChecksumBeforeAdmission(t *testing.T) {
	wanted := []byte("remote-part-payload")
	corrupted := append([]byte(nil), wanted...)
	corrupted[len(corrupted)-1] ^= 1
	reference := ch019TestReference(t, wanted, ch019TestChecksum(wanted))
	cache := ch019TestCache(t, true)

	_, err := cache.Get(context.Background(), reference, 0, func(context.Context, RemotePartReference) ([]byte, error) {
		return corrupted, nil
	})
	if !errors.Is(err, ErrRemotePartCacheChecksumMismatch) {
		t.Fatalf("Get error = %v, want checksum mismatch", err)
	}
	stats := cache.Stats()
	if stats.Entries != 0 || stats.Bytes != 0 || stats.ChecksumFailures != 1 {
		t.Fatalf("stats after rejected payload = %+v, want no entry and one checksum failure", stats)
	}
}

func TestRemotePartCacheAcceptsVerifiedChecksum(t *testing.T) {
	payload := []byte("verified-remote-part")
	reference := ch019TestReference(t, payload, ch019TestChecksum(payload))
	cache := ch019TestCache(t, true)

	got, err := cache.Get(context.Background(), reference, 0, func(context.Context, RemotePartReference) ([]byte, error) {
		return payload, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
	if stats := cache.Stats(); stats.Entries != 1 || stats.ChecksumFailures != 0 {
		t.Fatalf("stats after verified payload = %+v, want one entry and no failures", stats)
	}
}

func TestRemotePartCacheRejectsUnsupportedChecksumWhenVerificationEnabled(t *testing.T) {
	payload := []byte("opaque-checksum")
	reference := ch019TestReference(t, payload, "opaque:v1")
	cache := ch019TestCache(t, true)

	_, err := cache.Get(context.Background(), reference, 0, func(context.Context, RemotePartReference) ([]byte, error) {
		return payload, nil
	})
	if !errors.Is(err, ErrRemotePartCacheChecksumUnsupported) {
		t.Fatalf("Get error = %v, want unsupported checksum", err)
	}
	if stats := cache.Stats(); stats.Entries != 0 || stats.ChecksumFailures != 1 {
		t.Fatalf("stats after unsupported checksum = %+v, want no entry and one failure", stats)
	}
}

func TestRemotePartCacheKeepsChecksumVerificationOptIn(t *testing.T) {
	payload := []byte("verification-disabled")
	reference := ch019TestReference(t, payload, "opaque:v1")
	cache := ch019TestCache(t, false)

	if _, err := cache.Get(context.Background(), reference, 0, func(context.Context, RemotePartReference) ([]byte, error) {
		return payload, nil
	}); err != nil {
		t.Fatal(err)
	}
	if stats := cache.Stats(); stats.Entries != 1 || stats.ChecksumFailures != 0 {
		t.Fatalf("stats with verification disabled = %+v, want one entry and no failures", stats)
	}
}

func TestRemotePartCacheDoesNotRehashCacheHits(t *testing.T) {
	payload := []byte("cache-hit-payload")
	reference := ch019TestReference(t, payload, ch019TestChecksum(payload))
	cache := ch019TestCache(t, true)
	loads := 0
	loader := func(context.Context, RemotePartReference) ([]byte, error) {
		loads++
		return payload, nil
	}
	if _, err := cache.Get(context.Background(), reference, 0, loader); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Get(context.Background(), reference, 0, loader); err != nil {
		t.Fatal(err)
	}
	if loads != 1 {
		t.Fatalf("loader calls = %d, want one cache miss", loads)
	}
	if stats := cache.Stats(); stats.Hits != 1 || stats.ChecksumFailures != 0 {
		t.Fatalf("stats after cache hit = %+v, want one hit and no failures", stats)
	}
}
