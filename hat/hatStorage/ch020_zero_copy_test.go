package hatStorage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func ch020TestReference(t *testing.T, payload []byte) RemotePartReference {
	t.Helper()
	digest := sha256.Sum256(payload)
	reference, err := NewRemotePartReference(
		"https://example.invalid/ch020-part",
		"ch020-part.meta",
		"sha256:"+hex.EncodeToString(digest[:]),
		uint64(len(payload)),
	)
	if err != nil {
		t.Fatal(err)
	}
	return reference
}

func ch020TestCache(t *testing.T, verifyChecksums bool) *RemotePartCache {
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

func TestRemotePartCacheGetOwnedAdoptsLoaderBuffer(t *testing.T) {
	payload := []byte("owned-remote-part")
	reference := ch020TestReference(t, payload)
	cache := ch020TestCache(t, true)

	got, err := cache.GetOwned(context.Background(), reference, 0, func(context.Context, RemotePartReference) ([]byte, error) {
		return payload, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 || &got[0] != &payload[0] {
		t.Fatal("GetOwned copied the loader buffer")
	}
	if stats := cache.Stats(); stats.Entries != 1 || stats.Bytes != uint64(len(payload)) {
		t.Fatalf("stats after adoption = %+v, want one retained buffer", stats)
	}
}

func TestRemotePartCacheGetKeepsCopyingLoaderBuffer(t *testing.T) {
	payload := []byte("copied-remote-part")
	reference := ch020TestReference(t, payload)
	cache := ch020TestCache(t, false)

	got, err := cache.Get(context.Background(), reference, 0, func(context.Context, RemotePartReference) ([]byte, error) {
		return payload, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 || &got[0] == &payload[0] {
		t.Fatal("Get unexpectedly adopted the loader buffer")
	}
	payload[0] = 'X'
	if got[0] == payload[0] {
		t.Fatal("Get result changed when the loader buffer changed")
	}
}

func TestRemotePartCacheAcquireOwnedAdoptsLoaderBuffer(t *testing.T) {
	payload := []byte("owned-acquire-part")
	reference := ch020TestReference(t, payload)
	cache := ch020TestCache(t, false)

	handle, err := cache.AcquireOwned(context.Background(), reference, 0, func(context.Context, RemotePartReference) ([]byte, error) {
		return payload, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if handle == nil || len(handle.Bytes()) == 0 || &handle.Bytes()[0] != &payload[0] {
		t.Fatal("AcquireOwned copied the loader buffer")
	}
	handle.Release()
}

func TestRemotePartCacheOwnedLoaderStillValidatesPayload(t *testing.T) {
	wanted := []byte("owned-validated-part")
	corrupted := append([]byte(nil), wanted...)
	corrupted[0] ^= 1
	reference := ch020TestReference(t, wanted)
	cache := ch020TestCache(t, true)

	if _, err := cache.GetOwned(context.Background(), reference, 0, func(context.Context, RemotePartReference) ([]byte, error) {
		return corrupted, nil
	}); err == nil {
		t.Fatal("GetOwned accepted a corrupted payload")
	}
	if stats := cache.Stats(); stats.Entries != 0 || stats.Bytes != 0 {
		t.Fatalf("stats after rejected owned payload = %+v, want no retained bytes", stats)
	}
}
