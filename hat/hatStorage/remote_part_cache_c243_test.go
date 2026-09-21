package hatStorage

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestRemotePartCacheC243UsesImmutableChecksumKey(t *testing.T) {
	first, err := NewRemotePartReference("s3://bucket/parts/shared", "parts/shared.json", "sha256:first", 3)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRemotePartReference("s3://bucket/parts/shared", "parts/shared.json", "sha256:second", 3)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := NewRemotePartCache(RemotePartCacheOptions{MaxBytes: 6, MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	var loads atomic.Int32
	loader := func(_ context.Context, reference RemotePartReference) ([]byte, error) {
		loads.Add(1)
		if reference.Checksum() == first.Checksum() {
			return []byte("one"), nil
		}
		return []byte("two"), nil
	}
	if got, err := cache.Get(context.Background(), first, 0, loader); err != nil || string(got) != "one" {
		t.Fatalf("first read = %q/%v", got, err)
	}
	if got, err := cache.Get(context.Background(), second, 0, loader); err != nil || string(got) != "two" {
		t.Fatalf("second read = %q/%v", got, err)
	}
	if got, err := cache.Get(context.Background(), first, 0, loader); err != nil || string(got) != "one" {
		t.Fatalf("cached first read = %q/%v", got, err)
	}
	if loads.Load() != 2 {
		t.Fatalf("loader calls = %d, want two checksum-keyed misses", loads.Load())
	}
	if stats := cache.Stats(); stats.Misses != 2 || stats.Hits != 1 || stats.Entries != 2 {
		t.Fatalf("stats = %#v, want two misses, one hit, two entries", stats)
	}
}
