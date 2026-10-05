package hatStorage_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatStorage"
)

func TestCHU30ColumnEntriesShareCapacityWithWholeParts(t *testing.T) {
	reference, err := hatStorage.NewRemotePartReference("s3://bucket/parts/shared-capacity", "parts/shared-capacity", "sha256:shared-capacity", 1)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: 2, MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	wholeLoader := func(context.Context, hatStorage.RemotePartReference) ([]byte, error) {
		return []byte("x"), nil
	}
	columnLoader := func(_ context.Context, _ hatStorage.RemotePartReference, _ string) ([]byte, error) {
		return []byte("x"), nil
	}
	if _, err := cache.Get(context.Background(), reference, 1, wholeLoader); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.GetColumn(context.Background(), reference, "value", 1, columnLoader); err != nil {
		t.Fatal(err)
	}
	if stats := cache.Stats(); stats.Entries != 2 || stats.Bytes != 2 {
		t.Fatalf("mixed cache stats = %#v, want two one-byte entries", stats)
	}
	if _, err := cache.GetColumn(context.Background(), reference, "other", 1, columnLoader); err != nil {
		t.Fatal(err)
	}
	if stats := cache.Stats(); stats.Entries != 2 || stats.Bytes != 2 {
		t.Fatalf("post-eviction mixed cache stats = %#v, want bounded two entries", stats)
	}
}

func TestCHU30ColumnAcquireHonorsPinnedCapacity(t *testing.T) {
	reference, err := hatStorage.NewRemotePartReference("s3://bucket/parts/pinned-column", "parts/pinned-column", "sha256:pinned-column", 1)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: 1, MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	loader := func(_ context.Context, _ hatStorage.RemotePartReference, _ string) ([]byte, error) {
		return []byte("x"), nil
	}
	handle, err := cache.AcquireColumn(context.Background(), reference, "value", 1, loader)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Release()
	if _, err := cache.GetColumn(context.Background(), reference, "other", 1, loader); err != nil {
		t.Fatal(err)
	}
	if stats := cache.Stats(); stats.Entries != 1 || stats.Bytes != 1 || stats.Uncached != 1 {
		t.Fatalf("pinned column stats = %#v, want one retained entry and one uncached read", stats)
	}
	handle.Release()
	if _, err := cache.GetColumn(context.Background(), reference, "other", 1, loader); err != nil {
		t.Fatal(err)
	}
	if stats := cache.Stats(); stats.Entries != 1 || stats.Bytes != 1 {
		t.Fatalf("released column stats = %#v, want one retained entry", stats)
	}
}

func TestCHU30ColumnPayloadSizeIsIndependentFromPartSize(t *testing.T) {
	reference, err := hatStorage.NewRemotePartReference("s3://bucket/parts/column-size", "parts/column-size", "sha256:column-size", 1024)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	loader := func(_ context.Context, _ hatStorage.RemotePartReference, _ string) ([]byte, error) {
		return []byte("x"), nil
	}
	if _, err := cache.GetColumn(context.Background(), reference, "value", 1, loader); err != nil {
		t.Fatalf("column payload with smaller size = %v", err)
	}
}
