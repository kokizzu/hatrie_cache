package hatStorage_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"hatrie_cache/hat/hatStorage"
)

func TestCHU30RemotePartCacheSeparatesColumnsAndDeduplicates(t *testing.T) {
	reference, err := hatStorage.NewRemotePartReference("s3://bucket/parts/orders", "parts/orders", "sha256:orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: 8, MaxEntries: 4})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	loader := func(_ context.Context, _ hatStorage.RemotePartReference, column string) ([]byte, error) {
		calls.Add(1)
		return []byte(column), nil
	}
	first, err := cache.GetColumn(context.Background(), reference, "id", 1, loader)
	if err != nil {
		t.Fatal(err)
	}
	second, err := cache.GetColumn(context.Background(), reference, "id", 1, loader)
	if err != nil {
		t.Fatal(err)
	}
	other, err := cache.GetColumn(context.Background(), reference, "tag", 1, loader)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != "id" || string(second) != "id" || string(other) != "tag" {
		t.Fatalf("column values = %q/%q/%q", first, second, other)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("column loader calls = %d, want 2", got)
	}
	if stats := cache.Stats(); stats.Entries != 2 || stats.Loads != 2 || stats.Hits != 1 {
		t.Fatalf("column cache stats = %#v, want two entries/two loads/one hit", stats)
	}
	if _, err := cache.GetColumn(context.Background(), reference, "", 1, loader); !errors.Is(err, hatStorage.ErrRemotePartCacheColumnRequired) {
		t.Fatalf("empty column error = %v, want %v", err, hatStorage.ErrRemotePartCacheColumnRequired)
	}
}

func TestCHU30RemotePartCachePrefetchColumnsIsBoundedAndDeduplicated(t *testing.T) {
	cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: 8, MaxEntries: 8})
	if err != nil {
		t.Fatal(err)
	}
	makeReference := func(name string) hatStorage.RemotePartReference {
		reference, err := hatStorage.NewRemotePartReference("s3://bucket/parts/"+name, "parts/"+name, "sha256:"+name, 1)
		if err != nil {
			t.Fatal(err)
		}
		return reference
	}
	requests := []hatStorage.RemotePartColumnRequest{
		{Reference: makeReference("a"), Column: "id"},
		{Reference: makeReference("a"), Column: "id"},
		{Reference: makeReference("a"), Column: "tag"},
		{Reference: makeReference("b"), Column: "id"},
	}
	var calls atomic.Int32
	var active atomic.Int32
	var maximum atomic.Int32
	loader := func(_ context.Context, _ hatStorage.RemotePartReference, _ string) ([]byte, error) {
		calls.Add(1)
		current := active.Add(1)
		for {
			old := maximum.Load()
			if current <= old || maximum.CompareAndSwap(old, current) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		active.Add(-1)
		return []byte("x"), nil
	}
	if err := cache.PrefetchColumns(context.Background(), requests, hatStorage.RemotePartPrefetchOptions{MaxConcurrent: 2}, loader); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("column prefetch calls = %d, want 3", got)
	}
	if got := maximum.Load(); got > 2 {
		t.Fatalf("column prefetch maximum concurrency = %d, want <= 2", got)
	}
	if got := cache.Stats().Entries; got != 3 {
		t.Fatalf("column prefetch entries = %d, want 3", got)
	}
}

func TestCHU30RemotePartCacheColumnInvalidationIsIndependent(t *testing.T) {
	reference, err := hatStorage.NewRemotePartReference("s3://bucket/parts/users", "parts/users", "sha256:users", 0)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: 8})
	if err != nil {
		t.Fatal(err)
	}
	loader := func(_ context.Context, _ hatStorage.RemotePartReference, column string) ([]byte, error) {
		return []byte(column), nil
	}
	if _, err := cache.GetColumn(context.Background(), reference, "id", 1, loader); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.GetColumn(context.Background(), reference, "tag", 1, loader); err != nil {
		t.Fatal(err)
	}
	if !cache.InvalidateColumn(reference, "id") {
		t.Fatal("InvalidateColumn(id) = false, want true")
	}
	if cache.InvalidateColumn(reference, "id") {
		t.Fatal("InvalidateColumn(id) second call = true, want false")
	}
	if cache.Stats().Entries != 1 {
		t.Fatalf("entries after column invalidation = %d, want 1", cache.Stats().Entries)
	}
}
