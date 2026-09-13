package hatStorage_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"hatrie_cache/hat/hatStorage"
)

func TestRemotePartCacheDeduplicatesLoadsAndTracksStats(t *testing.T) {
	reference, err := hatStorage.NewRemotePartReference("s3://bucket/parts/a", "parts/a.json", "sha256:a", 3)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: 6, MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	loader := func(_ context.Context, got hatStorage.RemotePartReference) ([]byte, error) {
		if got.ObjectURI() != reference.ObjectURI() {
			t.Fatalf("loader reference = %q, want %q", got.ObjectURI(), reference.ObjectURI())
		}
		calls.Add(1)
		return []byte("abc"), nil
	}

	first, err := cache.Get(context.Background(), reference, 3, loader)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != "abc" {
		t.Fatalf("first bytes = %q", first)
	}
	second, err := cache.Get(context.Background(), reference, 1, loader)
	if err != nil {
		t.Fatal(err)
	}
	if string(second) != "abc" || calls.Load() != 1 {
		t.Fatalf("second bytes/calls = %q/%d, want abc/1", second, calls.Load())
	}

	stats := cache.Stats()
	if stats.Entries != 1 || stats.Bytes != 3 || stats.Misses != 1 || stats.Hits != 1 || stats.Loads != 1 {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestRemotePartCachePinsAndEvictsLowestPriorityFirst(t *testing.T) {
	cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: 6, MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	makeReference := func(name string) hatStorage.RemotePartReference {
		reference, createErr := hatStorage.NewRemotePartReference("s3://bucket/parts/"+name, "parts/"+name+".json", "sha256:"+name, 3)
		if createErr != nil {
			t.Fatal(createErr)
		}
		return reference
	}
	var calls atomic.Int32
	loader := func(_ context.Context, reference hatStorage.RemotePartReference) ([]byte, error) {
		calls.Add(1)
		return []byte(reference.ObjectURI()[len("s3://bucket/parts/"):]), nil
	}
	a := makeReference("aaa")
	b := makeReference("bbb")
	c := makeReference("ccc")
	if _, err := cache.Get(context.Background(), a, 1, loader); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Get(context.Background(), b, 9, loader); err != nil {
		t.Fatal(err)
	}
	lease, err := cache.Acquire(context.Background(), b, 9, loader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Get(context.Background(), c, 5, loader); err != nil {
		t.Fatal(err)
	}
	lease.Release()
	if _, err := cache.Get(context.Background(), a, 1, loader); err != nil {
		t.Fatal(err)
	}
	stats := cache.Stats()
	if stats.Evictions != 2 || stats.Entries != 2 || calls.Load() != 4 {
		t.Fatalf("stats/calls = %#v/%d, want two evictions, two entries, four loads", stats, calls.Load())
	}
}

func TestRemotePartCacheSingleFlightAndValidation(t *testing.T) {
	reference, err := hatStorage.NewRemotePartReference("s3://bucket/parts/shared", "parts/shared.json", "sha256:shared", 5)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	loader := func(_ context.Context, _ hatStorage.RemotePartReference) ([]byte, error) {
		calls.Add(1)
		close(started)
		<-release
		return []byte("hello"), nil
	}
	type result struct {
		value []byte
		err   error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			value, loadErr := cache.Get(context.Background(), reference, 1, loader)
			results <- result{value: value, err: loadErr}
		}()
	}
	<-started
	close(release)
	for range 2 {
		got := <-results
		if got.err != nil || string(got.value) != "hello" {
			t.Fatalf("single-flight result = %q/%v", got.value, got.err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("loader calls = %d, want one", calls.Load())
	}

	badSize, err := hatStorage.NewRemotePartReference("s3://bucket/parts/bad", "parts/bad.json", "sha256:bad", 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Get(context.Background(), badSize, 1, func(context.Context, hatStorage.RemotePartReference) ([]byte, error) {
		return []byte("wrong"), nil
	}); !errors.Is(err, hatStorage.ErrRemotePartCacheSizeMismatch) {
		t.Fatalf("size mismatch error = %v", err)
	}
}

func TestRemotePartCacheRejectsInvalidOptionsAndLoader(t *testing.T) {
	if _, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{}); !errors.Is(err, hatStorage.ErrRemotePartCacheDisabled) {
		t.Fatalf("disabled cache error = %v", err)
	}
	reference, err := hatStorage.NewRemotePartReference("s3://bucket/parts/p", "parts/p.json", "sha256:p", 1)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Get(context.Background(), reference, 0, nil); !errors.Is(err, hatStorage.ErrRemotePartCacheLoaderRequired) {
		t.Fatalf("missing loader error = %v", err)
	}
	if _, err := cache.Get(nil, reference, 0, func(context.Context, hatStorage.RemotePartReference) ([]byte, error) {
		return []byte("x"), nil
	}); !errors.Is(err, hatStorage.ErrRemotePartCacheContextRequired) {
		t.Fatalf("missing context error = %v", err)
	}
}

func TestRemotePartCacheLoaderFailureDoesNotPoisonKey(t *testing.T) {
	reference, err := hatStorage.NewRemotePartReference("s3://bucket/parts/retry", "parts/retry.json", "sha256:retry", 1)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	failing := func(context.Context, hatStorage.RemotePartReference) ([]byte, error) {
		calls++
		return nil, errors.New("temporary")
	}
	if _, err := cache.Get(context.Background(), reference, 0, failing); err == nil {
		t.Fatal("first load error = nil")
	}
	if _, err := cache.Get(context.Background(), reference, 0, func(context.Context, hatStorage.RemotePartReference) ([]byte, error) {
		calls++
		return []byte("x"), nil
	}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("loader calls = %d, want retry", calls)
	}
}

func TestRemotePartCacheConcurrentReleaseIsIdempotent(t *testing.T) {
	reference, err := hatStorage.NewRemotePartReference("s3://bucket/parts/release", "parts/release.json", "sha256:release", 1)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := cache.Acquire(context.Background(), reference, 1, func(context.Context, hatStorage.RemotePartReference) ([]byte, error) {
		return []byte("x"), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for range 4 {
		group.Add(1)
		go func() {
			defer group.Done()
			lease.Release()
		}()
	}
	group.Wait()
	if _, err := cache.Get(context.Background(), reference, 1, func(context.Context, hatStorage.RemotePartReference) ([]byte, error) {
		return []byte("x"), nil
	}); err != nil {
		t.Fatal(err)
	}
}
