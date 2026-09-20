package hatStorage_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func TestRemotePartCachePrefetchesWithBoundedConcurrency(t *testing.T) {
	cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: 4, MaxEntries: 4})
	if err != nil {
		t.Fatal(err)
	}
	references := make([]hatStorage.RemotePartReference, 4)
	for index := range references {
		name := string(rune('a' + index))
		references[index], err = hatStorage.NewRemotePartReference(
			"s3://bucket/parts/"+name,
			"parts/"+name+".json",
			"sha256:"+name,
			1,
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	var active atomic.Int32
	var maximum atomic.Int32
	loader := func(context.Context, hatStorage.RemotePartReference) ([]byte, error) {
		current := active.Add(1)
		for {
			observed := maximum.Load()
			if observed >= current || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		active.Add(-1)
		return []byte("x"), nil
	}
	if err := cache.Prefetch(context.Background(), references, hatStorage.RemotePartPrefetchOptions{
		MaxConcurrent: 2,
		Priority:      3,
	}, loader); err != nil {
		t.Fatal(err)
	}
	if got := maximum.Load(); got != 2 {
		t.Fatalf("maximum concurrent loads = %d, want 2", got)
	}
	stats := cache.Stats()
	if stats.Entries != len(references) || stats.Bytes != uint64(len(references)) || stats.Loads != uint64(len(references)) {
		t.Fatalf("prefetch stats = %#v, want four cached one-byte parts", stats)
	}
}

func TestRemotePartCachePrefetchDeduplicatesAndStopsOnError(t *testing.T) {
	cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: 2, MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	makeReference := func(name string) hatStorage.RemotePartReference {
		reference, createErr := hatStorage.NewRemotePartReference(
			"s3://bucket/parts/"+name,
			"parts/"+name+".json",
			"sha256:"+name,
			1,
		)
		if createErr != nil {
			t.Fatal(createErr)
		}
		return reference
	}
	first := makeReference("first")
	second := makeReference("second")
	var calls atomic.Int32
	loader := func(_ context.Context, reference hatStorage.RemotePartReference) ([]byte, error) {
		calls.Add(1)
		if reference.ObjectURI() == first.ObjectURI() {
			return nil, errors.New("remote read failed")
		}
		return []byte("x"), nil
	}
	if err := cache.Prefetch(context.Background(), []hatStorage.RemotePartReference{first, first}, hatStorage.RemotePartPrefetchOptions{MaxConcurrent: 1}, loader); err == nil {
		t.Fatal("prefetch error = nil, want loader failure")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("duplicate failing loads = %d, want one", got)
	}
	if err := cache.Prefetch(context.Background(), []hatStorage.RemotePartReference{second, second}, hatStorage.RemotePartPrefetchOptions{MaxConcurrent: 1}, loader); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("duplicate successful loads = %d, want two total", got)
	}
}

func TestRemotePartCachePrefetchRejectsInvalidConcurrency(t *testing.T) {
	cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	reference, err := hatStorage.NewRemotePartReference("s3://bucket/parts/invalid", "parts/invalid.json", "sha256:invalid", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Prefetch(context.Background(), []hatStorage.RemotePartReference{reference}, hatStorage.RemotePartPrefetchOptions{MaxConcurrent: -1}, func(context.Context, hatStorage.RemotePartReference) ([]byte, error) {
		return []byte("x"), nil
	}); !errors.Is(err, hatStorage.ErrRemotePartCacheInvalidConfig) {
		t.Fatalf("invalid concurrency error = %v", err)
	}
}

func TestRemotePartCacheColumnAwareLoadsUseIndependentRangeKeys(t *testing.T) {
	part, err := hatStorage.NewRemotePartReference(
		"s3://bucket/parts/columns",
		"parts/columns.json",
		"sha256:part",
		4096,
	)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := hatStorage.NewRemotePartColumnReference(part, "payload", "sha256:payload", 128, 3)
	if err != nil {
		t.Fatal(err)
	}
	other, err := hatStorage.NewRemotePartColumnReference(part, "other", "sha256:other", 512, 3)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: 6, MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	loader := func(_ context.Context, reference hatStorage.RemotePartColumnReference) ([]byte, error) {
		calls.Add(1)
		if reference.ColumnName() == "payload" {
			if reference.OffsetBytes() != 128 || reference.SizeBytes() != 3 {
				t.Fatalf("payload range = %d/%d, want 128/3", reference.OffsetBytes(), reference.SizeBytes())
			}
			return []byte("one"), nil
		}
		return []byte("two"), nil
	}
	got, err := cache.GetColumn(context.Background(), payload, 2, loader)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "one" {
		t.Fatalf("payload = %q, want one", got)
	}
	if _, err := cache.GetColumn(context.Background(), payload, 2, loader); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.GetColumn(context.Background(), other, 1, loader); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("column loader calls = %d, want two independent loads", got)
	}
	stats := cache.Stats()
	if stats.Entries != 2 || stats.Bytes != 6 || stats.Hits != 1 || stats.Loads != 2 {
		t.Fatalf("column cache stats = %#v, want two entries, six bytes, one hit, two loads", stats)
	}
}

func TestRemotePartCacheColumnPrefetchDeduplicatesAndValidatesSize(t *testing.T) {
	part, err := hatStorage.NewRemotePartReference("s3://bucket/parts/prefetch", "parts/prefetch.json", "sha256:part", 100)
	if err != nil {
		t.Fatal(err)
	}
	column, err := hatStorage.NewRemotePartColumnReference(part, "id", "sha256:id", 8, 1)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: 1, MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	loader := func(_ context.Context, _ hatStorage.RemotePartColumnReference) ([]byte, error) {
		calls.Add(1)
		return []byte("x"), nil
	}
	if err := cache.PrefetchColumns(context.Background(), []hatStorage.RemotePartColumnReference{column, column}, hatStorage.RemotePartPrefetchOptions{MaxConcurrent: 1}, loader); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("column prefetch calls = %d, want one", got)
	}
	bad, err := hatStorage.NewRemotePartColumnReference(part, "bad", "sha256:bad", 9, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.GetColumn(context.Background(), bad, 0, func(context.Context, hatStorage.RemotePartColumnReference) ([]byte, error) {
		return []byte("x"), nil
	}); !errors.Is(err, hatStorage.ErrRemotePartCacheSizeMismatch) {
		t.Fatalf("column size mismatch error = %v", err)
	}
}
