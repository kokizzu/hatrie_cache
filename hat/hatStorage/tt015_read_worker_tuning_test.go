package hatStorage_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hatStorage "hatrie_cache/hat/hatStorage"
)

func TestTT015RemotePartCacheGlobalPrefetchConcurrency(t *testing.T) {
	cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{
		MaxBytes:               8,
		MaxEntries:             8,
		MaxPrefetchConcurrency: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	makeReferences := func(prefix string) []hatStorage.RemotePartReference {
		references := make([]hatStorage.RemotePartReference, 4)
		for index := range references {
			name := prefix + string(rune('a'+index))
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
		return references
	}
	first := makeReferences("first-")
	second := makeReferences("second-")
	start := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	var loads atomic.Int32
	loader := func(ctx context.Context, _ hatStorage.RemotePartReference) ([]byte, error) {
		select {
		case <-start:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		current := active.Add(1)
		for {
			observed := maximum.Load()
			if observed >= current || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		loads.Add(1)
		time.Sleep(2 * time.Millisecond)
		active.Add(-1)
		return []byte("x"), nil
	}

	var workers sync.WaitGroup
	workers.Add(2)
	results := make(chan error, 2)
	go func() {
		defer workers.Done()
		results <- cache.Prefetch(context.Background(), first, hatStorage.RemotePartPrefetchOptions{MaxConcurrent: 4}, loader)
	}()
	go func() {
		defer workers.Done()
		results <- cache.Prefetch(context.Background(), second, hatStorage.RemotePartPrefetchOptions{MaxConcurrent: 4}, loader)
	}()
	close(start)
	workers.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := maximum.Load(); got > 2 {
		t.Fatalf("maximum concurrent remote reads = %d, want <= 2", got)
	}
	if got := loads.Load(); got != 8 {
		t.Fatalf("remote reads = %d, want 8", got)
	}
}

func TestTT015RemotePartCacheRejectsInvalidGlobalPrefetchConcurrency(t *testing.T) {
	for _, concurrency := range []int{-1, 1<<20 + 1} {
		if _, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{
			MaxBytes:               1,
			MaxPrefetchConcurrency: concurrency,
		}); err == nil {
			t.Fatalf("concurrency %d error = nil, want invalid configuration", concurrency)
		} else if !errors.Is(err, hatStorage.ErrRemotePartCacheInvalidConfig) {
			t.Fatalf("concurrency %d error = %v, want invalid configuration", concurrency, err)
		}
	}
}
