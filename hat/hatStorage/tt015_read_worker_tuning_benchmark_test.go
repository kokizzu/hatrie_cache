package hatStorage_test

import (
	"context"
	"sync"
	"testing"

	hatStorage "hatrie_cache/hat/hatStorage"
)

func BenchmarkTT015PrefetchBaseline(b *testing.B) {
	benchmarkTT015Prefetch(b, 0)
}

func BenchmarkTT015PrefetchGlobalLimit(b *testing.B) {
	benchmarkTT015Prefetch(b, 2)
}

func benchmarkTT015Prefetch(b *testing.B, maxPrefetchConcurrency int) {
	first, second := tt015BenchmarkReferences(b)
	loader := func(context.Context, hatStorage.RemotePartReference) ([]byte, error) {
		return []byte("x"), nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{
			MaxBytes:               8,
			MaxEntries:             8,
			MaxPrefetchConcurrency: maxPrefetchConcurrency,
		})
		if err != nil {
			b.Fatal(err)
		}
		var workers sync.WaitGroup
		workers.Add(2)
		errors := make(chan error, 2)
		go func() {
			defer workers.Done()
			errors <- cache.Prefetch(context.Background(), first, hatStorage.RemotePartPrefetchOptions{MaxConcurrent: 4}, loader)
		}()
		go func() {
			defer workers.Done()
			errors <- cache.Prefetch(context.Background(), second, hatStorage.RemotePartPrefetchOptions{MaxConcurrent: 4}, loader)
		}()
		workers.Wait()
		close(errors)
		for err := range errors {
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func tt015BenchmarkReferences(b *testing.B) ([]hatStorage.RemotePartReference, []hatStorage.RemotePartReference) {
	b.Helper()
	makeReferences := func(prefix string) []hatStorage.RemotePartReference {
		references := make([]hatStorage.RemotePartReference, 4)
		for index := range references {
			name := prefix + string(rune('a'+index))
			var err error
			references[index], err = hatStorage.NewRemotePartReference(
				"s3://bucket/parts/"+name,
				"parts/"+name+".json",
				"sha256:"+name,
				1,
			)
			if err != nil {
				b.Fatal(err)
			}
		}
		return references
	}
	return makeReferences("first-"), makeReferences("second-")
}
