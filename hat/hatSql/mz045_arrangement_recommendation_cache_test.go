//go:build !mz045baseline

package hatSql

import (
	"sync"
	"testing"
)

func TestMZ045ArrangementRecommendationCacheReusesVersionedWorkload(t *testing.T) {
	cache, err := NewSQLArrangementRecommendationCache(SQLArrangementRecommendationCacheOptions{MaxEntries: 4})
	if err != nil {
		t.Fatal(err)
	}
	candidates := mz045RecommendationCandidates()
	workload := SQLArrangementWorkload{FilterFields: []string{"tenant_id"}, GroupByFields: []string{"region"}}
	first := cache.Recommend("CACHE", "events", "v1", candidates, workload)
	second := cache.Recommend("cache", "events", "v1", candidates, SQLArrangementWorkload{
		FilterFields:  []string{" tenant_id "},
		GroupByFields: []string{"region"},
	})
	if first != second {
		t.Fatalf("cached recommendation = %#v, want %#v", second, first)
	}
	if first.Key != "region" {
		t.Fatalf("recommendation key = %q, want region", first.Key)
	}
	stats := cache.Stats()
	if stats.Entries != 1 || stats.Hits != 1 || stats.Misses != 1 {
		t.Fatalf("cache stats = %#v, want one entry, one hit, one miss", stats)
	}
}

func TestMZ045ArrangementRecommendationCacheVersionAndEmptyVersion(t *testing.T) {
	cache, err := NewSQLArrangementRecommendationCache(SQLArrangementRecommendationCacheOptions{MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	candidates := mz045RecommendationCandidates()
	workload := SQLArrangementWorkload{FilterFields: []string{"tenant_id"}}
	first := cache.Recommend("CACHE", "events", "v1", candidates, workload)
	if got := cache.Recommend("CACHE", "events", "v2", candidates, workload); got != first {
		t.Fatalf("new metadata version recommendation = %#v, want %#v", got, first)
	}
	if got := cache.Recommend("CACHE", "events", "", candidates, workload); got != first {
		t.Fatalf("uncached recommendation = %#v, want %#v", got, first)
	}
	stats := cache.Stats()
	if stats.Entries != 2 || stats.Hits != 0 || stats.Misses != 2 {
		t.Fatalf("version cache stats = %#v, want two misses and two entries", stats)
	}
	cache.Invalidate()
	if stats := cache.Stats(); stats.Entries != 0 {
		t.Fatalf("entries after invalidate = %d, want 0", stats.Entries)
	}
}

func TestMZ045ArrangementRecommendationCacheKeepsLocalityInTheFingerprint(t *testing.T) {
	cache, err := NewSQLArrangementRecommendationCache(SQLArrangementRecommendationCacheOptions{MaxEntries: 4})
	if err != nil {
		t.Fatal(err)
	}
	candidates := []SQLArrangementMetadata{
		{Key: "user_id:west", Kind: "hash-index", Fields: []string{"user_id"}, Locality: "us-west"},
		{Key: "user_id:east", Kind: "hash-index", Fields: []string{"user_id"}, Locality: "us-east"},
	}
	workload := SQLArrangementWorkload{FilterFields: []string{"user_id"}, LocalityHints: []string{"us-east"}}
	first := cache.Recommend("CACHE", "events", "v1", candidates, workload)
	second := cache.Recommend("CACHE", "events", "v1", candidates, workload)
	if first.Key != "user_id:east" || second != first {
		t.Fatalf("locality recommendations = %#v and %#v, want east and a cache hit", first, second)
	}
}

func TestMZ045ArrangementRecommendationCacheEvictsAndIsConcurrent(t *testing.T) {
	cache, err := NewSQLArrangementRecommendationCache(SQLArrangementRecommendationCacheOptions{MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	candidates := mz045RecommendationCandidates()
	workload := SQLArrangementWorkload{OrderByFields: []string{"created_at"}}
	cache.Recommend("CACHE", "one", "v1", candidates, workload)
	cache.Recommend("CACHE", "two", "v1", candidates, workload)
	if stats := cache.Stats(); stats.Evictions != 1 || stats.Entries != 1 {
		t.Fatalf("eviction stats = %#v, want one eviction and one entry", stats)
	}
	const workers = 8
	const iterations = 100
	var group sync.WaitGroup
	group.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer group.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				cache.Recommend("CACHE", "concurrent", "v1", candidates, workload)
			}
		}(worker)
	}
	group.Wait()
}

func mz045RecommendationCandidates() []SQLArrangementMetadata {
	return []SQLArrangementMetadata{
		{Key: "tenant_id", Kind: "hash-index", Fields: []string{"tenant_id"}, MemoryBytes: 1024},
		{Key: "region", Kind: "sorted-aggregate", Fields: []string{"region"}, MemoryBytes: 2048},
		{Key: "created_at", Kind: "sorted", Fields: []string{"created_at"}, MemoryBytes: 4096},
	}
}

var mz045ArrangementRecommendationSink SQLArrangementRecommendation

func BenchmarkMZ045ArrangementRecommendationBaseline(b *testing.B) {
	candidates := mz045RecommendationCandidates()
	workload := SQLArrangementWorkload{FilterFields: []string{"tenant_id"}, GroupByFields: []string{"region"}}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		mz045ArrangementRecommendationSink = RecommendSQLArrangement(candidates, workload)
	}
}

func BenchmarkMZ045ArrangementRecommendationCacheHit(b *testing.B) {
	cache, err := NewSQLArrangementRecommendationCache(SQLArrangementRecommendationCacheOptions{MaxEntries: 8})
	if err != nil {
		b.Fatal(err)
	}
	candidates := mz045RecommendationCandidates()
	workload := SQLArrangementWorkload{FilterFields: []string{"tenant_id"}, GroupByFields: []string{"region"}}
	cache.Recommend("CACHE", "events", "v1", candidates, workload)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		mz045ArrangementRecommendationSink = cache.Recommend("CACHE", "events", "v1", candidates, workload)
	}
}
