package hatSql

import (
	"context"
	"testing"
)

const mz045ArrangementCacheQuery = "FROM VALUES (1), (2), (3) AS values(id) SELECT id WHERE id >= 2"

type mz045ArrangementCacheResolver struct{}

func (mz045ArrangementCacheResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return []Row{{"id": int64(1)}}, nil
}

func (mz045ArrangementCacheResolver) ResolveSQLArrangementMetadata(string, string) ([]SQLArrangementMetadata, error) {
	return []SQLArrangementMetadata{
		{Key: "values_by_id", Kind: "HASH", Fields: []string{"id"}, Reused: true, Cardinality: 3, MemoryBytes: 512},
	}, nil
}

type mz045CountingArrangementCacheResolver struct {
	metadataCalls int
}

func (resolver *mz045CountingArrangementCacheResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return []Row{{"id": int64(1)}}, nil
}

func (resolver *mz045CountingArrangementCacheResolver) ResolveSQLArrangementMetadata(string, string) ([]SQLArrangementMetadata, error) {
	resolver.metadataCalls++
	return []SQLArrangementMetadata{
		{Key: "values_by_id", Kind: "HASH", Fields: []string{"id"}, Reused: true, Cardinality: 3, MemoryBytes: 512},
	}, nil
}

func TestMZ045ArrangementPlanCacheReusesVersionedRecommendation(t *testing.T) {
	first, err := CompileSQLQuery(mz045ArrangementCacheQuery)
	if err != nil {
		t.Fatal(err)
	}
	second, err := CompileSQLQuery("FROM VALUES(1),(2),(3) AS values(id) SELECT id WHERE id >= 2")
	if err != nil {
		t.Fatal(err)
	}
	if first.template.cacheKey == "" || first.template.cacheKey != second.template.cacheKey {
		t.Fatalf("equivalent query cache keys = %q and %q", first.template.cacheKey, second.template.cacheKey)
	}
	cache, err := NewSQLArrangementPlanCache(SQLArrangementPlanCacheOptions{MaxEntries: 8, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	resolver := &mz045CountingArrangementCacheResolver{}
	firstSteps := sqlExplainStepsWithArrangementPlanCache(first.template, resolver, nil, cache, "schema-v1")
	secondSteps := sqlExplainStepsWithArrangementPlanCache(second.template, resolver, nil, cache, "schema-v1")
	if resolver.metadataCalls != 1 {
		t.Fatalf("metadata calls after equivalent plans = %d, want 1", resolver.metadataCalls)
	}
	if len(firstSteps) == 0 || len(secondSteps) == 0 || len(firstSteps[0].Arrangements) == 0 || len(secondSteps[0].Arrangements) == 0 {
		t.Fatalf("cached arrangement plans missing: first=%#v second=%#v", firstSteps, secondSteps)
	}
	if !firstSteps[0].Arrangements[0].Recommended || !secondSteps[0].Arrangements[0].Recommended {
		t.Fatalf("cached recommendations not marked: first=%#v second=%#v", firstSteps[0].Arrangements, secondSteps[0].Arrangements)
	}
	stats := cache.Stats()
	if stats.Entries != 1 || stats.Hits != 1 || stats.Misses != 1 {
		t.Fatalf("arrangement plan cache stats = %#v, want one entry, hit, and miss", stats)
	}
	_ = sqlExplainStepsWithArrangementPlanCache(second.template, resolver, nil, cache, "schema-v2")
	if resolver.metadataCalls != 2 {
		t.Fatalf("metadata calls after version change = %d, want 2", resolver.metadataCalls)
	}
}

func TestMZ045ArrangementPlanCacheClonesCachedMetadata(t *testing.T) {
	query, err := CompileSQLQuery(mz045ArrangementCacheQuery)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := NewSQLArrangementPlanCache(SQLArrangementPlanCacheOptions{MaxEntries: 2, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	resolver := &mz045CountingArrangementCacheResolver{}
	first := sqlExplainStepsWithArrangementPlanCache(query.template, resolver, nil, cache, "schema-v1")
	first[0].Arrangements[0].Key = "caller-mutated"
	first[0].Arrangements[0].Fields[0] = "caller-mutated"
	second := sqlExplainStepsWithArrangementPlanCache(query.template, resolver, nil, cache, "schema-v1")
	if second[0].Arrangements[0].Key != "values_by_id" || second[0].Arrangements[0].Fields[0] != "id" {
		t.Fatalf("cached metadata was exposed to caller mutation: %#v", second[0].Arrangements)
	}
	if removed := cache.InvalidateVersion("schema-v1"); removed != 1 || cache.Stats().Entries != 0 {
		t.Fatalf("invalidate version removed=%d stats=%#v", removed, cache.Stats())
	}
}

func TestMZ045ArrangementPlanCacheOptionsWireIntoExplain(t *testing.T) {
	cache, err := NewSQLArrangementPlanCache(SQLArrangementPlanCacheOptions{MaxEntries: 2, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	resolver := &mz045CountingArrangementCacheResolver{}
	options := SQLQueryOptions{
		ArrangementPlanCache:        cache,
		ArrangementPlanCacheVersion: "schema-v1",
	}
	for range 2 {
		result, err := ExecuteSQLQueryParameters(context.Background(), "EXPLAIN "+mz045ArrangementCacheQuery, resolver, nil, options)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Plan) == 0 || len(result.Plan[0].Arrangements) == 0 || !result.Plan[0].Arrangements[0].Recommended {
			t.Fatalf("EXPLAIN arrangement plan = %#v", result.Plan)
		}
	}
	stats := cache.Stats()
	if resolver.metadataCalls != 1 || stats.Hits != 1 || stats.Misses != 1 {
		t.Fatalf("wired cache calls=%d stats=%#v, want one resolver call and one hit/miss", resolver.metadataCalls, stats)
	}
}

var mz045ArrangementCacheBenchmarkSink []SQLExplainStep

func BenchmarkMZ045ArrangementMetadataBaseline(b *testing.B) {
	query, err := CompileSQLQuery(mz045ArrangementCacheQuery)
	if err != nil {
		b.Fatal(err)
	}
	resolver := mz045ArrangementCacheResolver{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		mz045ArrangementCacheBenchmarkSink = sqlExplainStepsWithPartitionOrder(query.template, resolver, nil)
	}
}

func BenchmarkMZ045ArrangementMetadataCache(b *testing.B) {
	query, err := CompileSQLQuery(mz045ArrangementCacheQuery)
	if err != nil {
		b.Fatal(err)
	}
	resolver := mz045ArrangementCacheResolver{}
	cache, err := NewSQLArrangementPlanCache(SQLArrangementPlanCacheOptions{MaxEntries: 8, MaxBytes: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	if steps := sqlExplainStepsWithArrangementPlanCache(query.template, resolver, nil, cache, "schema-v1"); len(steps) == 0 {
		b.Fatal("initial cached explain plan is empty")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		mz045ArrangementCacheBenchmarkSink = sqlExplainStepsWithArrangementPlanCache(query.template, resolver, nil, cache, "schema-v1")
	}
}
