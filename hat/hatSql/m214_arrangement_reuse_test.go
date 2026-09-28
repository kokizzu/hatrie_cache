package hatSql

import "testing"

type m214ArrangementReuseResolver struct {
	metadataCalls int
}

func (resolver *m214ArrangementReuseResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return []Row{{"id": int64(1)}}, nil
}

func (resolver *m214ArrangementReuseResolver) ResolveSQLArrangementMetadata(string, string) ([]SQLArrangementMetadata, error) {
	resolver.metadataCalls++
	return []SQLArrangementMetadata{
		{Key: "events_by_id", Kind: "HASH", Fields: []string{"id"}, Reused: true, Cardinality: 1, MemoryBytes: 64},
	}, nil
}

func TestM214ArrangementPlanCacheReusesCompatibleSourceMetadata(t *testing.T) {
	first, err := CompileSQLQuery("FROM CACHE('events') SELECT id WHERE id >= 1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := CompileSQLQuery("FROM CACHE('events') SELECT id WHERE id >= 2")
	if err != nil {
		t.Fatal(err)
	}
	cache, err := NewSQLArrangementPlanCache(SQLArrangementPlanCacheOptions{MaxEntries: 8, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	resolver := &m214ArrangementReuseResolver{}
	firstSteps := sqlExplainStepsWithArrangementPlanCache(first.template, resolver, nil, cache, "schema-v1")
	secondSteps := sqlExplainStepsWithArrangementPlanCache(second.template, resolver, nil, cache, "schema-v1")
	if len(firstSteps) == 0 || len(secondSteps) == 0 {
		t.Fatal("compatible plans returned empty explain steps")
	}
	if resolver.metadataCalls != 1 {
		t.Fatalf("metadata calls = %d, want one shared source lookup", resolver.metadataCalls)
	}
	stats := cache.Stats()
	if stats.Entries != 1 || stats.Misses != 1 || stats.Hits != 1 {
		t.Fatalf("cache stats = %#v, want one entry, miss, and hit", stats)
	}
}
