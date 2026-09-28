package hatSql

import "testing"

var m214ArrangementReuseBenchmarkSink []SQLExplainStep

func BenchmarkM214CompatibleArrangementPlanCache(b *testing.B) {
	first, err := CompileSQLQuery("FROM CACHE('events') SELECT id WHERE id >= 1")
	if err != nil {
		b.Fatal(err)
	}
	second, err := CompileSQLQuery("FROM CACHE('events') SELECT id WHERE id >= 2")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		cache, err := NewSQLArrangementPlanCache(SQLArrangementPlanCacheOptions{MaxEntries: 8, MaxBytes: 1 << 20})
		if err != nil {
			b.Fatal(err)
		}
		resolver := &m214ArrangementReuseResolver{}
		m214ArrangementReuseBenchmarkSink = sqlExplainStepsWithArrangementPlanCache(first.template, resolver, nil, cache, "schema-v1")
		m214ArrangementReuseBenchmarkSink = sqlExplainStepsWithArrangementPlanCache(second.template, resolver, nil, cache, "schema-v1")
	}
}
