package hatSql

import "testing"

func BenchmarkM039PartitionOrderLookup(b *testing.B) {
	registry := NewSQLPartitionOrderRegistry(0)
	if err := registry.Register(SQLPartitionOrderDeclaration{
		Source:          "CACHE",
		Key:             "events",
		PartitionFields: []string{"region"},
		OrderFields:     []SQLPartitionOrderField{{Field: "event_time", Desc: true}},
	}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, available, err := registry.ResolveSQLPartitionOrder("CACHE", "events"); err != nil || !available {
			b.Fatal("partition order lookup failed")
		}
	}
}

func BenchmarkM039ExplainPartitionOrder(b *testing.B) {
	registry := NewSQLPartitionOrderRegistry(0)
	if err := registry.Register(SQLPartitionOrderDeclaration{
		Source:          "CACHE",
		Key:             "events",
		PartitionFields: []string{"region"},
		OrderFields:     []SQLPartitionOrderField{{Field: "event_time", Desc: true}},
	}); err != nil {
		b.Fatal(err)
	}
	query := &sqlQuery{
		from:    &sqlSource{kind: "CACHE", key: "events"},
		selects: []sqlSelectItem{{expr: sqlExpr{kind: "field", name: "region"}}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		steps := sqlExplainStepsWithPartitionOrder(query, nil, registry)
		if len(steps) == 0 || steps[0].PartitionOrder == nil {
			b.Fatal("partition order metadata missing")
		}
	}
}

func BenchmarkM039ExplainWithoutPartitionOrder(b *testing.B) {
	query := &sqlQuery{
		from:    &sqlSource{kind: "CACHE", key: "events"},
		selects: []sqlSelectItem{{expr: sqlExpr{kind: "field", name: "region"}}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		steps := sqlExplainStepsWithResolver(query, nil)
		if len(steps) == 0 {
			b.Fatal("legacy explain returned no steps")
		}
	}
}
