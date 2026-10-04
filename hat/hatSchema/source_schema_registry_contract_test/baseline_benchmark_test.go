package source_schema_registry_contract_test

import (
	"testing"

	"hatrie_cache/hat/hatSchema"
)

func BenchmarkSourceSchemaRegistryBaseline(b *testing.B) {
	previous := benchmarkSchema(1, false)
	next := benchmarkSchema(2, true)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		report, err := hatSchema.CheckRollingCompatibility(previous, next)
		if err != nil || !report.Compatible {
			b.Fatalf("compatibility check failed: report=%+v err=%v", report, err)
		}
	}
}

func benchmarkSchema(version uint64, withAddedColumn bool) hatSchema.Schema {
	return hatSchema.Schema{
		Version: version,
		Sources: map[string]hatSchema.Source{
			"orders": benchmarkSource(withAddedColumn),
		},
	}
}

func benchmarkSource(withAddedColumn bool) hatSchema.Source {
	columns := []hatSchema.Column{
		{Name: "id", Type: hatSchema.TypeInteger, NotNull: true},
		{Name: "region", Type: hatSchema.TypeText},
		{Name: "updated_at", Type: hatSchema.TypeTimestamp},
	}
	if withAddedColumn {
		columns = append(columns, hatSchema.Column{Name: "metadata", Type: hatSchema.TypeJSON})
	}
	return hatSchema.Source{Name: "orders", Columns: columns}
}
