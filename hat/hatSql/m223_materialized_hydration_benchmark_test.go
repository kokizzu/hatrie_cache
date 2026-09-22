package hatSql

import (
	"context"
	"testing"
)

func m223HydrationBenchmarkRows(count int) []Row {
	rows := make([]Row, count)
	for index := range rows {
		rows[index] = Row{
			"id":   "user-" + string(rune('a'+index%26)),
			"name": "name-" + string(rune('a'+index%26)),
		}
	}
	return rows
}

func BenchmarkM223MaterializedViewCreateControl(b *testing.B) {
	definition := m223HydrationDefinition()
	rows := m223HydrationBenchmarkRows(256)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		views := NewMaterializedViews()
		if _, err := views.Create(context.Background(), definition, &m223HydrationResolver{rows: rows}, QueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM223MaterializedViewColdHydrate(b *testing.B) {
	definition := m223HydrationDefinition()
	rows := m223HydrationBenchmarkRows(256)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		views := NewMaterializedViews()
		if _, err := views.CreateCold(definition); err != nil {
			b.Fatal(err)
		}
		if _, err := views.Hydrate(context.Background(), definition.Name, &m223HydrationResolver{rows: rows}, QueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
