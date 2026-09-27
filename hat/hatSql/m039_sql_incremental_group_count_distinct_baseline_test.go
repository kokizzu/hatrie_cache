package hatSql

import "testing"

func BenchmarkM039RebuildSQLGroupCountDistinct(b *testing.B) {
	updates := m039GroupCountDistinctInitialRows(10_000)
	updates = append(updates, DifferentialRow{
		Key:  "remove-row",
		Time: 2,
		Diff: -1,
		Row: Row{
			"group": "group-42",
			"value": int64(0),
		},
	}, DifferentialRow{
		Key:  "add-row",
		Time: 2,
		Diff: 1,
		Row: Row{
			"group": "group-42",
			"value": int64(100),
		},
	})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := GroupCountDistinctInt64DifferentialRows(updates, m039GroupCountDistinctKey, m039GroupCountDistinctValue); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM039IncrementalSQLGroupCountDistinct(b *testing.B) {
	compiled, err := CompileSQLQuery(m039SQLGroupCountDistinctQuery)
	if err != nil {
		b.Fatal(err)
	}
	operator, err := compiled.CompileIncrementalGroupCountDistinct()
	if err != nil {
		b.Fatal(err)
	}
	if _, err := operator.Apply(m039GroupCountDistinctInitialRows(10_000)); err != nil {
		b.Fatal(err)
	}
	update := []DifferentialRow{{
		Key:  "duplicate-row",
		Time: 2,
		Diff: 1,
		Row:  Row{"group": "group-42", "value": int64(42)},
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := operator.Apply(update); err != nil {
			b.Fatal(err)
		}
	}
}
