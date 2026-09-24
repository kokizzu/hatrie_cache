package hatSchema

import "testing"

func BenchmarkTT027LegacyGeneratedMaterialization(b *testing.B) {
	source := benchmarkTT027GeneratedSource(b, false)
	benchmarkTT027Materialization(b, source)
}

func BenchmarkTT027ValidatedGeneratedMaterialization(b *testing.B) {
	source := benchmarkTT027GeneratedSource(b, true)
	benchmarkTT027Materialization(b, source)
}

func BenchmarkTT027LegacyConstructor(b *testing.B) {
	columns := benchmarkTT027GeneratedColumns(false)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if NewMaterializedSource(columns) == nil {
			b.Fatal("NewMaterializedSource() returned nil")
		}
	}
}

func BenchmarkTT027ValidatedConstructor(b *testing.B) {
	columns := benchmarkTT027GeneratedColumns(true)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := NewValidatedMaterializedSource(columns); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkTT027GeneratedSource(b *testing.B, validated bool) *MaterializedSource {
	b.Helper()
	columns := benchmarkTT027GeneratedColumns(validated)
	if validated {
		source, err := NewValidatedMaterializedSource(columns)
		if err != nil {
			b.Fatal(err)
		}
		return source
	}
	return NewMaterializedSource(columns)
}

func benchmarkTT027GeneratedColumns(validated bool) []DerivedColumn {
	columns := []DerivedColumn{
		{Name: "base"},
		{
			Name: "doubled",
			Generated: func(row Row) (interface{}, error) {
				return row["base"].(int64) * 2, nil
			},
		},
		{
			Name: "quadrupled",
			Generated: func(row Row) (interface{}, error) {
				return row["doubled"].(int64) * 2, nil
			},
		},
	}
	if validated {
		columns[1].GeneratedDependencies = []string{"base"}
		columns[2].GeneratedDependencies = []string{"doubled"}
		columns[1], columns[2] = columns[2], columns[1]
	}
	return columns
}

func benchmarkTT027Materialization(b *testing.B, source *MaterializedSource) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := source.materializeRowLocked(Row{"base": int64(index)}); err != nil {
			b.Fatal(err)
		}
	}
}
