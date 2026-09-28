package hatSql

import "testing"

func BenchmarkM037SQLIncrementalGroupMinMaxRebuild(b *testing.B) {
	rows := differentialGroupMinMaxBenchmarkRows()
	key := func(row SQLRow) string { return row["group"].(string) }
	value := func(row SQLRow) (int64, error) { return sqlIncrementalInt64(row["value"]) }
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := GroupMinMaxInt64DifferentialRows(rows, key, value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM037SQLIncrementalGroupMinMaxApply(b *testing.B) {
	compiled, err := CompileSQLQuery(m037SQLGroupMinMaxQuery)
	if err != nil {
		b.Fatal(err)
	}
	operator, err := compiled.CompileIncrementalGroupAggregate()
	if err != nil {
		b.Fatal(err)
	}
	seed := differentialGroupMinMaxBenchmarkRows()
	if _, err := operator.Apply(seed); err != nil {
		b.Fatal(err)
	}
	delta := []DifferentialRow{{Key: "warm", Diff: 1, Row: Row{"group": "group-0", "value": int64(0)}}}
	retract := []DifferentialRow{{Key: "warm", Diff: -1, Row: Row{"group": "group-0", "value": int64(0)}}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := operator.Apply(delta); err != nil {
			b.Fatal(err)
		}
		if _, err := operator.Apply(retract); err != nil {
			b.Fatal(err)
		}
	}
}
