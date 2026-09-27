package hatSql

import "testing"

func BenchmarkMZ037SQLIncrementalTopKDirect(b *testing.B) {
	topK, err := NewIncrementalTopK(IncrementalTopKDefinition{
		K: 8,
		OrderKey: func(row Row) (interface{}, error) {
			return row["score"], nil
		},
		Descending: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	seedMZ037DirectTopKBenchmark(b, topK)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := topK.Apply(mz037ReplacementUpdates(index)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMZ037SQLIncrementalTopKAdapter(b *testing.B) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id, src.score ORDER BY src.score DESC LIMIT 8")
	if err != nil {
		b.Fatal(err)
	}
	operator, err := compiled.CompileIncrementalTopK()
	if err != nil {
		b.Fatal(err)
	}
	seedMZ037SQLTopKBenchmark(b, operator)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := operator.Apply(mz037ReplacementUpdates(index)); err != nil {
			b.Fatal(err)
		}
	}
}

func seedMZ037DirectTopKBenchmark(b *testing.B, topK *IncrementalTopK) {
	b.Helper()
	updates := make([]DifferentialRow, 64)
	for index := range updates {
		updates[index] = DifferentialRow{
			Key:  mz037BenchmarkKey(index),
			Diff: 1,
			Row:  Row{"id": mz037BenchmarkKey(index), "score": int64(index)},
		}
	}
	if _, err := topK.Apply(updates); err != nil {
		b.Fatal(err)
	}
}

func seedMZ037SQLTopKBenchmark(b *testing.B, operator *SQLIncrementalTopK) {
	b.Helper()
	updates := make([]DifferentialRow, 64)
	for index := range updates {
		updates[index] = DifferentialRow{
			Key:  mz037BenchmarkKey(index),
			Diff: 1,
			Row:  Row{"id": mz037BenchmarkKey(index), "score": int64(index)},
		}
	}
	if _, err := operator.Apply(updates); err != nil {
		b.Fatal(err)
	}
}

func mz037ReplacementUpdates(index int) []DifferentialRow {
	key := mz037BenchmarkKey(0)
	return []DifferentialRow{
		{Key: key, Diff: -1},
		{Key: key, Diff: 1, Row: Row{"id": key, "score": int64(1000 + index)}},
	}
}

func mz037BenchmarkKey(index int) string {
	return "k" + string(rune('a'+index%26)) + string(rune('0'+index/26))
}
