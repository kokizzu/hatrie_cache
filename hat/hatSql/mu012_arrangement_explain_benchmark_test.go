package hatSql

import "testing"

var mu012ExplainBenchmarkSink int

func BenchmarkMU012ExplainArrangementMetadata(b *testing.B) {
	b.Run("without", func(b *testing.B) {
		b.ReportAllocs()
		resolver := mu012ArrangementSourceOnlyResolver{}
		for index := 0; index < b.N; index++ {
			result, err := ExecuteSQLQuery("EXPLAIN SELECT id FROM CACHE('events')", resolver)
			if err != nil {
				b.Fatal(err)
			}
			mu012ExplainBenchmarkSink += len(result.Plan)
		}
	})
	b.Run("with", func(b *testing.B) {
		b.ReportAllocs()
		resolver := mu012ArrangementResolver{}
		for index := 0; index < b.N; index++ {
			result, err := ExecuteSQLQuery("EXPLAIN SELECT id FROM CACHE('events')", resolver)
			if err != nil {
				b.Fatal(err)
			}
			mu012ExplainBenchmarkSink += len(result.Plan)
		}
	})
}

type mu012ArrangementSourceOnlyResolver struct{}

func (mu012ArrangementSourceOnlyResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return []Row{{"id": int64(1)}}, nil
}
