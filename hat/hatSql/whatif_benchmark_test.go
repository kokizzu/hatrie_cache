package hatSql

import (
	"context"
	"testing"
)

func BenchmarkExplainSQLWhatIf(b *testing.B) {
	rows := make([]SQLRow, 10_000)
	for index := range rows {
		rows[index] = SQLRow{
			"id":     int64(index),
			"region": []string{"apac", "emea", "us", "latam"}[index%4],
			"score":  int64(index % 1000),
		}
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	request := SQLWhatIfRequest{
		Query: "SELECT id FROM CACHE('events') WHERE region = 'apac'",
		Index: SQLWhatIfIndex{Kind: SQLWhatIfIndexEquality, Fields: []string{"region"}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		report, err := ExplainSQLWhatIf(context.Background(), request, resolver)
		if err != nil || !report.Beneficial {
			b.Fatalf("what-if report=%#v err=%v", report, err)
		}
	}
}

func BenchmarkExplainSQLWhatIfWithStatistics(b *testing.B) {
	resolver := benchmarkWhatIfStatisticsResolver{}
	request := SQLWhatIfRequest{
		Query: "SELECT id FROM CACHE('events') WHERE score >= 900",
		Index: SQLWhatIfIndex{Kind: SQLWhatIfIndexRange, Fields: []string{"score"}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		report, err := ExplainSQLWhatIf(context.Background(), request, resolver)
		if err != nil || !report.Beneficial {
			b.Fatalf("statistics what-if report=%#v err=%v", report, err)
		}
	}
}

type benchmarkWhatIfStatisticsResolver struct{}

func (benchmarkWhatIfStatisticsResolver) ResolveSQLSource(string, string) ([]SQLRow, error) {
	return nil, nil
}

func (benchmarkWhatIfStatisticsResolver) SQLWhatIfSourceStatistics(string, string, []string) (SQLWhatIfSourceStatistics, bool, error) {
	return SQLWhatIfSourceStatistics{
		Rows:  100_000,
		Bytes: 8_000_000,
		Fields: map[string]SQLWhatIfFieldStatistics{
			"score": {Rows: 100_000, DistinctValues: 100_000, Minimum: int64(0), Maximum: int64(99_999), AverageValueBytes: 8},
		},
	}, true, nil
}
