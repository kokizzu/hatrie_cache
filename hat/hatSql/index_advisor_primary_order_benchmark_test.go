package hatSql

import (
	"strconv"
	"testing"
)

var sqlPrimaryOrderAdvisorBenchmarkSink []SQLPrimaryOrderRecommendation

func newSQLPrimaryOrderAdvisorBenchmark() *SQLIndexAdvisor {
	advisor := NewSQLIndexAdvisor(128)
	for index := 0; index < 64; index++ {
		advisor.counts[sqlIndexAdvisorKey{key: "table_" + strconv.Itoa(index/8), field: "field_" + strconv.Itoa(index%8)}] = uint64(index + 1)
	}
	return advisor
}

func BenchmarkSQLIndexAdvisorPrimaryOrderRecommendations(b *testing.B) {
	advisor := newSQLPrimaryOrderAdvisorBenchmark()
	b.ReportAllocs()
	for b.Loop() {
		sqlPrimaryOrderAdvisorBenchmarkSink = advisor.PrimaryOrderRecommendations()
	}
}

func BenchmarkSQLIndexAdvisorPerFieldRecommendations(b *testing.B) {
	advisor := newSQLPrimaryOrderAdvisorBenchmark()
	var sink []SQLIndexRecommendation
	b.ReportAllocs()
	for b.Loop() {
		sink = advisor.Recommendations()
	}
	if len(sink) == 0 {
		b.Fatal("empty recommendation result")
	}
}
