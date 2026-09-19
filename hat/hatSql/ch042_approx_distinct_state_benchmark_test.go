package hatSql

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

var ch042ApproxDistinctResult SQLQueryResult

func ch042ApproxDistinctRows() approximateAggregateSource {
	rows := make(approximateAggregateSource, 10000)
	for index := range rows {
		rows[index] = SQLRow{"visitor": index % 4000}
	}
	return rows
}

func BenchmarkCH042ApproxDistinctBaseline(b *testing.B) {
	rows := ch042ApproxDistinctRows()
	query := `SELECT APPROX_COUNT_DISTINCT(visitor, 10) AS visitors FROM CACHE('events')`
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQuery(query, rows)
		if err != nil {
			b.Fatal(err)
		}
		ch042ApproxDistinctResult = result
	}
}

func BenchmarkCH042ApproxDistinctState(b *testing.B) {
	rows := ch042ApproxDistinctRows()
	query := `SELECT APPROX_COUNT_DISTINCT_STATE(visitor, 10) AS state FROM CACHE('events')`
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQuery(query, rows)
		if err != nil {
			b.Fatal(err)
		}
		ch042ApproxDistinctResult = result
	}
	b.StopTimer()
	if len(ch042ApproxDistinctResult.Rows) == 1 {
		if wire, ok := ch042ApproxDistinctResult.Rows[0]["state"].([]byte); ok {
			b.ReportMetric(float64(len(wire)), "wire-bytes/op")
		}
	}
}

func BenchmarkCH042ApproxDistinctMerge(b *testing.B) {
	rows := make(approximateAggregateSource, 8)
	for index := range rows {
		sketch, err := hatDataStructure.NewHyperLogLog(10)
		if err != nil {
			b.Fatal(err)
		}
		for value := index * 500; value < (index+1)*500; value++ {
			sketch.AddJSONString(string(rune(value)))
		}
		wire, err := sketch.MarshalAggregateState()
		if err != nil {
			b.Fatal(err)
		}
		rows[index] = SQLRow{"state": wire}
	}
	query := `SELECT APPROX_COUNT_DISTINCT_MERGE(state) AS visitors FROM CACHE('states')`
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQuery(query, rows)
		if err != nil {
			b.Fatal(err)
		}
		ch042ApproxDistinctResult = result
	}
}
