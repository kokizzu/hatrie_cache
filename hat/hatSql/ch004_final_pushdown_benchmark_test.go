package hatSql

import (
	"context"
	"strconv"
	"testing"
)

var benchmarkCH004FinalPushdownSink SQLQueryResult

func BenchmarkCH004FinalPushdownBaseline(b *testing.B) {
	rawRows, _ := ch004FinalPushdownBenchmarkRows()
	resolver := SourceResolverFunc(func(name, key string) ([]Row, error) {
		if name != "CACHE" || key != "events" {
			return nil, nil
		}
		return rawRows, nil
	})
	benchmarkCH004FinalPushdown(b, resolver)
}

func BenchmarkCH004FinalPushdownFastPath(b *testing.B) {
	rawRows, finalRows := ch004FinalPushdownBenchmarkRows()
	resolver := &ch004FinalPushdownResolver{rawRows: rawRows, finalRows: finalRows}
	benchmarkCH004FinalPushdown(b, resolver)
}

func benchmarkCH004FinalPushdown(b *testing.B, resolver SQLSourceResolver) {
	b.ReportAllocs()
	options := SQLQueryOptions{FinalSourceOptions: ch004FinalSourceOptionsResolver(ch004ReplacingFinalOptions)}
	query := "FROM CACHE('events') AS event FINAL SELECT event.id, event.value"
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkCH004FinalPushdownSink = result
	}
}

func ch004FinalPushdownBenchmarkRows() ([]SQLRow, []SQLRow) {
	const uniqueRows = 512
	rawRows := make([]SQLRow, 0, uniqueRows*2)
	finalRows := make([]SQLRow, 0, uniqueRows)
	for index := 0; index < uniqueRows; index++ {
		id := "id-" + strconv.Itoa(index)
		rawRows = append(rawRows,
			SQLRow{"id": id, "version": uint64(1), "value": "old"},
			SQLRow{"id": id, "version": uint64(2), "value": "new"},
		)
		finalRows = append(finalRows, SQLRow{"id": id, "version": uint64(2), "value": "new"})
	}
	return rawRows, finalRows
}
