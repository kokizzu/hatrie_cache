package hatSql

import (
	"math"
	"reflect"
	"testing"
)

func TestM065SQLWindowFrameMatchesRecompute(t *testing.T) {
	rows := []Row{
		{"bucket": "a", "seq": int64(1), "value": int64(1)},
		{"bucket": "a", "seq": int64(3), "value": int64(3)},
		{"bucket": "b", "seq": int64(1), "value": int64(10)},
		{"bucket": "a", "seq": int64(2), "value": int64(2)},
		{"bucket": "a", "seq": int64(4), "value": int64(4)},
		{"bucket": "b", "seq": int64(2), "value": int64(20)},
	}
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return rows, nil
	})
	result, err := ExecuteSQLQuery(`
		FROM CACHE('events') AS event
		SELECT event.bucket, event.seq,
			SUM(event.value) OVER (
				PARTITION BY event.bucket
				ORDER BY event.seq
				ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
			) AS running
		ORDER BY event.bucket, event.seq
	`, resolver)
	if err != nil {
		t.Fatalf("execute incremental-window candidate: %v", err)
	}
	want := []SQLRow{
		{"bucket": "a", "seq": int64(1), "running": float64(1)},
		{"bucket": "a", "seq": int64(2), "running": float64(3)},
		{"bucket": "a", "seq": int64(3), "running": float64(6)},
		{"bucket": "a", "seq": int64(4), "running": float64(9)},
		{"bucket": "b", "seq": int64(1), "running": float64(10)},
		{"bucket": "b", "seq": int64(2), "running": float64(30)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("window rows = %#v, want %#v", result.Rows, want)
	}
}

func TestM065SQLIncrementalWindowPreservesNullAverageAndDescendingOrder(t *testing.T) {
	rows := []Row{
		{"seq": int64(1), "value": int64(1)},
		{"seq": int64(2), "value": nil},
		{"seq": int64(3), "value": int64(5)},
		{"seq": int64(4), "value": int64(9)},
	}
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return rows, nil
	})
	result, err := ExecuteSQLQuery(`
		FROM CACHE('events') AS event
		SELECT event.seq,
			AVG(event.value) OVER (
				ORDER BY event.seq DESC
				ROWS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS
			) AS rolling_avg,
			SUM(event.value) OVER (
				ORDER BY event.seq DESC
				ROWS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS
			) AS rolling_sum
		ORDER BY event.seq DESC
	`, resolver)
	if err != nil {
		t.Fatalf("execute descending incremental window: %v", err)
	}
	want := []SQLRow{
		{"seq": int64(4), "rolling_avg": float64(9), "rolling_sum": float64(9)},
		{"seq": int64(3), "rolling_avg": float64(7), "rolling_sum": float64(14)},
		{"seq": int64(2), "rolling_avg": float64(5), "rolling_sum": float64(5)},
		{"seq": int64(1), "rolling_avg": float64(1), "rolling_sum": float64(1)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("descending window rows = %#v, want %#v", result.Rows, want)
	}
}

func TestM065SQLIncrementalWindowRecoversAfterNonFiniteValueLeavesFrame(t *testing.T) {
	rows := []Row{
		{"seq": int64(1), "value": float64(1)},
		{"seq": int64(2), "value": math.NaN()},
		{"seq": int64(3), "value": float64(3)},
		{"seq": int64(4), "value": float64(4)},
	}
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return rows, nil
	})
	result, err := ExecuteSQLQuery(`
		FROM CACHE('events') AS event
		SELECT event.seq,
			SUM(event.value) OVER (
				ORDER BY event.seq
				ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
			) AS rolling_sum
		ORDER BY event.seq
	`, resolver)
	if err != nil {
		t.Fatalf("execute non-finite incremental window: %v", err)
	}
	if got := result.Rows[0]["rolling_sum"]; got != float64(1) {
		t.Fatalf("first sum = %#v, want 1", got)
	}
	if got, ok := result.Rows[1]["rolling_sum"].(float64); !ok || !math.IsNaN(got) {
		t.Fatalf("second sum = %#v, want NaN", result.Rows[1]["rolling_sum"])
	}
	if got, ok := result.Rows[2]["rolling_sum"].(float64); !ok || !math.IsNaN(got) {
		t.Fatalf("third sum = %#v, want NaN", result.Rows[2]["rolling_sum"])
	}
	if got := result.Rows[3]["rolling_sum"]; got != float64(7) {
		t.Fatalf("fourth sum = %#v, want 7 after NaN leaves frame", got)
	}
}

var benchmarkM065SQLWindowFrameSink SQLQueryResult

func BenchmarkM065SQLWindowFrame(b *testing.B) {
	rows := make([]Row, 4096)
	for index := range rows {
		rows[index] = Row{
			"bucket": "a",
			"seq":    int64(index),
			"value":  int64(index % 97),
		}
	}
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return rows, nil
	})
	query := `
		FROM CACHE('events') AS event
		SELECT event.seq,
			SUM(event.value) OVER (
				ORDER BY event.seq
				ROWS BETWEEN 127 PRECEDING AND CURRENT ROW
			) AS running
		ORDER BY event.seq
	`
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQuery(query, resolver)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkM065SQLWindowFrameSink = result
	}
}
