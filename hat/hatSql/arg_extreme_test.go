package hatSql

import (
	"context"
	"strconv"
	"testing"
)

type argExtremeSource []SQLRow

var benchmarkSQLArgExtremeSink interface{}

func (rows argExtremeSource) ResolveSQLSource(name string, key string) ([]SQLRow, error) {
	return rows, nil
}

func TestSQLArgExtremeAggregates(t *testing.T) {
	rows := argExtremeSource{
		{"region": "east", "payload": "east-early", "score": 2},
		{"region": "east", "payload": "east-late", "score": 9},
		{"region": "east", "payload": "east-tie", "score": 9},
		{"region": "east", "payload": nil, "score": 20},
		{"region": "west", "payload": "west-late", "score": 7},
		{"region": "west", "payload": "west-early", "score": 1},
	}

	result, err := ExecuteSQLQuery(`
		SELECT region,
			ARGMAX(payload, score) AS latest,
			ARGMIN(payload, score) AS earliest
		FROM CACHE('events')
		GROUP BY region
		ORDER BY region
	`, rows)
	if err != nil {
		t.Fatalf("execute argMax/argMin: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("row count = %d, want 2", len(result.Rows))
	}
	if got := result.Rows[0]["latest"]; got != "east-late" {
		t.Fatalf("east latest = %#v, want east-late", got)
	}
	if got := result.Rows[0]["earliest"]; got != "east-early" {
		t.Fatalf("east earliest = %#v, want east-early", got)
	}
	if got := result.Rows[1]["latest"]; got != "west-late" {
		t.Fatalf("west latest = %#v, want west-late", got)
	}
	if got := result.Rows[1]["earliest"]; got != "west-early" {
		t.Fatalf("west earliest = %#v, want west-early", got)
	}
}

func TestSQLArgExtremeFilterAndValidation(t *testing.T) {
	rows := argExtremeSource{
		{"region": "east", "payload": "east", "score": 3},
		{"region": "west", "payload": "west", "score": 8},
	}
	result, err := ExecuteSQLQuery(`
		SELECT ARGMAX(payload, score) FILTER (WHERE region = 'east') AS winner
		FROM CACHE('events')
	`, rows)
	if err != nil {
		t.Fatalf("execute filtered argMax: %v", err)
	}
	if got := result.Rows[0]["winner"]; got != "east" {
		t.Fatalf("filtered winner = %#v, want east", got)
	}

	for _, query := range []string{
		`SELECT ARGMAX(payload) FROM CACHE('events')`,
		`SELECT ARGMIN(payload, score, region) FROM CACHE('events')`,
	} {
		if _, err := ExecuteSQLQuery(query, rows); err == nil {
			t.Fatalf("query %q succeeded, want argument-count error", query)
		}
	}
}

func TestSQLArgExtremeWindow(t *testing.T) {
	rows := argExtremeSource{
		{"payload": "first", "score": 1},
		{"payload": "third", "score": 3},
		{"payload": "second", "score": 2},
	}
	result, err := ExecuteSQLQuery(`
		SELECT payload,
			ARGMAX(payload, score) OVER (ORDER BY score ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running
		FROM CACHE('events')
		ORDER BY score
	`, rows)
	if err != nil {
		t.Fatalf("execute window argMax: %v", err)
	}
	want := []interface{}{"first", "second", "third"}
	if len(result.Rows) != len(want) {
		t.Fatalf("window row count = %d, want %d", len(result.Rows), len(want))
	}
	for index, expected := range want {
		if got := result.Rows[index]["running"]; got != expected {
			t.Fatalf("window row %d = %#v, want %#v", index, got, expected)
		}
	}
}

func TestSQLArgExtremeGlobalStreamPlan(t *testing.T) {
	query, err := parseSQLQuery(`SELECT ARGMAX(payload, score) AS latest, ARGMIN(payload, score) AS earliest FROM CACHE('events')`)
	if err != nil {
		t.Fatalf("parse argMax/argMin: %v", err)
	}
	aggregates, ok := sqlGlobalStreamAggregates(query)
	if !ok || len(aggregates) != 2 {
		t.Fatalf("global aggregate plan = %#v, %t; want two constant-state aggregates", aggregates, ok)
	}
}

func TestSQLArgExtremeDirectPredicatePlan(t *testing.T) {
	query, err := parseSQLQuery(`SELECT ARGMAX(payload, score) AS winner FROM CACHE('events') WHERE score >= 5`)
	if err != nil {
		t.Fatalf("parse filtered argMax: %v", err)
	}
	aggregates, ok := sqlArgExtremeMaterializedPlan(query)
	if !ok || !sqlArgExtremeDirectSourcePlan(query, aggregates) {
		t.Fatalf("direct filtered plan = %#v, %t; want direct source predicate", aggregates, ok)
	}
}

func TestSQLArgExtremeWhere(t *testing.T) {
	rows := argExtremeSource{
		{"payload": "low", "score": 1},
		{"payload": "middle", "score": 2},
		{"payload": "high", "score": 3},
	}
	result, err := ExecuteSQLQuery(`
		SELECT ARGMAX(payload, score) AS latest, ARGMIN(payload, score) AS earliest
		FROM CACHE('events')
		WHERE score >= 2
	`, rows)
	if err != nil {
		t.Fatalf("execute argMax/argMin WHERE: %v", err)
	}
	if got := result.Rows[0]["latest"]; got != "high" {
		t.Fatalf("filtered latest = %#v, want high", got)
	}
	if got := result.Rows[0]["earliest"]; got != "middle" {
		t.Fatalf("filtered earliest = %#v, want middle", got)
	}
}

func TestSQLArgExtremeHonorsResultByteLimit(t *testing.T) {
	rows := argExtremeSource{{"payload": "winner", "score": 1}}
	_, err := ExecuteSQLQueryContext(context.Background(), `SELECT ARGMAX(payload, score) AS winner FROM CACHE('events')`, rows, SQLQueryOptions{MaxResultBytes: 1})
	if err == nil {
		t.Fatal("argMax exceeded result-byte budget without an error")
	}
}

func BenchmarkSQLArgExtreme(b *testing.B) {
	rows := makeArgExtremeBenchmarkSource(10000)
	query := `SELECT ARGMAX(payload, score) AS latest, ARGMIN(payload, score) AS earliest FROM CACHE('events')`
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQuery(query, rows)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkSQLArgExtremeSink = result.Rows[0]
	}
}

func BenchmarkSQLArgExtremeSortWorkaround(b *testing.B) {
	rows := makeArgExtremeBenchmarkSource(10000)
	maximum := `SELECT payload AS selected FROM CACHE('events') ORDER BY score DESC LIMIT 1`
	minimum := `SELECT payload AS selected FROM CACHE('events') ORDER BY score ASC LIMIT 1`
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		maximumResult, err := ExecuteSQLQuery(maximum, rows)
		if err != nil {
			b.Fatal(err)
		}
		minimumResult, err := ExecuteSQLQuery(minimum, rows)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkSQLArgExtremeSink = []SQLRow{maximumResult.Rows[0], minimumResult.Rows[0]}
	}
}

func BenchmarkSQLArgExtremeFiltered(b *testing.B) {
	rows := makeArgExtremeBenchmarkSource(10000)
	query := `SELECT ARGMAX(payload, score) AS latest, ARGMIN(payload, score) AS earliest FROM CACHE('events') WHERE score >= 5000`
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQuery(query, rows)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkSQLArgExtremeSink = result.Rows[0]
	}
}

func BenchmarkSQLArgExtremeFilteredSortWorkaround(b *testing.B) {
	rows := makeArgExtremeBenchmarkSource(10000)
	maximum := `SELECT payload AS selected FROM CACHE('events') WHERE score >= 5000 ORDER BY score DESC LIMIT 1`
	minimum := `SELECT payload AS selected FROM CACHE('events') WHERE score >= 5000 ORDER BY score ASC LIMIT 1`
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		maximumResult, err := ExecuteSQLQuery(maximum, rows)
		if err != nil {
			b.Fatal(err)
		}
		minimumResult, err := ExecuteSQLQuery(minimum, rows)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkSQLArgExtremeSink = []SQLRow{maximumResult.Rows[0], minimumResult.Rows[0]}
	}
}

func makeArgExtremeBenchmarkSource(count int) argExtremeSource {
	rows := make(argExtremeSource, count)
	for index := range rows {
		rows[index] = SQLRow{
			"payload": "payload-" + strconv.Itoa(index),
			"score":   int64(index),
		}
	}
	return rows
}
