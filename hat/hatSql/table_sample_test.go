package hatSql

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestSQLTableSampleBernoulliRepeatable(t *testing.T) {
	rows := sampleRows(100)
	query := `
		SELECT id
		FROM CACHE('events')
		TABLESAMPLE BERNOULLI (50) REPEATABLE (7)
		ORDER BY id
	`
	first, err := ExecuteSQLQuery(query, approximateAggregateSource(rows))
	if err != nil {
		t.Fatalf("first Bernoulli sample: %v", err)
	}
	second, err := ExecuteSQLQuery(query, approximateAggregateSource(rows))
	if err != nil {
		t.Fatalf("second Bernoulli sample: %v", err)
	}
	if !reflect.DeepEqual(first.Rows, second.Rows) {
		t.Fatalf("repeatable Bernoulli samples differ: first=%#v second=%#v", first.Rows, second.Rows)
	}
	if len(first.Rows) == 0 || len(first.Rows) == len(rows) {
		t.Fatalf("Bernoulli sample rows = %d, want a strict subset", len(first.Rows))
	}
	for index, row := range first.Rows {
		if got, ok := row["id"].(int); !ok || index > 0 && got <= first.Rows[index-1]["id"].(int) {
			t.Fatalf("Bernoulli sample row %d = %#v, want increasing integer id", index, row)
		}
	}
	analysis, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, approximateAggregateSource(rows))
	if err != nil {
		t.Fatalf("explain sampled query: %v", err)
	}
	if !strings.Contains(fmt.Sprint(analysis.Plan), "TABLESAMPLE BERNOULLI") {
		t.Fatalf("sampled plan = %#v, want TABLESAMPLE BERNOULLI", analysis.Plan)
	}
}

func TestSQLTableSampleReservoirRepeatable(t *testing.T) {
	rows := sampleRows(10)
	query := `
		SELECT id
		FROM CACHE('events')
		TABLESAMPLE RESERVOIR (3) REPEATABLE (11)
	`
	first, err := ExecuteSQLQuery(query, approximateAggregateSource(rows))
	if err != nil {
		t.Fatalf("first reservoir sample: %v", err)
	}
	second, err := ExecuteSQLQuery(query, approximateAggregateSource(rows))
	if err != nil {
		t.Fatalf("second reservoir sample: %v", err)
	}
	if len(first.Rows) != 3 || !reflect.DeepEqual(first.Rows, second.Rows) {
		t.Fatalf("reservoir samples = %#v and %#v, want identical three-row samples", first.Rows, second.Rows)
	}
	for index, row := range first.Rows {
		if got, ok := row["id"].(int); !ok || index > 0 && got <= first.Rows[index-1]["id"].(int) {
			t.Fatalf("reservoir sample row %d = %#v, want source order", index, row)
		}
	}
}

func TestSQLTableSampleStreamsMaterializedSemantics(t *testing.T) {
	rows := sampleRows(20)
	query := `SELECT id FROM CACHE('events') TABLESAMPLE RESERVOIR (4) REPEATABLE (19)`
	result, err := ExecuteSQLQuery(query, approximateAggregateSource(rows))
	if err != nil {
		t.Fatalf("materialized sample: %v", err)
	}
	streamed := make([]SQLRow, 0, len(result.Rows))
	err = ExecuteSQLQueryRows(context.Background(), query, approximateAggregateSource(rows), nil, SQLQueryOptions{}, func(columns []string, row SQLRow) error {
		streamed = append(streamed, row)
		return nil
	})
	if err != nil {
		t.Fatalf("stream sampled query: %v", err)
	}
	if !reflect.DeepEqual(streamed, result.Rows) {
		t.Fatalf("streamed sample = %#v, want %#v", streamed, result.Rows)
	}
}

func TestSQLTableSampleRetainsSourceRowBudget(t *testing.T) {
	query, err := parseSQLQueryParameters(`SELECT id FROM CACHE('events') TABLESAMPLE RESERVOIR (1)`, nil)
	if err != nil {
		t.Fatalf("parse sampled query: %v", err)
	}
	control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{MaxRows: 2})
	if err != nil {
		t.Fatalf("new execution control: %v", err)
	}
	defer cancel()
	if _, err := executeSQLQueryWithMetrics(query, approximateAggregateSource(sampleRows(3)), nil, nil, control); err == nil || !strings.Contains(err.Error(), "exceeds the 2 row limit") {
		t.Fatalf("sampled oversized source error = %v, want row-budget failure", err)
	}
}

func TestSQLTableSampleValidation(t *testing.T) {
	rows := sampleRows(2)
	for _, query := range []string{
		`SELECT id FROM CACHE('events') TABLESAMPLE BERNOULLI (-1)`,
		`SELECT id FROM CACHE('events') TABLESAMPLE BERNOULLI (101)`,
		`SELECT id FROM CACHE('events') TABLESAMPLE RESERVOIR (0)`,
		`SELECT id FROM CACHE('events') TABLESAMPLE RESERVOIR (1) TABLESAMPLE RESERVOIR (1)`,
	} {
		if _, err := ExecuteSQLQuery(query, approximateAggregateSource(rows)); err == nil {
			t.Fatalf("query %q succeeded, want sampling validation error", query)
		}
	}
}

func sampleRows(count int) []SQLRow {
	rows := make([]SQLRow, count)
	for index := range rows {
		rows[index] = SQLRow{"id": index}
	}
	return rows
}

func BenchmarkSQLTableSample(b *testing.B) {
	rows := sampleRows(10000)
	for _, benchmark := range []struct {
		name  string
		query string
	}{
		{name: "Bernoulli10", query: `SELECT id FROM CACHE('events') TABLESAMPLE BERNOULLI (10) REPEATABLE (7)`},
		{name: "Reservoir100", query: `SELECT id FROM CACHE('events') TABLESAMPLE RESERVOIR (100) REPEATABLE (7)`},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				result, err := ExecuteSQLQuery(benchmark.query, approximateAggregateSource(rows))
				if err != nil {
					b.Fatalf("execute table sample: %v", err)
				}
				if len(result.Rows) == 0 {
					b.Fatal("sample unexpectedly returned no rows")
				}
			}
		})
	}
}
