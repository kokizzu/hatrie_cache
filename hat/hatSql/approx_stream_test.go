package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type approximateAggregateStreamSource struct {
	rows []SQLRow
}

func (source approximateAggregateStreamSource) ResolveSQLSource(string, string) ([]SQLRow, error) {
	return nil, errors.New("approximate aggregate stream source must not be materialized")
}

func (source approximateAggregateStreamSource) StreamSQLSource(ctx context.Context, _ string, _ string, visit func(SQLRow) error) error {
	for _, row := range source.rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(row); err != nil {
			return err
		}
	}
	return nil
}

func TestSQLApproximateAggregatesUseStreamingState(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "distinct",
			query: `SELECT APPROX_COUNT_DISTINCT(visitor, 10) AS visitors FROM CACHE('events')`,
		},
		{
			name:  "percentile",
			query: `SELECT APPROX_PERCENTILE(latency, 0.95, 0.01) AS p95 FROM CACHE('events')`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query, err := parseSQLQuery(test.query)
			if err != nil {
				t.Fatalf("parseSQLQuery() error = %v", err)
			}
			aggregates, ok := sqlGlobalStreamAggregates(query)
			if !ok {
				t.Fatal("sqlGlobalStreamAggregates() rejected approximate aggregate")
			}
			if len(aggregates) != 1 || aggregates[0].name != query.selects[0].expr.name {
				t.Fatalf("stream aggregates = %#v, query expression = %#v", aggregates, query.selects[0].expr)
			}
		})
	}
}

func TestSQLApproximateAggregatesStreamingMatchesMaterialized(t *testing.T) {
	rows := approximateAggregateSource{
		{"visitor": "a", "latency": 10.0},
		{"visitor": "b", "latency": 20.0},
		{"visitor": "a", "latency": 30.0},
		{"visitor": nil, "latency": 40.0},
	}
	query := `SELECT APPROX_COUNT_DISTINCT(visitor, 10) AS visitors, APPROX_PERCENTILE(latency, 0.95, 0.01) AS p95 FROM CACHE('events')`
	want, err := ExecuteSQLQuery(query, rows)
	if err != nil {
		t.Fatalf("materialized execution error = %v", err)
	}
	var got SQLQueryResult
	err = ExecuteSQLQueryRows(context.Background(), query, approximateAggregateStreamSource{rows: rows}, nil, SQLQueryOptions{}, func(columns []string, row SQLRow) error {
		got.Columns = columns
		got.Rows = append(got.Rows, row)
		return nil
	})
	if err != nil {
		t.Fatalf("streaming execution error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("streaming result = %#v, materialized result = %#v", got, want)
	}
}

func TestSQLApproximateAggregatesResultAPIUsesStreamingSource(t *testing.T) {
	query := `SELECT APPROX_COUNT_DISTINCT(visitor, 10) AS visitors, APPROX_PERCENTILE(latency, 0.95, 0.01) AS p95 FROM CACHE('events')`
	result, err := ExecuteSQLQuery(query, approximateAggregateStreamSource{rows: []SQLRow{
		{"visitor": "a", "latency": 10.0},
		{"visitor": "b", "latency": 20.0},
	}})
	if err != nil {
		t.Fatalf("streaming result API error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["visitors"] == nil || result.Rows[0]["p95"] == nil {
		t.Fatalf("streaming result = %#v, want one populated row", result)
	}
}

func TestSQLApproximateAggregatesUseDirectSourceValues(t *testing.T) {
	query, err := parseSQLQuery(`SELECT APPROX_COUNT_DISTINCT(visitor, 10) AS visitors, APPROX_PERCENTILE(latency, 0.95, 0.01) AS p95 FROM CACHE('events')`)
	if err != nil {
		t.Fatalf("parseSQLQuery() error = %v", err)
	}
	aggregates, ok := sqlGlobalStreamAggregates(query)
	if !ok {
		t.Fatal("sqlGlobalStreamAggregates() rejected approximate aggregate")
	}
	if sqlApproximateDirectSourcePlan(query, aggregates) != true {
		t.Fatal("sqlApproximateDirectSourcePlan() = false, want true")
	}
}
