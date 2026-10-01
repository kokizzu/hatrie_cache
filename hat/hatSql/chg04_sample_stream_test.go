package hatSql

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

type chg04SampleStreamResolver struct {
	rows         []SQLRow
	streamCalls  int
	resolveCalls int
}

func (resolver *chg04SampleStreamResolver) ResolveSQLSource(string, string) ([]SQLRow, error) {
	resolver.resolveCalls++
	return resolver.rows, nil
}

func (resolver *chg04SampleStreamResolver) StreamSQLSource(ctx context.Context, _, _ string, visit func(Row) error) error {
	resolver.streamCalls++
	for _, row := range resolver.rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(row); err != nil {
			return err
		}
	}
	return nil
}

func TestSQLTableSampleBernoulliStreamsWithoutMaterializingSource(t *testing.T) {
	rows := sampleRows(100)
	query := `SELECT id FROM CACHE('events') TABLESAMPLE BERNOULLI (50) REPEATABLE (7)`
	want, err := ExecuteSQLQuery(query, approximateAggregateSource(rows))
	if err != nil {
		t.Fatalf("materialized sample: %v", err)
	}

	resolver := &chg04SampleStreamResolver{rows: rows}
	got := make([]SQLRow, 0, len(want.Rows))
	err = ExecuteSQLQueryRows(context.Background(), query, resolver, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		got = append(got, row)
		return nil
	})
	if err != nil {
		t.Fatalf("streamed sample: %v", err)
	}
	if !reflect.DeepEqual(got, want.Rows) {
		t.Fatalf("streamed sample = %#v, want %#v", got, want.Rows)
	}
	if resolver.streamCalls != 1 {
		t.Fatalf("StreamSQLSource calls = %d, want 1", resolver.streamCalls)
	}
	if resolver.resolveCalls != 0 {
		t.Fatalf("ResolveSQLSource calls = %d, want 0", resolver.resolveCalls)
	}
}

func TestSQLTableSampleStreamingAppliesFilterAfterSampling(t *testing.T) {
	rows := sampleRows(100)
	query := `SELECT id FROM CACHE('events') TABLESAMPLE BERNOULLI (50) REPEATABLE (7) WHERE id >= 25 LIMIT 3`
	want, err := ExecuteSQLQuery(query, approximateAggregateSource(rows))
	if err != nil {
		t.Fatalf("materialized filtered sample: %v", err)
	}
	resolver := &chg04SampleStreamResolver{rows: rows}
	got := make([]SQLRow, 0, len(want.Rows))
	err = ExecuteSQLQueryRows(context.Background(), query, resolver, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		got = append(got, row)
		return nil
	})
	if err != nil {
		t.Fatalf("streamed filtered sample: %v", err)
	}
	if !reflect.DeepEqual(got, want.Rows) {
		t.Fatalf("streamed filtered sample = %#v, want %#v", got, want.Rows)
	}
}

func TestSQLTableSampleStreamingKeepsReservoirFallback(t *testing.T) {
	query := `SELECT id FROM CACHE('events') TABLESAMPLE RESERVOIR (4) REPEATABLE (19)`
	resolver := &chg04SampleStreamResolver{rows: sampleRows(20)}
	if err := ExecuteSQLQueryRows(context.Background(), query, resolver, nil, SQLQueryOptions{}, func([]string, SQLRow) error {
		return nil
	}); err != nil {
		t.Fatalf("streamed reservoir sample: %v", err)
	}
	if resolver.streamCalls != 0 || resolver.resolveCalls != 1 {
		t.Fatalf("reservoir resolver calls = stream %d, resolve %d; want materialized fallback", resolver.streamCalls, resolver.resolveCalls)
	}
}

func TestSQLTableSampleStreamingRetainsSourceRowBudget(t *testing.T) {
	query := `SELECT id FROM CACHE('events') TABLESAMPLE BERNOULLI (100)`
	resolver := &chg04SampleStreamResolver{rows: sampleRows(3)}
	err := ExecuteSQLQueryRows(context.Background(), query, resolver, nil, SQLQueryOptions{MaxRows: 2}, func([]string, SQLRow) error {
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "exceeds the 2 row limit") {
		t.Fatalf("streamed sampled oversized source error = %v, want row-budget failure", err)
	}
}

var chg04SampleStreamSink int

func BenchmarkSQLTableSampleRowsBaseline(b *testing.B) {
	rows := sampleRows(10000)
	query := `SELECT id FROM CACHE('events') TABLESAMPLE BERNOULLI (10) REPEATABLE (7)`
	resolver := approximateAggregateSource(rows)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		count := 0
		err := ExecuteSQLQueryRows(context.Background(), query, resolver, nil, SQLQueryOptions{}, func(_ []string, _ SQLRow) error {
			count++
			return nil
		})
		if err != nil {
			b.Fatalf("materialized sampled query: %v", err)
		}
		chg04SampleStreamSink = count
	}
}

func BenchmarkSQLTableSampleRows(b *testing.B) {
	rows := sampleRows(10000)
	query := `SELECT id FROM CACHE('events') TABLESAMPLE BERNOULLI (10) REPEATABLE (7)`
	resolver := &chg04SampleStreamResolver{rows: rows}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		count := 0
		err := ExecuteSQLQueryRows(context.Background(), query, resolver, nil, SQLQueryOptions{}, func(_ []string, _ SQLRow) error {
			count++
			return nil
		})
		if err != nil {
			b.Fatalf("stream sampled query: %v", err)
		}
		chg04SampleStreamSink = count
	}
}
