package hatSql_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestExplainSQLWhatIfEqualityReportsReadReductionWithoutChangingQueryResults(t *testing.T) {
	rows := []hatSql.Row{
		{"id": int64(1), "region": "apac", "score": int64(10)},
		{"id": int64(2), "region": "emea", "score": int64(20)},
		{"id": int64(3), "region": "apac", "score": int64(30)},
		{"id": int64(4), "region": "us", "score": int64(40)},
	}
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return hatSql.CloneRows(rows), nil
	})
	query := "SELECT id FROM CACHE('events') WHERE region = 'apac'"
	before, err := hatSql.ExecuteQuery(query, resolver)
	if err != nil {
		t.Fatal(err)
	}
	report, err := hatSql.ExplainSQLWhatIf(context.Background(), hatSql.SQLWhatIfRequest{
		Query: query,
		Index: hatSql.SQLWhatIfIndex{Kind: hatSql.SQLWhatIfIndexEquality, Fields: []string{"region"}},
	}, &resolver)
	if err != nil {
		t.Fatal(err)
	}
	after, err := hatSql.ExecuteQuery(query, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("query result changed around what-if analysis: before=%#v after=%#v", before, after)
	}
	if !report.Supported || !report.Beneficial {
		t.Fatalf("what-if report = %#v, want supported and beneficial", report)
	}
	if report.Source != "events" || report.SourceRows != 4 || report.BaselineRowsRead != 4 || report.HypotheticalRowsRead != 2 || report.RowsSkipped != 2 {
		t.Fatalf("what-if cardinalities = %#v", report)
	}
	if report.ExistingIndex {
		t.Fatal("hypothetical index unexpectedly reported as existing")
	}
}

func TestExplainSQLWhatIfRejectsMismatchedSourceAndUnsupportedShape(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"id": int64(1)}}, nil
	})
	if _, err := hatSql.ExplainSQLWhatIf(context.Background(), hatSql.SQLWhatIfRequest{
		Query: "SELECT id FROM CACHE('events')",
		Index: hatSql.SQLWhatIfIndex{Source: "other", Kind: hatSql.SQLWhatIfIndexEquality, Fields: []string{"id"}},
	}, resolver); err == nil {
		t.Fatal("mismatched source error = nil")
	}
	report, err := hatSql.ExplainSQLWhatIf(context.Background(), hatSql.SQLWhatIfRequest{
		Query: "SELECT id FROM CACHE('events') ORDER BY id",
		Index: hatSql.SQLWhatIfIndex{Kind: hatSql.SQLWhatIfIndexEquality, Fields: []string{"id"}},
	}, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if report.Supported || report.Beneficial || len(report.Notes) == 0 {
		t.Fatalf("unsupported shape report = %#v", report)
	}
}

func TestExplainSQLWhatIfUsesExistingIndexMetadata(t *testing.T) {
	resolver := whatIfIndexStatsResolver{
		rows: []hatSql.Row{{"id": int64(1), "region": "apac"}, {"id": int64(2), "region": "emea"}},
	}
	report, err := hatSql.ExplainSQLWhatIf(context.Background(), hatSql.SQLWhatIfRequest{
		Query: "SELECT id FROM CACHE('events') WHERE region = 'apac'",
		Index: hatSql.SQLWhatIfIndex{Kind: hatSql.SQLWhatIfIndexEquality, Fields: []string{"region"}},
	}, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if !report.ExistingIndex || report.HypotheticalRowsRead != 1 || report.RowsSkipped != 1 {
		t.Fatalf("existing-index report = %#v", report)
	}
}

func TestExplainSQLWhatIfUsesSourceStatisticsWithoutReadingRows(t *testing.T) {
	resolver := statsOnlyWhatIfResolver{}
	report, err := hatSql.ExplainSQLWhatIf(context.Background(), hatSql.SQLWhatIfRequest{
		Query: "SELECT id FROM CACHE('events') WHERE score >= 90",
		Index: hatSql.SQLWhatIfIndex{Kind: hatSql.SQLWhatIfIndexRange, Fields: []string{"score"}},
	}, &resolver)
	if err != nil {
		t.Fatal(err)
	}
	if resolver.rowsRead {
		t.Fatal("source rows were read despite available statistics")
	}
	if !report.Supported || !report.Beneficial || report.SourceRows != 100 || report.HypotheticalRowsRead != 10 || report.RowsSkipped != 90 || report.SourceBytes != 10000 {
		t.Fatalf("statistics report = %#v", report)
	}
	if len(report.Notes) == 0 {
		t.Fatal("statistics report has no provenance note")
	}
}

func TestExplainSQLWhatIfReportsOrderAndGroupBenefits(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{
			{"region": "apac", "id": int64(2)},
			{"region": "emea", "id": int64(1)},
			{"region": "apac", "id": int64(1)},
		}, nil
	})
	order, err := hatSql.ExplainSQLWhatIf(context.Background(), hatSql.SQLWhatIfRequest{
		Query: "SELECT id FROM CACHE('events') ORDER BY region, id",
		Index: hatSql.SQLWhatIfIndex{Kind: hatSql.SQLWhatIfIndexOrder, Fields: []string{"region", "id"}},
	}, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if !order.Supported || !order.Beneficial || order.PotentialBenefit != "can eliminate the requested sort" {
		t.Fatalf("order report = %#v", order)
	}
	group, err := hatSql.ExplainSQLWhatIf(context.Background(), hatSql.SQLWhatIfRequest{
		Query: "SELECT region, COUNT(*) AS total FROM CACHE('events') GROUP BY region",
		Index: hatSql.SQLWhatIfIndex{Kind: hatSql.SQLWhatIfIndexGroup, Fields: []string{"region"}},
	}, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if !group.Supported || !group.Beneficial || len(group.Notes) == 0 {
		t.Fatalf("group report = %#v", group)
	}
}

func TestExplainSQLWhatIfHonorsCanceledContextBeforeReadingSource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		called = true
		return nil, errors.New("source should not be read")
	})
	_, err := hatSql.ExplainSQLWhatIf(ctx, hatSql.SQLWhatIfRequest{
		Query: "SELECT id FROM CACHE('events') WHERE id = 1",
		Index: hatSql.SQLWhatIfIndex{Kind: hatSql.SQLWhatIfIndexEquality, Fields: []string{"id"}},
	}, resolver)
	if !errors.Is(err, context.Canceled) || called {
		t.Fatalf("canceled what-if = err %v, source called %t", err, called)
	}
}

func TestExplainSQLWhatIfSaturatesLargeIndexEstimate(t *testing.T) {
	maximumInt := int(^uint(0) >> 1)
	resolver := largeStatsWhatIfResolver{rows: maximumInt, averageValueBytes: 7}
	report, err := hatSql.ExplainSQLWhatIf(context.Background(), hatSql.SQLWhatIfRequest{
		Query: "SELECT score FROM CACHE('events') ORDER BY score",
		Index: hatSql.SQLWhatIfIndex{Kind: hatSql.SQLWhatIfIndexOrder, Fields: []string{"score"}},
	}, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if report.EstimatedIndexBytes != maximumInt {
		t.Fatalf("large index estimate = %d, want saturation at %d", report.EstimatedIndexBytes, maximumInt)
	}
	if report.EstimatedWriteBytesPerMutation <= 0 {
		t.Fatalf("large write estimate = %d, want a positive bounded estimate", report.EstimatedWriteBytesPerMutation)
	}
}

type whatIfIndexStatsResolver struct {
	rows []hatSql.Row
}

func (resolver whatIfIndexStatsResolver) ResolveSQLSource(string, string) ([]hatSql.Row, error) {
	return hatSql.CloneRows(resolver.rows), nil
}

func (resolver whatIfIndexStatsResolver) SQLJSONIndexStats(key string, fields ...string) (hatSql.JSONIndexStats, bool, error) {
	if key != "events" || !reflect.DeepEqual(fields, []string{"region"}) {
		return hatSql.JSONIndexStats{}, false, nil
	}
	return hatSql.JSONIndexStats{Key: key, Fields: fields, Rows: 2, DistinctKeys: 2}, true, nil
}

func (resolver whatIfIndexStatsResolver) SQLJSONIndexValueEstimate(key, field string, value interface{}) (int, bool, bool, error) {
	if key != "events" || field != "region" {
		return 0, false, false, nil
	}
	count := 0
	for _, row := range resolver.rows {
		if row[field] == value {
			count++
		}
	}
	return count, true, true, nil
}

type statsOnlyWhatIfResolver struct {
	rowsRead bool
}

func (resolver *statsOnlyWhatIfResolver) ResolveSQLSource(string, string) ([]hatSql.Row, error) {
	resolver.rowsRead = true
	return nil, errors.New("statistics-backed source must not be read")
}

func (statsOnlyWhatIfResolver) SQLWhatIfSourceStatistics(string, string, []string) (hatSql.SQLWhatIfSourceStatistics, bool, error) {
	return hatSql.SQLWhatIfSourceStatistics{
		Source: "events",
		Rows:   100,
		Bytes:  10000,
		Fields: map[string]hatSql.SQLWhatIfFieldStatistics{
			"score": {Rows: 100, DistinctValues: 100, Minimum: int64(0), Maximum: int64(99), AverageValueBytes: 8},
		},
	}, true, nil
}

type largeStatsWhatIfResolver struct {
	rows              int
	averageValueBytes int
}

func (largeStatsWhatIfResolver) ResolveSQLSource(string, string) ([]hatSql.Row, error) {
	return nil, errors.New("large statistics source must not be read")
}

func (resolver largeStatsWhatIfResolver) SQLWhatIfSourceStatistics(string, string, []string) (hatSql.SQLWhatIfSourceStatistics, bool, error) {
	return hatSql.SQLWhatIfSourceStatistics{
		Source: "events",
		Rows:   resolver.rows,
		Fields: map[string]hatSql.SQLWhatIfFieldStatistics{
			"score": {Rows: resolver.rows, AverageValueBytes: resolver.averageValueBytes},
		},
	}, true, nil
}
