package hatCache

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSQLJSONIndexHealthRefreshesOnlineIndex(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"team":"core"},{"team":"core"},{"name":"missing"}]`)
	if err := trie.CreateSQLJSONFieldIndex("people", "team"); err != nil {
		t.Fatal(err)
	}
	health, ok, err := trie.SQLJSONIndexHealth("people", "team")
	if err != nil || !ok {
		t.Fatalf("SQLJSONIndexHealth() = %#v, %v, %v", health, ok, err)
	}
	if health.Rows != 3 || health.IndexedRows != 2 || health.NullRows != 1 || health.DistinctKeys != 1 || !health.Current {
		t.Fatalf("index health = %#v", health)
	}
	trie.UpsertString("people", `[{"team":"platform"}]`)
	health, ok, err = trie.SQLJSONIndexHealth("people", "team")
	if err != nil || !ok || health.Rows != 1 || health.IndexedRows != 1 || health.DistinctKeys != 1 || !health.Current {
		t.Fatalf("refreshed index health = %#v, %v, %v", health, ok, err)
	}
}

func TestSQLJSONTextIndexAcceleratesTokenContains(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("articles", `[
  {"id":1,"body":"Fast cache indexes for Go"},
  {"id":2,"body":"A slow database migration"},
  {"id":3,"body":"Go query planning and cache design"}
]`)
	if err := trie.CreateSQLJSONTextIndex("articles", "body"); err != nil {
		t.Fatalf("CreateSQLJSONTextIndex() error = %v", err)
	}
	result, err := ExecuteSQLQuery(`
FROM CACHE('articles') AS article
WHERE CONTAINS(article.body, 'go cache')
SELECT article.id
ORDER BY article.id`, trie)
	if err != nil {
		t.Fatalf("token query error = %v", err)
	}
	if !reflect.DeepEqual(result.Rows, []SQLRow{{"id": float64(1)}, {"id": float64(3)}}) {
		t.Fatalf("token query rows = %#v", result.Rows)
	}
	explained, err := ExecuteSQLQuery(`
EXPLAIN ANALYZE FROM CACHE('articles') AS article
WHERE CONTAINS(article.body, 'go cache')
SELECT article.id
ORDER BY article.id`, trie)
	if err != nil {
		t.Fatalf("token query EXPLAIN error = %v", err)
	}
	if len(explained.Rows) == 0 || explained.Rows[0]["node"] != "INDEX SCAN" {
		t.Fatalf("token query plan = %#v, want INDEX SCAN", explained.Rows)
	}
	result, err = ExecuteSQLQuery(`
FROM CACHE('articles') AS article
WHERE CONTAINS(article.body, 'database')
SELECT article.id`, trie)
	if err != nil || !reflect.DeepEqual(result.Rows, []SQLRow{{"id": float64(2)}}) {
		t.Fatalf("single token query = %#v, %v", result, err)
	}
	_, err = ExecuteSQLQuery(`
FROM CACHE('articles') AS article
WHERE CONTAINS(article.id, '1')
SELECT article.id`, trie)
	if err == nil || !strings.Contains(err.Error(), "CONTAINS expects TEXT arguments") {
		t.Fatalf("non-text token query error = %v", err)
	}
}

func TestSQLJSONRangeStatisticsUseOrderedIndex(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("events", `[{"score":1},{"score":2},{"score":3},{"score":4},{"score":5},{"name":"missing"}]`)
	if err := trie.CreateSQLJSONFieldIndex("events", "score"); err != nil {
		t.Fatal(err)
	}
	stats, ok, err := trie.SQLJSONRangeStats("events", "score", 2)
	if err != nil || !ok {
		t.Fatalf("SQLJSONRangeStats() = %#v, %v, %v", stats, ok, err)
	}
	if stats.Rows != 5 || stats.NullRows != 1 || !reflect.DeepEqual(stats.Buckets, []SQLJSONRangeHistogramBucket{
		{Lower: float64(1), Upper: float64(3), Rows: 3},
		{Lower: float64(4), Upper: float64(5), Rows: 2},
	}) {
		t.Fatalf("range stats = %#v", stats)
	}
	rows, exact, available, err := trie.SQLJSONRangeEstimate("events", "score", ">=", float64(3))
	if err != nil || !available || !exact || rows != 3 {
		t.Fatalf("SQLJSONRangeEstimate() = (%d, %v, %v, %v), want (3, true, true, nil)", rows, exact, available, err)
	}
}

func TestExecuteSQLQueryEmitsStructuredObservability(t *testing.T) {
	t.Parallel()
	var events []SQLQueryEvent
	result, err := ExecuteSQLQueryParameters(context.Background(), "FROM VALUES (7) AS v(value) SELECT value", SQLSourceResolverFunc(nil), nil, SQLQueryOptions{
		QueryID:            "query-observe-7",
		SlowQueryThreshold: time.Nanosecond,
		Observer: SQLQueryObserverFunc(func(event SQLQueryEvent) {
			events = append(events, event)
		}),
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	if result.QueryID != "query-observe-7" {
		t.Fatalf("result query ID = %q", result.QueryID)
	}
	if len(events) != 1 {
		t.Fatalf("observability events = %#v, want one", events)
	}
	event := events[0]
	if event.QueryID != result.QueryID || !event.OK || !event.Slow || event.ElapsedNanos < 0 || event.OutputRows != 1 || event.OutputColumns != 1 || event.ResultBytes <= 0 {
		t.Fatalf("observability event = %#v", event)
	}
}

func TestExecuteSQLQueryObservabilityRecordsCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var events []SQLQueryEvent
	result, err := ExecuteSQLQueryParameters(ctx, "FROM VALUES (7) AS v(value) SELECT value", SQLSourceResolverFunc(nil), nil, SQLQueryOptions{
		QueryID: "query-cancelled",
		Observer: SQLQueryObserverFunc(func(event SQLQueryEvent) {
			events = append(events, event)
		}),
	})
	if err != context.Canceled {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v, want context cancellation", err)
	}
	if result.QueryID != "query-cancelled" || len(events) != 1 {
		t.Fatalf("result/events = %#v/%#v", result, events)
	}
	event := events[0]
	if event.OK || !event.Canceled || event.CancellationReason != context.Canceled.Error() || event.Error != context.Canceled.Error() {
		t.Fatalf("cancellation event = %#v", event)
	}
}

func TestExecuteSQLQueryRejectsNegativeSlowQueryThreshold(t *testing.T) {
	t.Parallel()
	_, err := ExecuteSQLQueryParameters(context.Background(), "FROM VALUES (7) AS v(value) SELECT value", SQLSourceResolverFunc(nil), nil, SQLQueryOptions{SlowQueryThreshold: -time.Nanosecond})
	if err == nil || !strings.Contains(err.Error(), "budgets cannot be negative") {
		t.Fatalf("negative slow query threshold error = %v", err)
	}
}

func TestExecuteSQLQueryPageEmitsStructuredObservability(t *testing.T) {
	t.Parallel()
	var events []SQLQueryEvent
	result, err := ExecuteSQLQueryPage(context.Background(), "FROM VALUES (1), (2) AS v(value) SELECT value ORDER BY value", SQLSourceResolverFunc(nil), nil, SQLQueryOptions{
		QueryID: "query-page",
		Observer: SQLQueryObserverFunc(func(event SQLQueryEvent) {
			events = append(events, event)
		}),
	}, 1, "")
	if err != nil || result.QueryID != "query-page" || len(result.Rows) != 1 || len(events) != 1 {
		t.Fatalf("page result/events = %#v/%#v/%v", result, events, err)
	}
	if event := events[0]; event.QueryID != result.QueryID || !event.OK || event.OutputRows != 1 || event.ResultBytes <= 0 {
		t.Fatalf("page event = %#v", event)
	}
}

func TestExecuteSQLQueryWithLeftJoinGroupingHavingAndOrder(t *testing.T) {
	t.Parallel()

	source := `
WITH users (id, team_id, name) AS (
  VALUES (1, 10, 'Ivi'), (2, 10, 'Lia'), (3, 20, 'No team')
), teams (id, name) AS (
  VALUES (10, 'Core')
)
SELECT t.name AS team, COUNT(*) AS members
FROM users AS u
LEFT JOIN teams AS t ON u.team_id = t.id
WHERE u.name IS NOT NULL
GROUP BY t.name
HAVING COUNT(*) > 0
ORDER BY members DESC, team ASC`

	result, err := ExecuteSQLQuery(source, SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := SQLQueryResult{
		Columns: []string{"team", "members"},
		Rows: []SQLRow{
			{"team": "Core", "members": int64(2)},
			{"team": nil, "members": int64(1)},
		},
	}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("ExecuteSQLQuery() = %#v, want %#v", result, want)
	}
}

func TestExecuteSQLQuerySupportsCacheSourcesCrossJoinAndPredicates(t *testing.T) {
	t.Parallel()

	resolver := SQLSourceResolverFunc(func(name string, key string) ([]SQLRow, error) {
		if name != "CACHE" {
			return nil, nil
		}
		switch key {
		case "users":
			return []SQLRow{{"id": int64(1), "name": "Ivi"}, {"id": int64(2), "name": "Lia"}}, nil
		case "regions":
			return []SQLRow{{"region": "apac"}, {"region": "eu"}}, nil
		default:
			return nil, nil
		}
	})

	result, err := ExecuteSQLQuery(`
SELECT u.name, r.region
FROM CACHE('users') AS u
CROSS JOIN CACHE('regions') AS r
WHERE u.name LIKE 'I%'
ORDER BY r.region DESC`, resolver)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := SQLQueryResult{
		Columns: []string{"name", "region"},
		Rows: []SQLRow{
			{"name": "Ivi", "region": "eu"},
			{"name": "Ivi", "region": "apac"},
		},
	}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("ExecuteSQLQuery() = %#v, want %#v", result, want)
	}
}

func TestExecuteSQLQueryAcceptsSourceFirstClauseOrder(t *testing.T) {
	t.Parallel()

	result, err := ExecuteSQLQuery(`
FROM VALUES (1, 'Ivi'), (2, 'Lia') AS users(id, name)
WHERE id > 1
SELECT name
ORDER BY name`, SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := SQLQueryResult{Columns: []string{"name"}, Rows: []SQLRow{{"name": "Lia"}}}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("ExecuteSQLQuery() = %#v, want %#v", result, want)
	}
}

func TestExecuteSQLQueryReportsJoinSuggestion(t *testing.T) {
	t.Parallel()

	source := "SELECT * FROM VALUES (1) AS a JION VALUES (1) AS b ON a.column1 = b.column1"
	_, err := ExecuteSQLQuery(source, SQLSourceResolverFunc(nil))
	if err == nil {
		t.Fatal("ExecuteSQLQuery() error = nil, want syntax error")
	}
	formatted := FormatSQLDiagnostic(source, err)
	for _, want := range []string{"expected JOIN", "did you mean `JOIN`?", "JION"} {
		if !strings.Contains(formatted, want) {
			t.Fatalf("FormatSQLDiagnostic() = %q, want substring %q", formatted, want)
		}
	}
}
