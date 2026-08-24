package hatriecache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

type sqlStreamingTestResolver struct {
	rows           []SQLRow
	resolveCalls   int
	streamCalls    int
	indexRows      []SQLRow
	indexRowsByKey map[string][]SQLRow
	indexCalls     int
}

type sqlSelectiveIndexTestResolver struct {
	rows        []SQLRow
	calls       []string
	unavailable map[string]bool
}

type sqlSnapshotStreamingTestResolver struct {
	locked, released bool
}

func (resolver *sqlSnapshotStreamingTestResolver) LockSQLSnapshot() func() {
	resolver.locked = true
	return func() { resolver.released = true }
}

func (resolver *sqlSnapshotStreamingTestResolver) ResolveSQLSource(string, string) ([]SQLRow, error) {
	return nil, errors.New("streaming resolver must not materialize its source")
}

func (resolver *sqlSnapshotStreamingTestResolver) StreamSQLSource(_ context.Context, _ string, _ string, visit func(SQLRow) error) error {
	if !resolver.locked {
		return errors.New("streaming source was read without its snapshot lock")
	}
	return visit(SQLRow{"id": int64(1)})
}

func TestExecuteSQLQueryRowsHoldsAndReleasesSnapshot(t *testing.T) {
	t.Parallel()
	resolver := &sqlSnapshotStreamingTestResolver{}
	err := ExecuteSQLQueryRows(context.Background(), "FROM CACHE('people') AS person SELECT person.id", resolver, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		if row["id"] != int64(1) {
			return fmt.Errorf("row = %#v, want id 1", row)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("streaming snapshot query: %v", err)
	}
	if !resolver.locked || !resolver.released {
		t.Fatalf("streaming snapshot lifecycle locked=%t released=%t, want both true", resolver.locked, resolver.released)
	}
}

func (resolver *sqlSelectiveIndexTestResolver) ResolveSQLSource(string, string) ([]SQLRow, error) {
	return resolver.rows, nil
}

func (resolver *sqlSelectiveIndexTestResolver) ResolveSQLIndexedSource(_ string, _ string, field string, value interface{}) ([]SQLRow, bool, error) {
	resolver.calls = append(resolver.calls, field)
	if resolver.unavailable[field] {
		return nil, false, nil
	}
	rows := make([]SQLRow, 0, len(resolver.rows))
	for _, row := range resolver.rows {
		if reflect.DeepEqual(row[field], value) {
			rows = append(rows, row)
		}
	}
	return rows, true, nil
}

func (resolver *sqlSelectiveIndexTestResolver) SQLJSONIndexStats(_ string, fields ...string) (SQLJSONIndexStats, bool, error) {
	if len(fields) != 1 {
		return SQLJSONIndexStats{}, false, nil
	}
	field := fields[0]
	switch field {
	case "kind":
		return SQLJSONIndexStats{Rows: 100, DistinctKeys: 2}, true, nil
	case "id":
		return SQLJSONIndexStats{Rows: 100, DistinctKeys: 100}, true, nil
	default:
		return SQLJSONIndexStats{}, false, nil
	}
}

func TestExecuteSQLQueryChoosesMostSelectiveIndexedConjunct(t *testing.T) {
	t.Parallel()
	resolver := &sqlSelectiveIndexTestResolver{rows: []SQLRow{
		{"id": int64(7), "kind": "common"},
		{"id": int64(8), "kind": "common"},
	}}
	result, err := ExecuteSQLQuery("FROM CACHE('events') AS event WHERE event.kind = 'common' AND event.id = 7 SELECT event.id", resolver)
	if err != nil {
		t.Fatalf("selective indexed query: %v", err)
	}
	if want := []SQLRow{{"id": int64(7)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("selective indexed rows = %#v, want %#v", result.Rows, want)
	}
	if want := []string{"id"}; !reflect.DeepEqual(resolver.calls, want) {
		t.Fatalf("index probes = %#v, want most-selective %v", resolver.calls, want)
	}
}

func TestExecuteSQLQueryExplainAnalyzeReportsIndexEstimateError(t *testing.T) {
	t.Parallel()
	resolver := &sqlSelectiveIndexTestResolver{rows: []SQLRow{
		{"id": int64(7), "kind": "common"},
		{"id": int64(8), "kind": "common"},
	}}
	result, err := ExecuteSQLQuery("EXPLAIN ANALYZE FROM CACHE('events') AS event WHERE event.kind = 'common' SELECT event.id", resolver)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range result.Plan {
		if step.Node == "INDEX SCAN" {
			if step.EstimatedRows == nil || *step.EstimatedRows != 50 || step.ActualOutputRows == nil || *step.ActualOutputRows != 2 || step.EstimateErrorRows == nil || *step.EstimateErrorRows != -48 {
				t.Fatalf("index estimate step = %#v, want estimated/actual/error 50/2/-48", step)
			}
			return
		}
	}
	t.Fatalf("EXPLAIN ANALYZE plan = %#v, want INDEX SCAN", result.Plan)
}

func TestExecuteSQLQueryFallsBackWhenMostSelectiveIndexIsUnavailable(t *testing.T) {
	t.Parallel()
	resolver := &sqlSelectiveIndexTestResolver{
		rows:        []SQLRow{{"id": int64(7), "kind": "common"}, {"id": int64(8), "kind": "common"}},
		unavailable: map[string]bool{"id": true},
	}
	result, err := ExecuteSQLQuery("FROM CACHE('events') AS event WHERE event.kind = 'common' AND event.id = 7 SELECT event.id", resolver)
	if err != nil {
		t.Fatalf("fallback indexed query: %v", err)
	}
	if want := []SQLRow{{"id": int64(7)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("fallback indexed rows = %#v, want %#v", result.Rows, want)
	}
	if want := []string{"id", "kind"}; !reflect.DeepEqual(resolver.calls, want) {
		t.Fatalf("fallback index probes = %#v, want %v", resolver.calls, want)
	}
}

func TestExecuteSQLQueryExplainAnalyzeReportsIndexCandidateDecision(t *testing.T) {
	t.Parallel()
	resolver := &sqlSelectiveIndexTestResolver{
		rows:        []SQLRow{{"id": int64(7), "kind": "common"}},
		unavailable: map[string]bool{"id": true},
	}
	result, err := ExecuteSQLQuery("EXPLAIN ANALYZE FROM CACHE('people') AS p WHERE p.kind = 'common' AND p.id = 7 SELECT p.id", resolver)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range result.Plan {
		if step.Node != "INDEX CANDIDATES" {
			continue
		}
		if step.ActualInputRows == nil || *step.ActualInputRows != 2 || step.ActualOutputRows == nil || *step.ActualOutputRows != 1 || !strings.Contains(step.Detail, "p.id = 7 estimated_rows=1 rejected: index unavailable") || !strings.Contains(step.Detail, `p.kind = "common" estimated_rows=50 selected`) {
			t.Fatalf("index candidate plan = %#v, want candidate estimates, rejection, and selection", step)
		}
		return
	}
	t.Fatalf("EXPLAIN ANALYZE plan = %#v, want INDEX CANDIDATES", result.Plan)
}

func (resolver *sqlStreamingTestResolver) ResolveSQLIndexedSource(name, key, field string, value interface{}) ([]SQLRow, bool, error) {
	if name != "CACHE" || field != "id" {
		return nil, false, fmt.Errorf("unexpected index source %s(%q).%s", name, key, field)
	}
	resolver.indexCalls++
	if value == nil {
		return nil, true, nil
	}
	indexedRows := resolver.indexRows
	if resolver.indexRowsByKey != nil {
		var ok bool
		indexedRows, ok = resolver.indexRowsByKey[key]
		if !ok {
			return nil, false, nil
		}
	}
	var rows []SQLRow
	for _, row := range indexedRows {
		if reflect.DeepEqual(row["id"], value) {
			rows = append(rows, row)
		}
	}
	return rows, true, nil
}

func TestExecuteSQLQueryRowsStreamsChainedIndexedJoins(t *testing.T) {
	t.Parallel()
	resolver := &sqlStreamingTestResolver{
		rows: []SQLRow{{"id": int64(1), "team_id": int64(10)}, {"id": int64(2), "team_id": int64(20)}},
		indexRowsByKey: map[string][]SQLRow{
			"teams":  {{"id": int64(10), "group_id": int64(100)}, {"id": int64(20), "group_id": int64(200)}},
			"groups": {{"id": int64(100), "name": "Core"}},
		},
	}
	var got []SQLRow
	err := ExecuteSQLQueryRows(context.Background(), "FROM CACHE('people') AS person INNER JOIN CACHE('teams') AS team ON person.team_id = team.id LEFT JOIN CACHE('groups') AS group ON team.group_id = group.id SELECT person.id, group.name AS group_name", resolver, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		got = append(got, row)
		return nil
	})
	if err != nil {
		t.Fatalf("stream chained indexed joins: %v", err)
	}
	if want := []SQLRow{{"id": int64(1), "group_name": "Core"}, {"id": int64(2), "group_name": nil}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("stream chained indexed joins = %#v, want %#v", got, want)
	}
	if resolver.resolveCalls != 0 || resolver.streamCalls != 1 || resolver.indexCalls != 6 {
		t.Fatalf("resolver calls materialized=%d stream=%d index=%d, want 0/1/6", resolver.resolveCalls, resolver.streamCalls, resolver.indexCalls)
	}
}

func TestExecuteSQLQueryRowsStreamsBoundedTopNOrdering(t *testing.T) {
	t.Parallel()
	query := "FROM VALUES (0, 3), (1, 1), (2, NULL), (3, 2), (4, 1), (5, 0) AS values(id, score) SELECT id, score ORDER BY score ASC NULLS LAST, id LIMIT 3 OFFSET 1"
	want, err := ExecuteSQLQuery(query, SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("materialized baseline: %v", err)
	}
	var got []SQLRow
	err = ExecuteSQLQueryRows(context.Background(), query, SQLSourceResolverFunc(nil), nil, SQLQueryOptions{MaxResultBytes: 1 << 20}, func(_ []string, row SQLRow) error {
		got = append(got, row)
		return nil
	})
	if err != nil {
		t.Fatalf("bounded top-N stream: %v", err)
	}
	if !reflect.DeepEqual(got, want.Rows) {
		t.Fatalf("bounded top-N rows = %#v, want %#v", got, want.Rows)
	}
}

func TestExecuteSQLQueryRowsStreamsBoundedTopNProperty(t *testing.T) {
	t.Parallel()
	random := rand.New(rand.NewSource(20260825))
	for iteration := 0; iteration < 80; iteration++ {
		values := make([]string, 1+random.Intn(20))
		for id := range values {
			score := "NULL"
			if random.Intn(4) != 0 {
				score = strconv.Itoa(random.Intn(7) - 3)
			}
			values[id] = fmt.Sprintf("(%d, %s)", id, score)
		}
		descending := random.Intn(2) == 0
		nullOrder := "NULLS LAST"
		if random.Intn(2) == 0 {
			nullOrder = "NULLS FIRST"
		}
		order := "ASC"
		if descending {
			order = "DESC"
		}
		limit := 1 + random.Intn(8)
		offset := random.Intn(8)
		query := fmt.Sprintf("FROM VALUES %s AS values(id, score) SELECT id, score ORDER BY score %s %s, id LIMIT %d OFFSET %d", strings.Join(values, ", "), order, nullOrder, limit, offset)
		want, err := ExecuteSQLQuery(query, SQLSourceResolverFunc(nil))
		if err != nil {
			t.Fatalf("iteration %d materialized baseline: %v", iteration, err)
		}
		got := []SQLRow{}
		err = ExecuteSQLQueryRows(context.Background(), query, SQLSourceResolverFunc(nil), nil, SQLQueryOptions{MaxResultBytes: 1 << 20}, func(_ []string, row SQLRow) error {
			got = append(got, row)
			return nil
		})
		if err != nil {
			t.Fatalf("iteration %d bounded top-N stream: %v", iteration, err)
		}
		if !reflect.DeepEqual(got, want.Rows) {
			t.Fatalf("iteration %d bounded top-N rows = %#v, want %#v\nquery=%s", iteration, got, want.Rows, query)
		}
	}
}

func TestExecuteSQLQueryRowsStreamsIndexedJoinsHonorMaxRows(t *testing.T) {
	t.Parallel()
	resolver := &sqlStreamingTestResolver{
		rows:      []SQLRow{{"id": int64(1), "team_id": int64(10)}},
		indexRows: []SQLRow{{"id": int64(10), "name": "Core"}, {"id": int64(10), "name": "Backup"}},
	}
	err := ExecuteSQLQueryRows(context.Background(), "FROM CACHE('people') AS person JOIN CACHE('teams') AS team ON person.team_id = team.id SELECT person.id, team.name", resolver, nil, SQLQueryOptions{MaxRows: 1}, func([]string, SQLRow) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "SQL join exceeds the 1 row limit") {
		t.Fatalf("streamed indexed join MaxRows error = %v, want join-row limit", err)
	}
	err = ExecuteSQLQueryRows(context.Background(), "FROM CACHE('people') AS person JOIN CACHE('teams') AS team ON person.team_id = team.id SELECT person.id, team.name", resolver, nil, SQLQueryOptions{MaxJoinWork: 1}, func([]string, SQLRow) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "join work budget exceeded") {
		t.Fatalf("streamed indexed join MaxJoinWork error = %v, want join-work limit", err)
	}
}

func TestExecuteSQLQueryRowsStreamsIndexedInnerAndLeftJoin(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, join string
		want       []SQLRow
	}{
		{"inner", "JOIN", []SQLRow{{"id": int64(1), "team": "Core"}}},
		{"left", "LEFT JOIN", []SQLRow{{"id": int64(1), "team": "Core"}, {"id": int64(2), "team": nil}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolver := &sqlStreamingTestResolver{rows: []SQLRow{{"id": int64(1), "team_id": int64(10)}, {"id": int64(2), "team_id": int64(20)}}, indexRows: []SQLRow{{"id": int64(10), "name": "Core"}}}
			var got []SQLRow
			err := ExecuteSQLQueryRows(context.Background(), "FROM CACHE('people') AS person "+test.join+" CACHE('teams') AS team ON person.team_id = team.id SELECT person.id, team.name AS team", resolver, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
				got = append(got, row)
				return nil
			})
			if err != nil {
				t.Fatalf("stream indexed %s join: %v", test.name, err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("stream indexed %s rows = %#v, want %#v", test.name, got, test.want)
			}
			if resolver.resolveCalls != 0 || resolver.streamCalls != 1 || resolver.indexCalls != 3 {
				t.Fatalf("resolver calls materialized=%d stream=%d index=%d, want 0/1/3", resolver.resolveCalls, resolver.streamCalls, resolver.indexCalls)
			}
		})
	}
	explained, err := ExecuteSQLQuery("EXPLAIN FROM VALUES (1) AS values(value) WHERE value IN (1, 2) SELECT value", SQLSourceResolverFunc(nil))
	if err != nil || len(explained.Plan) < 2 || explained.Plan[1].Detail != "value IN (1, 2)" {
		t.Fatalf("IN EXPLAIN = %#v/%v, want readable membership filter", explained.Plan, err)
	}
}

func (resolver *sqlStreamingTestResolver) ResolveSQLSource(string, string) ([]SQLRow, error) {
	resolver.resolveCalls++
	return nil, errors.New("materialized source resolution must not be used for a streamed query")
}

func (resolver *sqlStreamingTestResolver) StreamSQLSource(ctx context.Context, name string, key string, visit func(SQLRow) error) error {
	if name != "CACHE" || key != "people" {
		return fmt.Errorf("stream source = %s(%q), want CACHE(people)", name, key)
	}
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

// These cases are the compact regression suite for the subset documented in
// SQL.md. They deliberately cover semantics, not only successful parsing.
func TestCompileSQLProductionScalarMatrix(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		want   CacheCommandRequest
	}{
		{"SELECT exists FROM cache WHERE key = 'a'", CacheCommandRequest{Command: "EXISTS", Key: "a"}},
		{"SELECT ttl FROM cache WHERE key = 'a'", CacheCommandRequest{Command: "TTL", Key: "a"}},
		{"SELECT dump FROM cache WHERE key = 'a'", CacheCommandRequest{Command: "DUMP", Key: "a"}},
		{"INSERT INTO cache (key, counter, ttl_seconds) VALUES ('a', -7, 4)", CacheCommandRequest{Command: "SETINTX", Key: "a", Value: "-7", TTLSeconds: int64Pointer(4)}},
		{"INSERT INTO cache (key, value, unix_seconds) VALUES ('a', 'v', 99)", CacheCommandRequest{Command: "SETSTR", Key: "a", Value: "v", UnixSeconds: int64Pointer(99)}},
		{"UPDATE cache SET ttl_seconds = 4 WHERE key = 'a'", CacheCommandRequest{Command: "EXPIRE", Key: "a", TTLSeconds: int64Pointer(4)}},
		{"UPDATE cache SET unix_seconds = 99 WHERE key = 'a'", CacheCommandRequest{Command: "EXPIREAT", Key: "a", UnixSeconds: int64Pointer(99)}},
		{"CALL GET('a')", CacheCommandRequest{Command: "GET", Key: "a"}},
		{"CALL SETSTR('a', 'v')", CacheCommandRequest{Command: "SETSTR", Key: "a", Value: "v"}},
	} {
		got, err := CompileSQL(test.source)
		if err != nil {
			t.Fatalf("CompileSQL(%q) error = %v", test.source, err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Fatalf("CompileSQL(%q) = %#v, want %#v", test.source, got, test.want)
		}
	}
}

func TestExecuteSQLQueryRowsStreamsCompatibleSourceRows(t *testing.T) {
	t.Parallel()
	resolver := &sqlStreamingTestResolver{rows: []SQLRow{
		{"name": "Ari", "age": int64(12)},
		{"name": "Bea", "age": int64(21)},
		{"name": "Cai", "age": int64(34)},
		{"name": "Dee", "age": int64(45)},
	}}
	var columns []string
	var rows []SQLRow
	err := ExecuteSQLQueryRows(context.Background(), "FROM CACHE('people') AS p WHERE p.age >= 21 SELECT p.name, p.age + 1 AS next_age OFFSET 1 LIMIT 2", resolver, nil, SQLQueryOptions{}, func(gotColumns []string, row SQLRow) error {
		columns = append([]string(nil), gotColumns...)
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if resolver.resolveCalls != 0 || resolver.streamCalls != 1 {
		t.Fatalf("resolver calls = materialized:%d streamed:%d, want 0/1", resolver.resolveCalls, resolver.streamCalls)
	}
	if want := []string{"name", "next_age"}; !reflect.DeepEqual(columns, want) {
		t.Fatalf("columns = %#v, want %#v", columns, want)
	}
	if want := []SQLRow{{"name": "Cai", "next_age": int64(35)}, {"name": "Dee", "next_age": int64(46)}}; !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %#v, want %#v", rows, want)
	}
	if err := ExecuteSQLQueryRows(context.Background(), "FROM CACHE('people') AS p SELECT p.name ORDER BY p.name", resolver, nil, SQLQueryOptions{}, func([]string, SQLRow) error { return nil }); err == nil || !strings.Contains(err.Error(), "cannot stream") {
		t.Fatalf("ordered ExecuteSQLQueryRows() error = %v, want a streamability error", err)
	}
}

func TestExecuteSQLQueryRowsStreamsGlobalAggregates(t *testing.T) {
	t.Parallel()
	rows := []SQLRow{{"age": int64(12)}, {"age": int64(21)}, {"age": nil}, {"age": int64(34)}}
	query := "FROM CACHE('people') AS person WHERE person.age >= 20 SELECT COUNT(*) AS total, SUM(person.age) AS sum_age, AVG(person.age) AS average_age, MIN(person.age) AS min_age, MAX(person.age) AS max_age"
	materialized, err := ExecuteSQLQuery(query, SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) { return cloneSQLRows(rows), nil }))
	if err != nil {
		t.Fatal(err)
	}
	resolver := &sqlStreamingTestResolver{rows: cloneSQLRows(rows)}
	var columns []string
	var got []SQLRow
	err = ExecuteSQLQueryRows(context.Background(), query, resolver, nil, SQLQueryOptions{}, func(gotColumns []string, row SQLRow) error {
		columns = append([]string(nil), gotColumns...)
		got = append(got, row)
		return nil
	})
	if err != nil {
		t.Fatalf("stream global aggregates: %v", err)
	}
	if resolver.resolveCalls != 0 || resolver.streamCalls != 1 {
		t.Fatalf("aggregate resolver calls = materialized:%d streamed:%d, want 0/1", resolver.resolveCalls, resolver.streamCalls)
	}
	if !reflect.DeepEqual(columns, materialized.Columns) || !reflect.DeepEqual(got, materialized.Rows) {
		t.Fatalf("stream global aggregate = %#v/%#v, want %#v/%#v", columns, got, materialized.Columns, materialized.Rows)
	}
}

func TestExecuteSQLQueryRowsStreamsGlobalAggregatesEmptyInput(t *testing.T) {
	t.Parallel()
	rows := []SQLRow{{"age": nil}, {"age": int64(12)}}
	query := "FROM CACHE('people') AS person WHERE person.age > 99 SELECT COUNT(*) AS total, COUNT(person.age) AS numeric_count, SUM(person.age) AS sum_age, AVG(person.age) AS average_age, MIN(person.age) AS min_age, MAX(person.age) AS max_age"
	materialized, err := ExecuteSQLQuery(query, SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) { return cloneSQLRows(rows), nil }))
	if err != nil {
		t.Fatal(err)
	}
	resolver := &sqlStreamingTestResolver{rows: cloneSQLRows(rows)}
	var got []SQLRow
	err = ExecuteSQLQueryRows(context.Background(), query, resolver, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		got = append(got, row)
		return nil
	})
	if err != nil {
		t.Fatalf("stream empty global aggregates: %v", err)
	}
	if !reflect.DeepEqual(got, materialized.Rows) {
		t.Fatalf("stream empty global aggregate = %#v, want %#v", got, materialized.Rows)
	}
}

func TestSQLPreparedQueryCacheReusesImmutableTemplateAndBindsFreshParameters(t *testing.T) {
	t.Parallel()
	cache := NewSQLPreparedQueryCache(2)
	resolver := SQLSourceResolverFunc(func(name string, key string) ([]SQLRow, error) {
		if name != "CACHE" || key != "people" {
			return nil, fmt.Errorf("source = %s(%q), want CACHE(people)", name, key)
		}
		return []SQLRow{{"id": int64(1)}, {"id": int64(2)}, {"id": int64(3)}}, nil
	})
	query := "FROM CACHE($1) AS people WHERE people.id >= $2 SELECT people.id"
	options := SQLQueryOptions{PreparedCache: cache}
	first, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, []interface{}{"people", int64(2)}, options)
	if err != nil || !reflect.DeepEqual(first.Rows, []SQLRow{{"id": int64(2)}, {"id": int64(3)}}) {
		t.Fatalf("first cached query = %#v/%v", first, err)
	}
	second, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, []interface{}{"people", int64(3)}, options)
	if err != nil || !reflect.DeepEqual(second.Rows, []SQLRow{{"id": int64(3)}}) {
		t.Fatalf("second cached query = %#v/%v; cached template must not retain first parameters", second, err)
	}
	if stats := cache.Stats(); stats.Entries != 1 || stats.Misses != 1 || stats.Hits != 1 {
		t.Fatalf("cache stats = %#v, want one entry, one miss, and one hit", stats)
	}
	if _, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, []interface{}{"people"}, options); err == nil || !strings.Contains(err.Error(), "parameter $2 has no supplied parameter") {
		t.Fatalf("missing cached parameter error = %v, want precise parameter diagnostic", err)
	}
}

func TestSQLPreparedQueryCacheBindsValuesAndNestedQueryParameters(t *testing.T) {
	t.Parallel()
	cache := NewSQLPreparedQueryCache(2)
	query := "FROM (FROM VALUES ($1), ($2) AS input(id) WHERE input.id >= $3 SELECT input.id) AS nested SELECT nested.id"
	result, err := ExecuteSQLQueryParameters(context.Background(), query, SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return nil, fmt.Errorf("resolver must not be called for VALUES")
	}), []interface{}{int64(1), int64(3), int64(2)}, SQLQueryOptions{PreparedCache: cache})
	if err != nil {
		t.Fatalf("execute first values query: %v", err)
	}
	if got, want := result.Rows, []SQLRow{{"id": int64(3)}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("first values rows = %#v, want %#v", got, want)
	}
	result, err = ExecuteSQLQueryParameters(context.Background(), query, SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return nil, fmt.Errorf("resolver must not be called for VALUES")
	}), []interface{}{int64(4), int64(5), int64(4)}, SQLQueryOptions{PreparedCache: cache})
	if err != nil {
		t.Fatalf("execute second values query: %v", err)
	}
	if got, want := result.Rows, []SQLRow{{"id": int64(4)}, {"id": int64(5)}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("second values rows = %#v, want %#v", got, want)
	}
}

func TestSQLPreparedQueryCacheIsBoundedAndSafeForConcurrentBindings(t *testing.T) {
	t.Parallel()
	cache := NewSQLPreparedQueryCache(1)
	first := "FROM VALUES (1) AS first_row(id) SELECT first_row.id"
	second := "FROM VALUES (2) AS second_row(id) SELECT second_row.id"
	if _, err := parseSQLQueryWithCache(first, nil, cache); err != nil {
		t.Fatalf("parse first query: %v", err)
	}
	if _, err := parseSQLQueryWithCache(second, nil, cache); err != nil {
		t.Fatalf("parse second query: %v", err)
	}
	if _, err := parseSQLQueryWithCache(first, nil, cache); err != nil {
		t.Fatalf("parse evicted first query: %v", err)
	}
	if got, want := cache.Stats(), (SQLPreparedQueryCacheStats{Entries: 1, Misses: 3}); got != want {
		t.Fatalf("bounded cache stats = %#v, want %#v", got, want)
	}

	cache = NewSQLPreparedQueryCache(2)
	resolver := SQLSourceResolverFunc(func(name, key string) ([]SQLRow, error) {
		if name != "CACHE" || key != "people" {
			return nil, fmt.Errorf("unexpected source %s(%q)", name, key)
		}
		return []SQLRow{{"id": int64(1)}, {"id": int64(2)}, {"id": int64(3)}}, nil
	})
	query := "FROM CACHE('people') AS person WHERE person.id >= $1 SELECT person.id"
	errors := make(chan error, 32)
	for minimum := int64(1); minimum <= 32; minimum++ {
		minimum := minimum
		go func() {
			result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, []interface{}{minimum}, SQLQueryOptions{PreparedCache: cache})
			if err != nil {
				errors <- err
				return
			}
			expected := []SQLRow{}
			for id := minimum; id <= 3; id++ {
				expected = append(expected, SQLRow{"id": id})
			}
			if !reflect.DeepEqual(result.Rows, expected) {
				errors <- fmt.Errorf("minimum %d rows = %#v, want %#v", minimum, result.Rows, expected)
				return
			}
			errors <- nil
		}()
	}
	for count := 0; count < cap(errors); count++ {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
	if stats := cache.Stats(); stats.Entries != 1 || stats.Hits != 31 || stats.Misses != 1 {
		t.Fatalf("concurrent cache stats = %#v, want one miss and 31 hits", stats)
	}
}

func TestSQLPreparedQueryCacheEvictsLeastRecentlyUsedTemplate(t *testing.T) {
	t.Parallel()
	cache := NewSQLPreparedQueryCache(2)
	first := "FROM VALUES (1) AS first_row(id) SELECT first_row.id"
	second := "FROM VALUES (2) AS second_row(id) SELECT second_row.id"
	third := "FROM VALUES (3) AS third_row(id) SELECT third_row.id"
	for _, query := range []string{first, second, first, third, first} {
		if _, err := parseSQLQueryWithCache(query, nil, cache); err != nil {
			t.Fatalf("parse cached query %q: %v", query, err)
		}
	}
	if got, want := cache.Stats(), (SQLPreparedQueryCacheStats{Entries: 2, Hits: 2, Misses: 3}); got != want {
		t.Fatalf("LRU cache stats = %#v, want %#v", got, want)
	}
}

func TestExecuteSQLQueryReordersConnectedInnerHashJoinsByCardinality(t *testing.T) {
	t.Parallel()
	resolver := SQLSourceResolverFunc(func(name, key string) ([]SQLRow, error) {
		if name != "CACHE" {
			return nil, fmt.Errorf("unexpected source %s", name)
		}
		switch key {
		case "orders":
			return []SQLRow{
				{"id": int64(1), "member_id": int64(10)},
				{"id": int64(2), "member_id": int64(20)},
				{"id": int64(3), "member_id": int64(10)},
			}, nil
		case "members":
			return []SQLRow{
				{"id": int64(10), "group_id": int64(1), "name": "Ada"},
				{"id": int64(20), "group_id": int64(1), "name": "Ivi"},
			}, nil
		case "groups":
			return []SQLRow{{"id": int64(1), "name": "Core"}}, nil
		default:
			return nil, fmt.Errorf("unexpected cache key %q", key)
		}
	})
	query := "FROM CACHE('orders') AS orders JOIN CACHE('members') AS members ON orders.member_id = members.id JOIN CACHE('groups') AS groups ON members.group_id = groups.id SELECT orders.id, members.name AS member, groups.name AS group_name"
	result, err := ExecuteSQLQuery(query, resolver)
	if err != nil {
		t.Fatalf("execute reordered join: %v", err)
	}
	if got, want := result.Rows, []SQLRow{
		{"id": int64(1), "member": "Ada", "group_name": "Core"},
		{"id": int64(2), "member": "Ivi", "group_name": "Core"},
		{"id": int64(3), "member": "Ada", "group_name": "Core"},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("reordered rows = %#v, want %#v", got, want)
	}
	analysis, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, resolver)
	if err != nil {
		t.Fatalf("explain reordered join: %v", err)
	}
	for _, step := range analysis.Plan {
		if step.Node == "JOIN REORDER" && strings.Contains(step.Detail, "groups") && strings.Contains(step.Detail, "1 row") {
			return
		}
	}
	t.Fatalf("EXPLAIN ANALYZE plan = %#v, want JOIN REORDER beginning with the smallest source", analysis.Plan)
}

func TestCompileSQLProductionRejectsAmbiguousOrUnsafeForms(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		"INSERT INTO cache (key, value, counter) VALUES ('a', 'v', 1)",
		"INSERT INTO cache (key, value, ttl_seconds, unix_seconds) VALUES ('a', 'v', 1, 2)",
		"CALL SETSTR(key => 'a', value => 'v', key => 'again')",
		"CALL SETSTR('a', value => 'v')",
		"CALL SETSTR(key => 'a', values => JSON '[1]')",
		"CALL BATCH(key => 'a')",
	} {
		if _, err := CompileSQL(source); err == nil {
			t.Fatalf("CompileSQL(%q) error = nil, want rejection", source)
		}
	}
}

func TestCompileSQLDottedCollectionAliases(t *testing.T) {
	t.Parallel()
	for source, want := range map[string]string{
		"CALL CMS.CREATE(key => 'frequency', value => 1024, subkey => 4)": "CMS.CREATE",
		"CALL CMS.ADD(key => 'frequency', value => 'home')":               "CMS.ADD",
		"CALL TOPK.CREATE(key => 'popular', value => 100)":                "TOPK.CREATE",
		"CALL BF.CREATE(key => 'seen', value => 1000)":                    "BF.CREATE",
		"CALL RT.PUT(key => 'index', subkey => 'a', value => 'Ada')":      "RT.PUT",
		"CALL FW.RANGE(key => 'scores', value => 2, subkey => 8)":         "FW.RANGE",
	} {
		request, err := CompileSQL(source)
		if err != nil {
			t.Fatalf("CompileSQL(%q) error = %v", source, err)
		}
		if request.Command != want {
			t.Fatalf("CompileSQL(%q) command = %q, want %q", source, request.Command, want)
		}
	}
	for alias := range dottedCommandAliases {
		request, err := CompileSQL("CALL " + alias + "(key => 'x')")
		if err != nil {
			t.Fatalf("CompileSQL() did not accept dotted alias %q: %v", alias, err)
		}
		if request.Command != alias {
			t.Fatalf("CompileSQL() command for dotted alias %q = %q", alias, request.Command)
		}
	}
}

func TestDottedCollectionAliasesNormalizeToExistingCommands(t *testing.T) {
	t.Parallel()
	for alias, want := range dottedCommandAliases {
		if got := normalizedCommand(alias); got != want {
			t.Fatalf("normalizedCommand(%q) = %q, want %q", alias, got, want)
		}
	}
	for alias, want := range map[string]string{
		"CMS.CREATE": "CREATECMS", "CMS.ADD": "ADDCMS", "TOPK.CREATE": "CREATETOPK", "TOPK.ADD": "ADDTOPK",
		"BF.CREATE": "CREATEBF", "RT.PUT": "PUTRT", "FW.RANGE": "RANGEFW", "SET.ADD": "ADDSET",
	} {
		if got := normalizedCommand(alias); got != want {
			t.Fatalf("normalizedCommand(%q) = %q, want %q", alias, got, want)
		}
	}
}

func TestExecuteSQLQueryUsesStandardBooleanPrecedence(t *testing.T) {
	t.Parallel()

	result, err := ExecuteSQLQuery(`
FROM VALUES (1, FALSE, FALSE), (2, TRUE, FALSE), (3, FALSE, TRUE) AS values(id, b, c)
WHERE id = 1 OR b = TRUE AND c = TRUE
SELECT id
ORDER BY id`, SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := SQLQueryResult{Columns: []string{"id"}, Rows: []SQLRow{{"id": int64(1)}}}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("standard AND-before-OR result = %#v, want %#v", result, want)
	}
}

func TestExecuteSQLQuerySupportsNotAndDistinct(t *testing.T) {
	t.Parallel()
	result, err := ExecuteSQLQuery(`
FROM VALUES ('a', TRUE), ('a', TRUE), ('b', FALSE), ('c', FALSE) AS values(label, disabled)
WHERE NOT disabled
SELECT DISTINCT label
ORDER BY label DESC`, SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := SQLQueryResult{Columns: []string{"label"}, Rows: []SQLRow{{"label": "c"}, {"label": "b"}}}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("NOT/DISTINCT result = %#v, want %#v", result, want)
	}
}

func TestExecuteSQLQuerySupportsRightAndFullOuterJoin(t *testing.T) {
	t.Parallel()
	resolver := SQLSourceResolverFunc(func(name, key string) ([]SQLRow, error) {
		switch key {
		case "left":
			return []SQLRow{{"id": int64(1), "left_value": "a"}, {"id": int64(2), "left_value": "b"}}, nil
		case "right":
			return []SQLRow{{"id": int64(2), "right_value": "x"}, {"id": int64(3), "right_value": "y"}}, nil
		}
		return nil, nil
	})
	result, err := ExecuteSQLQuery(`
FROM CACHE('left') AS l
FULL OUTER JOIN CACHE('right') AS r ON l.id = r.id
SELECT l.id AS left_id, r.id AS right_id, l.left_value, r.right_value
ORDER BY right_id`, resolver)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery(FULL OUTER JOIN) error = %v", err)
	}
	want := SQLQueryResult{Columns: []string{"left_id", "right_id", "left_value", "right_value"}, Rows: []SQLRow{
		{"left_id": int64(1), "right_id": nil, "left_value": "a", "right_value": nil},
		{"left_id": int64(2), "right_id": int64(2), "left_value": "b", "right_value": "x"},
		{"left_id": nil, "right_id": int64(3), "left_value": nil, "right_value": "y"},
	}}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("FULL OUTER JOIN result = %#v, want %#v", result, want)
	}
	right, err := ExecuteSQLQuery(`FROM CACHE('left') AS l RIGHT JOIN CACHE('right') AS r ON l.id = r.id SELECT r.id ORDER BY r.id`, resolver)
	if err != nil || !reflect.DeepEqual(right.Rows, []SQLRow{{"id": int64(2)}, {"id": int64(3)}}) {
		t.Fatalf("RIGHT JOIN result = %#v, %v", right, err)
	}
}

func TestExecuteSQLQueryUsesHashJoinForEqualityOuterJoins(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, join string
		want       []SQLRow
	}{
		{"left", "LEFT JOIN", []SQLRow{{"left_id": int64(1), "right_id": nil}, {"left_id": int64(2), "right_id": int64(2)}}},
		{"right", "RIGHT JOIN", []SQLRow{{"left_id": int64(2), "right_id": int64(2)}, {"left_id": nil, "right_id": int64(3)}}},
		{"full", "FULL JOIN", []SQLRow{{"left_id": int64(1), "right_id": nil}, {"left_id": int64(2), "right_id": int64(2)}, {"left_id": nil, "right_id": int64(3)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			query := "FROM VALUES (1), (2) AS left_side(id) " + test.join + " VALUES (2), (3) AS right_side(id) ON left_side.id = right_side.id SELECT left_side.id AS left_id, right_side.id AS right_id"
			result, err := ExecuteSQLQuery(query, SQLSourceResolverFunc(nil))
			if err != nil {
				t.Fatalf("%s outer join error = %v", test.name, err)
			}
			if !reflect.DeepEqual(result.Rows, test.want) {
				t.Fatalf("%s outer join rows = %#v, want %#v", test.name, result.Rows, test.want)
			}
			explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, SQLSourceResolverFunc(nil))
			if err != nil || explained.Stats == nil {
				t.Fatalf("%s outer join explain/error/stats = %#v/%v/%#v", test.name, explained.Plan, err, explained.Stats)
			}
			for _, step := range explained.Plan {
				if step.Node == "HASH JOIN" {
					return
				}
			}
			t.Fatalf("%s outer join plan = %#v, want HASH JOIN", test.name, explained.Plan)
		})
	}
	explained, err := ExecuteSQLQuery("EXPLAIN FROM VALUES (2) AS values(value) WHERE value BETWEEN 1 AND 3 SELECT value", SQLSourceResolverFunc(nil))
	if err != nil || len(explained.Plan) < 2 || explained.Plan[1].Detail != "value BETWEEN 1 AND 3" {
		t.Fatalf("BETWEEN EXPLAIN = %#v/%v, want readable range filter", explained.Plan, err)
	}
}

func TestExecuteSQLQueryJoinsMultipleSourceKindsInOnePipeline(t *testing.T) {
	t.Parallel()
	resolver := SQLSourceResolverFunc(func(name, key string) ([]SQLRow, error) {
		switch key {
		case "users":
			return []SQLRow{
				{"id": int64(1), "team_id": int64(10), "region_id": int64(100), "enabled": true},
				{"id": int64(2), "team_id": int64(20), "region_id": int64(200), "enabled": true},
				{"id": int64(3), "team_id": int64(30), "region_id": int64(100), "enabled": true},
				{"id": int64(4), "team_id": int64(10), "region_id": int64(100), "enabled": false},
			}, nil
		case "regions":
			return []SQLRow{
				{"id": int64(100), "name": "APAC", "enabled": true},
				{"id": int64(200), "name": "Europe", "enabled": false},
			}, nil
		default:
			return nil, fmt.Errorf("unexpected source %s(%q)", name, key)
		}
	})

	result, err := ExecuteSQLQuery(`
WITH enabled_teams(id, name) AS (VALUES (10, 'Core'), (20, 'Edge'))
FROM CACHE('users') AS u
INNER JOIN enabled_teams AS t ON u.team_id = t.id
LEFT JOIN (
  FROM CACHE('regions') AS source
  WHERE source.enabled = TRUE
  SELECT source.id, source.name AS region
) AS r ON u.region_id = r.id
CROSS JOIN VALUES ('production') AS environment(name)
WHERE u.enabled = TRUE
SELECT u.id, t.name AS team, r.region, environment.name AS environment
ORDER BY u.id`, resolver)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery(multiple source joins) error = %v", err)
	}
	want := SQLQueryResult{Columns: []string{"id", "team", "region", "environment"}, Rows: []SQLRow{
		{"id": int64(1), "team": "Core", "region": "APAC", "environment": "production"},
		{"id": int64(2), "team": "Edge", "region": nil, "environment": "production"},
	}}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("multiple source join result = %#v, want %#v", result, want)
	}
}

func TestExecuteSQLQueryPushesBaseFilterIntoHashJoin(t *testing.T) {
	t.Parallel()
	result, err := ExecuteSQLQuery(`
EXPLAIN ANALYZE
FROM VALUES (1), (2), (3) AS left_values(id)
INNER JOIN VALUES (2), (3) AS right_values(id) ON left_values.id = right_values.id
WHERE left_values.id = 2
SELECT left_values.id`, SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if want := []SQLRow{{"id": int64(2)}}; result.Stats == nil || result.Stats.OutputRows != len(want) {
		t.Fatalf("EXPLAIN ANALYZE stats = %#v, want %#v", result.Stats, want)
	}
	var filterIndex, joinIndex = -1, -1
	for index, step := range result.Plan {
		switch step.Node {
		case "FILTER":
			if step.ActualInputRows != nil && step.ActualOutputRows != nil && *step.ActualInputRows == 3 && *step.ActualOutputRows == 1 {
				filterIndex = index
			}
		case "HASH JOIN":
			if step.ActualInputRows != nil && step.ActualOutputRows != nil && *step.ActualInputRows == 3 && *step.ActualOutputRows == 1 {
				joinIndex = index
			}
		}
	}
	if filterIndex < 0 || joinIndex < 0 || filterIndex >= joinIndex {
		t.Fatalf("plan = %#v, want base filter 3→1 before hash join 3→1", result.Plan)
	}
}

func TestExecuteSQLQueryContextEnforcesBudgetsAndCancellation(t *testing.T) {
	t.Parallel()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := ExecuteSQLQueryContext(cancelled, "FROM VALUES (1) AS values(id) SELECT id", SQLSourceResolverFunc(nil), SQLQueryOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled query error = %v, want context.Canceled", err)
	}

	for _, test := range []struct {
		name, source string
		options      SQLQueryOptions
		want         string
	}{
		{
			name:    "join_work",
			source:  "FROM VALUES (1), (2), (3) AS left_values(id) CROSS JOIN VALUES (1), (2), (3) AS right_values(id) SELECT left_values.id",
			options: SQLQueryOptions{MaxJoinWork: 4},
			want:    "join work budget",
		},
		{
			name:    "result_bytes",
			source:  "FROM VALUES ('this row is deliberately longer than the budget') AS values(value) SELECT value",
			options: SQLQueryOptions{MaxResultBytes: 8},
			want:    "result byte budget",
		},
		{
			name:    "sort_bytes",
			source:  "FROM VALUES ('long value'), ('another long value') AS values(value) SELECT value ORDER BY value",
			options: SQLQueryOptions{MaxSortBytes: 8},
			want:    "sort memory budget",
		},
		{
			name:    "group_bytes",
			source:  "FROM VALUES ('long value'), ('another long value') AS values(value) GROUP BY value SELECT value",
			options: SQLQueryOptions{MaxGroupBytes: 8},
			want:    "group memory budget",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ExecuteSQLQueryContext(context.Background(), test.source, SQLSourceResolverFunc(nil), test.options)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ExecuteSQLQueryContext() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestExecuteSQLQueryObserverIncludesSafeOperatorCounters(t *testing.T) {
	t.Parallel()
	var events []SQLQueryEvent
	result, err := ExecuteSQLQueryContext(context.Background(), "FROM VALUES (1), (2), (3) AS values(id) WHERE id > 1 SELECT id ORDER BY id", SQLSourceResolverFunc(nil), SQLQueryOptions{
		QueryID: "operators-1",
		Observer: SQLQueryObserverFunc(func(event SQLQueryEvent) {
			events = append(events, event)
		}),
	})
	if err != nil {
		t.Fatalf("observed query: %v", err)
	}
	if len(result.Rows) != 2 || len(events) != 1 {
		t.Fatalf("result/events = %#v/%#v, want two rows/one event", result.Rows, events)
	}
	if events[0].QueryID != "operators-1" || !events[0].OK {
		t.Fatalf("event = %#v, want successful named event", events[0])
	}
	operators := map[string]SQLQueryOperator{}
	for _, operator := range events[0].Operators {
		operators[operator.Node] = operator
	}
	for _, node := range []string{"SCAN", "FILTER", "PROJECT", "SORT"} {
		operator, ok := operators[node]
		if !ok || operator.InputRows < 0 || operator.OutputRows < 0 || operator.ElapsedNanos < 0 {
			t.Fatalf("event operators = %#v, want safe %s counter", events[0].Operators, node)
		}
	}
}

func TestExecuteSQLQueryContextSpillsExternalSortWithinDiskBudget(t *testing.T) {
	t.Parallel()
	query := "FROM VALUES (3, DATE '2026-08-03'), (1, DATE '2026-08-01'), (2, DATE '2026-08-02'), (4, DATE '2026-08-04') AS values(id, occurred_on) SELECT id, occurred_on, CAST(id AS DECIMAL) AS amount ORDER BY occurred_on DESC, id"
	want, err := ExecuteSQLQueryContext(context.Background(), query, SQLSourceResolverFunc(nil), SQLQueryOptions{})
	if err != nil {
		t.Fatalf("unbounded baseline: %v", err)
	}
	directory := t.TempDir()
	got, err := ExecuteSQLQueryContext(context.Background(), query, SQLSourceResolverFunc(nil), SQLQueryOptions{
		MaxSortBytes:   64,
		SpillDirectory: directory,
		MaxSpillBytes:  1 << 20,
	})
	if err != nil {
		t.Fatalf("spill sort: %v", err)
	}
	if !reflect.DeepEqual(got.Rows, want.Rows) {
		t.Fatalf("spill sort rows = %#v, want %#v", got.Rows, want.Rows)
	}
	explained, err := ExecuteSQLQueryContext(context.Background(), "EXPLAIN ANALYZE "+query, SQLSourceResolverFunc(nil), SQLQueryOptions{
		MaxSortBytes:   64,
		SpillDirectory: directory,
		MaxSpillBytes:  1 << 20,
	})
	if err != nil {
		t.Fatalf("explain spill sort: %v", err)
	}
	seen := false
	for _, step := range explained.Plan {
		seen = seen || step.Node == "EXTERNAL SORT"
	}
	if !seen {
		t.Fatalf("spill sort plan = %#v, want EXTERNAL SORT", explained.Plan)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read spill directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory retains temporary files: %#v", entries)
	}
	_, err = ExecuteSQLQueryContext(context.Background(), query, SQLSourceResolverFunc(nil), SQLQueryOptions{
		MaxSortBytes:   64,
		SpillDirectory: directory,
		MaxSpillBytes:  1,
	})
	if err == nil || !strings.Contains(err.Error(), "spill disk budget") {
		t.Fatalf("spill disk budget error = %v, want spill disk budget", err)
	}
	entries, err = os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read spill directory after failure: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("spill failure retains temporary files: %#v", entries)
	}
}

func TestExecuteSQLQueryContextSpillsDirectGroupedAggregatesWithinDiskBudget(t *testing.T) {
	t.Parallel()
	values := make([]string, 0, 60)
	for id := 0; id < 60; id++ {
		values = append(values, fmt.Sprintf("(%d, %d)", id%7, id-30))
	}
	query := "FROM VALUES " + strings.Join(values, ", ") + " AS values(team, score) SELECT team, COUNT(*) AS members, COUNT(score) AS scored_members, SUM(score) AS total, AVG(score) AS average, MIN(score) AS minimum, MAX(score) AS maximum GROUP BY team ORDER BY team ASC NULLS LAST"
	want, err := ExecuteSQLQueryContext(context.Background(), query, SQLSourceResolverFunc(nil), SQLQueryOptions{})
	if err != nil {
		t.Fatalf("unbounded baseline: %v", err)
	}
	directory := t.TempDir()
	got, err := ExecuteSQLQueryContext(context.Background(), query, SQLSourceResolverFunc(nil), SQLQueryOptions{
		MaxGroupBytes:  1,
		SpillDirectory: directory,
		MaxSpillBytes:  1 << 20,
	})
	if err != nil {
		t.Fatalf("spill grouped aggregate: %v", err)
	}
	if !reflect.DeepEqual(got.Rows, want.Rows) {
		t.Fatalf("spill grouped aggregate rows = %#v, want %#v", got.Rows, want.Rows)
	}
	explained, err := ExecuteSQLQueryContext(context.Background(), "EXPLAIN ANALYZE "+query, SQLSourceResolverFunc(nil), SQLQueryOptions{
		MaxGroupBytes:  1,
		SpillDirectory: directory,
		MaxSpillBytes:  1 << 20,
	})
	if err != nil {
		t.Fatalf("explain spill grouped aggregate: %v", err)
	}
	seen := false
	for _, step := range explained.Plan {
		seen = seen || step.Node == "EXTERNAL GROUP AGGREGATE"
	}
	if !seen {
		t.Fatalf("spill grouped aggregate plan = %#v, want EXTERNAL GROUP AGGREGATE", explained.Plan)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read spill directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory retains temporary files: %#v", entries)
	}
	_, err = ExecuteSQLQueryContext(context.Background(), query, SQLSourceResolverFunc(nil), SQLQueryOptions{
		MaxGroupBytes:  1,
		SpillDirectory: directory,
		MaxSpillBytes:  1,
	})
	if err == nil || !strings.Contains(err.Error(), "spill disk budget") {
		t.Fatalf("group spill disk budget error = %v, want spill disk budget", err)
	}
	entries, err = os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read spill directory after failure: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("group spill failure retains temporary files: %#v", entries)
	}
}

func TestExecuteSQLQueryContextStreamsSpilledDirectGroupedAggregates(t *testing.T) {
	t.Parallel()
	rows := []SQLRow{
		{"team": "red", "score": int64(3)},
		{"team": "blue", "score": int64(-2)},
		{"team": "red", "score": int64(7)},
		{"team": "blue", "score": int64(5)},
		{"team": nil, "score": int64(1)},
	}
	query := "FROM CACHE('people') AS person WHERE person.score >= 0 SELECT person.team, COUNT(*) AS members, SUM(person.score) AS total, AVG(person.score) AS average GROUP BY person.team ORDER BY person.team ASC NULLS LAST"
	baseline := SQLSourceResolverFunc(func(name, key string) ([]SQLRow, error) {
		if name != "CACHE" || key != "people" {
			return nil, fmt.Errorf("source = %s(%q), want CACHE(people)", name, key)
		}
		return cloneSQLRows(rows), nil
	})
	want, err := ExecuteSQLQueryContext(context.Background(), query, baseline, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("materialized baseline: %v", err)
	}
	resolver := &sqlStreamingTestResolver{rows: cloneSQLRows(rows)}
	got, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		MaxGroupBytes:  1,
		SpillDirectory: t.TempDir(),
		MaxSpillBytes:  1 << 20,
	})
	if err != nil {
		t.Fatalf("streamed spill grouped aggregate: %v", err)
	}
	if !reflect.DeepEqual(got.Rows, want.Rows) {
		t.Fatalf("streamed spill grouped aggregate rows = %#v, want %#v", got.Rows, want.Rows)
	}
	if resolver.resolveCalls != 0 || resolver.streamCalls != 1 {
		t.Fatalf("resolver calls materialized=%d streamed=%d, want 0/1", resolver.resolveCalls, resolver.streamCalls)
	}
	explained, err := ExecuteSQLQueryContext(context.Background(), "EXPLAIN ANALYZE "+query, resolver, SQLQueryOptions{
		MaxGroupBytes:  1,
		SpillDirectory: t.TempDir(),
		MaxSpillBytes:  1 << 20,
	})
	if err != nil {
		t.Fatalf("explain streamed spill grouped aggregate: %v", err)
	}
	seenStream, seenFilter, seenAggregate := false, false, false
	for _, step := range explained.Plan {
		if step.Node == "STREAM SCAN" {
			if step.ActualOutputRows == nil || *step.ActualOutputRows != len(rows) {
				t.Fatalf("stream scan plan = %#v, want %d source rows", step, len(rows))
			}
			seenStream = true
		}
		if step.Node == "FILTER" {
			if step.ActualInputRows == nil || *step.ActualInputRows != len(rows) || step.ActualOutputRows == nil || *step.ActualOutputRows != 4 {
				t.Fatalf("stream filter plan = %#v, want 5 input and 4 output rows", step)
			}
			seenFilter = true
		}
		seenAggregate = seenAggregate || step.Node == "EXTERNAL GROUP AGGREGATE"
	}
	if !seenStream || !seenFilter || !seenAggregate {
		t.Fatalf("streamed spill grouped aggregate plan = %#v, want STREAM SCAN, FILTER, and EXTERNAL GROUP AGGREGATE", explained.Plan)
	}
	if resolver.resolveCalls != 0 || resolver.streamCalls != 2 {
		t.Fatalf("resolver calls after EXPLAIN materialized=%d streamed=%d, want 0/2", resolver.resolveCalls, resolver.streamCalls)
	}
	limited := &sqlStreamingTestResolver{rows: cloneSQLRows(rows)}
	_, err = ExecuteSQLQueryContext(context.Background(), query, limited, SQLQueryOptions{
		MaxRows:        len(rows) - 1,
		MaxGroupBytes:  1,
		SpillDirectory: t.TempDir(),
		MaxSpillBytes:  1 << 20,
	})
	if err == nil || !strings.Contains(err.Error(), "source \"person\" exceeds the 4 row limit") {
		t.Fatalf("streamed spill MaxRows error = %v, want source row-limit diagnostic", err)
	}
	if limited.resolveCalls != 0 || limited.streamCalls != 1 {
		t.Fatalf("limited resolver calls materialized=%d streamed=%d, want 0/1", limited.resolveCalls, limited.streamCalls)
	}
}

func TestExecuteSQLQueryContextSpilledGroupAggregatePreservesFloatAdditionOrder(t *testing.T) {
	t.Parallel()
	values := make([]string, 0, 96)
	for index := 0; index < 96; index++ {
		score := "1"
		switch index % 3 {
		case 0:
			score = "10000000000000000"
		case 2:
			score = "-10000000000000000"
		}
		values = append(values, "(0, "+score+")")
	}
	query := "FROM VALUES " + strings.Join(values, ", ") + " AS values(team, score) SELECT team, SUM(score) AS total, AVG(score) AS average GROUP BY team ORDER BY team"
	want, err := ExecuteSQLQueryContext(context.Background(), query, SQLSourceResolverFunc(nil), SQLQueryOptions{})
	if err != nil {
		t.Fatalf("unbounded baseline: %v", err)
	}
	got, err := ExecuteSQLQueryContext(context.Background(), query, SQLSourceResolverFunc(nil), SQLQueryOptions{
		MaxGroupBytes:  1,
		SpillDirectory: t.TempDir(),
		MaxSpillBytes:  1 << 20,
	})
	if err != nil {
		t.Fatalf("spill grouped aggregate: %v", err)
	}
	if !reflect.DeepEqual(got.Rows, want.Rows) {
		t.Fatalf("spill float aggregate rows = %#v, want %#v", got.Rows, want.Rows)
	}
}

func TestExecuteSQLQueryContextSpilledGroupAggregateProperty(t *testing.T) {
	t.Parallel()
	random := rand.New(rand.NewSource(20260827))
	for iteration := 0; iteration < 40; iteration++ {
		values := make([]string, 1+random.Intn(40))
		for index := range values {
			team := "NULL"
			if random.Intn(4) != 0 {
				team = strconv.Itoa(random.Intn(5) - 2)
			}
			score := "NULL"
			if random.Intn(4) != 0 {
				score = strconv.Itoa(random.Intn(13) - 6)
			}
			values[index] = "(" + team + ", " + score + ")"
		}
		direction := "ASC"
		if random.Intn(2) == 0 {
			direction = "DESC"
		}
		nulls := "NULLS LAST"
		if random.Intn(2) == 0 {
			nulls = "NULLS FIRST"
		}
		limit := 1 + random.Intn(6)
		offset := random.Intn(5)
		query := fmt.Sprintf("FROM VALUES %s AS values(team, score) SELECT team, COUNT(*) AS members, COUNT(score) AS scored_members, SUM(score) AS total, AVG(score) AS average, MIN(score) AS minimum, MAX(score) AS maximum GROUP BY team ORDER BY team %s %s LIMIT %d OFFSET %d", strings.Join(values, ", "), direction, nulls, limit, offset)
		want, err := ExecuteSQLQueryContext(context.Background(), query, SQLSourceResolverFunc(nil), SQLQueryOptions{})
		if err != nil {
			t.Fatalf("iteration %d unbounded baseline: %v", iteration, err)
		}
		got, err := ExecuteSQLQueryContext(context.Background(), query, SQLSourceResolverFunc(nil), SQLQueryOptions{
			MaxGroupBytes:  1,
			SpillDirectory: t.TempDir(),
			MaxSpillBytes:  1 << 20,
		})
		if err != nil {
			t.Fatalf("iteration %d spill grouped aggregate: %v", iteration, err)
		}
		if !reflect.DeepEqual(got.Rows, want.Rows) {
			t.Fatalf("iteration %d spill grouped aggregate rows = %#v, want %#v\nquery=%s", iteration, got.Rows, want.Rows, query)
		}
	}
}

func TestExecuteSQLQueryContextSpillSortPreservesStableOrderAcrossMergePasses(t *testing.T) {
	t.Parallel()
	values := make([]string, 0, 80)
	for id := 0; id < 80; id++ {
		values = append(values, fmt.Sprintf("(%d, %d)", id, id%4))
	}
	query := "FROM VALUES " + strings.Join(values, ", ") + " AS values(id, score) SELECT id, score ORDER BY score LIMIT 17 OFFSET 11"
	want, err := ExecuteSQLQueryContext(context.Background(), query, SQLSourceResolverFunc(nil), SQLQueryOptions{})
	if err != nil {
		t.Fatalf("unbounded baseline: %v", err)
	}
	directory := t.TempDir()
	got, err := ExecuteSQLQueryContext(context.Background(), query, SQLSourceResolverFunc(nil), SQLQueryOptions{
		MaxSortBytes:   1,
		SpillDirectory: directory,
		MaxSpillBytes:  1 << 20,
	})
	if err != nil {
		t.Fatalf("many-run spill sort: %v", err)
	}
	if !reflect.DeepEqual(got.Rows, want.Rows) {
		t.Fatalf("many-run spill sort rows = %#v, want %#v", got.Rows, want.Rows)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read spill directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory retains temporary files: %#v", entries)
	}
}

func TestExecuteSQLQueryContextSpillSortPreservesNestedAndTypedValues(t *testing.T) {
	t.Parallel()
	timestamp := time.Date(2026, time.August, 24, 9, 10, 11, 12, time.UTC)
	resolver := SQLSourceResolverFunc(func(name, key string) ([]SQLRow, error) {
		if name != "CACHE" || key != "people" {
			return nil, fmt.Errorf("unexpected source %s(%q)", name, key)
		}
		return []SQLRow{
			{"id": int64(2), "meta": map[string]interface{}{"tags": []interface{}{"beta", float64(2)}, "nested": map[string]interface{}{"active": true}}, "occurred_at": timestamp, "amount": json.Number("2.50")},
			{"id": int64(1), "meta": map[string]interface{}{"tags": []interface{}{"alpha", float64(1)}, "nested": map[string]interface{}{"active": false}}, "occurred_at": timestamp.Add(-time.Hour), "amount": json.Number("1.25")},
		}, nil
	})
	query := "FROM CACHE('people') AS person SELECT person.id, person.meta, person.occurred_at, person.amount ORDER BY person.id"
	want, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("unbounded baseline: %v", err)
	}
	got, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		MaxSortBytes:   1,
		SpillDirectory: t.TempDir(),
		MaxSpillBytes:  1 << 20,
	})
	if err != nil {
		t.Fatalf("typed spill sort: %v", err)
	}
	if !reflect.DeepEqual(got.Rows, want.Rows) {
		t.Fatalf("typed spill sort rows = %#v, want %#v", got.Rows, want.Rows)
	}
}

func TestExecuteSQLQueryParametersBindTypedValuesAndDiagnosePositions(t *testing.T) {
	t.Parallel()
	resolver := SQLSourceResolverFunc(func(name, key string) ([]SQLRow, error) {
		if name != "CACHE" || key != "people" {
			return nil, fmt.Errorf("resolved %s(%q), want CACHE(people)", name, key)
		}
		return []SQLRow{{"id": int64(1), "name": "Ivi"}, {"id": int64(2), "name": "Lia"}}, nil
	})
	result, err := ExecuteSQLQueryParameters(context.Background(), `
FROM CACHE($1) AS people
WHERE people.id = $2
SELECT people.name, $3 AS requested_name`, resolver, []interface{}{"people", int64(2), "Lia"}, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	if want := (SQLQueryResult{Columns: []string{"name", "requested_name"}, Rows: []SQLRow{{"name": "Lia", "requested_name": "Lia"}}}); !reflect.DeepEqual(result, want) {
		t.Fatalf("parameterized result = %#v, want %#v", result, want)
	}
	for _, test := range []struct {
		source, want string
		column       int
	}{
		{"FROM VALUES ($0) AS values(value) SELECT value", "parameter indexes start at $1", 14},
		{"FROM VALUES ($2) AS values(value) SELECT value", "no supplied parameter", 14},
	} {
		_, err := ExecuteSQLQueryParameters(context.Background(), test.source, SQLSourceResolverFunc(nil), []interface{}{"one"}, SQLQueryOptions{})
		diagnostic, ok := err.(*SQLDiagnostic)
		if !ok || !strings.Contains(diagnostic.Message, test.want) || diagnostic.Column != test.column {
			t.Fatalf("parameter diagnostic = %#v, want %q at column %d", err, test.want, test.column)
		}
	}
}

func TestExecuteSQLQueryUsesOneSnapshotForRepeatedSources(t *testing.T) {
	t.Parallel()
	calls := 0
	resolver := SQLSourceResolverFunc(func(name, key string) ([]SQLRow, error) {
		calls++
		if calls == 1 {
			return []SQLRow{{"id": int64(1), "version": "first"}}, nil
		}
		return []SQLRow{{"id": int64(1), "version": "changed"}}, nil
	})
	result, err := ExecuteSQLQuery(`
FROM CACHE('users') AS left_users
INNER JOIN CACHE('users') AS right_users ON left_users.id = right_users.id
SELECT left_users.version AS left_version, right_users.version AS right_version`, resolver)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("resolver calls = %d, want one snapshot read", calls)
	}
	if want := []SQLRow{{"left_version": "first", "right_version": "first"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("snapshot result = %#v, want %#v", result.Rows, want)
	}
}

func TestExecuteSQLQueryUsesThreeValuedNullLogic(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		op          string
		left, right interface{}
		want        interface{}
	}{
		{"=", nil, nil, nil}, {"<>", nil, int64(1), nil}, {"<", int64(1), nil, nil},
		{"AND", true, nil, nil}, {"AND", false, nil, false}, {"OR", true, nil, true}, {"OR", false, nil, nil},
	} {
		if got := sqlBinaryValue(test.op, test.left, test.right); got != test.want {
			t.Fatalf("%#v %s %#v = %#v, want %#v", test.left, test.op, test.right, got, test.want)
		}
	}
}

func TestHatTrieOptionalSQLJSONFieldIndexRefreshesAndPlansIndexScan(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("users", `[{"id":1,"team_id":10},{"id":2,"team_id":20},{"id":3,"team_id":20}]`)
	if err := trie.CreateSQLJSONFieldIndex("users", "team_id"); err != nil {
		t.Fatalf("CreateSQLJSONFieldIndex() error = %v", err)
	}
	query := "EXPLAIN ANALYZE FROM CACHE('users') AS users WHERE users.team_id = 20 SELECT users.id ORDER BY users.id"
	result, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("indexed query error = %v", err)
	}
	if result.Stats == nil || result.Stats.OutputRows != 2 || len(result.Plan) == 0 || result.Plan[0].Node != "INDEX SCAN" {
		t.Fatalf("indexed plan = %#v, stats = %#v", result.Plan, result.Stats)
	}
	trie.UpsertString("users", `[{"id":4,"team_id":20}]`)
	result, err = ExecuteSQLQuery(query, trie)
	if err != nil || result.Stats == nil || result.Stats.OutputRows != 1 {
		t.Fatalf("refreshed indexed query = %#v, %v", result, err)
	}
}

func TestHatTrieOptionalSQLJSONFieldIndexSupportsRangePredicates(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("users", `[{"id":1,"team_id":10},{"id":2,"team_id":20},{"id":3,"team_id":30}]`)
	if err := trie.CreateSQLJSONFieldIndex("users", "team_id"); err != nil {
		t.Fatal(err)
	}
	query := "FROM CACHE('users') AS users WHERE users.team_id >= 20 SELECT users.id ORDER BY users.id"
	result, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("range index query error = %v", err)
	}
	if want := []SQLRow{{"id": float64(2)}, {"id": float64(3)}}; !reflect.DeepEqual(result.Rows[:2], want) {
		t.Fatalf("range index rows = %#v, want %#v", result.Rows[:2], want)
	}
	reversed, err := ExecuteSQLQuery("FROM CACHE('users') AS users WHERE 20 <= users.team_id SELECT users.id ORDER BY users.id", trie)
	if err != nil || !reflect.DeepEqual(reversed.Rows, result.Rows) {
		t.Fatalf("reversed range rows/error = %#v/%v, want %#v", reversed.Rows, err, result.Rows)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil || explained.Stats == nil || len(explained.Plan) == 0 || explained.Plan[0].Node != "INDEX SCAN" {
		t.Fatalf("range index plan/error/stats = %#v/%v/%#v", explained.Plan, err, explained.Stats)
	}
}

func TestHatTrieOptionalSQLJSONFieldIndexSelectsConjunctivePredicate(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("users", `[{"id":1,"team_id":20,"enabled":false},{"id":2,"team_id":20,"enabled":true},{"id":3,"team_id":30,"enabled":true}]`)
	if err := trie.CreateSQLJSONFieldIndex("users", "team_id"); err != nil {
		t.Fatal(err)
	}
	query := "FROM CACHE('users') AS users WHERE users.team_id = 20 AND users.enabled = TRUE SELECT users.id"
	result, err := ExecuteSQLQuery(query, trie)
	if err != nil || !reflect.DeepEqual(result.Rows, []SQLRow{{"id": float64(2)}}) {
		t.Fatalf("conjunctive index rows/error = %#v/%v", result.Rows, err)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil || explained.Stats == nil {
		t.Fatalf("conjunctive index plan/error/stats = %#v/%v/%#v", explained.Plan, err, explained.Stats)
	}
	for _, step := range explained.Plan {
		if step.Node == "INDEX SCAN" {
			return
		}
	}
	t.Fatalf("conjunctive index plan = %#v, want INDEX SCAN", explained.Plan)
}

func TestHatTrieSQLCompositeJSONIndexPlansAndReportsStatistics(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("users", `[{"id":1,"team_id":20,"enabled":true},{"id":2,"team_id":20,"enabled":false},{"id":3,"team_id":20,"enabled":true},{"id":4,"team_id":30,"enabled":true}]`)
	if err := trie.CreateSQLJSONCompositeIndex("users", "team_id", "enabled"); err != nil {
		t.Fatalf("CreateSQLJSONCompositeIndex() error = %v", err)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE FROM CACHE('users') AS users WHERE users.team_id = 20 AND users.enabled = TRUE SELECT users.id ORDER BY users.id", trie)
	if err != nil {
		t.Fatalf("composite index query error = %v", err)
	}
	if !strings.Contains(fmt.Sprint(explained.Plan), "INDEX SCAN") {
		t.Fatalf("composite index plan = %#v, want INDEX SCAN", explained.Plan)
	}
	result, err := ExecuteSQLQuery("FROM CACHE('users') AS users WHERE users.team_id = 20 AND users.enabled = TRUE SELECT users.id ORDER BY users.id", trie)
	if err != nil || !reflect.DeepEqual(result.Rows, []SQLRow{{"id": float64(1)}, {"id": float64(3)}}) {
		t.Fatalf("composite index result = %#v, %v, want indexed ids 1 and 3", result, err)
	}
	stats, ok, err := trie.SQLJSONIndexStats("users", "team_id", "enabled")
	if err != nil || !ok || stats.Rows != 4 || stats.DistinctKeys != 3 || stats.MinRowsPerKey != 1 || stats.MaxRowsPerKey != 2 || stats.AverageRowsPerKey != 4.0/3.0 || !reflect.DeepEqual(stats.Fields, []string{"team_id", "enabled"}) {
		t.Fatalf("SQLJSONIndexStats() = %#v/%t/%v, want four rows and three composite keys", stats, ok, err)
	}
	trie.UpsertString("users", `[{"id":4,"team_id":20,"enabled":true}]`)
	result, err = ExecuteSQLQuery("FROM CACHE('users') AS users WHERE users.team_id = 20 AND users.enabled = TRUE SELECT users.id", trie)
	if err != nil || !reflect.DeepEqual(result.Rows, []SQLRow{{"id": float64(4)}}) {
		t.Fatalf("refreshed composite index result = %#v, %v, want the replacement row", result, err)
	}
	stats, ok, err = trie.SQLJSONIndexStats("users", "team_id", "enabled")
	if err != nil || !ok || stats.Rows != 1 || stats.DistinctKeys != 1 || stats.MinRowsPerKey != 1 || stats.MaxRowsPerKey != 1 || stats.AverageRowsPerKey != 1 {
		t.Fatalf("refreshed SQLJSONIndexStats() = %#v/%t/%v, want one row and one key", stats, ok, err)
	}
}

func TestHatTrieSQLJSONIndexStatsExposeSkewAndExplainEstimate(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("events", `[{"id":1,"kind":"hot"},{"id":2,"kind":"hot"},{"id":3,"kind":"hot"},{"id":4,"kind":"hot"},{"id":5,"kind":"hot"},{"id":6,"kind":"warm"},{"id":7,"kind":"warm"},{"id":8,"kind":"cold"}]`)
	if err := trie.CreateSQLJSONFieldIndex("events", "kind"); err != nil {
		t.Fatal(err)
	}
	stats, ok, err := trie.SQLJSONIndexStats("events", "kind")
	if err != nil || !ok {
		t.Fatalf("SQLJSONIndexStats() = %#v/%t/%v", stats, ok, err)
	}
	if stats.Rows != 8 || stats.DistinctKeys != 3 || stats.MinRowsPerKey != 1 || stats.MaxRowsPerKey != 5 || stats.AverageRowsPerKey != 8.0/3.0 {
		t.Fatalf("SQLJSONIndexStats() = %#v, want rows=8 distinct=3 min=1 max=5 average=%v", stats, 8.0/3.0)
	}
	if want := []SQLJSONIndexFrequencyBucket{{RowsPerKey: 1, DistinctKeys: 1}, {RowsPerKey: 2, DistinctKeys: 1}, {RowsPerKey: 5, DistinctKeys: 1}}; !reflect.DeepEqual(stats.FrequencyHistogram, want) {
		t.Fatalf("SQLJSONIndexStats().FrequencyHistogram = %#v, want %#v", stats.FrequencyHistogram, want)
	}
	result, err := ExecuteSQLQuery("EXPLAIN ANALYZE FROM CACHE('events') AS event WHERE event.kind = 'hot' SELECT event.id", trie)
	if err != nil || result.Stats == nil {
		t.Fatalf("indexed EXPLAIN ANALYZE = %#v/%v/%#v", result.Plan, err, result.Stats)
	}
	for _, step := range result.Plan {
		if step.Node == "INDEX SCAN" {
			if step.EstimatedRows == nil || *step.EstimatedRows != 5 || step.ActualOutputRows == nil || *step.ActualOutputRows != 5 || step.EstimateErrorRows == nil || *step.EstimateErrorRows != 0 {
				t.Fatalf("indexed estimate/actual/error = %#v, want 5/5/0", step)
			}
			return
		}
	}
	t.Fatalf("indexed EXPLAIN ANALYZE plan = %#v, want INDEX SCAN", result.Plan)
}

func TestHatTrieSQLJSONFieldIndexAvoidsSortForCompatibleOrder(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1,"age":2},{"id":2,"age":null},{"id":3},{"id":4,"age":1},{"id":5,"age":2}]`)
	if err := trie.CreateSQLJSONFieldIndex("people", "age"); err != nil {
		t.Fatal(err)
	}
	result, err := ExecuteSQLQuery("FROM CACHE('people') AS person SELECT person.id, person.age ORDER BY person.age DESC NULLS FIRST", trie)
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{
		{"id": float64(2), "age": nil},
		{"id": float64(3), "age": nil},
		{"id": float64(1), "age": float64(2)},
		{"id": float64(5), "age": float64(2)},
		{"id": float64(4), "age": float64(1)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("ordered index rows = %#v, want %#v", result.Rows, want)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE FROM CACHE('people') AS person SELECT person.id, person.age ORDER BY person.age DESC NULLS FIRST", trie)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, step := range explained.Plan {
		seen[step.Node] = true
	}
	if !seen["INDEX ORDER SCAN"] || seen["SORT"] {
		t.Fatalf("EXPLAIN ANALYZE nodes = %#v, want INDEX ORDER SCAN without SORT", explained.Plan)
	}
}

func TestHatTrieSQLJSONFieldIndexAvoidsHashGroupingForCompatibleOrder(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"age":2},{"age":null},{},{"age":2},{"age":1}]`)
	if err := trie.CreateSQLJSONFieldIndex("people", "age"); err != nil {
		t.Fatal(err)
	}
	result, err := ExecuteSQLQuery("FROM CACHE('people') AS person SELECT person.age, COUNT(*) AS members GROUP BY person.age ORDER BY person.age DESC NULLS FIRST", trie)
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{
		{"age": nil, "members": int64(2)},
		{"age": float64(2), "members": int64(2)},
		{"age": float64(1), "members": int64(1)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("ordered index groups = %#v, want %#v", result.Rows, want)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE FROM CACHE('people') AS person SELECT person.age, COUNT(*) AS members GROUP BY person.age ORDER BY person.age DESC NULLS FIRST", trie)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, step := range explained.Plan {
		seen[step.Node] = true
	}
	if !seen["INDEX ORDER SCAN"] || !seen["INDEX GROUP AGGREGATE"] || seen["SORT"] {
		t.Fatalf("EXPLAIN ANALYZE nodes = %#v, want INDEX ORDER SCAN + INDEX GROUP AGGREGATE without SORT", explained.Plan)
	}
}

func TestHatTrieSQLJSONFieldIndexOrderFastPathPreservesDistinctAndWindowSemantics(t *testing.T) {
	t.Parallel()
	data := `[{"id":1,"age":2,"team":1},{"id":2,"age":1,"team":1},{"id":1,"age":0,"team":1}]`
	plain := newTestTrie(t)
	plain.UpsertString("people", data)
	indexed := newTestTrie(t)
	indexed.UpsertString("people", data)
	if err := indexed.CreateSQLJSONFieldIndex("people", "age"); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		"FROM CACHE('people') AS person SELECT DISTINCT person.id ORDER BY person.age",
		"FROM CACHE('people') AS person SELECT person.id, ROW_NUMBER() OVER (ORDER BY person.team) AS position ORDER BY person.age",
	} {
		want, err := ExecuteSQLQuery(query, plain)
		if err != nil {
			t.Fatal(err)
		}
		got, err := ExecuteSQLQuery(query, indexed)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got.Rows, want.Rows) {
			t.Fatalf("indexed result for %q = %#v, want non-index result %#v", query, got.Rows, want.Rows)
		}
		explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, indexed)
		if err != nil {
			t.Fatal(err)
		}
		for _, step := range explained.Plan {
			if step.Node == "INDEX ORDER SCAN" {
				t.Fatalf("EXPLAIN ANALYZE for %q used unsafe index ordering: %#v", query, explained.Plan)
			}
		}
	}
}

func TestHatTrieSQLJSONFieldIndexOrderedAggregationStreamsConstantState(t *testing.T) {
	t.Parallel()
	data := `[
        {"age":1,"score":2},{"age":1,"score":4},{"age":1,"score":6},
        {"age":2,"score":3},{"age":2,"score":9},{"age":3,"score":5},
        {"age":3},{"score":7}
    ]`
	plain := newTestTrie(t)
	trie := newTestTrie(t)
	plain.UpsertString("people", data)
	trie.UpsertString("people", data)
	if err := trie.CreateSQLJSONFieldIndex("people", "age"); err != nil {
		t.Fatal(err)
	}
	query := "FROM CACHE('people') AS person SELECT person.age, COUNT(*) AS members, COUNT(person.score) AS scored_members, SUM(person.score) AS total, AVG(person.score) AS average, MIN(person.score) AS minimum, MAX(person.score) AS maximum GROUP BY person.age ORDER BY person.age ASC NULLS LAST"
	want, err := ExecuteSQLQuery(query, plain)
	if err != nil {
		t.Fatalf("unbounded baseline: %v", err)
	}
	got, err := ExecuteSQLQueryContext(context.Background(), query, trie, SQLQueryOptions{MaxGroupBytes: 1})
	if err != nil {
		t.Fatalf("streamed ordered aggregate: %v", err)
	}
	if !reflect.DeepEqual(got.Rows, want.Rows) {
		t.Fatalf("streamed ordered aggregate rows = %#v, want %#v", got.Rows, want.Rows)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatalf("explain streamed ordered aggregate: %v", err)
	}
	for _, step := range explained.Plan {
		if step.Node == "INDEX GROUP AGGREGATE" && strings.Contains(step.Detail, "streaming") {
			return
		}
	}
	t.Fatalf("streamed ordered aggregate plan = %#v, want streaming INDEX GROUP AGGREGATE", explained.Plan)
}

func TestHatTrieSQLJSONFieldIndexOrderFastPathMatchesGeneralSortDifferential(t *testing.T) {
	t.Parallel()
	random := rand.New(rand.NewSource(20260824))
	orders := []string{
		"ORDER BY person.age",
		"ORDER BY person.age DESC",
		"ORDER BY person.age NULLS LAST",
		"ORDER BY person.age DESC NULLS FIRST",
	}
	for iteration := 0; iteration < 48; iteration++ {
		rows := make([]SQLRow, 1+random.Intn(12))
		for index := range rows {
			var age interface{}
			switch random.Intn(5) {
			case 0:
				age = nil
			case 1:
				age = random.Intn(4)
			case 2:
				age = []string{"Ada", "Zed", "é"}[random.Intn(3)]
			case 3:
				age = random.Intn(2) == 0
			default:
				age = random.Intn(4)
			}
			rows[index] = SQLRow{"id": index, "age": age}
		}
		data, err := json.Marshal(rows)
		if err != nil {
			t.Fatal(err)
		}
		plain := newTestTrie(t)
		plain.UpsertString("people", string(data))
		indexed := newTestTrie(t)
		indexed.UpsertString("people", string(data))
		if err := indexed.CreateSQLJSONFieldIndex("people", "age"); err != nil {
			t.Fatal(err)
		}
		for _, order := range orders {
			query := "FROM CACHE('people') AS person SELECT person.id, person.age " + order
			want, err := ExecuteSQLQuery(query, plain)
			if err != nil {
				t.Fatalf("iteration %d plain %q: %v", iteration, order, err)
			}
			got, err := ExecuteSQLQuery(query, indexed)
			if err != nil {
				t.Fatalf("iteration %d indexed %q: %v", iteration, order, err)
			}
			if !reflect.DeepEqual(got.Rows, want.Rows) {
				t.Fatalf("iteration %d indexed %q = %#v, want %#v", iteration, order, got.Rows, want.Rows)
			}
		}
	}
}

func TestHatTrieSQLJSONIndexValueEstimateUsesExactPostingCount(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("events", `[{"id":1,"kind":"hot"},{"id":2,"kind":"hot"},{"id":3,"kind":"hot"},{"id":4,"kind":"hot"},{"id":5,"kind":"hot"},{"id":6,"kind":"cold"}]`)
	if err := trie.CreateSQLJSONFieldIndex("events", "kind"); err != nil {
		t.Fatal(err)
	}
	rows, known, available, err := trie.SQLJSONIndexValueEstimate("events", "kind", "hot")
	if err != nil || !available || !known || rows != 5 {
		t.Fatalf("SQLJSONIndexValueEstimate(hot) = %d/%t/%t/%v, want 5/true/true/nil", rows, known, available, err)
	}
	rows, known, available, err = trie.SQLJSONIndexValueEstimate("events", "kind", "missing")
	if err != nil || !available || !known || rows != 0 {
		t.Fatalf("SQLJSONIndexValueEstimate(missing) = %d/%t/%t/%v, want 0/true/true/nil", rows, known, available, err)
	}
	result, err := ExecuteSQLQuery("EXPLAIN ANALYZE FROM CACHE('events') AS event WHERE event.kind = 'hot' SELECT event.id", trie)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range result.Plan {
		if step.Node == "INDEX SCAN" && step.EstimatedRows != nil {
			if *step.EstimatedRows != 5 {
				t.Fatalf("hot INDEX SCAN estimate = %d, want exact 5", *step.EstimatedRows)
			}
			return
		}
	}
	t.Fatalf("EXPLAIN ANALYZE plan = %#v, want INDEX SCAN estimate", result.Plan)
}

func TestHatTrieSQLCompositeJSONIndexRejectsInvalidDefinitions(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	for _, fields := range [][]string{nil, {"team_id"}, {"", "enabled"}, {"team_id", "team_id"}} {
		if err := trie.CreateSQLJSONCompositeIndex("users", fields...); err == nil {
			t.Fatalf("CreateSQLJSONCompositeIndex(%q) succeeded, want validation error", fields)
		}
	}
	var nilTrie *HatTrie
	if err := nilTrie.CreateSQLJSONCompositeIndex("users", "team_id", "enabled"); err == nil {
		t.Fatal("nil CreateSQLJSONCompositeIndex() succeeded, want validation error")
	}
}

func TestHatTrieOptionalSQLJSONFieldIndexProbesInnerJoin(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("users", `[{"id":1,"name":"Ivi"},{"id":2,"name":"Lia"},{"id":3,"name":"Noe"}]`)
	if err := trie.CreateSQLJSONFieldIndex("users", "id"); err != nil {
		t.Fatal(err)
	}
	query := "FROM VALUES (2), (3) AS wanted(id) INNER JOIN CACHE('users') AS users ON wanted.id = users.id SELECT users.name ORDER BY users.name"
	result, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("indexed join error = %v", err)
	}
	if want := []SQLRow{{"name": "Lia"}, {"name": "Noe"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("indexed join rows = %#v, want %#v", result.Rows, want)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil || explained.Stats == nil || len(explained.Plan) == 0 {
		t.Fatalf("indexed join explain/error/stats = %#v/%v/%#v", explained.Plan, err, explained.Stats)
	}
	found := false
	for _, step := range explained.Plan {
		found = found || step.Node == "INDEX JOIN"
	}
	if !found {
		t.Fatalf("indexed join plan = %#v, want INDEX JOIN", explained.Plan)
	}
}

func TestHatTrieCompositeJSONIndexProbesMultiColumnInnerAndLeftJoin(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("members", `[{"team_id":10,"role":"admin","name":"Ada"},{"team_id":10,"role":"viewer","name":"Bea"},{"team_id":20,"role":"admin","name":"Cai"}]`)
	if err := trie.CreateSQLJSONCompositeIndex("members", "team_id", "role"); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, join string
		want       []SQLRow
	}{
		{"inner", "INNER JOIN", []SQLRow{{"team_id": int64(10), "role": "admin", "name": "Ada"}, {"team_id": int64(20), "role": "admin", "name": "Cai"}}},
		{"left", "LEFT JOIN", []SQLRow{{"team_id": int64(10), "role": "admin", "name": "Ada"}, {"team_id": int64(10), "role": nil, "name": nil}, {"team_id": int64(20), "role": "admin", "name": "Cai"}, {"team_id": int64(30), "role": "admin", "name": nil}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			query := "FROM VALUES (10, 'admin'), (20, 'admin'), (30, 'admin'), (10, NULL) AS wanted(team_id, role) " + test.join + " CACHE('members') AS member ON wanted.team_id = member.team_id AND wanted.role = member.role SELECT wanted.team_id, wanted.role, member.name ORDER BY wanted.team_id"
			result, err := ExecuteSQLQuery(query, trie)
			if err != nil {
				t.Fatalf("composite indexed %s join: %v", test.name, err)
			}
			if !reflect.DeepEqual(result.Rows, test.want) {
				t.Fatalf("composite indexed %s rows = %#v, want %#v", test.name, result.Rows, test.want)
			}
			explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
			if err != nil || explained.Stats == nil {
				t.Fatalf("composite indexed %s explain/error/stats = %#v/%v/%#v", test.name, explained.Plan, err, explained.Stats)
			}
			for _, step := range explained.Plan {
				if step.Node == "COMPOSITE INDEX JOIN" {
					return
				}
			}
			t.Fatalf("composite indexed %s plan = %#v, want COMPOSITE INDEX JOIN", test.name, explained.Plan)
		})
	}
}

func TestHatTrieOptionalSQLJSONFieldIndexProbesLeftJoin(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("users", `[{"id":2,"name":"Lia"}]`)
	if err := trie.CreateSQLJSONFieldIndex("users", "id"); err != nil {
		t.Fatal(err)
	}
	query := "FROM VALUES (1), (2) AS wanted(id) LEFT JOIN CACHE('users') AS users ON wanted.id = users.id SELECT wanted.id, users.name ORDER BY wanted.id"
	result, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("indexed left join error = %v", err)
	}
	if want := []SQLRow{{"id": int64(1), "name": nil}, {"id": int64(2), "name": "Lia"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("indexed left join rows = %#v, want %#v", result.Rows, want)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil || explained.Stats == nil {
		t.Fatalf("indexed left join explain/error/stats = %#v/%v/%#v", explained.Plan, err, explained.Stats)
	}
	for _, step := range explained.Plan {
		if step.Node == "INDEX JOIN" {
			return
		}
	}
	t.Fatalf("indexed left join plan = %#v, want INDEX JOIN", explained.Plan)
}

func TestHatTrieOptionalSQLJSONFieldIndexProbesRangeInnerAndLeftJoin(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"age":18,"name":"Ada"},{"age":21,"name":"Bea"},{"age":30,"name":"Cai"}]`)
	if err := trie.CreateSQLJSONFieldIndex("people", "age"); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		kind string
		want []SQLRow
	}{
		{"INNER", []SQLRow{{"minimum": int64(20), "name": "Bea"}, {"minimum": int64(20), "name": "Cai"}}},
		{"LEFT", []SQLRow{{"minimum": int64(20), "name": "Bea"}, {"minimum": int64(20), "name": "Cai"}, {"minimum": int64(40), "name": nil}}},
	} {
		t.Run(test.kind, func(t *testing.T) {
			query := "FROM VALUES (20), (40) AS wanted(minimum) " + test.kind + " JOIN CACHE('people') AS person ON wanted.minimum <= person.age SELECT wanted.minimum, person.name ORDER BY wanted.minimum, person.name"
			result, err := ExecuteSQLQuery(query, trie)
			if err != nil || !reflect.DeepEqual(result.Rows, test.want) {
				t.Fatalf("range %s join = %#v/%v, want %#v", test.kind, result.Rows, err, test.want)
			}
			explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
			if err != nil || explained.Stats == nil || !strings.Contains(fmt.Sprint(explained.Plan), "RANGE INDEX JOIN") {
				t.Fatalf("range %s explain = %#v/%v", test.kind, explained.Plan, err)
			}
		})
	}
}

func TestHatTrieOptionalSQLJSONFieldIndexProbesRangeJoinWithAdditionalPredicate(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"age":18,"name":"Ada","enabled":true},{"age":21,"name":"Bea","enabled":false},{"age":30,"name":"Cai","enabled":true}]`)
	if err := trie.CreateSQLJSONFieldIndex("people", "age"); err != nil {
		t.Fatal(err)
	}
	query := "FROM VALUES (20), (40) AS wanted(minimum) LEFT JOIN CACHE('people') AS person ON wanted.minimum <= person.age AND person.enabled = TRUE SELECT wanted.minimum, person.name ORDER BY wanted.minimum"
	result, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{{"minimum": int64(20), "name": "Cai"}, {"minimum": int64(40), "name": nil}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("range join with extra predicate = %#v, want %#v", result.Rows, want)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range explained.Plan {
		if step.Node == "RANGE INDEX JOIN" {
			return
		}
	}
	t.Fatalf("range join with extra predicate plan = %#v, want RANGE INDEX JOIN", explained.Plan)
}

func TestHatTrieOptionalSQLJSONFieldIndexProbesRightJoin(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("users", `[{"id":2,"name":"Lia"}]`)
	if err := trie.CreateSQLJSONFieldIndex("users", "id"); err != nil {
		t.Fatal(err)
	}
	query := "FROM CACHE('users') AS users RIGHT JOIN VALUES (1), (2) AS wanted(id) ON users.id = wanted.id SELECT wanted.id, users.name ORDER BY wanted.id"
	result, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("indexed right join error = %v", err)
	}
	if want := []SQLRow{{"id": int64(1), "name": nil}, {"id": int64(2), "name": "Lia"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("indexed right join rows = %#v, want %#v", result.Rows, want)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil || explained.Stats == nil {
		t.Fatalf("indexed right join explain/error/stats = %#v/%v/%#v", explained.Plan, err, explained.Stats)
	}
	for _, step := range explained.Plan {
		if step.Node == "INDEX RIGHT JOIN" {
			return
		}
	}
	t.Fatalf("indexed right join plan = %#v, want INDEX RIGHT JOIN", explained.Plan)
}

func TestHatTrieOptionalSQLJSONFieldIndexRightJoinRespectsWhere(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("users", `[{"id":2,"name":"Lia"}]`)
	if err := trie.CreateSQLJSONFieldIndex("users", "id"); err != nil {
		t.Fatal(err)
	}
	result, err := ExecuteSQLQuery("FROM CACHE('users') AS users RIGHT JOIN VALUES (1), (2) AS wanted(id) ON users.id = wanted.id WHERE users.name = 'Lia' SELECT wanted.id", trie)
	if err != nil {
		t.Fatalf("right join with WHERE error = %v", err)
	}
	if want := []SQLRow{{"id": int64(2)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("right join with WHERE rows = %#v, want %#v", result.Rows, want)
	}
	result, err = ExecuteSQLQuery("EXPLAIN ANALYZE FROM CACHE('users') AS users RIGHT JOIN VALUES (1), (2) AS wanted(id) ON users.id = wanted.id WHERE users.name = 'Lia' SELECT wanted.id", trie)
	if err != nil || result.Stats == nil {
		t.Fatalf("right join with WHERE explain/error/stats = %#v/%v/%#v", result.Plan, err, result.Stats)
	}
	for _, step := range result.Plan {
		if step.Node == "INDEX RIGHT JOIN" {
			return
		}
	}
	t.Fatalf("right join with WHERE plan = %#v, want INDEX RIGHT JOIN", result.Plan)
}

func TestHatTrieOptionalSQLJSONFieldIndexRightJoinDoesNotMatchNulls(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("users", `[{"id":null,"name":"Null"},{"id":2,"name":"Lia"}]`)
	if err := trie.CreateSQLJSONFieldIndex("users", "id"); err != nil {
		t.Fatal(err)
	}
	result, err := ExecuteSQLQuery("FROM CACHE('users') AS users RIGHT JOIN VALUES (NULL), (2) AS wanted(id) ON users.id = wanted.id SELECT wanted.id, users.name ORDER BY wanted.id", trie)
	if err != nil {
		t.Fatalf("indexed right join with NULL error = %v", err)
	}
	if want := []SQLRow{{"id": nil, "name": nil}, {"id": int64(2), "name": "Lia"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("indexed right join with NULL rows = %#v, want %#v", result.Rows, want)
	}
}

func TestHatTrieOptionalSQLJSONFieldIndexRightJoinHonorsExecutionLimits(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("users", `[{"id":2,"name":"Lia"}]`)
	if err := trie.CreateSQLJSONFieldIndex("users", "id"); err != nil {
		t.Fatal(err)
	}
	if _, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('users') AS users RIGHT JOIN VALUES (1), (2) AS wanted(id) ON users.id = wanted.id SELECT wanted.id", trie, SQLQueryOptions{MaxRows: 1}); err == nil || !strings.Contains(err.Error(), `SQL source "wanted" exceeds the 1 row limit`) {
		t.Fatalf("indexed right join MaxRows error = %v, want right-source limit", err)
	}
	if _, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('users') AS users RIGHT JOIN VALUES (2) AS wanted(id) ON users.id = wanted.id SELECT wanted.id", trie, SQLQueryOptions{MaxJoinWork: 1}); err == nil || !strings.Contains(err.Error(), "join work budget exceeded") {
		t.Fatalf("indexed right join MaxJoinWork error = %v, want join-work limit", err)
	}
}

func TestExecuteSQLQuerySupportsTimestampLiteralsAndDiagnostics(t *testing.T) {
	t.Parallel()
	result, err := ExecuteSQLQuery("FROM VALUES ('before', TIMESTAMP '2026-08-22T08:00:00Z'), ('after', TIMESTAMP '2026-08-22T10:00:00Z') AS events(label, occurred_at) WHERE occurred_at >= TIMESTAMP '2026-08-22T09:00:00Z' SELECT label", SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("timestamp query error = %v", err)
	}
	if want := []SQLRow{{"label": "after"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("timestamp rows = %#v, want %#v", result.Rows, want)
	}
	if _, err := ExecuteSQLQuery("FROM VALUES (TIMESTAMP 'not-a-time') AS events(occurred_at) SELECT occurred_at", SQLSourceResolverFunc(nil)); err == nil || !strings.Contains(err.Error(), "RFC3339") {
		t.Fatalf("invalid timestamp error = %v, want RFC3339 diagnostic", err)
	}
}

func TestExecuteSQLQuerySupportsDateLiteralsAndDiagnostics(t *testing.T) {
	t.Parallel()
	result, err := ExecuteSQLQuery("FROM VALUES ('before', DATE '2026-08-21'), ('after', DATE '2026-08-23') AS events(label, occurred_on) WHERE occurred_on > DATE '2026-08-22' SELECT label", SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("date query error = %v", err)
	}
	if want := []SQLRow{{"label": "after"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("date rows = %#v, want %#v", result.Rows, want)
	}
	if _, err := ExecuteSQLQuery("FROM VALUES (DATE '2026-02-30') AS events(occurred_on) SELECT occurred_on", SQLSourceResolverFunc(nil)); err == nil || !strings.Contains(err.Error(), "YYYY-MM-DD") {
		t.Fatalf("invalid date error = %v, want date diagnostic", err)
	}
}

func TestExecuteSQLQueryDiagnosesIncompatibleLiteralComparisonTypes(t *testing.T) {
	t.Parallel()
	_, err := ExecuteSQLQuery("FROM VALUES (1) AS values(value) WHERE 1 = '1' SELECT value", SQLSourceResolverFunc(nil))
	if err == nil || !strings.Contains(err.Error(), "cannot compare NUMBER with TEXT") {
		t.Fatalf("comparison error = %v, want NUMBER/TEXT diagnostic", err)
	}
	formatted := FormatSQLDiagnostic("FROM VALUES (1) AS values(value) WHERE 1 = '1' SELECT value", err)
	if !strings.Contains(formatted, "query:1:") || !strings.Contains(formatted, "^") {
		t.Fatalf("formatted comparison diagnostic = %q, want source span", formatted)
	}
}

func TestExecuteSQLQueryUsesCaseSensitiveUTF8BinaryStringCollation(t *testing.T) {
	t.Parallel()
	result, err := ExecuteSQLQuery("FROM VALUES ('a'), ('Z'), ('é') AS values(value) SELECT value ORDER BY value", SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"value": "Z"}, {"value": "a"}, {"value": "é"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("binary string order = %#v, want %#v", result.Rows, want)
	}
}

func TestExecuteSQLQuerySupportsExplicitNullOrder(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, order string
		want        []SQLRow
	}{
		{"ascending_last", "value ASC NULLS LAST", []SQLRow{{"value": int64(1)}, {"value": int64(2)}, {"value": nil}}},
		{"descending_first", "value DESC NULLS FIRST", []SQLRow{{"value": nil}, {"value": int64(2)}, {"value": int64(1)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := ExecuteSQLQuery("FROM VALUES (NULL), (2), (1) AS values(value) SELECT value ORDER BY "+test.order, SQLSourceResolverFunc(nil))
			if err != nil {
				t.Fatalf("explicit null ordering: %v", err)
			}
			if !reflect.DeepEqual(result.Rows, test.want) {
				t.Fatalf("explicit null ordering rows = %#v, want %#v", result.Rows, test.want)
			}
		})
	}
}

func TestExecuteSQLQueryWindowOrderHonorsExplicitNullOrder(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, order string
		want        []SQLRow
	}{
		{"last", "value NULLS LAST", []SQLRow{{"value": int64(1), "position": int64(1)}, {"value": int64(2), "position": int64(2)}, {"value": nil, "position": int64(3)}}},
		{"first", "value NULLS FIRST", []SQLRow{{"value": nil, "position": int64(1)}, {"value": int64(1), "position": int64(2)}, {"value": int64(2), "position": int64(3)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := ExecuteSQLQuery("FROM VALUES (NULL), (2), (1) AS values(value) SELECT value, ROW_NUMBER() OVER (ORDER BY "+test.order+") AS position ORDER BY position", SQLSourceResolverFunc(nil))
			if err != nil {
				t.Fatalf("window explicit null ordering: %v", err)
			}
			if !reflect.DeepEqual(result.Rows, test.want) {
				t.Fatalf("window explicit null ordering rows = %#v, want %#v", result.Rows, test.want)
			}
		})
	}
}

func TestExecuteSQLQuerySupportsFetchFirst(t *testing.T) {
	t.Parallel()
	result, err := ExecuteSQLQuery("FROM VALUES (3), (1), (2) AS values(value) SELECT value ORDER BY value FETCH FIRST 2 ROWS ONLY", SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("FETCH FIRST query: %v", err)
	}
	if want := []SQLRow{{"value": int64(1)}, {"value": int64(2)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("FETCH FIRST rows = %#v, want %#v", result.Rows, want)
	}
}

func TestExecuteSQLQuerySupportsInWithNullSemantics(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, where string
		want        []SQLRow
	}{
		{"match", "value IN (1, NULL)", []SQLRow{{"value": int64(1)}}},
		{"not_in_null_is_unknown", "value NOT IN (1, NULL)", []SQLRow{}},
		{"not_in_without_null", "value NOT IN (1, 3)", []SQLRow{{"value": int64(2)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := ExecuteSQLQuery("FROM VALUES (1), (2), (NULL) AS values(value) WHERE "+test.where+" SELECT value ORDER BY value NULLS LAST", SQLSourceResolverFunc(nil))
			if err != nil {
				t.Fatalf("IN query: %v", err)
			}
			if !reflect.DeepEqual(result.Rows, test.want) {
				t.Fatalf("IN rows = %#v, want %#v", result.Rows, test.want)
			}
		})
	}
}

func TestExecuteSQLQuerySupportsBetweenWithNullSemantics(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, where string
		want        []SQLRow
	}{
		{"inclusive", "value BETWEEN 2 AND 3", []SQLRow{{"value": int64(2)}, {"value": int64(3)}}},
		{"not_between", "value NOT BETWEEN 2 AND 3", []SQLRow{{"value": int64(1)}}},
		{"null_bound_is_unknown", "value BETWEEN 1 AND NULL", []SQLRow{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := ExecuteSQLQuery("FROM VALUES (1), (2), (3), (NULL) AS values(value) WHERE "+test.where+" SELECT value ORDER BY value NULLS LAST", SQLSourceResolverFunc(nil))
			if err != nil {
				t.Fatalf("BETWEEN query: %v", err)
			}
			if !reflect.DeepEqual(result.Rows, test.want) {
				t.Fatalf("BETWEEN rows = %#v, want %#v", result.Rows, test.want)
			}
		})
	}
}

func TestExecuteSQLQuerySupportsCaseExpressions(t *testing.T) {
	t.Parallel()
	result, err := ExecuteSQLQuery("FROM VALUES (1), (2), (NULL) AS values(value) SELECT value, CASE WHEN value = 1 THEN 'one' WHEN value = 2 THEN 'two' ELSE 'other' END AS searched, CASE value WHEN 1 THEN 'one' WHEN NULL THEN 'null' ELSE 'other' END AS simple ORDER BY value NULLS LAST", SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("CASE query: %v", err)
	}
	want := []SQLRow{
		{"value": int64(1), "searched": "one", "simple": "one"},
		{"value": int64(2), "searched": "two", "simple": "other"},
		{"value": nil, "searched": "other", "simple": "other"},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("CASE rows = %#v, want %#v", result.Rows, want)
	}
	result, err = ExecuteSQLQuery("FROM VALUES (1) AS values(value) SELECT CASE WHEN value = 1 THEN 'selected' ELSE CAST('not a number' AS NUMBER) END AS value", SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("CASE must evaluate only its selected branch: %v", err)
	}
	if want := []SQLRow{{"value": "selected"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("lazy CASE rows = %#v, want %#v", result.Rows, want)
	}
	functions := NewSQLFunctionRegistry()
	if err := functions.Register(SQLFunctionDefinition{Name: "double_case", Arguments: []string{"value"}, ArgumentTypes: []string{"INTEGER"}, Language: "GO", Source: "return value * 2"}); err != nil {
		t.Fatalf("register CASE function: %v", err)
	}
	result, err = ExecuteSQLQuery("FROM VALUES (1), (2) AS values(value) SELECT CASE WHEN value = 1 THEN double_case(value) ELSE double_case(0) END AS value ORDER BY value", sqlFunctionTestResolver{functions: functions})
	if err != nil {
		t.Fatalf("CASE custom function query: %v", err)
	}
	if want := []SQLRow{{"value": int64(0)}, {"value": int64(2)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("CASE custom function rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLGeneratedReferenceCasesForJoinsGroupsAndSets(t *testing.T) {
	t.Parallel()
	random := rand.New(rand.NewSource(20260822))
	for iteration := 0; iteration < 96; iteration++ {
		left, right := make([]int, 1+random.Intn(5)), make([]int, 1+random.Intn(5))
		for index := range left {
			left[index] = random.Intn(3)
		}
		for index := range right {
			right[index] = random.Intn(3)
		}
		values := func(rows []int) string {
			parts := make([]string, len(rows))
			for i, value := range rows {
				parts[i] = fmt.Sprintf("(%d)", value)
			}
			return strings.Join(parts, ", ")
		}
		query := "FROM VALUES " + values(left) + " AS left_values(id) INNER JOIN VALUES " + values(right) + " AS right_values(id) ON left_values.id = right_values.id SELECT left_values.id ORDER BY left_values.id"
		got, err := ExecuteSQLQuery(query, SQLSourceResolverFunc(nil))
		if err != nil {
			t.Fatalf("iteration %d join error = %v", iteration, err)
		}
		wantIDs := []int{}
		for _, l := range left {
			for _, r := range right {
				if l == r {
					wantIDs = append(wantIDs, l)
				}
			}
		}
		sort.Ints(wantIDs)
		if len(got.Rows) != len(wantIDs) {
			t.Fatalf("iteration %d join rows = %#v, want ids %#v", iteration, got.Rows, wantIDs)
		}
		for index, want := range wantIDs {
			if got.Rows[index]["id"] != int64(want) {
				t.Fatalf("iteration %d join row %d = %#v, want %d", iteration, index, got.Rows[index], want)
			}
		}

		setQuery := "FROM VALUES " + values(left) + " AS a(id) SELECT id UNION FROM VALUES " + values(right) + " AS b(id) SELECT id"
		setResult, err := ExecuteSQLQuery(setQuery, SQLSourceResolverFunc(nil))
		if err != nil {
			t.Fatal(err)
		}
		set := map[int]bool{}
		setWant := []int{}
		for _, value := range append(append([]int{}, left...), right...) {
			if !set[value] {
				set[value] = true
				setWant = append(setWant, value)
			}
		}
		if len(setResult.Rows) != len(setWant) {
			t.Fatalf("iteration %d union rows = %#v, want %#v", iteration, setResult.Rows, setWant)
		}
		for index, want := range setWant {
			if setResult.Rows[index]["id"] != int64(want) {
				t.Fatalf("iteration %d union row = %#v, want %d", iteration, setResult.Rows[index], want)
			}
		}
		groupResult, err := ExecuteSQLQuery("FROM VALUES "+values(left)+" AS values(id) GROUP BY id SELECT id, COUNT(*) AS count ORDER BY id", SQLSourceResolverFunc(nil))
		if err != nil {
			t.Fatal(err)
		}
		counts := map[int]int{}
		for _, value := range left {
			counts[value]++
		}
		rowIndex := 0
		for _, value := range []int{0, 1, 2} {
			if counts[value] == 0 {
				continue
			}
			row := groupResult.Rows[rowIndex]
			if row["id"] != int64(value) || row["count"] != int64(counts[value]) {
				t.Fatalf("iteration %d group row = %#v, want %d/%d", iteration, row, value, counts[value])
			}
			rowIndex++
		}
	}
}

func TestSQLGeneratedSQLiteNullPaginationDifferential(t *testing.T) {
	t.Parallel()
	random := rand.New(rand.NewSource(20260824))
	for iteration := 0; iteration < 48; iteration++ {
		values := make([]string, 1+random.Intn(8))
		for index := range values {
			if random.Intn(3) == 0 {
				values[index] = "NULL"
			} else {
				values[index] = strconv.Itoa(random.Intn(5))
			}
		}
		rows := strings.Join(func() []string {
			out := make([]string, len(values))
			for index, value := range values {
				out[index] = "(" + value + ")"
			}
			return out
		}(), ", ")
		hatrieQuery := "WITH data(value) AS (VALUES " + rows + ") FROM data WHERE value IS NULL OR value BETWEEN 1 AND 3 SELECT value ORDER BY value NULLS LAST LIMIT 3 OFFSET 1"
		got, err := ExecuteSQLQuery(hatrieQuery, SQLSourceResolverFunc(nil))
		if err != nil {
			t.Fatalf("iteration %d HatTrie query: %v", iteration, err)
		}
		sqliteQuery := "WITH data(value) AS (VALUES " + rows + ") SELECT value FROM data WHERE value IS NULL OR value BETWEEN 1 AND 3 ORDER BY value NULLS LAST LIMIT 3 OFFSET 1"
		output, err := exec.Command("sqlite3", "-json", ":memory:", sqliteQuery).Output()
		if err != nil {
			t.Fatalf("iteration %d SQLite query: %v", iteration, err)
		}
		if len(output) == 0 {
			output = []byte("[]")
		}
		var want, normalized []map[string]interface{}
		if err := json.Unmarshal(output, &want); err != nil {
			t.Fatalf("iteration %d decode SQLite JSON: %v", iteration, err)
		}
		encoded, err := json.Marshal(got.Rows)
		if err != nil {
			t.Fatalf("iteration %d encode HatTrie rows: %v", iteration, err)
		}
		if err := json.Unmarshal(encoded, &normalized); err != nil {
			t.Fatalf("iteration %d decode HatTrie JSON: %v", iteration, err)
		}
		if !reflect.DeepEqual(normalized, want) {
			t.Fatalf("iteration %d null/pagination differential mismatch\nquery=%s\nhatrie=%#v\nsqlite=%#v", iteration, hatrieQuery, normalized, want)
		}
	}
}

func TestSQLGeneratedSQLiteDerivedExpressionDifferential(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 is required for differential testing")
	}
	random := rand.New(rand.NewSource(20260826))
	for iteration := 0; iteration < 64; iteration++ {
		values := make([]string, 1+random.Intn(12))
		for id := range values {
			raw := float64(random.Intn(17)-8) / 2
			score := "NULL"
			if random.Intn(4) != 0 {
				score = strconv.Itoa(random.Intn(9) - 4)
			}
			values[id] = fmt.Sprintf("(%d, '%.2f', %s)", id, raw, score)
		}
		limit := 1 + random.Intn(6)
		rows := strings.Join(values, ", ")
		hatrieQuery := fmt.Sprintf("FROM (FROM VALUES %s AS source(id, raw, score) WHERE source.score IS NULL OR source.score BETWEEN -3 AND 3 SELECT source.id, source.raw, source.score) AS filtered WHERE CAST(filtered.raw AS NUMBER) BETWEEN -2.5 AND 2.5 AND (filtered.score IS NULL OR filtered.score IN (-2, -1, 0, 1, NULL)) SELECT filtered.id, filtered.raw, filtered.score, CASE WHEN filtered.score BETWEEN 1 AND 2 THEN 'middle' WHEN filtered.score IS NULL THEN 'missing' ELSE 'other' END AS bucket ORDER BY filtered.score DESC NULLS LAST, filtered.id FETCH FIRST %d ROWS ONLY", rows, limit)
		got, err := ExecuteSQLQuery(hatrieQuery, SQLSourceResolverFunc(nil))
		if err != nil {
			t.Fatalf("iteration %d HatTrie query: %v\nquery=%s", iteration, err, hatrieQuery)
		}
		sqliteQuery := fmt.Sprintf("WITH source(id, raw, score) AS (VALUES %s), filtered AS (SELECT id, raw, score FROM source WHERE score IS NULL OR score BETWEEN -3 AND 3) SELECT id, raw, score, CASE WHEN score BETWEEN 1 AND 2 THEN 'middle' WHEN score IS NULL THEN 'missing' ELSE 'other' END AS bucket FROM filtered WHERE CAST(raw AS REAL) BETWEEN -2.5 AND 2.5 AND (score IS NULL OR score IN (-2, -1, 0, 1, NULL)) ORDER BY score DESC NULLS LAST, id LIMIT %d", rows, limit)
		output, err := exec.Command("sqlite3", "-json", ":memory:", sqliteQuery).Output()
		if err != nil {
			t.Fatalf("iteration %d SQLite query: %v\nquery=%s", iteration, err, sqliteQuery)
		}
		if len(output) == 0 {
			output = []byte("[]")
		}
		var want, normalized []map[string]interface{}
		if err := json.Unmarshal(output, &want); err != nil {
			t.Fatalf("iteration %d decode SQLite JSON: %v", iteration, err)
		}
		encoded, err := json.Marshal(got.Rows)
		if err != nil {
			t.Fatalf("iteration %d encode HatTrie rows: %v", iteration, err)
		}
		if err := json.Unmarshal(encoded, &normalized); err != nil {
			t.Fatalf("iteration %d decode HatTrie JSON: %v", iteration, err)
		}
		if !reflect.DeepEqual(normalized, want) {
			t.Fatalf("iteration %d derived/expression differential mismatch\nhatrie_query=%s\nsqlite_query=%s\nhatrie=%#v\nsqlite=%#v", iteration, hatrieQuery, sqliteQuery, normalized, want)
		}
	}
}

func TestExecuteSQLQuerySupportsPartitionedWindowFunctions(t *testing.T) {
	t.Parallel()
	result, err := ExecuteSQLQuery(`
FROM VALUES ('a', 2), ('a', 5), ('b', 3), ('b', 7) AS values(team, score)
SELECT team, score,
       ROW_NUMBER() OVER (PARTITION BY team ORDER BY score) AS row_number,
       RANK() OVER (PARTITION BY team ORDER BY score) AS rank,
       SUM(score) OVER (PARTITION BY team ORDER BY score) AS running_score
ORDER BY team, score`, SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("window query error = %v", err)
	}
	want := []SQLRow{
		{"team": "a", "score": int64(2), "row_number": int64(1), "rank": int64(1), "running_score": float64(2)},
		{"team": "a", "score": int64(5), "row_number": int64(2), "rank": int64(2), "running_score": float64(7)},
		{"team": "b", "score": int64(3), "row_number": int64(1), "rank": int64(1), "running_score": float64(3)},
		{"team": "b", "score": int64(7), "row_number": int64(2), "rank": int64(2), "running_score": float64(10)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("window rows = %#v, want %#v", result.Rows, want)
	}
}

func TestExecuteSQLQuerySupportsDenseRankLagAndLeadWindows(t *testing.T) {
	t.Parallel()
	result, err := ExecuteSQLQuery(`
FROM VALUES ('a', 2), ('a', 2), ('a', 5), ('b', 3) AS values(team, score)
SELECT team, score,
       DENSE_RANK() OVER (PARTITION BY team ORDER BY score) AS dense_rank,
       LAG(score) OVER (PARTITION BY team ORDER BY score) AS previous_score,
       LEAD(score, 1, -1) OVER (PARTITION BY team ORDER BY score) AS next_score
ORDER BY team, score`, SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("window query error = %v", err)
	}
	want := []SQLRow{
		{"team": "a", "score": int64(2), "dense_rank": int64(1), "previous_score": nil, "next_score": int64(2)},
		{"team": "a", "score": int64(2), "dense_rank": int64(1), "previous_score": int64(2), "next_score": int64(5)},
		{"team": "a", "score": int64(5), "dense_rank": int64(2), "previous_score": int64(2), "next_score": int64(-1)},
		{"team": "b", "score": int64(3), "dense_rank": int64(1), "previous_score": nil, "next_score": int64(-1)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("window rows = %#v, want %#v", result.Rows, want)
	}
}

func TestExecuteSQLQuerySupportsRecursiveCTEHierarchy(t *testing.T) {
	t.Parallel()
	result, err := ExecuteSQLQuery(`
WITH RECURSIVE ancestors(node, parent, level) AS (
  FROM VALUES (1, NULL, 0) AS seed(id, parent_id, depth) SELECT id, parent_id, depth
  UNION ALL
  FROM VALUES (2, 1), (3, 2) AS nodes(id, parent_id)
  INNER JOIN ancestors AS previous ON nodes.parent_id = previous.node
  SELECT nodes.id, nodes.parent_id, previous.level + 1 AS depth
)
FROM ancestors
SELECT node, level
ORDER BY node`, SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("recursive CTE error = %v", err)
	}
	if want := []SQLRow{{"node": int64(1), "level": int64(0)}, {"node": int64(2), "level": int64(1)}, {"node": int64(3), "level": int64(2)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("recursive CTE rows = %#v, want %#v", result.Rows, want)
	}
}

func TestExecuteSQLQueryRecursiveCTESearchAndCycleColumns(t *testing.T) {
	t.Parallel()
	result, err := ExecuteSQLQuery(`
WITH RECURSIVE walk(node, level) AS (
  FROM VALUES (1, 0) AS seed(node, level) SELECT node, level
  UNION ALL
  FROM VALUES (2, 1), (1, 2) AS edges(node, parent)
  INNER JOIN walk AS previous ON edges.parent = previous.node
  SELECT edges.node, previous.level + 1 AS level
)
SEARCH BREADTH FIRST BY node SET search_order
CYCLE node SET is_cycle
FROM walk
SELECT node, level, search_order, is_cycle
ORDER BY search_order`, SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("recursive SEARCH/CYCLE query error = %v", err)
	}
	want := []SQLRow{
		{"node": int64(1), "level": int64(0), "search_order": int64(1), "is_cycle": false},
		{"node": int64(2), "level": int64(1), "search_order": int64(2), "is_cycle": false},
		{"node": int64(1), "level": int64(2), "search_order": int64(3), "is_cycle": true},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("recursive SEARCH/CYCLE rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLDifferentialAgainstSQLiteForJoinsGroupsAndWindows(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI is unavailable; install SQLite to run SQL differential cases")
	}
	resolver := SQLSourceResolverFunc(func(name, key string) ([]SQLRow, error) {
		if name != "CACHE" {
			return nil, fmt.Errorf("unexpected source %s", name)
		}
		switch key {
		case "people":
			return []SQLRow{{"id": int64(1), "team_id": int64(10), "score": int64(9)}, {"id": int64(2), "team_id": int64(10), "score": int64(7)}, {"id": int64(3), "team_id": int64(20), "score": int64(4)}}, nil
		case "teams":
			return []SQLRow{{"id": int64(10), "label": "Core"}}, nil
		case "events":
			return []SQLRow{{"id": int64(1), "occurred_at": "2026-08-21T23:00:00Z"}, {"id": int64(2), "occurred_at": "2026-08-22T09:00:00Z"}}, nil
		case "left_side":
			return []SQLRow{{"id": int64(1)}, {"id": int64(2)}}, nil
		case "right_side":
			return []SQLRow{{"id": int64(2)}, {"id": int64(3)}}, nil
		default:
			return nil, fmt.Errorf("unexpected key %q", key)
		}
	})
	setup := `CREATE TABLE people(id INTEGER, team_id INTEGER, score INTEGER); INSERT INTO people VALUES(1,10,9),(2,10,7),(3,20,4); CREATE TABLE teams(id INTEGER, label TEXT); INSERT INTO teams VALUES(10,'Core'); CREATE TABLE events(id INTEGER, occurred_at TEXT); INSERT INTO events VALUES(1,'2026-08-21T23:00:00Z'),(2,'2026-08-22T09:00:00Z'); CREATE TABLE left_side(id INTEGER); INSERT INTO left_side VALUES(1),(2); CREATE TABLE right_side(id INTEGER); INSERT INTO right_side VALUES(2),(3);`
	for _, test := range []struct{ name, hatrie, sqlite string }{
		{"inner_filter", "SELECT p.id, t.label FROM CACHE('people') AS p JOIN CACHE('teams') AS t ON p.team_id = t.id WHERE p.score >= 7 ORDER BY p.id", "SELECT p.id, t.label FROM people AS p JOIN teams AS t ON p.team_id = t.id WHERE p.score >= 7 ORDER BY p.id"},
		{"left_join", "SELECT p.id, t.label FROM CACHE('people') AS p LEFT JOIN CACHE('teams') AS t ON p.team_id = t.id ORDER BY p.id", "SELECT p.id, t.label FROM people AS p LEFT JOIN teams AS t ON p.team_id = t.id ORDER BY p.id"},
		{"right_join", "SELECT l.id AS left_id, r.id AS right_id FROM CACHE('left_side') AS l RIGHT JOIN CACHE('right_side') AS r ON l.id = r.id ORDER BY r.id", "SELECT l.id AS left_id, r.id AS right_id FROM left_side AS l RIGHT JOIN right_side AS r ON l.id = r.id ORDER BY r.id"},
		{"full_join", "SELECT l.id AS left_id, r.id AS right_id FROM CACHE('left_side') AS l FULL JOIN CACHE('right_side') AS r ON l.id = r.id ORDER BY COALESCE(l.id, r.id)", "SELECT l.id AS left_id, r.id AS right_id FROM left_side AS l FULL JOIN right_side AS r ON l.id = r.id ORDER BY COALESCE(l.id, r.id)"},
		{"group", "SELECT p.team_id, COUNT(*) AS count, SUM(p.score) AS total FROM CACHE('people') AS p GROUP BY p.team_id ORDER BY p.team_id", "SELECT p.team_id, COUNT(*) AS count, SUM(p.score) AS total FROM people AS p GROUP BY p.team_id ORDER BY p.team_id"},
		{"window", "SELECT p.id, ROW_NUMBER() OVER (ORDER BY p.score DESC) AS position FROM CACHE('people') AS p ORDER BY p.id", "SELECT p.id, ROW_NUMBER() OVER (ORDER BY p.score DESC) AS position FROM people AS p ORDER BY p.id"},
		{"timestamp", "SELECT e.id FROM CACHE('events') AS e WHERE CAST(e.occurred_at AS TIMESTAMP) >= TIMESTAMP '2026-08-22T00:00:00Z' ORDER BY e.id", "SELECT e.id FROM events AS e WHERE e.occurred_at >= '2026-08-22T00:00:00Z' ORDER BY e.id"},
		{"recursive", "WITH RECURSIVE walk(value) AS (FROM VALUES (1) AS seed(value) SELECT value UNION ALL FROM walk AS previous WHERE previous.value < 3 SELECT previous.value + 1 AS value) FROM walk SELECT value ORDER BY value", "WITH RECURSIVE walk(value) AS (SELECT 1 UNION ALL SELECT value + 1 FROM walk WHERE value < 3) SELECT value FROM walk ORDER BY value"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := ExecuteSQLQuery(test.hatrie, resolver)
			if err != nil {
				t.Fatalf("hatrie query: %v", err)
			}
			output, err := exec.Command("sqlite3", "-json", ":memory:", setup+test.sqlite).Output()
			if err != nil {
				t.Fatalf("sqlite query: %v", err)
			}
			var want, normalized []map[string]interface{}
			if err := json.Unmarshal(output, &want); err != nil {
				t.Fatalf("decode sqlite JSON: %v", err)
			}
			encoded, err := json.Marshal(got.Rows)
			if err != nil {
				t.Fatalf("encode hatrie rows: %v", err)
			}
			if err := json.Unmarshal(encoded, &normalized); err != nil {
				t.Fatalf("decode normalized hatrie rows: %v", err)
			}
			if !reflect.DeepEqual(normalized, want) {
				t.Fatalf("SQLite differential mismatch\nhatrie=%#v\nsqlite=%#v", normalized, want)
			}
		})
	}
	t.Run("indexed_filter", func(t *testing.T) {
		trie := newTestTrie(t)
		trie.UpsertString("people", `[{"id":1,"team_id":10,"score":9},{"id":2,"team_id":10,"score":7},{"id":3,"team_id":20,"score":4}]`)
		if err := trie.CreateSQLJSONFieldIndex("people", "team_id"); err != nil {
			t.Fatal(err)
		}
		query := "FROM CACHE('people') AS p WHERE p.team_id = 10 SELECT p.id, p.score ORDER BY p.id"
		got, err := ExecuteSQLQuery(query, trie)
		if err != nil {
			t.Fatalf("indexed Hatrie query: %v", err)
		}
		output, err := exec.Command("sqlite3", "-json", ":memory:", "CREATE TABLE people(id INTEGER, team_id INTEGER, score INTEGER); INSERT INTO people VALUES(1,10,9),(2,10,7),(3,20,4); SELECT p.id, p.score FROM people AS p WHERE p.team_id = 10 ORDER BY p.id").Output()
		if err != nil {
			t.Fatalf("indexed SQLite query: %v", err)
		}
		var want, normalized []map[string]interface{}
		if err := json.Unmarshal(output, &want); err != nil {
			t.Fatalf("decode indexed SQLite JSON: %v", err)
		}
		encoded, err := json.Marshal(got.Rows)
		if err != nil {
			t.Fatalf("encode indexed Hatrie rows: %v", err)
		}
		if err := json.Unmarshal(encoded, &normalized); err != nil {
			t.Fatalf("decode indexed Hatrie rows: %v", err)
		}
		if !reflect.DeepEqual(normalized, want) {
			t.Fatalf("indexed SQLite differential mismatch\nhatrie=%#v\nsqlite=%#v", normalized, want)
		}
		explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
		if err != nil || explained.Stats == nil {
			t.Fatalf("indexed explain/error/stats = %#v/%v/%#v", explained.Plan, err, explained.Stats)
		}
		for _, step := range explained.Plan {
			if step.Node == "INDEX SCAN" {
				return
			}
		}
		t.Fatalf("indexed differential plan = %#v, want INDEX SCAN", explained.Plan)
	})
	// SQLite establishes the unbounded cross-product cardinality; Hatrie's
	// separate resource policy must then reject that same valid result before it
	// exceeds the caller's configured row budget.
	output, err := exec.Command("sqlite3", "-json", ":memory:", "SELECT a.value AS left_value, b.value AS right_value FROM (SELECT 1 AS value UNION ALL SELECT 2) AS a CROSS JOIN (SELECT 1 AS value UNION ALL SELECT 2) AS b").Output()
	if err != nil {
		t.Fatalf("sqlite resource baseline: %v", err)
	}
	var sqliteRows []map[string]interface{}
	if err := json.Unmarshal(output, &sqliteRows); err != nil || len(sqliteRows) != 4 {
		t.Fatalf("sqlite resource baseline rows = %#v, error = %v, want four rows", sqliteRows, err)
	}
	_, err = ExecuteSQLQueryContext(context.Background(), "FROM VALUES (1), (2) AS a(value) CROSS JOIN VALUES (1), (2) AS b(value) SELECT a.value AS left_value, b.value AS right_value", SQLSourceResolverFunc(nil), SQLQueryOptions{MaxRows: 3})
	if err == nil || !strings.Contains(err.Error(), "row limit") {
		t.Fatalf("bounded cross-product error = %v, want row-limit rejection", err)
	}
}

func TestExecuteSQLQueryEnforcesRecursiveCTEDepthLimit(t *testing.T) {
	t.Parallel()
	_, err := ExecuteSQLQueryContext(context.Background(), `
WITH RECURSIVE sequence(value) AS (
  FROM VALUES (1) AS seed(value) SELECT value
  UNION ALL
  FROM sequence AS previous WHERE previous.value < 3 SELECT previous.value + 1 AS value
)
FROM sequence SELECT value`, SQLSourceResolverFunc(nil), SQLQueryOptions{MaxRecursionDepth: 1})
	if err == nil || !strings.Contains(err.Error(), "recursion depth") || !strings.Contains(err.Error(), "maximum 1") {
		t.Fatalf("recursive depth error = %v, want configured limit", err)
	}
}

func TestExecuteSQLQueryDetectsRecursiveCTECycles(t *testing.T) {
	t.Parallel()
	_, err := ExecuteSQLQueryContext(context.Background(), `
WITH RECURSIVE cycle(value) AS (
  FROM VALUES (1) AS seed(value) SELECT value
  UNION ALL
  FROM cycle AS previous SELECT previous.value
)
FROM cycle SELECT value`, SQLSourceResolverFunc(nil), SQLQueryOptions{DetectRecursiveCycles: true})
	if err == nil || !strings.Contains(err.Error(), "cycle") || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("recursive cycle error = %v, want cycle diagnostic", err)
	}
}

func TestExecuteSQLQuerySupportsUnionAndUnionAll(t *testing.T) {
	t.Parallel()
	resolver := SQLSourceResolverFunc(nil)
	union, err := ExecuteSQLQuery(`
FROM VALUES (1), (2) AS left_values(value) SELECT value
UNION
FROM VALUES (2), (3) AS right_values(value) SELECT value`, resolver)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery(UNION) error = %v", err)
	}
	if want := []SQLRow{{"value": int64(1)}, {"value": int64(2)}, {"value": int64(3)}}; !reflect.DeepEqual(union.Rows, want) {
		t.Fatalf("UNION rows = %#v, want %#v", union.Rows, want)
	}
	all, err := ExecuteSQLQuery(`
FROM VALUES (1), (2) AS left_values(value) SELECT value
UNION ALL
FROM VALUES (2), (3) AS right_values(value) SELECT value`, resolver)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery(UNION ALL) error = %v", err)
	}
	if want := []SQLRow{{"value": int64(1)}, {"value": int64(2)}, {"value": int64(2)}, {"value": int64(3)}}; !reflect.DeepEqual(all.Rows, want) {
		t.Fatalf("UNION ALL rows = %#v, want %#v", all.Rows, want)
	}
	for operator, want := range map[string][]SQLRow{
		"INTERSECT": {{"value": int64(2)}},
		"EXCEPT":    {{"value": int64(1)}},
	} {
		result, err := ExecuteSQLQuery("FROM VALUES (1), (2) AS left_values(value) SELECT value "+operator+" FROM VALUES (2), (3) AS right_values(value) SELECT value", resolver)
		if err != nil || !reflect.DeepEqual(result.Rows, want) {
			t.Fatalf("%s result = %#v, %v; want %#v", operator, result, err, want)
		}
	}
}

func TestExecuteSQLQueryUnionAllDiagnosticsPointAtTheOffendingToken(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, source, message string
		line, column          int
	}{
		{
			name: "missing_right_query",
			source: `FROM VALUES (1) AS a(value) SELECT value
UNION ALL`,
			message: "UNION ALL requires a query after it",
			line:    2,
			column:  10,
		},
		{
			name: "duplicate_all",
			source: `FROM VALUES (1) AS a(value) SELECT value
UNION ALL ALL
FROM VALUES (2) AS b(value) SELECT value`,
			message: `unexpected "ALL"`,
			line:    2,
			column:  11,
		},
		{
			name: "semicolon_instead_of_right_query",
			source: `FROM VALUES (1) AS a(value) SELECT value
UNION ALL;`,
			message: "UNION ALL requires a query after it",
			line:    2,
			column:  10,
		},
		{
			name: "derived_query_closes_after_all",
			source: `FROM (
  FROM VALUES (1) AS a(value) SELECT value
  UNION ALL
) AS derived
SELECT value`,
			message: "UNION ALL requires a query after it",
			line:    4,
			column:  1,
		},
		{
			name: "repeated_union_operator",
			source: `FROM VALUES (1) AS a(value) SELECT value
UNION ALL UNION
FROM VALUES (2) AS b(value) SELECT value`,
			message: `unexpected "UNION"`,
			line:    2,
			column:  11,
		},
		{
			name: "repeated_intersect_operator",
			source: `FROM VALUES (1) AS a(value) SELECT value
UNION ALL INTERSECT
FROM VALUES (2) AS b(value) SELECT value`,
			message: `unexpected "INTERSECT"`,
			line:    2,
			column:  11,
		},
		{
			name: "repeated_except_operator",
			source: `FROM VALUES (1) AS a(value) SELECT value
UNION ALL EXCEPT
FROM VALUES (2) AS b(value) SELECT value`,
			message: `unexpected "EXCEPT"`,
			line:    2,
			column:  11,
		},
		{
			name: "punctuation_cannot_start_right_query",
			source: `FROM VALUES (1) AS a(value) SELECT value
UNION ALL ,`,
			message: `unexpected ","`,
			line:    2,
			column:  11,
		},
		{
			name: "literal_cannot_start_right_query",
			source: `FROM VALUES (1) AS a(value) SELECT value
UNION ALL 1`,
			message: `unexpected "1"`,
			line:    2,
			column:  11,
		},
		{
			name: "incomplete_select_right_query",
			source: `FROM VALUES (1) AS a(value) SELECT value
UNION ALL
SELECT value`,
			message: "query requires FROM",
			line:    3,
			column:  13,
		},
		{
			name: "incomplete_from_right_query",
			source: `FROM VALUES (1) AS a(value) SELECT value
UNION ALL
FROM VALUES (2) AS b(value)`,
			message: "query requires SELECT",
			line:    3,
			column:  len("FROM VALUES (2) AS b(value)") + 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ExecuteSQLQuery(test.source, SQLSourceResolverFunc(nil))
			diagnostic, ok := err.(*SQLDiagnostic)
			if !ok {
				t.Fatalf("ExecuteSQLQuery() error = %T %[1]v, want SQLDiagnostic", err)
			}
			if !strings.Contains(diagnostic.Message, test.message) || diagnostic.Line != test.line || diagnostic.Column != test.column {
				t.Fatalf("diagnostic = %#v, want message containing %q at %d:%d", diagnostic, test.message, test.line, test.column)
			}
			formatted := FormatSQLDiagnostic(test.source, err)
			wantLocation := fmt.Sprintf("--> query:%d:%d", test.line, test.column)
			if !strings.Contains(formatted, wantLocation) {
				t.Fatalf("FormatSQLDiagnostic() = %q, want %q", formatted, wantLocation)
			}
			line := strings.Split(test.source, "\n")[test.line-1]
			wantExcerpt := fmt.Sprintf("%d | %s\n  | %s^", test.line, line, strings.Repeat(" ", test.column-1))
			if !strings.Contains(formatted, wantExcerpt) {
				t.Fatalf("FormatSQLDiagnostic() = %q, want Rust-style excerpt %q", formatted, wantExcerpt)
			}
		})
	}
}

func TestExecuteSQLQueryExplainDescribesWithoutReadingSources(t *testing.T) {
	t.Parallel()
	resolver := SQLSourceResolverFunc(func(name, key string) ([]SQLRow, error) {
		t.Fatalf("EXPLAIN unexpectedly resolved %s(%q)", name, key)
		return nil, nil
	})
	result, err := ExecuteSQLQuery(`
EXPLAIN
FROM CACHE('must-not-be-read') AS people
WHERE age >= 18
SELECT name
ORDER BY name
LIMIT 2`, resolver)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery(EXPLAIN) error = %v", err)
	}
	if result.Stats != nil {
		t.Fatalf("EXPLAIN stats = %#v, want nil without ANALYZE", result.Stats)
	}
	if want := []string{"node", "detail", "estimated_rows"}; !reflect.DeepEqual(result.Columns, want) {
		t.Fatalf("EXPLAIN columns = %#v, want %#v", result.Columns, want)
	}
	if want := []string{"SCAN", "FILTER", "PROJECT", "SORT", "LIMIT"}; len(result.Plan) != len(want) {
		t.Fatalf("EXPLAIN plan = %#v, want %d steps", result.Plan, len(want))
	} else {
		for index, node := range want {
			if result.Plan[index].Node != node {
				t.Fatalf("EXPLAIN step %d = %#v, want node %q", index, result.Plan[index], node)
			}
		}
	}
	if len(result.Rows) != len(result.Plan) || result.Rows[0]["detail"] != `CACHE("must-not-be-read") AS people` {
		t.Fatalf("EXPLAIN rows = %#v, want plan rows without source reads", result.Rows)
	}
}

func TestExecuteSQLQueryExplainIdentifiesEqualityJoinCandidates(t *testing.T) {
	t.Parallel()
	result, err := ExecuteSQLQuery("EXPLAIN FROM VALUES (1) AS left_side(id) LEFT JOIN VALUES (1) AS right_side(id) ON left_side.id = right_side.id SELECT left_side.id", SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("EXPLAIN equality join: %v", err)
	}
	for _, step := range result.Plan {
		if step.Node == "EQUALITY JOIN" && strings.Contains(step.Detail, "HASH JOIN") && strings.Contains(step.Detail, "LEFT JOIN") {
			return
		}
	}
	t.Fatalf("EXPLAIN equality join plan = %#v, want EQUALITY JOIN with HASH JOIN eligibility", result.Plan)
}

func TestExecuteSQLQueryExplainAnalyzeReturnsMeasuredExecutionStats(t *testing.T) {
	t.Parallel()
	result, err := ExecuteSQLQuery(`
EXPLAIN ANALYZE
FROM VALUES (3), (1), (2) AS values(value)
WHERE value >= 2
SELECT value
ORDER BY value
LIMIT 1`, SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("ExecuteSQLQuery(EXPLAIN ANALYZE) error = %v", err)
	}
	if result.Stats == nil {
		t.Fatal("EXPLAIN ANALYZE stats = nil")
	}
	if result.Stats.OutputRows != 1 || result.Stats.OutputColumns != 1 || result.Stats.ResultBytes <= 0 || result.Stats.PlanSteps != len(result.Plan) || result.Stats.ElapsedNanos < 0 {
		t.Fatalf("EXPLAIN ANALYZE stats = %#v, plan = %#v", result.Stats, result.Plan)
	}
	if want := []string{"node", "detail", "estimated_rows", "actual_rows", "estimate_error_rows", "result_bytes", "elapsed_ns"}; !reflect.DeepEqual(result.Columns, want) {
		t.Fatalf("EXPLAIN ANALYZE columns = %#v, want %#v", result.Columns, want)
	}
	last := result.Rows[len(result.Rows)-1]
	if last["node"] != "ANALYZE" || last["actual_rows"] != 1 || last["elapsed_ns"] != result.Stats.ElapsedNanos {
		t.Fatalf("EXPLAIN ANALYZE summary = %#v, stats = %#v", last, result.Stats)
	}
}

func TestExecuteSQLQueryExplainAnalyzeReportsPerOperatorStats(t *testing.T) {
	t.Parallel()
	result, err := ExecuteSQLQuery(`
EXPLAIN ANALYZE
FROM VALUES (1), (2), (3) AS left_values(id)
INNER JOIN VALUES (2), (3), (4) AS right_values(id) ON left_values.id = right_values.id
WHERE left_values.id >= 2
SELECT COUNT(*) AS total
ORDER BY total DESC
LIMIT 1
UNION ALL
FROM VALUES (9) AS trailing(id)
SELECT 1 AS total`, SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("ExecuteSQLQuery(EXPLAIN ANALYZE) error = %v", err)
	}
	if result.Stats == nil || result.Stats.OutputRows != 2 {
		t.Fatalf("EXPLAIN ANALYZE stats = %#v, want two output rows", result.Stats)
	}
	for _, node := range []string{"SCAN", "HASH JOIN", "FILTER", "AGGREGATE", "PROJECT", "SORT", "LIMIT", "SET"} {
		var step *SQLExplainStep
		for index := range result.Plan {
			if result.Plan[index].Node == node {
				step = &result.Plan[index]
				break
			}
		}
		if step == nil {
			t.Fatalf("EXPLAIN ANALYZE plan = %#v, missing %s", result.Plan, node)
		}
		if step.ActualInputRows == nil || step.ActualOutputRows == nil || step.ElapsedNanos == nil {
			t.Fatalf("EXPLAIN ANALYZE %s step = %#v, want input rows, output rows, and elapsed ns", node, step)
		}
	}
	for _, want := range []struct {
		node          string
		input, output int
	}{
		{"HASH JOIN", 5, 2},
		{"FILTER", 3, 2},
		{"AGGREGATE", 2, 1},
		{"SORT", 1, 1},
		{"LIMIT", 1, 1},
		{"SET", 2, 2},
	} {
		for _, step := range result.Plan {
			if step.Node == want.node && step.ActualInputRows != nil && step.ActualOutputRows != nil && *step.ActualInputRows == want.input && *step.ActualOutputRows == want.output {
				goto found
			}
		}
		t.Fatalf("EXPLAIN ANALYZE plan = %#v, missing %s %d→%d", result.Plan, want.node, want.input, want.output)
	found:
	}
}

func TestExecuteSQLQueryExplainDiagnosticsPointAtTheOffendingToken(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, source, message string
		line, column          int
	}{
		{"missing_query", "EXPLAIN", "EXPLAIN requires a query after it", 1, 8},
		{"semicolon_instead_of_query", "EXPLAIN ;", "EXPLAIN requires a query after it", 1, 9},
		{"analyze_missing_query", "EXPLAIN ANALYZE", "EXPLAIN ANALYZE requires a query after it", 1, 16},
		{"repeated_analyze", "EXPLAIN ANALYZE ANALYZE FROM VALUES (1) AS a(value) SELECT value", `unexpected "ANALYZE"`, 1, 17},
		{"from_without_select", "EXPLAIN FROM VALUES (1) AS a(value)", "query requires SELECT", 1, len("EXPLAIN FROM VALUES (1) AS a(value)") + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ExecuteSQLQuery(test.source, SQLSourceResolverFunc(nil))
			diagnostic, ok := err.(*SQLDiagnostic)
			if !ok {
				t.Fatalf("ExecuteSQLQuery() error = %T %[1]v, want SQLDiagnostic", err)
			}
			if !strings.Contains(diagnostic.Message, test.message) || diagnostic.Line != test.line || diagnostic.Column != test.column {
				t.Fatalf("diagnostic = %#v, want message containing %q at %d:%d", diagnostic, test.message, test.line, test.column)
			}
			formatted := FormatSQLDiagnostic(test.source, err)
			wantLocation := fmt.Sprintf("--> query:%d:%d", test.line, test.column)
			if !strings.Contains(formatted, wantLocation) {
				t.Fatalf("FormatSQLDiagnostic() = %q, want %q", formatted, wantLocation)
			}
			line := strings.Split(test.source, "\n")[test.line-1]
			wantExcerpt := fmt.Sprintf("%d | %s\n  | %s^", test.line, line, strings.Repeat(" ", test.column-1))
			if !strings.Contains(formatted, wantExcerpt) {
				t.Fatalf("FormatSQLDiagnostic() = %q, want Rust-style excerpt %q", formatted, wantExcerpt)
			}
		})
	}
}

func TestExecuteSQLQuerySupportsDerivedTableSubqueries(t *testing.T) {
	t.Parallel()
	result, err := ExecuteSQLQuery(`
FROM (
  FROM VALUES ('a', 2), ('b', 5), ('c', 9) AS values(label, score)
  WHERE score >= 5
  SELECT label, score * 2 AS doubled
) AS filtered
WHERE doubled < 15
SELECT label, doubled`, SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("ExecuteSQLQuery(derived table) error = %v", err)
	}
	want := SQLQueryResult{Columns: []string{"label", "doubled"}, Rows: []SQLRow{{"label": "b", "doubled": int64(10)}}}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("derived-table result = %#v, want %#v", result, want)
	}
}

func TestExecuteSQLQueryAggregateLimitOffsetAndNullMatrix(t *testing.T) {
	t.Parallel()

	result, err := ExecuteSQLQuery(`
WITH data (team, score, note) AS (
  VALUES ('a', 2, NULL), ('a', 4, 'x'), ('b', 3, 'y'), ('b', 9, 'z')
)
SELECT team, COUNT(score) AS n, SUM(score) AS total, AVG(score) AS avg, MIN(score) AS min, MAX(score) AS max
FROM data
WHERE note IS NULL OR note LIKE '%x%'
GROUP BY team
HAVING COUNT(*) >= 1
ORDER BY total DESC
LIMIT 1 OFFSET 0`, SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := SQLQueryResult{Columns: []string{"team", "n", "total", "avg", "min", "max"}, Rows: []SQLRow{{
		"team": "a", "n": int64(2), "total": float64(6), "avg": float64(3), "min": float64(2), "max": float64(4),
	}}}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("aggregate result = %#v, want %#v", result, want)
	}
}

func TestHatTrieSQLSourceDataTypeMatrix(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("object", `{"name":"Ada","active":true,"age":37}`)
	trie.UpsertString("array", `[{"name":"Ada"},{"name":"Lin","active":false}]`)
	trie.UpsertString("scalar", `42`)
	object, err := trie.ResolveSQLSource("CACHE", "object")
	if err != nil || len(object) != 1 || object[0]["name"] != "Ada" || object[0]["active"] != true {
		t.Fatalf("object source = %#v, %v", object, err)
	}
	array, err := trie.ResolveSQLSource("CACHE", "array")
	if err != nil || len(array) != 2 || array[1]["active"] != false {
		t.Fatalf("array source = %#v, %v", array, err)
	}
	if _, err := trie.ResolveSQLSource("CACHE", "scalar"); err == nil {
		t.Fatal("scalar CACHE source error = nil, want JSON row-shape diagnostic")
	}
	keys, err := trie.ResolveSQLSource("KEYS", "")
	if err != nil || len(keys) < 3 {
		t.Fatalf("KEYS source = %#v, %v", keys, err)
	}
	for _, field := range []string{"key", "type", "ttl_ms", "on_disk", "size_bytes", "value_preview"} {
		if _, ok := keys[0][field]; !ok {
			t.Fatalf("KEYS row missing %q: %#v", field, keys[0])
		}
	}
	if _, err := trie.ResolveSQLSource("UNKNOWN", ""); err == nil {
		t.Fatal("unknown SQL source error = nil")
	}
}

func TestExecuteSQLQueryCastsTypedValuesAndDiagnosesDynamicFailures(t *testing.T) {
	t.Parallel()
	result, err := ExecuteSQLQuery(`
FROM VALUES ('42', '2026-08-22', '2026-08-22T09:00:00Z', 1) AS values(raw_number, raw_date, raw_timestamp, raw_boolean)
SELECT CAST(raw_number AS NUMBER) AS number_value,
       CAST(raw_date AS DATE) AS date_value,
       CAST(raw_timestamp AS TIMESTAMP) AS timestamp_value,
       CAST(raw_boolean AS BOOLEAN) AS boolean_value`, SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("ExecuteSQLQuery(CAST) error = %v", err)
	}
	want := SQLQueryResult{Columns: []string{"number_value", "date_value", "timestamp_value", "boolean_value"}, Rows: []SQLRow{{
		"number_value": float64(42), "date_value": sqlDate("2026-08-22"),
		"timestamp_value": mustSQLTimestamp(t, "2026-08-22T09:00:00Z"), "boolean_value": true,
	}}}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("CAST result = %#v, want %#v", result, want)
	}

	invalidCastQuery := "FROM VALUES ('not-a-number') AS values(raw) SELECT CAST(raw AS NUMBER)"
	_, err = ExecuteSQLQuery(invalidCastQuery, SQLSourceResolverFunc(nil))
	if err == nil || !strings.Contains(err.Error(), "CAST cannot convert TEXT value") || !strings.Contains(err.Error(), "NUMBER") {
		t.Fatalf("invalid dynamic CAST error = %v, want clear TEXT-to-NUMBER diagnostic", err)
	}
	diagnostic, ok := err.(*SQLDiagnostic)
	if !ok || diagnostic.Line != 1 || diagnostic.Column != strings.Index(invalidCastQuery, "CAST")+1 {
		t.Fatalf("invalid dynamic CAST diagnostic = %#v, want a source span at CAST", err)
	}
	if formatted := FormatSQLDiagnostic(invalidCastQuery, err); !strings.Contains(formatted, "error: CAST cannot convert TEXT value") || !strings.Contains(formatted, "--> query:1:") || !strings.Contains(formatted, "^") {
		t.Fatalf("invalid dynamic CAST formatted diagnostic = %q, want Rust-style source excerpt", formatted)
	}
	_, err = ExecuteSQLQuery("FROM VALUES ('1') AS values(raw) SELECT CAST(raw AS BOOLEAN)", SQLSourceResolverFunc(nil))
	if err == nil || !strings.Contains(err.Error(), "CAST cannot convert TEXT value") || !strings.Contains(err.Error(), "BOOLEAN") {
		t.Fatalf("ambiguous text-to-BOOLEAN CAST error = %v, want strict true/false diagnostic", err)
	}

	for name, query := range map[string]string{
		"where":    "FROM VALUES ('not-a-number') AS values(raw) WHERE CAST(raw AS NUMBER) > 0 SELECT raw",
		"is null":  "FROM VALUES ('not-a-number') AS values(raw) WHERE CAST(raw AS NUMBER) IS NULL SELECT raw",
		"having":   "FROM VALUES ('not-a-number') AS values(raw) SELECT COUNT(*) HAVING CAST(raw AS NUMBER) > 0",
		"group by": "FROM VALUES ('not-a-number') AS values(raw) GROUP BY CAST(raw AS NUMBER) SELECT COUNT(*)",
		"order by": "FROM VALUES ('not-a-number') AS values(raw) SELECT raw ORDER BY CAST(raw AS NUMBER)",
		"join on":  "FROM VALUES ('not-a-number') AS left_values(raw) INNER JOIN VALUES (1) AS right_values(id) ON CAST(left_values.raw AS NUMBER) = right_values.id SELECT right_values.id",
		"window":   "FROM VALUES ('not-a-number') AS values(raw) SELECT ROW_NUMBER() OVER (ORDER BY CAST(raw AS NUMBER))",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ExecuteSQLQuery(query, SQLSourceResolverFunc(nil))
			if err == nil || !strings.Contains(err.Error(), "CAST cannot convert TEXT value") {
				t.Fatalf("%s dynamic CAST error = %v, want clear diagnostic", name, err)
			}
		})
	}
}

func TestSQLCastSyntaxDiagnosticsIdentifyTheFault(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		source, want string
	}{
		{"FROM VALUES ('42') AS values(raw) SELECT CAST(raw NUMBER)", "expected AS"},
		{"FROM VALUES ('42') AS values(raw) SELECT CAST(raw AS MONEY)", "unsupported CAST target"},
		{"FROM VALUES ('42') AS values(raw) SELECT CAST(raw AS NUMBER", "expected )"},
	} {
		t.Run(test.source, func(t *testing.T) {
			err := ValidateSQLQuery(test.source)
			diagnostic, ok := err.(*SQLDiagnostic)
			if !ok || !strings.Contains(diagnostic.Message, test.want) || diagnostic.Line != 1 || diagnostic.Column < 1 {
				t.Fatalf("ValidateSQLQuery() error = %#v, want source-spanned diagnostic containing %q", err, test.want)
			}
			if formatted := FormatSQLDiagnostic(test.source, err); !strings.Contains(formatted, "--> query:1:") || !strings.Contains(formatted, "^") {
				t.Fatalf("formatted CAST syntax diagnostic = %q, want Rust-style source excerpt", formatted)
			}
		})
	}
}

func TestExecuteSQLQueryPreservesExactDecimalValues(t *testing.T) {
	t.Parallel()
	result, err := ExecuteSQLQuery(`
FROM VALUES ('2.30'), ('10.01'), ('2.3') AS values(raw)
WHERE CAST(raw AS DECIMAL) < DECIMAL '3.00'
SELECT CAST(raw AS DECIMAL) AS value
ORDER BY CAST(raw AS DECIMAL)`, SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("ExecuteSQLQuery(DECIMAL) error = %v", err)
	}
	want := SQLQueryResult{Columns: []string{"value"}, Rows: []SQLRow{{"value": sqlDecimal("2.30")}, {"value": sqlDecimal("2.3")}}}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("DECIMAL CAST result = %#v, want %#v", result, want)
	}

	result, err = ExecuteSQLQuery("FROM VALUES (DECIMAL '9007199254740993.000000000000000001') AS values(value) WHERE value > DECIMAL '9007199254740993' SELECT value", SQLSourceResolverFunc(nil))
	if err != nil || !reflect.DeepEqual(result.Rows, []SQLRow{{"value": sqlDecimal("9007199254740993.000000000000000001")}}) {
		t.Fatalf("exact DECIMAL comparison = %#v, %v", result, err)
	}
}

func TestHatTrieSQLTypedJSONFieldsValidateAndConvertRows(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("users", `[{"id":1,"active":true,"name":"Ivi","score":1.5,"joined_on":"2026-08-22","changed_at":"2026-08-22T09:00:00Z","balance":"9007199254740993.000000000000000001","extra":{"source":"import"}}]`)
	result, err := ExecuteSQLQuery(`
FROM CACHE('users') AS users(id INTEGER, active BOOLEAN, name TEXT, score NUMBER, joined_on DATE, changed_at TIMESTAMP, balance DECIMAL, extra JSON)
WHERE users.joined_on = DATE '2026-08-22'
SELECT users.id, users.active, users.name, users.score, users.joined_on, users.changed_at, users.balance, users.extra`, trie)
	if err != nil {
		t.Fatalf("typed CACHE query error = %v", err)
	}
	want := SQLQueryResult{Columns: []string{"id", "active", "name", "score", "joined_on", "changed_at", "balance", "extra"}, Rows: []SQLRow{{
		"id": int64(1), "active": true, "name": "Ivi", "score": float64(1.5), "joined_on": sqlDate("2026-08-22"),
		"changed_at": mustSQLTimestamp(t, "2026-08-22T09:00:00Z"), "balance": sqlDecimal("9007199254740993.000000000000000001"), "extra": map[string]interface{}{"source": "import"},
	}}}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("typed CACHE result = %#v, want %#v", result, want)
	}

	trie.UpsertString("users", `[{"id":1},{"id":"not-an-integer"}]`)
	invalidQuery := "FROM CACHE('users') AS users(id INTEGER) SELECT users.id"
	_, err = ExecuteSQLQuery(invalidQuery, trie)
	diagnostic, ok := err.(*SQLDiagnostic)
	if !ok || !strings.Contains(diagnostic.Message, `CACHE("users") row 2 field "id" expects INTEGER, got TEXT`) || diagnostic.Column != strings.Index(invalidQuery, "INTEGER")+1 {
		t.Fatalf("typed JSON field diagnostic = %#v, want source-spanned row/field/type error", err)
	}
}

func TestExecuteSQLQuerySupportsRowsWindowFramesAndMovingAggregates(t *testing.T) {
	t.Parallel()
	result, err := ExecuteSQLQuery(`
FROM VALUES (1, 10), (2, 20), (3, 30) AS values(id, amount)
SELECT id,
       SUM(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS moving_sum,
       AVG(amount) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS forward_avg,
       MIN(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS local_min,
       MAX(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS local_max
ORDER BY id`, SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("ExecuteSQLQuery(ROWS frame) error = %v", err)
	}
	want := SQLQueryResult{Columns: []string{"id", "moving_sum", "forward_avg", "local_min", "local_max"}, Rows: []SQLRow{
		{"id": int64(1), "moving_sum": float64(10), "forward_avg": float64(15), "local_min": float64(10), "local_max": float64(20)},
		{"id": int64(2), "moving_sum": float64(30), "forward_avg": float64(25), "local_min": float64(10), "local_max": float64(30)},
		{"id": int64(3), "moving_sum": float64(50), "forward_avg": float64(30), "local_min": float64(20), "local_max": float64(30)},
	}}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("ROWS frame result = %#v, want %#v", result, want)
	}
	invalidFrame := "FROM VALUES (1) AS values(value) SELECT SUM(value) OVER (ORDER BY value ROWS BETWEEN 1 FOLLOWING AND CURRENT ROW)"
	err = ValidateSQLQuery(invalidFrame)
	diagnostic, ok := err.(*SQLDiagnostic)
	if !ok || !strings.Contains(diagnostic.Message, "ROWS frame start must not follow its end") || diagnostic.Column < 1 {
		t.Fatalf("invalid ROWS frame diagnostic = %#v, want source-spanned ordering error", err)
	}
}

func mustSQLTimestamp(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func TestHatrieTypesShareOneLogicalKeyNamespace(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	if err := trie.PutMapChecked("shared", "field", "map value"); err != nil {
		t.Fatal(err)
	}
	if added, err := trie.AddSetChecked("shared", "set member"); err != nil || added != 1 {
		t.Fatalf("AddSetChecked() = (%d, %v), want (1, nil)", added, err)
	}
	if got := trie.GetMap("shared"); len(got) != 0 {
		t.Fatalf("GetMap(shared) = %#v, want replaced map to be absent", got)
	}
	if got := trie.GetSet("shared"); len(got) != 1 || got[0] != "set member" {
		t.Fatalf("GetSet(shared) = %#v, want set member", got)
	}
}

func TestExecuteSQLQueryProductionRejectsStructuralErrors(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		"SELECT column1 FROM VALUES (1) SELECT column1",
		"FROM VALUES (1) AS a JOIN VALUES (1) AS b SELECT *",
		"FROM VALUES (1) AS a SELECT column1 LIMIT 1 LIMIT 1",
		"FROM VALUES (1) AS a WHERE column1 = 1 WHERE column1 = 1 SELECT column1",
	} {
		if _, err := ExecuteSQLQuery(source, SQLSourceResolverFunc(nil)); err == nil {
			t.Fatalf("ExecuteSQLQuery(%q) error = nil, want structural rejection", source)
		}
	}
}

func TestMonitoringSQLRoutesRejectMalformedRequests(t *testing.T) {
	t.Parallel()

	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{}).Handler()
	for _, test := range []struct {
		method, path, body string
		want               int
	}{
		{http.MethodGet, "/api/sql", "", http.StatusMethodNotAllowed},
		{http.MethodPost, "/api/sql", "{", http.StatusBadRequest},
		{http.MethodPost, "/api/sql", `{}`, http.StatusBadRequest},
		{http.MethodGet, "/api/sql/functions", "", http.StatusMethodNotAllowed},
		{http.MethodPost, "/api/sql/functions", "{", http.StatusBadRequest},
		{http.MethodPost, "/api/sql/functions", `{"name":"bad","arguments":["value"],"argument_types":["BOGUS"],"language":"LUA","source":"return value"}`, http.StatusBadRequest},
	} {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
		handler.ServeHTTP(response, request)
		if response.Code != test.want {
			t.Fatalf("%s %s status = %d, want %d: %s", test.method, test.path, response.Code, test.want, response.Body.String())
		}
	}
}

func FuzzSQLParsersDoNotPanic(f *testing.F) {
	for _, seed := range []string{
		"SELECT value FROM cache WHERE key = 'x'",
		"FROM VALUES (1) AS a SELECT column1",
		"CALL SETSTR(key => 'x', value => 'y')",
		"CREATE FUNCTION f(x INTEGER) LANGUAGE GO AS 'return x > 0'",
		"'\\\"; SELECT *",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, source string) {
		_, _ = CompileSQL(source)
		_ = ValidateSQLQuery(source)
		_, _ = CompileSQLFunction(source)
	})
}

func FuzzExecuteSQLQueryDoesNotPanic(f *testing.F) {
	for _, seed := range []string{
		"FROM VALUES (1), (2) AS t(value) SELECT DISTINCT value",
		"FROM VALUES (1) AS a(id) FULL JOIN VALUES (2) AS b(id) ON a.id = b.id SELECT a.id, b.id",
		"FROM VALUES (1) AS a(value) SELECT value UNION FROM VALUES (1) AS b(value) SELECT value",
		"FROM (FROM VALUES (1) AS a(value) SELECT value) AS derived SELECT value",
		"SELECT FROM VALUES (1) AS a(value)",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, source string) {
		_, _ = ExecuteSQLQuery(source, SQLSourceResolverFunc(func(name, key string) ([]SQLRow, error) {
			if name == "CACHE" {
				return []SQLRow{{"id": int64(1), "value": "seed"}}, nil
			}
			return nil, nil
		}))
	})
}
