package hatSql_test

import (
	"reflect"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

const c211JoinOrderQuery = "FROM CACHE('large') AS l INNER JOIN CACHE('medium') AS m ON l.id = m.id INNER JOIN CACHE('small') AS s ON m.id = s.id SELECT l.id"

var c211JoinOrderBenchmarkSink int

type c211JoinOrderResolver struct {
	sources map[string][]hatSql.Row
	calls   []string
}

func (resolver *c211JoinOrderResolver) ResolveSQLSource(_, key string) ([]hatSql.Row, error) {
	resolver.calls = append(resolver.calls, key)
	return resolver.sources[key], nil
}

func c211JoinOrderBenchmarkResolver() c211JoinOrderResolver {
	return c211JoinOrderResolver{sources: map[string][]hatSql.Row{
		"large":  c211JoinOrderRows(500),
		"medium": c211JoinOrderRows(200),
		"small":  c211JoinOrderRows(20),
	}}
}

type c211StatisticsJoinOrderResolver struct {
	c211JoinOrderResolver
	cardinalities map[string]int
}

func (resolver *c211StatisticsJoinOrderResolver) SQLSourceCardinality(_, key string) (int, bool, bool, error) {
	rows, found := resolver.cardinalities[key]
	return rows, found, found, nil
}

func TestC211StatisticsJoinOrderResolvesSourcesInEstimatedOrder(t *testing.T) {
	base := c211JoinOrderBenchmarkResolver()
	resolver := &c211StatisticsJoinOrderResolver{
		c211JoinOrderResolver: base,
		cardinalities: map[string]int{
			"large":  500,
			"medium": 200,
			"small":  20,
		},
	}
	result, err := hatSql.ExecuteSQLQuery(c211JoinOrderQuery, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(result.Rows), 20; got != want {
		t.Fatalf("joined rows = %d, want %d", got, want)
	}
	if got, want := strings.Join(resolver.calls, ","), "small,medium,large"; got != want {
		t.Fatalf("source resolution order = %q, want %q", got, want)
	}
	if got, want := result.Rows[0], (hatSql.Row{"id": int64(0)}); !reflect.DeepEqual(got, want) {
		t.Fatalf("first row = %#v, want %#v", got, want)
	}
}

func TestC211JoinOrderKeepsTextOrderWithoutStatistics(t *testing.T) {
	resolver := c211JoinOrderBenchmarkResolver()
	result, err := hatSql.ExecuteSQLQuery(c211JoinOrderQuery, &resolver)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(result.Rows), 20; got != want {
		t.Fatalf("joined rows = %d, want %d", got, want)
	}
	if got, want := strings.Join(resolver.calls, ","), "large,medium,small"; got != want {
		t.Fatalf("fallback source resolution order = %q, want %q", got, want)
	}
}

type c211PartialStatisticsResolver struct {
	c211JoinOrderResolver
}

func (resolver *c211PartialStatisticsResolver) SQLSourceCardinality(_, key string) (int, bool, bool, error) {
	if key == "small" {
		return 0, false, false, nil
	}
	return len(resolver.sources[key]), true, true, nil
}

func TestC211JoinOrderFallsBackWhenStatisticsAreIncomplete(t *testing.T) {
	base := c211JoinOrderBenchmarkResolver()
	resolver := &c211PartialStatisticsResolver{c211JoinOrderResolver: base}
	if _, err := hatSql.ExecuteSQLQuery(c211JoinOrderQuery, resolver); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(resolver.calls, ","), "large,medium,small"; got != want {
		t.Fatalf("incomplete-statistics source resolution order = %q, want %q", got, want)
	}
}

func TestC211StatisticsJoinOrderPreservesDuplicateOutputOrder(t *testing.T) {
	sources := map[string][]hatSql.Row{
		"large":  {{"id": int64(1), "position": int64(10)}, {"id": int64(1), "position": int64(11)}},
		"medium": {{"id": int64(1), "position": int64(20)}, {"id": int64(1), "position": int64(21)}},
		"small":  {{"id": int64(1), "position": int64(30)}, {"id": int64(1), "position": int64(31)}},
	}
	query := "FROM CACHE('large') AS l INNER JOIN CACHE('medium') AS m ON l.id = m.id INNER JOIN CACHE('small') AS s ON m.id = s.id SELECT l.position AS left_position, m.position AS middle_position, s.position AS right_position"
	baselineResolver := &c211JoinOrderResolver{sources: sources}
	baseline, err := hatSql.ExecuteSQLQuery(query, baselineResolver)
	if err != nil {
		t.Fatal(err)
	}
	statisticsResolver := &c211StatisticsJoinOrderResolver{
		c211JoinOrderResolver: c211JoinOrderResolver{sources: sources},
		cardinalities:         map[string]int{"large": 2, "medium": 2, "small": 2},
	}
	statistics, err := hatSql.ExecuteSQLQuery(query, statisticsResolver)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(statistics.Rows, baseline.Rows) {
		t.Fatalf("statistics rows = %#v, baseline rows = %#v", statistics.Rows, baseline.Rows)
	}
}

func TestC211TypedTableCardinalityExcludesDeletedRows(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "events",
		Columns: []hatSql.TypedTableColumn{{Name: "id", Kind: hatSql.TypedTableInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b", "c"} {
		if _, err := table.Upsert(key, []hatSql.TypedTableValue{hatSql.TypedInt64(1)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := table.Delete("b"); err != nil {
		t.Fatal(err)
	}
	rows, exact, available, err := table.SQLSourceCardinality("CACHE", "events")
	if err != nil {
		t.Fatal(err)
	}
	if rows != 2 || !exact || !available {
		t.Fatalf("cardinality = (%d, %v, %v), want (2, true, true)", rows, exact, available)
	}
	if _, _, available, err := table.SQLSourceCardinality("CACHE", "other"); err != nil || available {
		t.Fatalf("unknown source cardinality = (%v, %v), want unavailable without error", available, err)
	}
}

type c211JoinOrderStatsBenchmarkResolver struct {
	sources       map[string][]hatSql.Row
	cardinalities map[string]int
}

func (resolver c211JoinOrderStatsBenchmarkResolver) ResolveSQLSource(_, key string) ([]hatSql.Row, error) {
	return resolver.sources[key], nil
}

func (resolver c211JoinOrderStatsBenchmarkResolver) SQLSourceCardinality(_, key string) (int, bool, bool, error) {
	rows, found := resolver.cardinalities[key]
	return rows, found, found, nil
}

func c211JoinOrderStatsBenchmarkResolverValue() c211JoinOrderStatsBenchmarkResolver {
	return c211JoinOrderStatsBenchmarkResolver{
		sources: map[string][]hatSql.Row{
			"large":  c211JoinOrderRows(500),
			"medium": c211JoinOrderRows(200),
			"small":  c211JoinOrderRows(20),
		},
		cardinalities: map[string]int{
			"large":  500,
			"medium": 200,
			"small":  20,
		},
	}
}

func c211JoinOrderRows(count int) []hatSql.Row {
	rows := make([]hatSql.Row, count)
	for index := range rows {
		rows[index] = hatSql.Row{"id": int64(index)}
	}
	return rows
}

func BenchmarkC211JoinOrderBaseline(b *testing.B) {
	resolver := c211JoinOrderBenchmarkResolver()
	b.ReportAllocs()
	for range b.N {
		result, err := hatSql.ExecuteSQLQuery(c211JoinOrderQuery, &resolver)
		if err != nil {
			b.Fatal(err)
		}
		c211JoinOrderBenchmarkSink = len(result.Rows)
	}
}

func BenchmarkC211JoinOrderStatistics(b *testing.B) {
	resolver := c211JoinOrderStatsBenchmarkResolverValue()
	b.ReportAllocs()
	for range b.N {
		result, err := hatSql.ExecuteSQLQuery(c211JoinOrderQuery, resolver)
		if err != nil {
			b.Fatal(err)
		}
		c211JoinOrderBenchmarkSink = len(result.Rows)
	}
}
