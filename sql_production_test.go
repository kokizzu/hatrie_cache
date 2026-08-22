package hatriecache

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
)

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
	if result.Stats.OutputRows != 1 || result.Stats.OutputColumns != 1 || result.Stats.PlanSteps != len(result.Plan) || result.Stats.ElapsedNanos < 0 {
		t.Fatalf("EXPLAIN ANALYZE stats = %#v, plan = %#v", result.Stats, result.Plan)
	}
	if want := []string{"node", "detail", "estimated_rows", "actual_rows", "elapsed_ns"}; !reflect.DeepEqual(result.Columns, want) {
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
