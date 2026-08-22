package hatriecache

import (
	"net/http"
	"net/http/httptest"
	"reflect"
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
