package hatriecache

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestHatTrieResolvesSQLCacheAndKeySources(t *testing.T) {
	t.Parallel()

	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1,"name":"Ivi"},{"id":2,"name":"Lia"}]`)
	trie.UpsertString("config", "enabled")

	result, err := ExecuteSQLQuery(`
FROM CACHE('people') AS p
CROSS JOIN KEYS AS k
WHERE k.key = 'config'
SELECT p.name, k.type
ORDER BY p.id`, trie)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := SQLQueryResult{Columns: []string{"name", "type"}, Rows: []SQLRow{{"name": "Ivi", "type": "string"}, {"name": "Lia", "type": "string"}}}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("ExecuteSQLQuery() = %#v, want %#v", result, want)
	}
}

func TestMonitoringSQLRouteExecutesReadOnlyQueryAndFormatsSyntaxErrors(t *testing.T) {
	t.Parallel()

	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1,"name":"Ivi"}]`)
	handler := NewMonitoringHandler(trie, MonitoringOptions{}).Handler()

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(`{"query":"FROM CACHE('people') AS p SELECT p.name"}`))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", response.Code, response.Body.String())
	}
	var result SQLQueryResult
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("response JSON error = %v", err)
	}
	if want := (SQLQueryResult{Columns: []string{"name"}, Rows: []SQLRow{{"name": "Ivi"}}}); !reflect.DeepEqual(result, want) {
		t.Fatalf("result = %#v, want %#v", result, want)
	}

	response = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(`{"query":"FROM CACHE($1) AS p WHERE p.id = $2 SELECT p.name","parameters":["people",1]}`))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("parameterized query status = %d, want 200: %s", response.Code, response.Body.String())
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("parameterized query JSON error = %v", err)
	}
	if want := (SQLQueryResult{Columns: []string{"name"}, Rows: []SQLRow{{"name": "Ivi"}}}); !reflect.DeepEqual(result, want) {
		t.Fatalf("parameterized result = %#v, want %#v", result, want)
	}

	response = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(`{"query":"EXPLAIN ANALYZE FROM CACHE('people') AS p SELECT p.name"}`))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("EXPLAIN ANALYZE status = %d, want 200: %s", response.Code, response.Body.String())
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("EXPLAIN ANALYZE response JSON error = %v", err)
	}
	if result.Stats == nil || result.Stats.OutputRows != 1 || len(result.Plan) == 0 || result.Rows[len(result.Rows)-1]["node"] != "ANALYZE" {
		t.Fatalf("EXPLAIN ANALYZE result = %#v, want plan and execution statistics", result)
	}

	response = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(`{"query":"FROM KEYS JION KEYS SELECT *"}`))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("bad query status = %d, want 400", response.Code)
	}
	if body := response.Body.String(); !strings.Contains(body, "did you mean `JOIN`?") || !strings.Contains(body, "query:1:") {
		t.Fatalf("bad query body = %q, want formatted compiler diagnostic", body)
	}
}

func TestMonitoringSQLRoutePaginatesWithBoundOpaqueCursor(t *testing.T) {
	t.Parallel()
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{}).Handler()
	query := "FROM VALUES (1), (2), (3), (4), (5) AS values(id) SELECT id ORDER BY id"
	requestBody := `{"query":"` + query + `","page_size":2}`
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(requestBody))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("first page status = %d: %s", response.Code, response.Body.String())
	}
	var first SQLQueryResult
	if err := json.Unmarshal(response.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"id": float64(1)}, {"id": float64(2)}}; !reflect.DeepEqual(first.Rows, want) || !first.HasMore || first.NextCursor == "" {
		t.Fatalf("first page = %#v, want rows %#v and a cursor", first, want)
	}

	response = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(`{"query":"`+query+`","page_size":2,"cursor":"`+first.NextCursor+`"}`))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(response, request)
	var second SQLQueryResult
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &second) != nil {
		t.Fatalf("second page status/body = %d/%s", response.Code, response.Body.String())
	}
	if want := []SQLRow{{"id": float64(3)}, {"id": float64(4)}}; !reflect.DeepEqual(second.Rows, want) || !second.HasMore {
		t.Fatalf("second page = %#v, want %#v", second, want)
	}

	response = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(`{"query":"FROM VALUES (99) AS values(id) SELECT id","page_size":2,"cursor":"`+first.NextCursor+`"}`))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "cursor does not match") {
		t.Fatalf("mismatched cursor status/body = %d/%s", response.Code, response.Body.String())
	}
}

func TestMonitoringSQLFunctionRouteRegistersTypedGoFunction(t *testing.T) {
	t.Parallel()

	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{}).Handler()
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/sql/functions", strings.NewReader(`{"name":"eligible","arguments":["age","score"],"argument_types":["INTEGER","INTEGER"],"language":"GO","source":"return age > 10 && score < 9"}`))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("register status = %d, want 200: %s", response.Code, response.Body.String())
	}

	response = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(`{"query":"FROM VALUES ('Ivi', 12, 7), ('Lia', 4, 7) AS people(name, age, score) WHERE eligible(age, score) SELECT name"}`))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("query status = %d, want 200: %s", response.Code, response.Body.String())
	}
	var result SQLQueryResult
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("query response JSON error = %v", err)
	}
	if want := (SQLQueryResult{Columns: []string{"name"}, Rows: []SQLRow{{"name": "Ivi"}}}); !reflect.DeepEqual(result, want) {
		t.Fatalf("query result = %#v, want %#v", result, want)
	}
}
