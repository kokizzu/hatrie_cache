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
