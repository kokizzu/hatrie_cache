package hatriecache

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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

func TestMonitoringSQLRouteUsesHatTrieJSONFieldIndex(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1,"team_id":20},{"id":2,"team_id":30}]`)
	if err := trie.CreateSQLJSONFieldIndex("people", "team_id"); err != nil {
		t.Fatal(err)
	}
	handler := NewMonitoringHandler(trie, MonitoringOptions{}).Handler()
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(`{"query":"EXPLAIN ANALYZE FROM CACHE('people') AS p WHERE p.team_id = 20 SELECT p.id"}`))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", response.Code, response.Body.String())
	}
	var result SQLQueryResult
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Stats == nil || len(result.Plan) == 0 || result.Plan[0].Node != "INDEX SCAN" {
		t.Fatalf("plan = %#v, stats = %#v, want an HTTP INDEX SCAN", result.Plan, result.Stats)
	}
	response = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(`{"query":"EXPLAIN ANALYZE FROM CACHE('people') AS p WHERE p.team_id >= 20 SELECT p.id"}`))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("range status = %d, want 200: %s", response.Code, response.Body.String())
	}
	result = SQLQueryResult{}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Stats == nil || len(result.Plan) == 0 || result.Plan[0].Node != "INDEX SCAN" {
		t.Fatalf("range plan = %#v, stats = %#v, want an HTTP INDEX SCAN", result.Plan, result.Stats)
	}
}

func TestMonitoringSQLRouteUsesHatTrieCompositeJSONIndex(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1,"team_id":20,"enabled":true},{"id":2,"team_id":20,"enabled":false},{"id":3,"team_id":30,"enabled":true}]`)
	if err := trie.CreateSQLJSONCompositeIndex("people", "team_id", "enabled"); err != nil {
		t.Fatal(err)
	}
	handler := NewMonitoringHandler(trie, MonitoringOptions{}).Handler()
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(`{"query":"EXPLAIN ANALYZE FROM CACHE('people') AS p WHERE p.team_id = 20 AND p.enabled = TRUE SELECT p.id"}`))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", response.Code, response.Body.String())
	}
	var result SQLQueryResult
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Stats == nil || len(result.Plan) == 0 || result.Plan[0].Node != "INDEX SCAN" {
		t.Fatalf("plan = %#v, stats = %#v, want an HTTP composite INDEX SCAN", result.Plan, result.Stats)
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

func TestMonitoringSQLRouteStreamsNDJSONRows(t *testing.T) {
	t.Parallel()
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{}).Handler()
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(`{"query":"FROM VALUES (1), (2), (3) AS values(id) WHERE id > 1 SELECT id","stream":true}`))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Header().Get("Content-Type"), "application/x-ndjson") {
		t.Fatalf("status/content-type = %d/%q, want 200/application/x-ndjson: %s", response.Code, response.Header().Get("Content-Type"), response.Body.String())
	}
	var messages []struct {
		Type    string   `json:"type"`
		Columns []string `json:"columns"`
		Row     SQLRow   `json:"row"`
		Rows    int      `json:"rows"`
	}
	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		var message struct {
			Type    string   `json:"type"`
			Columns []string `json:"columns"`
			Row     SQLRow   `json:"row"`
			Rows    int      `json:"rows"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &message); err != nil {
			t.Fatal(err)
		}
		messages = append(messages, message)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(messages) != 4 || messages[0].Type != "columns" || !reflect.DeepEqual(messages[0].Columns, []string{"id"}) || !reflect.DeepEqual(messages[1].Row, SQLRow{"id": float64(2)}) || !reflect.DeepEqual(messages[2].Row, SQLRow{"id": float64(3)}) || messages[3].Type != "done" || messages[3].Rows != 2 {
		t.Fatalf("NDJSON messages = %#v", messages)
	}

	response = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(`{"query":"FROM VALUES (1), (2) AS values(id) SELECT id ORDER BY id","stream":true}`))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "cannot stream") {
		t.Fatalf("ordered stream status/body = %d/%s, want streamability diagnostic", response.Code, response.Body.String())
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

func TestMonitoringSQLFunctionRoutePersistsAcrossRestart(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "sql-functions.json")
	registry, err := OpenSQLFunctionRegistry(SQLFunctionRegistryOptions{PersistencePath: path})
	if err != nil {
		t.Fatal(err)
	}
	first := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{SQLFunctions: registry}).Handler()
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/sql/functions", strings.NewReader(`{"name":"eligible","arguments":["age","score"],"argument_types":["INTEGER","INTEGER"],"language":"GO","source":"return age > 10 && score < 9"}`))
	request.Header.Set("Content-Type", "application/json")
	first.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		registry.Close()
		t.Fatalf("register status = %d, want 200: %s", response.Code, response.Body.String())
	}
	registry.Close()

	restarted, err := OpenSQLFunctionRegistry(SQLFunctionRegistryOptions{PersistencePath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	second := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{SQLFunctions: restarted}).Handler()
	response = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(`{"query":"FROM VALUES ('Ivi', 12, 7), ('Lia', 4, 7) AS people(name, age, score) WHERE eligible(age, score) SELECT name"}`))
	request.Header.Set("Content-Type", "application/json")
	second.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("restarted query status = %d, want 200: %s", response.Code, response.Body.String())
	}
	var result SQLQueryResult
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("restarted query JSON error = %v", err)
	}
	if want := (SQLQueryResult{Columns: []string{"name"}, Rows: []SQLRow{{"name": "Ivi"}}}); !reflect.DeepEqual(result, want) {
		t.Fatalf("restarted query result = %#v, want %#v", result, want)
	}
}
