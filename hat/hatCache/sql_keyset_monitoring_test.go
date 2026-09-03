package hatCache

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestMonitoringSQLKeysetPagination(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("events", `[{"id":"a","score":1},{"id":"b","score":2}]`)
	trie.UpsertString("plain-events", `[{"id":"x","score":1}]`)
	if err := trie.CreateSQLJSONFieldIndex("events", "score"); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewMonitoringHandler(trie, MonitoringOptions{}).Handler())
	defer server.Close()
	query := "SELECT e.id FROM CACHE('events') AS e ORDER BY e.score"
	post := func(request SQLQueryRequest) SQLQueryResult {
		body, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.Post(server.URL+"/api/sql", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("keyset HTTP status = %d", response.StatusCode)
		}
		var result SQLQueryResult
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	first := post(SQLQueryRequest{Query: query, PageSize: 1, Keyset: true})
	second := post(SQLQueryRequest{Query: query, PageSize: 1, Cursor: first.NextCursor, Keyset: true})
	if want := []SQLRow{{"id": "a"}}; !reflect.DeepEqual(first.Rows, want) {
		t.Fatalf("first keyset HTTP rows = %#v, want %#v", first.Rows, want)
	}
	if want := []SQLRow{{"id": "b"}}; !reflect.DeepEqual(second.Rows, want) {
		t.Fatalf("second keyset HTTP rows = %#v, want %#v", second.Rows, want)
	}
	body, err := json.Marshal(SQLQueryRequest{Query: "SELECT e.id FROM CACHE('plain-events') AS e ORDER BY e.score", PageSize: 1, Keyset: true})
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Post(server.URL+"/api/sql", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("unindexed keyset HTTP status = %d, want %d", response.StatusCode, http.StatusBadRequest)
	}
	document := monitoringOpenAPIDocument(false)
	components := document["components"].(map[string]interface{})
	schemas := components["schemas"].(map[string]interface{})
	schema := schemas["SQLQueryRequest"].(map[string]interface{})
	properties := schema["properties"].(map[string]interface{})
	if _, ok := properties["keyset"]; !ok {
		t.Fatal("OpenAPI SQLQueryRequest is missing keyset")
	}
}
