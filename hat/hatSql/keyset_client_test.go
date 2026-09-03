package hatSql

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestConnQueryKeysetPageSendsKeysetFlag(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var payload QueryRequest
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if !payload.Keyset || payload.PageSize != 2 || payload.Cursor != "cursor" {
			t.Fatalf("keyset request = %#v", payload)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(QueryResult{Columns: []string{"id"}, Rows: []Row{{"id": 1}}})
	}))
	defer server.Close()
	result, err := NewConn(server.URL, "").QueryKeysetPage(t.Context(), "SELECT id", nil, 2, "cursor")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["id"] != float64(1) {
		t.Fatalf("keyset client result = %#v", result)
	}
}
