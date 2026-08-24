package hatSql_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestConnQueryAndRowsUseSQLWireModel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/sql" {
			t.Fatalf("path = %q", request.URL.Path)
		}
		var payload hatSql.QueryRequest
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if payload.Stream {
			writer.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = writer.Write([]byte("{\"type\":\"columns\",\"columns\":[\"id\"]}\n{\"type\":\"row\",\"row\":{\"id\":7}}\n{\"type\":\"done\"}\n"))
			return
		}
		_ = json.NewEncoder(writer).Encode(hatSql.QueryResult{Columns: []string{"id"}, Rows: []hatSql.Row{{"id": 7}}})
	}))
	defer server.Close()

	conn := hatSql.NewConn(server.URL, "token")
	result, err := conn.Query(context.Background(), "FROM KEYS SELECT key")
	if err != nil || len(result.Rows) != 1 {
		t.Fatalf("Query() = %#v, %v", result, err)
	}
	count, err := hatSql.QueryRows(context.Background(), conn, "FROM KEYS SELECT key", func(row hatSql.Row) error {
		if row["id"] != float64(7) {
			t.Fatalf("row = %#v", row)
		}
		return nil
	})
	if err != nil || count != 1 {
		t.Fatalf("QueryRows() = %d, %v", count, err)
	}
}
