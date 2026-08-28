package hatSql_test

import (
	"context"
	"encoding/json"
	"errors"
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

func TestQueryIteratorStreamsTypedRowsWithParameters(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var payload hatSql.QueryRequest
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if !payload.Stream || len(payload.Parameters) != 2 || payload.Parameters[0] != float64(7) || payload.Parameters[1] != "active" {
			t.Fatalf("stream payload = %#v", payload)
		}
		writer.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = writer.Write([]byte("{\"type\":\"columns\",\"columns\":[\"id\",\"state\"]}\n{\"type\":\"row\",\"row\":{\"id\":7,\"state\":\"active\"}}\n{\"type\":\"done\"}\n"))
	}))
	defer server.Close()

	rows, err := hatSql.QueryIterator[struct {
		ID    int    `json:"id"`
		State string `json:"state"`
	}](context.Background(), hatSql.NewConn(server.URL, ""), "SELECT id, state FROM CACHE('items') WHERE id = $1 AND state = $2", []interface{}{7, "active"})
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("first Next() = false, err = %v", rows.Err())
	}
	if got := rows.Row(); got.ID != 7 || got.State != "active" {
		t.Fatalf("Row() = %#v", got)
	}
	if rows.Next() {
		t.Fatal("second Next() = true, want false")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterator Err() = %v", err)
	}
}

func TestQueryIteratorPropagatesContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/x-ndjson")
		writer.WriteHeader(http.StatusOK)
		writer.(http.Flusher).Flush()
		<-request.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	rows, err := hatSql.QueryIterator[hatSql.Row](ctx, hatSql.NewConn(server.URL, ""), "SELECT * FROM CACHE('items')", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cancel()
	if rows.Next() {
		t.Fatal("Next() = true after context cancellation")
	}
	if err := rows.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Err() = %v, want context canceled", err)
	}
}
